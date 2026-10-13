import json
import logging
from collections import defaultdict
from collections import namedtuple
from enum import Enum
from typing import Dict
from typing import Generator
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Type

from django.core import exceptions
from django.db import router
from django.db import transaction
from django.db.models import ForeignKey
from django.db.models import Model
from django.db.models import signals
from django.db.models.deletion import Collector
from django.db.utils import IntegrityError

from morango.errors import MorangoDirtyParent
from morango.errors import MorangoMissingParent
from morango.models.certificates import Filter
from morango.models.core import DeletedModels
from morango.models.core import HardDeletedModels
from morango.models.core import Store
from morango.models.core import SyncableModel
from morango.registry import syncable_models
from morango.sync.db import begin_transaction
from morango.sync.stream.core import Buffer
from morango.sync.stream.core import Sink
from morango.sync.stream.core import Transform
from morango.sync.stream.source import MorangoSource
from morango.sync.stream.source import SourceTask
from morango.sync.utils import mute_signals

logger = logging.getLogger(__name__)

DIRTY_PARENT_MESSAGE = "Parent is dirty; could not deserialize."
MISSING_PARENT_MESSAGE = "Parent does not exist in Store; could not deserialize."

# the store state of an FK target, used to resolve validation failures
_TargetState = namedtuple("_TargetState", ["deleted", "hard_deleted", "dirty_bit"])


def _fk_cache_key(model: Type[SyncableModel], record_id: str) -> str:
    """The key `SyncableModel.cached_clean_fields` uses for a record in the `fk_cache`"""
    return "{id}_{db_table}".format(id=record_id, db_table=model._meta.db_table)


class DeserializeOutcome(Enum):
    """The resolved action for a `DeserializeTask`, which the sink applies"""

    SAVE = "save"
    DELETE = "delete"
    PROPAGATE_DELETE = "propagate_delete"
    PROPAGATE_HARD_DELETE = "propagate_hard_delete"
    ERROR = "error"


class DeserializeTask(SourceTask):
    """Carrier class for providing context through the deserialization pipeline."""

    __slots__ = ("store", "app_model", "fk_cache", "errors", "_propagated_deletion")

    HARD_DELETION = "hard"
    SOFT_DELETION = "soft"

    def __init__(self, store: Store, fk_cache: Dict):
        self.store = store
        self.fk_cache: Dict = fk_cache
        self.app_model: Optional[SyncableModel] = None
        self.errors: List[Exception] = []
        self._propagated_deletion: Optional[str] = None

    @property
    def id(self) -> str:
        return self.store.id

    @property
    def model(self) -> Type[SyncableModel]:
        return syncable_models.get_model(self.store.profile, self.store.model_name)

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0

    @property
    def outcome(self) -> DeserializeOutcome:
        """
        The resolved `DeserializeOutcome` for this task, in order of precedence
        :raises AssertionError: if the pipeline has not resolved an outcome
        """
        if self.has_errors:
            return DeserializeOutcome.ERROR
        if self.store.deleted:
            return DeserializeOutcome.DELETE
        if self._propagated_deletion == self.HARD_DELETION:
            return DeserializeOutcome.PROPAGATE_HARD_DELETE
        if self._propagated_deletion == self.SOFT_DELETION:
            return DeserializeOutcome.PROPAGATE_DELETE
        if self.app_model is not None:
            return DeserializeOutcome.SAVE
        raise AssertionError("DeserializeTask has no resolved outcome")

    def self_referential_fk(self) -> Optional[str]:
        """Return the attname of the self-referential FK on the task's model, or ``None``."""
        return syncable_models.get_self_referential_fk(self.model)

    def set_app_model(self, app_model: Optional[SyncableModel]) -> None:
        self.app_model = app_model

    def set_propagated_deletion(self, hard: bool) -> None:
        """Mark this task's record as deleted because a record it references was deleted"""
        self._propagated_deletion = self.HARD_DELETION if hard else self.SOFT_DELETION

    def add_error(self, error: Exception) -> None:
        self.errors.append(error)


class StoreModelSource(MorangoSource[DeserializeTask]):
    """
    Yields ``DeserializeTask`` objects for dirty store models that match the optional
    *sync_filter*.
    """

    __slots__ = ("fk_cache", "skip_errored")

    def __init__(
        self,
        profile: str,
        sync_filter: Optional[Filter] = None,
        dirty_only: bool = True,
        partition_order: str = "asc",
        fk_cache: Optional[Dict] = None,
        skip_errored: bool = False,
    ):
        """
        :param profile: The Morango model profile
        :param sync_filter: The Filter object for this sync
        :param dirty_only: Whether to filter on dirty records only
        :param partition_order: Controls how the filter specificity is applied, "asc" or "desc"
        :param fk_cache: Dictionary cache for FK references
        :param skip_errored: Whether to skip Store records with deserialization errors
        """
        super().__init__(profile, sync_filter, dirty_only, partition_order)
        self.fk_cache = fk_cache if fk_cache is not None else {}
        self.skip_errored = skip_errored

    def begin(self) -> None:
        """Reset fk_cache at the beginning of stream"""
        super().begin()
        self.fk_cache.clear()

    def stream_for_filter(
        self, partition_condition: Optional[str]
    ) -> Generator[DeserializeTask, None, None]:
        # the registry yields models in foreign key dependency order, so streaming model by model
        # ensures a record's foreign key targets are deserialized before it is
        for store_qs in syncable_models.get_store_querysets(self.profile):
            qs = store_qs
            if partition_condition is not None:
                qs = qs.filter(partition__startswith=partition_condition)
            if self.dirty_only:
                qs = qs.filter(dirty_bit=True)
            if self.skip_errored:
                qs = qs.exclude_has_deserialization_error()

            for store_model in qs.iterator():
                yield DeserializeTask(store_model, self.fk_cache)


def fk_references(app_model: SyncableModel) -> List[Tuple[ForeignKey, str]]:
    """The FK fields on the app model that hold a value, paired with that value"""
    references = []
    for field in app_model._meta.fields:
        if not isinstance(field, ForeignKey):
            continue
        target_id = getattr(app_model, field.attname)
        if target_id is not None:
            references.append((field, target_id))
    return references


def failed_target_error(task: DeserializeTask, field: ForeignKey, target_id: str) -> Exception:
    """The error for a task whose FK target failed to deserialize in the run"""
    if field.attname == task.self_referential_fk():
        return MorangoDirtyParent(DIRTY_PARENT_MESSAGE)
    return exceptions.ValidationError(
        "{} with id {} failed to deserialize".format(
            field.related_model._meta.verbose_name, target_id
        )
    )


def deleted_target_error(field: ForeignKey, target_id: str) -> Exception:
    """The error for a task whose FK target was deleted in the run"""
    return exceptions.ValidationError(
        "{} with id {} was deleted".format(field.related_model._meta.verbose_name, target_id)
    )


def row_key(model: Type[Model], pk) -> Tuple[str, str]:
    """Identifies an app row across models, as the concrete model's label and the primary key"""
    return model._meta.concrete_model._meta.label_lower, str(pk)


def delete_app_models(model: Type[SyncableModel], ids: List[str]) -> Set[Tuple[str, str]]:
    """
    Deletes app rows without tracking them, and Django cascades to the rows that reference them.

    :return: The row keys of the cascaded rows, not including the rows in `ids`
    """
    if not ids:
        return set()

    with mute_signals(signals.post_delete):
        # the collector checks for signal receivers when deciding to fast delete, so it must
        # collect while `post_delete` is muted
        collector = Collector(using=router.db_for_write(model))
        collector.collect(model.syncing_objects.filter(id__in=ids))

        keys = set()
        for related_model, instances in collector.data.items():
            keys.update(row_key(related_model, instance.pk) for instance in instances)
        for qs in collector.fast_deletes:
            keys.update(row_key(qs.model, pk) for pk in qs.values_list("pk", flat=True))

        collector.delete()

    return keys - {row_key(model, pk) for pk in ids}


def record_propagated_deletions(tasks: List[DeserializeTask]) -> None:
    """
    Adds the tasks to `DeletedModels`, and those with a propagated hard deletion to
    `HardDeletedModels`, so the next serialization marks their store records deleted
    """
    deleted = []
    hard_deleted = []
    for task in tasks:
        deleted.append(DeletedModels(id=task.id, profile=task.store.profile))
        if task.outcome == DeserializeOutcome.PROPAGATE_HARD_DELETE:
            hard_deleted.append(HardDeletedModels(id=task.id, profile=task.store.profile))

    if deleted:
        DeletedModels.objects.bulk_create(deleted, ignore_conflicts=True)
    if hard_deleted:
        HardDeletedModels.objects.bulk_create(hard_deleted, ignore_conflicts=True)


class AppModelDeserialize(Transform[DeserializeTask]):
    """
    Builds the unsaved app model from each task's serialized store data. Deleted store records
    pass through untouched, since deleting the app model is left to the sink.
    """

    def __init__(self, sync_filter: Optional[Filter] = None):
        self.sync_filter = sync_filter

    def transform(self, task: DeserializeTask) -> DeserializeTask:
        if task.store.deleted:
            return task

        try:
            app_model = task.model.deserialize(
                json.loads(task.store.serialized), sync_filter=self.sync_filter
            )
        except (ValueError, exceptions.ValidationError) as e:
            # `json.JSONDecodeError` is a `ValueError`
            task.add_error(e)
            return task

        app_model._morango_source_id = task.store.source_id
        app_model._morango_partition = task.store.partition
        app_model._morango_dirty_bit = False
        task.set_app_model(app_model)
        return task


class AppModelValidate(Transform[DeserializeTask]):
    """
    Validates each task's app model and resolves its outcome. The source streams FK targets ahead
    of the records that reference them, so a target validated earlier in the run is already in
    the shared `fk_cache` even though the sink may not have written it yet. A cache miss falls back
    to looking up the target in the app tables.

    Must be instantiated per pipeline run, since it tracks which records failed or were deleted
    during the run, so that the records referencing them can be resolved accordingly.

    Deletions spread to referencing records as in legacy deserialization. For models without a
    self-referential FK, a reference to any deleted record spreads its deletion. For models with
    one, only a validation failure of `ObjectDoesNotExist` spreads it.
    """

    def __init__(self, sync_filter: Optional[Filter] = None):
        self.sync_filter = sync_filter
        self.failed_ids: Set[str] = set()
        # maps the ID of a deleted record to whether the deletion is a hard deletion
        self.deleted_ids: Dict[str, bool] = {}

    def transform(self, task: DeserializeTask) -> DeserializeTask:
        if task.has_errors:
            self.failed_ids.add(task.id)
            return task

        if task.store.deleted:
            self.deleted_ids[task.id] = task.store.hard_deleted
            return task

        references = fk_references(task.app_model)

        for field, target_id in references:
            if target_id in self.deleted_ids:
                if task.self_referential_fk() is not None:
                    # the sink has not deleted the target's app row yet, so validation can't
                    # report the missing target; fail as legacy does once the row is gone
                    self._fail(task, deleted_target_error(field, target_id))
                else:
                    self._propagate_deletion(task, hard=self.deleted_ids[target_id])
                return task

        for field, target_id in references:
            if target_id in self.failed_ids:
                self._fail(task, failed_target_error(task, field, target_id))
                return task

        try:
            task.app_model.cached_clean_fields(task.fk_cache, sync_filter=self.sync_filter)
        except (exceptions.ValidationError, exceptions.ObjectDoesNotExist, ValueError) as e:
            self._classify_failure(task, references, e)
        return task

    def _classify_failure(
        self,
        task: DeserializeTask,
        references: List[Tuple[ForeignKey, str]],
        error: Exception,
    ) -> None:
        """
        Resolves a validation failure using the store records of the FK targets. Only runs on
        failure, so it costs at most one query per failed record and none per valid record.
        """
        targets = self._lookup_targets(references)

        # a reference to a deleted record propagates the deletion, instead of failing
        if task.self_referential_fk() is None or isinstance(error, exceptions.ObjectDoesNotExist):
            for _, target_id in references:
                target = targets.get(target_id)
                if target is not None and (target.deleted or target.hard_deleted):
                    self._propagate_deletion(task, hard=target.hard_deleted)
                    return

        self._fail(task, self._failure_error(task, references, targets, error))

    @staticmethod
    def _lookup_targets(
        references: List[Tuple[ForeignKey, str]],
    ) -> Dict[str, _TargetState]:
        """The store state of each FK target that has a store record, keyed by the target ID"""
        if not references:
            return {}
        target_ids = [target_id for _, target_id in references]
        return {
            target_id: _TargetState(deleted, hard_deleted, dirty_bit)
            for target_id, deleted, hard_deleted, dirty_bit in Store.objects.filter(
                id__in=target_ids
            ).values_list("id", "deleted", "hard_deleted", "dirty_bit")
        }

    def _failure_error(
        self,
        task: DeserializeTask,
        references: List[Tuple[ForeignKey, str]],
        targets: Dict[str, _TargetState],
        error: Exception,
    ) -> Exception:
        """
        The error to record for a failure that did not propagate a deletion. Problems with the
        parent take precedence, then missing targets, otherwise the original validation error.
        """
        self_ref_fk = task.self_referential_fk()
        for field, target_id in references:
            if field.attname != self_ref_fk:
                continue
            if target_id not in targets:
                return MorangoMissingParent(MISSING_PARENT_MESSAGE)
            if targets[target_id].dirty_bit and self._parent_not_written(
                task, field, target_id, error
            ):
                return MorangoDirtyParent(DIRTY_PARENT_MESSAGE)

        for field, target_id in references:
            if target_id not in targets:
                return IntegrityError(
                    "{from_model}.{from_field} references non-existent {to_model} instance with "
                    "id '{to_pk}'".format(
                        from_model=task.model.__name__,
                        from_field=field.attname,
                        to_model=field.related_model._meta.verbose_name,
                        to_pk=target_id,
                    )
                )

        return error

    @staticmethod
    def _parent_not_written(
        task: DeserializeTask, field: ForeignKey, parent_id: str, error: Exception
    ) -> bool:
        """
        Whether a failure is explained by a dirty parent's app row not being written. The parent
        is in `fk_cache` when its FK resolved, either because it was accepted earlier in the run or
        because an older app row exists, and then only loading the parent object can fail on it.
        Any other error is the record's own.
        """
        return _fk_cache_key(field.related_model, parent_id) not in task.fk_cache or isinstance(
            error, exceptions.ObjectDoesNotExist
        )

    def _propagate_deletion(self, task: DeserializeTask, hard: bool) -> None:
        task.set_propagated_deletion(hard)
        self.deleted_ids[task.id] = hard

    def _fail(self, task: DeserializeTask, error: Exception) -> None:
        task.add_error(error)
        self.failed_ids.add(task.id)


class DeserializeSink(Sink[List[DeserializeTask]]):
    """
    Writes each task's resolved outcome to the app tables, the store and the deletion tracking
    tables. Consumes chunks of tasks, which the pipeline partitions by model.

    Must be instantiated per pipeline run, since it tracks the records that failed and the app rows
    removed by cascading deletes. The buffer in front of the sink means a task is validated before
    the sink writes the chunks ahead of it, so a save is checked against both before saving.
    """

    def __init__(self):
        self.failed_ids: Set[str] = set()
        self.cascaded_keys: Set[Tuple[str, str]] = set()
        self.counts: Dict[DeserializeOutcome, int] = defaultdict(int)

    def consume(self, tasks: List[DeserializeTask]) -> None:
        by_outcome = defaultdict(list)
        for task in tasks:
            by_outcome[task.outcome].append(task)

        clean = []
        errored = []

        for task in by_outcome[DeserializeOutcome.ERROR]:
            self._record_error(task, task.errors[0])
            errored.append(task)

        # deletes run before saves, so the saves are checked against the rows they cascade to
        deletes = by_outcome[DeserializeOutcome.DELETE]
        self._delete_all(deletes)
        clean.extend(deletes)

        propagated = (
            by_outcome[DeserializeOutcome.PROPAGATE_DELETE]
            + by_outcome[DeserializeOutcome.PROPAGATE_HARD_DELETE]
        )
        record_propagated_deletions(propagated)
        clean.extend(propagated)

        self._save_all(by_outcome[DeserializeOutcome.SAVE], clean, errored)
        self._write_stores(clean, errored)

    def _delete_all(self, tasks: List[DeserializeTask]) -> None:
        """Deletes the tasks' app rows with one delete per model, recording the cascaded rows"""
        ids_by_model = defaultdict(list)
        for task in tasks:
            ids_by_model[task.model].append(task.id)
        for model, ids in ids_by_model.items():
            self.cascaded_keys |= delete_app_models(model, ids)

    def _save_all(
        self,
        tasks: List[DeserializeTask],
        clean: List[DeserializeTask],
        errored: List[DeserializeTask],
    ) -> None:
        """Saves the tasks' app models in arrival order, adding each task to `clean` or `errored`"""
        if not tasks:
            return
        with mute_signals(signals.pre_save, signals.post_save):
            for task in tasks:
                error = self._removed_target_error(task)
                if error is not None:
                    self._record_error(task, error)
                    errored.append(task)
                elif self._save(task):
                    clean.append(task)
                else:
                    errored.append(task)

    def _removed_target_error(self, task: DeserializeTask) -> Optional[Exception]:
        """
        The error for a task whose FK target was removed after the task was validated, because the
        target failed in the sink or a delete cascaded to it. FK constraints are deferred, so saving
        the task would only fail when the transaction commits.
        """
        for field, target_id in fk_references(task.app_model):
            if target_id in self.failed_ids:
                return failed_target_error(task, field, target_id)
            if row_key(field.related_model, target_id) in self.cascaded_keys:
                return deleted_target_error(field, target_id)
        return None

    def _write_stores(self, clean: List[DeserializeTask], errored: List[DeserializeTask]) -> None:
        """Clears the store records that were written, and saves the errors of those that weren't"""
        if clean:
            Store.objects.filter(id__in=[task.id for task in clean]).update(
                dirty_bit=False, deserialization_error=None, deserialization_exception=None
            )
        # errors are rare, and `bulk_update` is slower than saving each record
        for task in errored:
            task.store.save(update_fields=["deserialization_error", "deserialization_exception"])

        for task in clean + errored:
            self.counts[task.outcome] += 1

    def summary(self) -> Dict[str, int]:
        """The number of tasks consumed per outcome, counting failed saves as errors"""
        return {outcome.value: count for outcome, count in self.counts.items()}

    def _save(self, task: DeserializeTask) -> bool:
        """Saves the task's app model in its own savepoint, recording the error if it fails"""
        try:
            with transaction.atomic():
                task.app_model.save(update_dirty_bit_to=False)
        except (
            exceptions.ValidationError,
            exceptions.ObjectDoesNotExist,
            ValueError,
            IntegrityError,
        ) as e:
            self._record_error(task, e)
            return False
        return True

    def _record_error(self, task: DeserializeTask, error: Exception) -> None:
        if error not in task.errors:
            task.add_error(error)
        task.store.set_deserialization_error(error)
        self.failed_ids.add(task.id)
        log_deserialization_error(task, error)


def log_deserialization_error(task: DeserializeTask, error: Exception) -> None:
    """Logs a record that failed to deserialize, with the message used by legacy deserialization"""
    logger.warning(
        "Failed to deserialize Store record %s for %s: %s",
        task.id,
        task.model.__name__,
        error,
    )


def store_model_partition_fn(task: DeserializeTask) -> str:
    return task.store.model_name


def deserialize_store_model(
    store: Store, fk_cache: Dict, sync_filter: Optional[Filter] = None
) -> Optional[SyncableModel]:
    """
    Deserializes and validates a single store record outside of a pipeline run. Deleted records are
    applied: a deleted record's app model is deleted, and a record whose FK target was deleted is
    tracked as deleted. Neither the store record nor a valid app model is saved.

    :param store: The store record to deserialize
    :param fk_cache: Dictionary cache for FK references, shared across calls
    :param sync_filter: The Filter object for the current sync, if any
    :return: The validated app model, or None if the record was deleted
    :raises Exception: The error that the record failed with, after logging it
    """
    task = DeserializeTask(store, fk_cache)
    AppModelDeserialize(sync_filter=sync_filter).transform(task)
    AppModelValidate(sync_filter=sync_filter).transform(task)

    outcome = task.outcome
    if outcome == DeserializeOutcome.ERROR:
        error = task.errors[0]
        log_deserialization_error(task, error)
        raise error
    if outcome == DeserializeOutcome.DELETE:
        # without sink lag, a later call validates against the app tables where the cascaded rows
        # are already gone, so the cascaded keys aren't needed
        delete_app_models(task.model, [task.id])
        return None
    if outcome in (DeserializeOutcome.PROPAGATE_DELETE, DeserializeOutcome.PROPAGATE_HARD_DELETE):
        record_propagated_deletions([task])
        return None
    return task.app_model


CHUNK_SIZE = 500


def deserialize_from_store(
    profile: str,
    sync_filter: Optional[Filter] = None,
    skip_errored: bool = False,
) -> None:
    """
    Constructs and executes the deserialization pipeline, streaming dirty store records through
    transforms that resolve each record's outcome, into a sink that writes them in chunks.

    :param profile: The Morango model profile
    :param sync_filter: The Filter object for this sync
    :param skip_errored: Whether to skip Store records with deserialization errors
    """
    with begin_transaction(sync_filter, isolated=True):
        sink = DeserializeSink()
        (
            StoreModelSource(profile, sync_filter=sync_filter, skip_errored=skip_errored)
            .pipe(AppModelDeserialize(sync_filter=sync_filter))
            .pipe(AppModelValidate(sync_filter=sync_filter))
            .pipe(Buffer(size=CHUNK_SIZE, partition_fn=store_model_partition_fn))
            .end(sink)
        )
        logger.info("Deserialization done: %s", sink.summary())
