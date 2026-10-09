import json
from collections import namedtuple
from typing import Dict
from typing import Generator
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Type

from django.core import exceptions
from django.db.models import ForeignKey
from django.db.utils import IntegrityError

from morango.errors import MorangoDirtyParent
from morango.errors import MorangoMissingParent
from morango.models.certificates import Filter
from morango.models.core import Store
from morango.models.core import SyncableModel
from morango.registry import syncable_models
from morango.sync.stream.core import Transform
from morango.sync.stream.source import MorangoSource
from morango.sync.stream.source import SourceTask

DIRTY_PARENT_MESSAGE = "Parent is dirty; could not deserialize."
MISSING_PARENT_MESSAGE = "Parent does not exist in Store; could not deserialize."

# the store state of an FK target, used to resolve validation failures
_TargetState = namedtuple("_TargetState", ["deleted", "hard_deleted", "dirty_bit"])


class DeserializeOutcome:
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
    def outcome(self) -> str:
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

    Must be instantiated per pipeline run, since it tracks which records were accepted, failed or
    were deleted during the run, so that the records referencing them can be resolved accordingly.

    Deletions spread to referencing records as in legacy deserialization. For models without a
    self-referential FK, a reference to any deleted record spreads its deletion. For models with
    one, only a validation failure of `ObjectDoesNotExist` spreads it.
    """

    def __init__(self, sync_filter: Optional[Filter] = None):
        self.sync_filter = sync_filter
        self.accepted_ids: Set[str] = set()
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

        fk_references = self._fk_references(task.app_model)

        for field, target_id in fk_references:
            if target_id in self.deleted_ids:
                if task.self_referential_fk() is not None:
                    # the sink has not deleted the target's app row yet, so validation can't
                    # report the missing target; fail as legacy does once the row is gone
                    self._fail(task, self._deleted_target_error(field, target_id))
                else:
                    self._propagate_deletion(task, hard=self.deleted_ids[target_id])
                return task

        for field, target_id in fk_references:
            if target_id in self.failed_ids:
                self._fail(task, self._failed_target_error(task, field, target_id))
                return task

        try:
            task.app_model.cached_clean_fields(task.fk_cache, sync_filter=self.sync_filter)
        except (exceptions.ValidationError, exceptions.ObjectDoesNotExist, ValueError) as e:
            self._classify_failure(task, fk_references, e)
        else:
            self.accepted_ids.add(task.id)
        return task

    @staticmethod
    def _fk_references(app_model: SyncableModel) -> List[Tuple[ForeignKey, str]]:
        """The FK fields on the app model that hold a value, paired with that value"""
        references = []
        for field in app_model._meta.fields:
            if not isinstance(field, ForeignKey):
                continue
            target_id = getattr(app_model, field.attname)
            if target_id is not None:
                references.append((field, target_id))
        return references

    @staticmethod
    def _failed_target_error(task: DeserializeTask, field: ForeignKey, target_id: str) -> Exception:
        if field.attname == task.self_referential_fk():
            return MorangoDirtyParent(DIRTY_PARENT_MESSAGE)
        return exceptions.ValidationError(
            "{} with id {} failed to deserialize".format(
                field.related_model._meta.verbose_name, target_id
            )
        )

    @staticmethod
    def _deleted_target_error(field: ForeignKey, target_id: str) -> Exception:
        return exceptions.ValidationError(
            "{} with id {} was deleted".format(field.related_model._meta.verbose_name, target_id)
        )

    def _classify_failure(
        self,
        task: DeserializeTask,
        fk_references: List[Tuple[ForeignKey, str]],
        error: Exception,
    ) -> None:
        """
        Resolves a validation failure using the store records of the FK targets. Only runs on
        failure, so it costs at most one query per failed record and none per valid record.
        """
        targets = self._lookup_targets(fk_references)

        # a reference to a deleted record propagates the deletion, instead of failing
        if task.self_referential_fk() is None or isinstance(error, exceptions.ObjectDoesNotExist):
            for _, target_id in fk_references:
                target = targets.get(target_id)
                if target is not None and (target.deleted or target.hard_deleted):
                    self._propagate_deletion(task, hard=target.hard_deleted)
                    return

        self._fail(task, self._failure_error(task, fk_references, targets, error))

    @staticmethod
    def _lookup_targets(
        fk_references: List[Tuple[ForeignKey, str]],
    ) -> Dict[str, _TargetState]:
        """The store state of each FK target that has a store record, keyed by the target ID"""
        if not fk_references:
            return {}
        target_ids = [target_id for _, target_id in fk_references]
        return {
            target_id: _TargetState(deleted, hard_deleted, dirty_bit)
            for target_id, deleted, hard_deleted, dirty_bit in Store.objects.filter(
                id__in=target_ids
            ).values_list("id", "deleted", "hard_deleted", "dirty_bit")
        }

    def _failure_error(
        self,
        task: DeserializeTask,
        fk_references: List[Tuple[ForeignKey, str]],
        targets: Dict[str, _TargetState],
        error: Exception,
    ) -> Exception:
        """
        The error to record for a failure that did not propagate a deletion. Problems with the
        parent take precedence, then missing targets, otherwise the original validation error.
        """
        self_ref_fk = task.self_referential_fk()
        for field, target_id in fk_references:
            if field.attname != self_ref_fk:
                continue
            if target_id not in targets:
                return MorangoMissingParent(MISSING_PARENT_MESSAGE)
            if targets[target_id].dirty_bit and self._parent_not_written(target_id, error):
                return MorangoDirtyParent(DIRTY_PARENT_MESSAGE)

        for field, target_id in fk_references:
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

    def _parent_not_written(self, parent_id: str, error: Exception) -> bool:
        """
        Whether a failure is explained by a dirty parent's app row not being written. A parent
        accepted earlier in the run is in `fk_cache`, so its FK passed validation and only loading
        the parent object can fail on it. Any other error is the record's own.
        """
        return parent_id not in self.accepted_ids or isinstance(
            error, exceptions.ObjectDoesNotExist
        )

    def _propagate_deletion(self, task: DeserializeTask, hard: bool) -> None:
        task.set_propagated_deletion(hard)
        self.deleted_ids[task.id] = hard

    def _fail(self, task: DeserializeTask, error: Exception) -> None:
        task.add_error(error)
        self.failed_ids.add(task.id)
