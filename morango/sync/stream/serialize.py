import json
import logging
from typing import Generator, Iterable, Iterator, List, Optional, Type

from django.core.serializers.json import DjangoJSONEncoder
from django.db.models import Q

from morango.models.certificates import Filter
from morango.models.core import (
    DatabaseMaxCounter,
    DeletedModels,
    HardDeletedModels,
    InstanceIDModel,
    RecordMaxCounter,
    Store,
    SyncableModel,
)
from morango.registry import syncable_models
from morango.sync.stream.core import Buffer, Sink, Source, Transform, Unbuffer
from morango.utils import self_referential_fk

logger = logging.getLogger(__name__)


class SerializeTask(object):
    """Carrier class for providing context through the pipeline"""

    __slots__ = ("model", "obj", "store", "counter")

    def __init__(self, model: Type[SyncableModel], obj: SyncableModel):
        self.model = model
        self.obj = obj
        self.store: Optional[Store] = None
        self.counter: Optional[RecordMaxCounter] = None

    @property
    def is_store_update(self):
        return self.store is not None and not self.store._state.adding

    @property
    def is_counter_update(self):
        return self.counter is not None and not self.counter._state.adding

    def set_store(self, store_obj: Store):
        self.store = store_obj

    def set_counter(self, counter: RecordMaxCounter):
        self.counter = counter

    def self_referential_fk(self) -> Optional[str]:
        """Return the attname of the self-referential FK on *model*, or ``None``."""
        return self_referential_fk(self.model)


class AppModelSource(Source[SerializeTask]):
    """
    Yields ``SerializeTask`` objects for every syncable-model record that matches the
    optional *sync_filter*.
    """

    def __init__(
        self,
        profile: str,
        sync_filter: Optional[Filter] = None,
        dirty_only: bool = True,
        partition_order: str = "asc",
    ):
        """
        :param profile: The Morango model profile
        :param sync_filter: The Filter object for this sync
        :param dirty_only: Whether to filter on dirty records only
        :param partition_order: Controls how the filter specificity is applied, "asc" or "desc"
        """
        self.profile = profile
        self.sync_filter = sync_filter
        self.dirty_only = dirty_only
        self.partition_order = partition_order
        self._seen = set()

    def prefix_conditions(self) -> Generator[Optional[Q], None, None]:
        if self.sync_filter is None:
            # yield None once, so we do one query without a partition filter (everything)
            yield None
        else:
            partitions_prefixes = [str(prefix) for prefix in self.sync_filter]
            partition_iterator = sorted(
                partitions_prefixes,
                reverse=self.partition_order == "desc",
            )

            for prefix in partition_iterator:
                yield Q(_morango_partition__startswith=prefix)

    def stream(self) -> Generator[SerializeTask, None, None]:
        for partition_condition in self.prefix_conditions():
            for qs in syncable_models.get_model_querysets(self.profile):
                if partition_condition is not None:
                    qs = qs.filter(partition_condition)
                if self.dirty_only:
                    qs = qs.filter(_morango_dirty_bit=True)
                for obj in qs.iterator():
                    # partition filtering could result in overlaps, and since we're walking
                    # through the partitions one by one, we should avoid duplicates. Morango
                    # syncable models have unique IDs across the entire profile
                    if obj.id not in self._seen:
                        self._seen.add(obj.id)
                        yield SerializeTask(qs.model, obj)


class StoreLookup(Transform[List[SerializeTask]]):
    """
    For each `SerializeTask`, look up the corresponding store record (if any)
    and emit the tasks back.
    """

    def __init__(self, current_id: InstanceIDModel):
        self.current_id = current_id

    def transform(self, tasks: List[SerializeTask]) -> List[SerializeTask]:
        store_ids = [task.obj.id for task in tasks]
        stores = Store.objects.in_bulk(store_ids)
        counters = {}
        counters_qs = RecordMaxCounter.objects.filter(
            instance_id=self.current_id.id, store_model_id__in=store_ids
        )

        for counter in counters_qs:
            counters[counter.store_model_id] = counter

        for task in tasks:
            store_obj = stores.get(task.obj.id)
            if store_obj:
                task.set_store(store_obj)
            counter_obj = counters.get(task.obj.id)
            if counter_obj:
                task.set_counter(counter_obj)

        return tasks


class StoreUpdate(Transform[SerializeTask]):
    """Processes the updates to the Morango store and record counters."""

    def __init__(self, current_id: InstanceIDModel):
        self.current_id = current_id

    def transform(self, task: SerializeTask) -> SerializeTask:
        if task.is_store_update:
            self._handle_store_update(task)
        else:
            self._handle_store_create(task)

        if task.is_counter_update:
            task.counter.counter = self.current_id.counter
        else:
            task.set_counter(
                RecordMaxCounter(
                    counter=self.current_id.counter,
                    instance_id=self.current_id.id,
                    store_model_id=task.store.id,
                )
            )

        return task

    def _handle_store_update(self, task: SerializeTask):
        # if store record dirty and app record dirty, append store serialized
        # to conflicting data
        if task.store.dirty_bit:
            task.store.conflicting_serialized_data = (
                task.store.serialized + "\n" + task.store.conflicting_serialized_data
            )
            task.store.dirty_bit = False

        # set new serialized data on this store model
        ser_dict = json.loads(task.store.serialized)
        ser_dict.update(task.obj.serialize())
        task.store.serialized = DjangoJSONEncoder().encode(ser_dict)

        # update last saved bys
        task.store.last_saved_instance = self.current_id.id
        task.store.last_saved_counter = self.current_id.counter
        # update deleted flags in case it was previously deleted
        task.store.deleted = False
        task.store.hard_deleted = False
        # clear last_transfer_session_id
        task.store.last_transfer_session_id = None

        self_ref_fk = task.self_referential_fk()
        if self_ref_fk:
            new_fk_value = getattr(task.obj, self_ref_fk) or ""
            if new_fk_value != task.store._self_ref_fk:
                task.store._self_ref_fk = new_fk_value
                task.store._self_ref_order = self._compute_self_ref_order(new_fk_value)

    def _handle_store_create(self, task: SerializeTask):
        kwargs = {
            "id": task.obj.id,
            "serialized": DjangoJSONEncoder().encode(task.obj.serialize()),
            "last_saved_instance": self.current_id.id,
            "last_saved_counter": self.current_id.counter,
            "model_name": task.obj.morango_model_name,
            "profile": task.obj.morango_profile,
            "partition": task.obj._morango_partition,
            "source_id": task.obj._morango_source_id,
        }

        self_ref_fk = task.self_referential_fk()
        if self_ref_fk:
            self_ref_fk_value = getattr(task.obj, self_ref_fk) or ""
            kwargs["_self_ref_fk"] = self_ref_fk_value
            kwargs["_self_ref_order"] = self._compute_self_ref_order(self_ref_fk_value)

        task.set_store(Store(**kwargs))

    @staticmethod
    def _compute_self_ref_order(self_ref_fk_value):
        """
        Compute ``_self_ref_order`` for a self-referential store record.

        Returns ``0`` when the record has no parent (root), otherwise queries
        the parent ``Store`` row and returns the next order value.
        """
        if not self_ref_fk_value:
            return 0
        parent_order = (
            Store.objects.filter(id=self_ref_fk_value)
            .values_list("_self_ref_order", flat=True)
            .first()
        )
        return parent_order + 1 if parent_order is not None else None


class ModelPartitionBuffer(Buffer[List[SerializeTask]]):
    """Buffers tasks into chunks that have the same model class."""

    def __call__(self, tasks: Iterable[SerializeTask]) -> Iterator[List[SerializeTask]]:
        chunk = []
        last_model = None

        for task in tasks:
            if len(chunk) >= self.size or (last_model and last_model != task.model):
                yield chunk
                chunk = []
            last_model = task.model
            chunk.append(task)

        if chunk:
            yield chunk


class WriteSink(Sink[List[SerializeTask]]):
    """
    Consumes SerializeTask objects and writes the appropriate changes to the database.
    """

    def __init__(
        self,
        profile: str,
        current_id: InstanceIDModel,
        sync_filter: Optional[Filter] = None,
    ):
        self.profile = profile
        self.current_id = current_id
        self.sync_filter = sync_filter

    def _partition_tasks(self, tasks: List[SerializeTask]):
        stores_to_create = []
        stores_to_update = []
        counters_to_create = []
        counters_to_update = []

        for task in tasks:
            if task.is_store_update:
                stores_to_update.append(task.store)
            else:
                stores_to_create.append(task.store)

            if task.is_counter_update:
                counters_to_update.append(task.counter)
            else:
                counters_to_create.append(task.counter)

        return (
            stores_to_create,
            stores_to_update,
            counters_to_create,
            counters_to_update,
        )

    def consume(self, tasks: List[SerializeTask]):  # noqa: C901
        stores_to_create, stores_to_update, counters_to_create, counters_to_update = (
            self._partition_tasks(tasks)
        )

        if stores_to_create:
            created_stores = Store.objects.bulk_create(stores_to_create, ignore_conflicts=True)
            for created_store in created_stores:
                # if bulk_create has not marked it as saving been added, then it must have been
                # a conflict, so we'll add it to the update list
                if created_store._state.adding:
                    stores_to_update.append(created_store)

        if stores_to_update:
            # TODO: bulk_update performs poorly-- is there a better way?
            for store in stores_to_update:
                store.save()

        if counters_to_create:
            created_counters = RecordMaxCounter.objects.bulk_create(
                counters_to_create, ignore_conflicts=True
            )
            update_counter_ids = []
            for created_counter in created_counters:
                # if bulk_create has not marked it as saving been added, then it must have been
                # a conflict, so we'll add it to the update list
                if created_counter._state.adding:
                    update_counter_ids.append(created_counter.store_model_id)
            if update_counter_ids:
                counters_to_update.extend(
                    RecordMaxCounter.objects.filter(
                        instance_id=self.current_id.id,
                        store_model_id__in=update_counter_ids,
                    )
                )

        if counters_to_update:
            # TODO: bulk_update performs poorly-- is there a better way?
            for counter in counters_to_update:
                counter.save()

        app_model_ids = [task.obj.id for task in tasks]
        app_model = tasks[0].model
        app_model.syncing_objects.filter(id__in=app_model_ids).update(update_dirty_bit_to=False)

    def finalize(self):
        self._handle_deleted()
        self._handle_hard_deleted()
        self._update_counters()

    def _handle_deleted(self):
        deleted_ids = DeletedModels.objects.filter(profile=self.profile).values_list(
            "id", flat=True
        )

        deleted_store_records = Store.objects.filter(id__in=deleted_ids)
        deleted_store_records.update(
            dirty_bit=False,
            deleted=True,
            last_saved_instance=self.current_id.id,
            last_saved_counter=self.current_id.counter,
        )

        # update rmcs counters for deleted models that have our instance id
        RecordMaxCounter.objects.filter(
            instance_id=self.current_id.id, store_model_id__in=deleted_ids
        ).update(counter=self.current_id.counter)

        # get a list of deleted model ids that don't have an rmc for our instance id
        new_rmc_ids = deleted_store_records.exclude(
            recordmaxcounter__instance_id=self.current_id.id
        ).values_list("id", flat=True)

        RecordMaxCounter.objects.bulk_create(
            [
                RecordMaxCounter(
                    store_model_id=r_id,
                    instance_id=self.current_id.id,
                    counter=self.current_id.counter,
                )
                for r_id in new_rmc_ids
            ]
        )
        # clear deleted models table for this profile
        DeletedModels.objects.filter(profile=self.profile).delete()

    def _handle_hard_deleted(self):
        hard_deleted_ids = HardDeletedModels.objects.filter(profile=self.profile).values_list(
            "id", flat=True
        )

        hard_deleted_store_records = Store.objects.filter(id__in=hard_deleted_ids)
        hard_deleted_store_records.update(
            hard_deleted=True, serialized="{}", conflicting_serialized_data=""
        )
        HardDeletedModels.objects.filter(profile=self.profile).delete()

    def _update_counters(self):
        if not self.sync_filter:
            DatabaseMaxCounter.objects.update_or_create(
                instance_id=self.current_id.id,
                partition="",
                defaults={"counter": self.current_id.counter},
            )
        else:
            for f in self.sync_filter:
                DatabaseMaxCounter.objects.update_or_create(
                    instance_id=self.current_id.id,
                    partition=f,
                    defaults={"counter": self.current_id.counter},
                )


def serialize_into_store(
    profile: str, sync_filter: Optional[Filter] = None, dirty_only: bool = True
):
    """
    Constructs and executes the serialization pipeline, streaming dirty app models
    one-by-one through a pipeline that updates the Morango store and metadata.
    """
    from morango.models.core import InstanceIDModel
    from morango.sync.db import begin_transaction

    current_id = InstanceIDModel.get_current_instance_and_increment_counter()

    with begin_transaction(sync_filter, isolated=True):
        # Execute the main pipeline (consumes the source through to the sink).
        result_count = (
            AppModelSource(profile, sync_filter=sync_filter, dirty_only=dirty_only)
            .pipe(Buffer(size=500))
            .pipe(StoreLookup(current_id))
            .pipe(Unbuffer())
            .pipe(StoreUpdate(current_id))
            .pipe(ModelPartitionBuffer(size=500))
            .end(WriteSink(profile, current_id, sync_filter=sync_filter))
        )
        logger.info(f"Serialization done: {result_count} records")
