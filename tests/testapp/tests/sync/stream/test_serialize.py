import json

import mock
from django.db.models import Q
from django.test import SimpleTestCase

from morango.models.certificates import Filter
from morango.models.core import InstanceIDModel, RecordMaxCounter, Store, SyncableModel
from morango.sync.stream.serialize import (
    AppModelSource,
    ModelPartitionBuffer,
    SerializeTask,
    StoreLookup,
    StoreUpdate,
    WriteSink,
)


class SerializeTaskTestCase(SimpleTestCase):
    def setUp(self):
        self.model = mock.Mock(spec_set=SyncableModel)
        self.obj = mock.Mock(spec=SyncableModel)
        self.task = SerializeTask(self.model, self.obj)

    def test_is_store_update(self):
        self.assertFalse(self.task.is_store_update)
        store = mock.Mock(spec_set=Store)()
        store._state.adding = False
        self.task.set_store(store)
        self.assertTrue(self.task.is_store_update)

    def test_is_counter_update(self):
        self.assertFalse(self.task.is_counter_update)
        counter = mock.Mock(spec_set=RecordMaxCounter)()
        counter._state.adding = False
        self.task.set_counter(counter)
        self.assertTrue(self.task.is_counter_update)

    @mock.patch("morango.sync.stream.serialize.self_referential_fk")
    def test_self_referential_fk(self, mock_self_referential_fk):
        mock_self_referential_fk.return_value = "self_ref_fk"
        self.assertEqual(self.task.self_referential_fk(), "self_ref_fk")
        mock_self_referential_fk.assert_called_once_with(self.model)


class AppModelSourceTestCase(SimpleTestCase):
    def setUp(self):
        self.model = mock.Mock(spec_set=SyncableModel)

    def test_prefix_conditions__none(self):
        source = AppModelSource(profile="test")
        conditions = list(source.prefix_conditions())
        self.assertEqual(conditions, [None])

    def test_prefix_conditions__with_filter(self):
        sync_filter = Filter("a\nb")
        source = AppModelSource(profile="test", sync_filter=sync_filter)
        conditions = list(source.prefix_conditions())
        self.assertEqual(len(conditions), 2)
        self.assertEqual(str(conditions[0]), "(AND: ('_morango_partition__startswith', 'a'))")

    @mock.patch("morango.sync.stream.serialize.syncable_models.get_model_querysets")
    def test_stream__no_partition(self, mock_get_model_querysets):
        qs = mock.Mock()
        mock_get_model_querysets.return_value = [qs]
        model = qs.model
        obj = mock.Mock(id="123")
        qs.filter.return_value = qs
        qs.iterator.return_value = [obj]

        source = AppModelSource(profile="test")
        tasks = list(source.stream())

        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].model, model)
        self.assertEqual(tasks[0].obj, obj)
        mock_get_model_querysets.assert_called_once_with("test")
        qs.filter.assert_called_once_with(_morango_dirty_bit=True)

    @mock.patch("morango.sync.stream.serialize.syncable_models.get_model_querysets")
    def test_stream__seen_once(self, mock_get_model_querysets):
        qs = mock.Mock()
        mock_get_model_querysets.return_value = [qs]
        model = qs.model
        obj = mock.Mock(id="123")
        qs.filter.return_value = qs
        qs.iterator.return_value = [obj, obj]

        source = AppModelSource(profile="test")
        tasks = list(source.stream())

        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].model, model)
        self.assertEqual(tasks[0].obj, obj)
        mock_get_model_querysets.assert_called_once_with("test")
        qs.filter.assert_called_once_with(_morango_dirty_bit=True)

    @mock.patch("morango.sync.stream.serialize.syncable_models.get_model_querysets")
    def test_stream__partition(self, mock_get_model_querysets):
        qs = mock.Mock()
        mock_get_model_querysets.return_value = [qs]
        model = qs.model
        obj = mock.Mock(id="123")
        qs.filter.return_value = qs
        qs.iterator.return_value = [obj, obj]

        source = AppModelSource(profile="test", sync_filter=Filter("a"), dirty_only=False)
        tasks = list(source.stream())

        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].model, model)
        self.assertEqual(tasks[0].obj, obj)
        mock_get_model_querysets.assert_called_once_with("test")
        qs.filter.assert_called_once_with(Q(_morango_partition__startswith="a"))


class StoreLookupTestCase(SimpleTestCase):
    @mock.patch("morango.models.core.RecordMaxCounter.objects.filter")
    @mock.patch("morango.models.core.Store.objects.in_bulk")
    def test_transform(self, mock_bulk, mock_qs):
        current_id = mock.Mock(spec=InstanceIDModel)
        current_id.id = "inst_1"
        lookup = StoreLookup(current_id)

        obj1 = mock.Mock(id="obj_1")
        task1 = SerializeTask(mock.Mock(), obj1)
        obj2 = mock.Mock(id="obj_2")
        task2 = SerializeTask(mock.Mock(), obj2)

        store_obj1 = mock.Mock(spec=Store)
        store_obj2 = mock.Mock(spec=Store)
        mock_bulk.return_value = {"obj_1": store_obj1, "obj_2": store_obj2}

        counter_obj = mock.Mock(spec=RecordMaxCounter, store_model_id="obj_1")
        mock_qs.return_value = [counter_obj]

        results = lookup.transform([task1, task2])

        self.assertEqual(results[0].store, store_obj1)
        self.assertEqual(results[0].counter, counter_obj)
        self.assertEqual(results[1].store, store_obj2)
        self.assertEqual(results[1].counter, None)


class StoreUpdateTestCase(SimpleTestCase):
    @mock.patch("morango.sync.stream.serialize.StoreUpdate._handle_store_create")
    def test_transform__creates(self, mock_handle_store_create):
        current_id = mock.Mock(id="inst_1", counter=10)
        update = StoreUpdate(current_id)

        store = Store(id="123", serialized=json.dumps({"old": 1}), dirty_bit=False)

        task = SerializeTask(mock.Mock(), mock.Mock())
        mock_handle_store_create.side_effect = lambda _: task.set_store(store)

        update.transform(task)
        mock_handle_store_create.assert_called_once_with(task)
        self.assertEqual(task.counter.instance_id, "inst_1")
        self.assertEqual(task.counter.counter, 10)
        self.assertEqual(task.counter.store_model_id, "123")

    @mock.patch("morango.sync.stream.serialize.StoreUpdate._handle_store_update")
    def test_transform__updates(self, mock_handle_store_update):
        current_id = mock.Mock(id="inst_1", counter=10)
        update = StoreUpdate(current_id)

        store = Store(serialized=json.dumps({"old": 1}), dirty_bit=False)
        store._state.adding = False
        counter = RecordMaxCounter(counter=5)

        task = SerializeTask(mock.Mock(), mock.Mock())
        task.set_store(store)
        task.set_counter(counter)

        update.transform(task)
        mock_handle_store_update.assert_called_once_with(task)
        self.assertEqual(task.counter.counter, 10)

    def test_handle_store_update(self):
        current_id = mock.Mock(id="inst_1", counter=10)
        update = StoreUpdate(current_id)

        store = Store(serialized=json.dumps({"old": 1}), dirty_bit=False)
        obj = mock.Mock()
        obj.serialize.return_value = {"new": 2}

        task = SerializeTask(mock.Mock(), obj)
        task.set_store(store)

        update._handle_store_update(task)

        ser_data = json.loads(task.store.serialized)
        self.assertEqual(ser_data["old"], 1)
        self.assertEqual(ser_data["new"], 2)


class ModelPartitionBufferTestCase(SimpleTestCase):
    def test_buffer_splits_on_model_change(self):
        buff = ModelPartitionBuffer(size=10)
        m1, m2 = mock.Mock(), mock.Mock()
        tasks = [
            SerializeTask(m1, mock.Mock()),
            SerializeTask(m1, mock.Mock()),
            SerializeTask(m2, mock.Mock()),
        ]

        chunks = list(buff(tasks))
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0][0].model, m1)
        self.assertEqual(chunks[0][1].model, m1)
        self.assertEqual(chunks[1][0].model, m2)


class WriteSinkTestCase(SimpleTestCase):
    def setUp(self):
        self.profile = "test"
        self.current_id = mock.Mock(spec=InstanceIDModel)
        self.current_id.id = "inst_1"
        self.current_id.counter = 10
        self.sink = WriteSink(self.profile, self.current_id)

    @mock.patch("morango.sync.stream.serialize.RecordMaxCounter.objects.bulk_create")
    @mock.patch("morango.sync.stream.serialize.Store.objects.bulk_create")
    def test_consume(self, mock_store_bulk, mock_counter_bulk):
        model = mock.Mock()
        obj1 = mock.Mock(id="obj_1")
        task1 = SerializeTask(model, obj1)
        store1 = mock.Mock(spec_set=Store, id="obj_1")()
        store1._state.adding = True
        task1.set_store(store1)
        counter1 = mock.Mock(spec_set=RecordMaxCounter)()
        counter1._state.adding = True
        task1.set_counter(counter1)

        obj2 = mock.Mock(id="obj_2")
        task2 = SerializeTask(model, obj2)
        store2 = mock.Mock(spec_set=Store, id="obj_2")()
        store2._state.adding = False
        task2.set_store(store2)
        counter2 = mock.Mock(spec_set=RecordMaxCounter)()
        counter2._state.adding = False
        task2.set_counter(counter2)

        def _bulk_store_create(objs, **kwargs):
            for obj in objs:
                obj._state.adding = False
            return objs

        def _bulk_counter_create(objs, **kwargs):
            for obj in objs:
                obj._state.adding = False
            return objs

        mock_store_bulk.side_effect = _bulk_store_create
        mock_counter_bulk.side_effect = _bulk_counter_create

        self.sink.consume([task1, task2])

        # Verify Store operations
        mock_store_bulk.assert_called_once_with([store1], ignore_conflicts=True)
        store1.save.assert_not_called()
        store2.save.assert_called_once()

        # Verify Counter operations
        mock_counter_bulk.assert_called_once_with([counter1], ignore_conflicts=True)
        counter1.save.assert_not_called()
        counter2.save.assert_called_once()

        # Verify dirty bit update on app models
        model.syncing_objects.filter.assert_called_once_with(id__in=["obj_1", "obj_2"])
        model.syncing_objects.filter().update.assert_called_once_with(update_dirty_bit_to=False)

    @mock.patch("morango.sync.stream.serialize.RecordMaxCounter.objects.filter")
    @mock.patch("morango.sync.stream.serialize.RecordMaxCounter.objects.bulk_create")
    @mock.patch("morango.sync.stream.serialize.Store.objects.bulk_create")
    def test_consume__create_fail(self, mock_store_bulk, mock_counter_bulk, mock_counter_filter):
        model = mock.Mock()
        obj1 = mock.Mock(id="obj_1")
        task1 = SerializeTask(model, obj1)
        store1 = mock.Mock(spec_set=Store, id="obj_1")()
        store1._state.adding = True
        task1.set_store(store1)
        counter1 = mock.Mock(spec_set=RecordMaxCounter)()
        counter1._state.adding = True
        counter1.store_model_id = "123"
        task1.set_counter(counter1)

        # Mock bulk_create returns
        mock_store_bulk.return_value = [store1]
        mock_counter_bulk.return_value = [counter1]
        mock_counter_filter.return_value = [counter1]

        self.sink.consume([task1])

        mock_counter_filter.assert_called_once_with(
            instance_id="inst_1", store_model_id__in=["123"]
        )

        # Verify Store operations
        mock_store_bulk.assert_called_once_with([store1], ignore_conflicts=True)
        store1.save.assert_called_once()

        # Verify Counter operations
        mock_counter_bulk.assert_called_once_with([counter1], ignore_conflicts=True)
        counter1.save.assert_called_once()

        # Verify dirty bit update on app models
        model.syncing_objects.filter.assert_called_once_with(id__in=["obj_1"])
        model.syncing_objects.filter().update.assert_called_once_with(update_dirty_bit_to=False)

    @mock.patch("morango.sync.stream.serialize.RecordMaxCounter.objects.filter")
    @mock.patch("morango.sync.stream.serialize.RecordMaxCounter.objects.bulk_create")
    @mock.patch("morango.sync.stream.serialize.Store.objects.filter")
    @mock.patch("morango.sync.stream.serialize.DeletedModels.objects.filter")
    def test_handle_deleted(
        self, mock_deleted_filter, mock_store_filter, mock_rmc_bulk, mock_counter_filter
    ):
        mock_deleted_filter.return_value.values_list.return_value = ["del_1"]
        mock_store_records = mock.Mock()
        mock_store_filter.return_value = mock_store_records
        mock_store_records.exclude.return_value.values_list.return_value = ["del_1"]

        self.sink._handle_deleted()

        mock_deleted_filter.assert_called_with(profile=self.profile)
        mock_store_filter.assert_called_once_with(id__in=["del_1"])
        mock_store_records.update.assert_called_once_with(
            dirty_bit=False,
            deleted=True,
            last_saved_instance=self.current_id.id,
            last_saved_counter=self.current_id.counter,
        )
        mock_counter_filter.assert_called_once_with(
            instance_id=self.current_id.id, store_model_id__in=["del_1"]
        )
        mock_counter_filter().update.assert_called_once_with(counter=self.current_id.counter)
        mock_store_records.exclude.assert_called_once_with(
            recordmaxcounter__instance_id=self.current_id.id
        )
        mock_store_records.exclude().values_list.assert_called_once_with("id", flat=True)
        mock_rmc_bulk.assert_called_once()
        rmc = mock_rmc_bulk.call_args[0][0][0]
        self.assertEqual(rmc.store_model_id, "del_1")
        self.assertEqual(rmc.instance_id, self.current_id.id)
        self.assertEqual(rmc.counter, self.current_id.counter)
        mock_deleted_filter().delete.assert_called_once()

    @mock.patch("morango.sync.stream.serialize.HardDeletedModels.objects.filter")
    @mock.patch("morango.sync.stream.serialize.Store.objects.filter")
    def test_handle_hard_deleted(self, mock_store_filter, mock_hard_deleted_filter):
        mock_hard_deleted_filter.return_value.values_list.return_value = ["hard_del_1"]
        mock_store_records = mock.Mock()
        mock_store_filter.return_value = mock_store_records

        self.sink._handle_hard_deleted()

        mock_hard_deleted_filter.assert_called_with(profile=self.profile)
        mock_store_records.update.assert_called_once_with(
            hard_deleted=True, serialized="{}", conflicting_serialized_data=""
        )
        mock_hard_deleted_filter().delete.assert_called_once()

    @mock.patch("morango.sync.stream.serialize.DatabaseMaxCounter.objects.update_or_create")
    def test_update_counters(self, mock_update_or_create):
        self.sink.sync_filter = Filter("a")
        self.sink._update_counters()
        self.assertEqual(mock_update_or_create.call_count, 1)
        mock_update_or_create.assert_called_with(
            instance_id=self.current_id.id,
            partition="a",
            defaults={"counter": self.current_id.counter},
        )

    @mock.patch("morango.sync.stream.serialize.DatabaseMaxCounter.objects.update_or_create")
    def test_update_counters__no_filter(self, mock_update_or_create):
        self.sink._update_counters()
        self.assertEqual(mock_update_or_create.call_count, 1)
        mock_update_or_create.assert_called_with(
            instance_id=self.current_id.id,
            partition="",
            defaults={"counter": self.current_id.counter},
        )
