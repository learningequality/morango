import json
import uuid

import mock
from django.db.models import Q
from django.test import SimpleTestCase
from django.test import TestCase

from morango.models.certificates import Filter
from morango.models.core import InstanceIDModel
from morango.models.core import RecordMaxCounter
from morango.models.core import Store
from morango.models.core import SyncableModel
from morango.sync.stream.serialize import AppModelSource
from morango.sync.stream.serialize import SelfRefOrderLookup
from morango.sync.stream.serialize import SerializeTask
from morango.sync.stream.serialize import StoreLookup
from morango.sync.stream.serialize import StoreUpdate
from morango.sync.stream.serialize import WriteSink


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

    @mock.patch("morango.sync.stream.serialize.syncable_models.get_self_referential_fk")
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


class SelfRefOrderLookupTestCase(TestCase):
    def setUp(self):
        self.model = mock.Mock()

    def _task(self, obj_id=None, parent_id=None):
        obj = mock.Mock()
        obj.id = obj_id or uuid.uuid4().hex
        obj.parent_id = parent_id
        return SerializeTask(self.model, obj)

    @mock.patch(
        "morango.sync.stream.serialize.syncable_models.get_self_referential_fk",
        return_value="parent_id",
    )
    def test_transform__same_batch_parent_child(self, _mock_srf):
        parent = self._task(parent_id=None)
        child = self._task(parent_id=parent.obj.id)

        SelfRefOrderLookup().transform([parent, child])

        self.assertEqual(parent.self_ref_fk_value, "")
        self.assertEqual(parent.self_ref_order, 0)
        self.assertEqual(child.self_ref_fk_value, parent.obj.id)
        self.assertEqual(child.self_ref_order, 1)

    @mock.patch(
        "morango.sync.stream.serialize.syncable_models.get_self_referential_fk",
        return_value="parent_id",
    )
    def test_transform__same_batch_deeper_chain(self, _mock_srf):
        root = self._task(parent_id=None)
        child = self._task(parent_id=root.obj.id)
        grandchild = self._task(parent_id=child.obj.id)

        SelfRefOrderLookup().transform([root, child, grandchild])

        self.assertEqual(root.self_ref_order, 0)
        self.assertEqual(child.self_ref_order, 1)
        self.assertEqual(grandchild.self_ref_order, 2)

    @mock.patch(
        "morango.sync.stream.serialize.syncable_models.get_self_referential_fk",
        return_value="parent_id",
    )
    def test_transform__previous_batches_feed_later_children(self, _mock_srf):
        root = self._task(parent_id=None)
        child = self._task(parent_id=root.obj.id)
        grandchild = self._task(parent_id=child.obj.id)
        lookup = SelfRefOrderLookup()

        lookup.transform([root])
        lookup.transform([child])
        lookup.transform([grandchild])

        self.assertEqual(root.self_ref_order, 0)
        self.assertEqual(child.self_ref_order, 1)
        self.assertEqual(grandchild.self_ref_order, 2)

    @mock.patch(
        "morango.sync.stream.serialize.syncable_models.get_self_referential_fk",
        return_value="parent_id",
    )
    def test_transform__parent_in_store(self, _mock_srf):
        parent_store = _make_store(_self_ref_order=3)
        child = self._task(parent_id=parent_store.id)

        SelfRefOrderLookup().transform([child])

        self.assertEqual(child.self_ref_fk_value, parent_store.id)
        self.assertEqual(child.self_ref_order, 4)

    @mock.patch(
        "morango.sync.stream.serialize.syncable_models.get_self_referential_fk",
        return_value="parent_id",
    )
    def test_transform__missing_parent(self, _mock_srf):
        missing_parent_id = uuid.uuid4().hex
        child = self._task(parent_id=missing_parent_id)

        SelfRefOrderLookup().transform([child])

        self.assertEqual(child.self_ref_fk_value, missing_parent_id)
        self.assertIsNone(child.self_ref_order)

    @mock.patch(
        "morango.sync.stream.serialize.syncable_models.get_self_referential_fk",
        return_value=None,
    )
    def test_transform__non_self_ref(self, _mock_srf):
        task = self._task()

        SelfRefOrderLookup().transform([task])

        self.assertIsNone(task.self_ref_fk_value)
        self.assertIsNone(task.self_ref_order)


class StoreUpdateTestCase(SimpleTestCase):
    def _build_sync_obj(self, **overrides):
        obj = mock.Mock()
        obj.id = uuid.uuid4().hex
        obj.serialize.return_value = {}
        obj.morango_model_name = "mymodel"
        obj.morango_profile = "profile"
        obj._morango_partition = "partition"
        obj._morango_source_id = "src"
        for key, value in overrides.items():
            setattr(obj, key, value)
        return obj

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

    @mock.patch(
        "morango.sync.stream.serialize.syncable_models.get_self_referential_fk",
        return_value=None,
    )
    def test_handle_store_update(self, _mock_srf):
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

    @mock.patch(
        "morango.sync.stream.serialize.syncable_models.get_self_referential_fk",
        return_value=None,
    )
    def test_handle_store_create__non_self_ref(self, _mock_srf):
        current_id = mock.Mock(id="inst_1", counter=1)
        update = StoreUpdate(current_id)

        obj = self._build_sync_obj()

        task = SerializeTask(mock.Mock(), obj)
        update._handle_store_create(task)

        self.assertIsNone(task.store._self_ref_order)

    @mock.patch(
        "morango.sync.stream.serialize.syncable_models.get_self_referential_fk",
        return_value="parent_id",
    )
    def test_handle_store_create__self_ref_no_parent(self, _mock_srf):
        current_id = mock.Mock(id="inst_1", counter=1)
        update = StoreUpdate(current_id)

        obj = self._build_sync_obj(parent_id=None)  # no parent

        task = SerializeTask(mock.Mock(), obj)
        task.set_self_ref_fk_value("")
        task.set_self_ref_order(0)
        update._handle_store_create(task)

        self.assertEqual(task.store._self_ref_fk, "")
        self.assertEqual(task.store._self_ref_order, 0)

    @mock.patch(
        "morango.sync.stream.serialize.syncable_models.get_self_referential_fk",
        return_value="parent_id",
    )
    def test_handle_store_update__self_ref_fk_unchanged(self, _mock_srf):
        current_id = mock.Mock(id="inst_1", counter=2)
        update = StoreUpdate(current_id)

        parent_id = uuid.uuid4().hex
        store = Store(
            serialized=json.dumps({}),
            dirty_bit=False,
            _self_ref_fk=parent_id,
            _self_ref_order=5,
        )
        obj = self._build_sync_obj(parent_id=parent_id)  # same FK — no change

        task = SerializeTask(mock.Mock(), obj)
        task.set_self_ref_fk_value(parent_id)
        task.set_self_ref_order(5)
        task.set_store(store)
        update._handle_store_update(task)

        self.assertEqual(task.store._self_ref_fk, parent_id)
        self.assertEqual(task.store._self_ref_order, 5)


def _make_store(**kwargs):
    defaults = dict(
        id=uuid.uuid4().hex,
        profile="facilitydata",
        serialized="{}",
        last_saved_instance=uuid.uuid4().hex,
        last_saved_counter=1,
        partition="partition",
        source_id=uuid.uuid4().hex,
        model_name="facility",
    )
    defaults.update(kwargs)
    return Store.objects.create(**defaults)


class StoreUpdateSelfRefOrderDbTestCase(TestCase):
    def setUp(self):
        self.current_id = mock.Mock(id="inst_1", counter=1)
        self.update = StoreUpdate(self.current_id)

    def _task(self, self_ref_fk_field, fk_value):
        obj = mock.Mock()
        obj.id = uuid.uuid4().hex
        obj.serialize.return_value = {}
        obj.morango_model_name = "facility"
        obj.morango_profile = "facilitydata"
        obj._morango_partition = "partition"
        obj._morango_source_id = "src"
        setattr(obj, self_ref_fk_field, fk_value)
        return SerializeTask(mock.Mock(), obj)

    @mock.patch(
        "morango.sync.stream.serialize.syncable_models.get_self_referential_fk",
        return_value="parent_id",
    )
    def test_handle_store_create__self_ref_with_parent(self, _mock_srf):
        parent_store = _make_store(_self_ref_order=3)

        task = self._task("parent_id", parent_store.id)
        task.set_self_ref_fk_value(parent_store.id)
        task.set_self_ref_order(4)
        self.update._handle_store_create(task)

        self.assertEqual(task.store._self_ref_fk, parent_store.id)
        self.assertEqual(task.store._self_ref_order, 4)

    @mock.patch(
        "morango.sync.stream.serialize.syncable_models.get_self_referential_fk",
        return_value="parent_id",
    )
    def test_handle_store_create__self_ref_parent_not_in_store(self, _mock_srf):
        missing_parent_id = uuid.uuid4().hex

        task = self._task("parent_id", missing_parent_id)
        task.set_self_ref_fk_value(missing_parent_id)
        task.set_self_ref_order(None)
        self.update._handle_store_create(task)

        self.assertEqual(task.store._self_ref_fk, missing_parent_id)
        self.assertIsNone(task.store._self_ref_order)

    @mock.patch(
        "morango.sync.stream.serialize.syncable_models.get_self_referential_fk",
        return_value="parent_id",
    )
    def test_handle_store_update__self_ref_fk_changed(self, _mock_srf):
        old_parent_store = _make_store(_self_ref_order=0)
        new_parent_store = _make_store(_self_ref_order=7)

        store = Store(
            serialized=json.dumps({}),
            dirty_bit=False,
            _self_ref_fk=old_parent_store.id,
            _self_ref_order=0,
        )
        obj = mock.Mock()
        obj.serialize.return_value = {}
        obj.parent_id = new_parent_store.id  # FK changed

        task = SerializeTask(mock.Mock(), obj)
        task.set_self_ref_fk_value(new_parent_store.id)
        task.set_self_ref_order(8)
        task.set_store(store)
        self.update._handle_store_update(task)

        self.assertEqual(task.store._self_ref_fk, new_parent_store.id)
        self.assertEqual(task.store._self_ref_order, 8)


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
