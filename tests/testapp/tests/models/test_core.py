import uuid

import factory
import mock
from django.test import TestCase, override_settings
from django.utils import timezone
from facility_profile.models import Facility, MyUser

from morango.constants import transfer_stages, transfer_statuses
from morango.models.certificates import Filter
from morango.models.core import DatabaseMaxCounter, Store, SyncSession, TransferSession
from morango.sync.controller import MorangoProfileController

from ..helpers import RecordMaxCounterFactory, StoreFactory


class DatabaseMaxCounterFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = DatabaseMaxCounter


class BaseDatabaseMaxCounterTestCase(TestCase):
    def setUp(self):
        super(BaseDatabaseMaxCounterTestCase, self).setUp()
        self.instance_a = "a" * 32
        self.prefix_a = "AAA"
        self.user_prefix_a = "AAA:user_id:joe"

        self.instance_b = "b" * 32
        self.prefix_b = "BBB"
        self.user_prefix_b = "BBB:user_id:rick"
        self.user2_prefix_b = "BBB:user_id:emily"

        # instance A dmc
        DatabaseMaxCounterFactory(instance_id=self.instance_a, partition=self.prefix_a, counter=15)
        DatabaseMaxCounterFactory(
            instance_id=self.instance_a, partition=self.user_prefix_a, counter=20
        )
        DatabaseMaxCounterFactory(
            instance_id=self.instance_a, partition=self.user2_prefix_b, counter=17
        )

        # instance B dmc
        DatabaseMaxCounterFactory(
            instance_id=self.instance_b, partition=self.user_prefix_a, counter=10
        )
        DatabaseMaxCounterFactory(instance_id=self.instance_b, partition=self.prefix_b, counter=12)
        DatabaseMaxCounterFactory(
            instance_id=self.instance_b, partition=self.user_prefix_b, counter=5
        )
        DatabaseMaxCounterFactory(
            instance_id=self.instance_b, partition=self.user2_prefix_b, counter=2
        )


@override_settings(MORANGO_DISABLE_FSIC_V2_FORMAT=True)
class OldFilterMaxCounterTestCase(BaseDatabaseMaxCounterTestCase):
    def test_filter_not_in_dmc(self):
        fmcs = DatabaseMaxCounter.calculate_filter_specific_instance_counters(Filter("ZZZ"))
        self.assertEqual(fmcs, {})

    def test_instances_for_one_partition_but_not_other(self):
        fmcs = DatabaseMaxCounter.calculate_filter_specific_instance_counters(
            Filter(self.user_prefix_a + "\n" + self.user_prefix_b)
        )
        self.assertEqual(fmcs[self.instance_b], 10)

    def test_insufficient_instances_for_all_partitions(self):
        user_with_prefix = self.prefix_b + "user_id:richard"
        fmcs = DatabaseMaxCounter.calculate_filter_specific_instance_counters(
            Filter(self.prefix_a + "\n" + user_with_prefix)
        )
        self.assertFalse(fmcs)

    def test_single_partition_with_all_instances(self):
        fmcs = DatabaseMaxCounter.calculate_filter_specific_instance_counters(
            Filter(self.user_prefix_a)
        )
        self.assertEqual(fmcs[self.instance_a], 20)
        self.assertEqual(fmcs[self.instance_b], 10)

    def test_all_partitions_have_all_instances(self):
        fmcs = DatabaseMaxCounter.calculate_filter_specific_instance_counters(
            Filter(self.user_prefix_a + "\n" + self.user2_prefix_b)
        )
        self.assertEqual(fmcs[self.instance_a], 17)
        self.assertEqual(fmcs[self.instance_b], 10)

    def test_producer_vs_receiver_fsics(self):
        fsic_producer = DatabaseMaxCounter.calculate_filter_specific_instance_counters(
            Filter(self.user_prefix_a + "\n" + self.prefix_b), is_producer=True
        )
        self.assertEqual(fsic_producer.get(self.instance_a, 0), 20)
        self.assertEqual(fsic_producer.get(self.instance_b, 0), 12)
        fsic_receiver = DatabaseMaxCounter.calculate_filter_specific_instance_counters(
            Filter(self.user_prefix_a + "\n" + self.prefix_b), is_producer=False
        )
        self.assertEqual(fsic_receiver.get(self.instance_a, 0), 0)
        self.assertEqual(fsic_receiver.get(self.instance_b, 0), 10)


@override_settings(MORANGO_DISABLE_FSIC_V2_FORMAT=True)
class OldDatabaseMaxCounterUpdateCalculation(TestCase):
    def setUp(self):
        self.filter = "filter"

    def test_update_all_fsics(self):
        client_fsic = {"a" * 32: 2, "b" * 32: 2, "c" * 32: 2}
        server_fsic = {"a" * 32: 1, "b" * 32: 1, "c" * 32: 1}
        self.assertFalse(DatabaseMaxCounter.objects.filter(counter=2).exists())
        for instance_id, counter in server_fsic.items():
            DatabaseMaxCounter.objects.create(
                instance_id=instance_id, counter=counter, partition=self.filter
            )
        DatabaseMaxCounter.update_fsics(client_fsic, Filter(self.filter))
        self.assertTrue(DatabaseMaxCounter.objects.filter(counter=2).exists())
        self.assertFalse(DatabaseMaxCounter.objects.filter(counter=1).exists())

    def test_update_some_fsics(self):
        client_fsic = {"a" * 32: 1, "e" * 32: 2, "c" * 32: 1}
        server_fsic = {"a" * 32: 2, "b" * 32: 1, "c" * 32: 2}
        self.assertFalse(DatabaseMaxCounter.objects.filter(instance_id="e" * 32).exists())
        for instance_id, counter in server_fsic.items():
            DatabaseMaxCounter.objects.create(
                instance_id=instance_id, counter=counter, partition=self.filter
            )
        DatabaseMaxCounter.update_fsics(client_fsic, Filter(self.filter))
        self.assertTrue(DatabaseMaxCounter.objects.filter(instance_id="e" * 32).exists())

    def test_no_fsics_get_updated(self):
        client_fsic = {"a" * 32: 1, "b" * 32: 1, "c" * 32: 1}
        server_fsic = {"a" * 32: 2, "b" * 32: 2, "c" * 32: 2}
        self.assertFalse(DatabaseMaxCounter.objects.filter(counter=1).exists())
        for instance_id, counter in server_fsic.items():
            DatabaseMaxCounter.objects.create(
                instance_id=instance_id, counter=counter, partition=self.filter
            )
        DatabaseMaxCounter.update_fsics(client_fsic, Filter(self.filter))
        self.assertFalse(DatabaseMaxCounter.objects.filter(counter=1).exists())


class DatabaseMaxCounterTestCase(BaseDatabaseMaxCounterTestCase):
    def setUp(self):
        super(DatabaseMaxCounterTestCase, self).setUp()
        self.unknown_instance_id = uuid.uuid4().hex
        self.model1_id = uuid.uuid4().hex
        self.model2_id = uuid.uuid4().hex
        self.model3_id = uuid.uuid4().hex
        self.model4_id = uuid.uuid4().hex
        StoreFactory(
            id=self.model1_id,
            partition=self.prefix_a,
            serialized="store",
            last_saved_instance=self.instance_a,
            last_saved_counter=3,
        )
        StoreFactory(
            id=self.model2_id,
            partition=self.user_prefix_a,
            serialized="store",
            last_saved_instance=self.instance_a,
            last_saved_counter=3,
        )
        StoreFactory(
            id=self.model3_id,
            partition=self.prefix_b,
            serialized="store",
            last_saved_instance=self.instance_b,
            last_saved_counter=5,
        )
        StoreFactory(
            id=self.model4_id,
            partition=self.user_prefix_a,
            serialized="store",
            last_saved_instance=self.unknown_instance_id,
            last_saved_counter=7,
        )

    def setUpRecordMaxCounters(self):
        RecordMaxCounterFactory(
            instance_id=self.instance_a,
            store_model_id=self.model1_id,
            counter=101,
        )
        RecordMaxCounterFactory(
            instance_id=self.instance_a,
            store_model_id=self.model2_id,
            counter=102,
        )
        RecordMaxCounterFactory(
            instance_id=self.instance_b,
            store_model_id=self.model3_id,
            counter=103,
        )
        RecordMaxCounterFactory(
            instance_id=self.unknown_instance_id,
            store_model_id=self.model4_id,
            counter=104,
        )

    def test_get_instance_counters_for_partitions__producer(self):
        counters = DatabaseMaxCounter.get_instance_counters_for_partitions(
            [self.user_prefix_a], is_producer=True
        )
        self.assertEqual(1, len(counters))
        partition_counters = counters.get(self.user_prefix_a)
        self.assertEqual(1, len(partition_counters))
        self.assertEqual(20, partition_counters.get(self.instance_a))

    @override_settings(MORANGO_DISABLE_FSIC_REDUCTION=True)
    def test_get_instance_counters_for_partitions__producer__no_reduction(self):
        counters = DatabaseMaxCounter.get_instance_counters_for_partitions(
            [self.user_prefix_a], is_producer=True
        )
        self.assertEqual(1, len(counters))
        partition_counters = counters.get(self.user_prefix_a)
        self.assertEqual(2, len(partition_counters))
        self.assertEqual(20, partition_counters.get(self.instance_a))
        self.assertEqual(10, partition_counters.get(self.instance_b))

    def test_get_instance_counters_for_partitions__receiver(self):
        self.setUpRecordMaxCounters()
        counters = DatabaseMaxCounter.get_instance_counters_for_partitions([self.user_prefix_a])
        self.assertEqual(1, len(counters))
        partition_counters = counters.get(self.user_prefix_a)
        self.assertEqual(1, len(partition_counters))
        self.assertEqual(20, partition_counters.get(self.instance_a))

    @override_settings(MORANGO_DISABLE_FSIC_REDUCTION=True)
    def test_get_instance_counters_for_partitions__receiver__no_reduction(self):
        self.setUpRecordMaxCounters()
        counters = DatabaseMaxCounter.get_instance_counters_for_partitions([self.user_prefix_a])
        self.assertEqual(1, len(counters))
        partition_counters = counters.get(self.user_prefix_a)
        self.assertEqual(2, len(partition_counters))
        self.assertEqual(20, partition_counters.get(self.instance_a))
        self.assertEqual(10, partition_counters.get(self.instance_b))


class TransferSessionTestCase(TestCase):
    def setUp(self):
        super(TransferSessionTestCase, self).setUp()
        self.sync_session = SyncSession.objects.create(
            id=uuid.uuid4().hex,
            profile="facilitydata",
            last_activity_timestamp=timezone.now(),
        )
        self.instance = TransferSession.objects.create(
            id=uuid.uuid4().hex,
            sync_session=self.sync_session,
            push=True,
            last_activity_timestamp=timezone.now(),
        )

    def test_update_state(self):
        self.assertIsNone(self.instance.transfer_stage)
        self.assertIsNone(self.instance.transfer_stage_status)
        previous_activity = self.instance.last_activity_timestamp
        previous_sync_activity = self.sync_session.last_activity_timestamp

        self.instance.update_state(
            stage=transfer_stages.QUEUING, stage_status=transfer_statuses.PENDING
        )

        self.assertEqual(transfer_stages.QUEUING, self.instance.transfer_stage)
        self.assertEqual(transfer_statuses.PENDING, self.instance.transfer_stage_status)
        self.assertLess(previous_activity, self.instance.last_activity_timestamp)
        self.assertLess(previous_sync_activity, self.sync_session.last_activity_timestamp)

    def test_update_state__only_stage(self):
        self.assertIsNone(self.instance.transfer_stage)
        self.assertIsNone(self.instance.transfer_stage_status)
        previous_activity = self.instance.last_activity_timestamp
        previous_sync_activity = self.sync_session.last_activity_timestamp

        self.instance.update_state(stage=transfer_stages.QUEUING)

        self.assertEqual(transfer_stages.QUEUING, self.instance.transfer_stage)
        self.assertIsNone(self.instance.transfer_stage_status)
        self.assertLess(previous_activity, self.instance.last_activity_timestamp)
        self.assertLess(previous_sync_activity, self.sync_session.last_activity_timestamp)

    def test_update_state__only_status(self):
        self.assertIsNone(self.instance.transfer_stage)
        self.assertIsNone(self.instance.transfer_stage_status)
        previous_activity = self.instance.last_activity_timestamp
        previous_sync_activity = self.sync_session.last_activity_timestamp

        self.instance.update_state(stage_status=transfer_statuses.PENDING)

        self.assertIsNone(self.instance.transfer_stage)
        self.assertEqual(transfer_statuses.PENDING, self.instance.transfer_stage_status)
        self.assertLess(previous_activity, self.instance.last_activity_timestamp)
        self.assertLess(previous_sync_activity, self.sync_session.last_activity_timestamp)

    def test_update_state__none(self):
        self.assertIsNone(self.instance.transfer_stage)
        self.assertIsNone(self.instance.transfer_stage_status)
        previous_activity = self.instance.last_activity_timestamp
        previous_sync_activity = self.sync_session.last_activity_timestamp

        self.instance.update_state()

        self.assertIsNone(self.instance.transfer_stage)
        self.assertIsNone(self.instance.transfer_stage_status)
        self.assertEqual(previous_activity, self.instance.last_activity_timestamp)
        self.assertEqual(previous_sync_activity, self.sync_session.last_activity_timestamp)


class TransferSessionAndStoreTestCase(TestCase):
    def setUp(self):
        super(TransferSessionAndStoreTestCase, self).setUp()
        self.sync_session = SyncSession.objects.create(
            id=uuid.uuid4().hex,
            profile="facilitydata",
            last_activity_timestamp=timezone.now(),
        )
        self.instance = TransferSession.objects.create(
            id=uuid.uuid4().hex,
            sync_session=self.sync_session,
            push=True,
            last_activity_timestamp=timezone.now(),
        )
        self.controller = MorangoProfileController(MyUser.morango_profile)
        self.user = MyUser(username="tester")
        self.user.save()
        self.controller.serialize_into_store()
        stores = Store.objects.filter(model_name=MyUser.morango_model_name)
        self.assertEqual(1, stores.count())
        stores.update(last_transfer_session_id=self.instance.id)

    def test_get_touched_record_ids_for_model__instance(self):
        self.assertEqual(
            [self.user.id],
            list(self.instance.get_touched_record_ids_for_model(self.user)),
        )

    def test_get_touched_record_ids_for_model__class(self):
        self.assertEqual(
            [self.user.id], list(self.instance.get_touched_record_ids_for_model(MyUser))
        )

    def test_get_touched_record_ids_for_model__string(self):
        self.assertEqual(
            [self.user.id],
            list(self.instance.get_touched_record_ids_for_model(MyUser.morango_model_name)),
        )


class SyncableModelTestCase(TestCase):
    @mock.patch("morango.models.core.UUIDModelMixin.clean_fields")
    def test_clean_fields(self, mock_super_clean_fields):
        f = Facility(name="test")
        sync_filter = Filter("test")
        f.clean_fields(exclude=["test1"], sync_filter=sync_filter)
        mock_super_clean_fields.assert_called_once_with(exclude=["test1"])

    @mock.patch("morango.models.core.SyncableModel.clean_fields")
    def test_cached_clean_fields(self, mock_clean_fields):
        f = Facility(name="test")
        sync_filter = Filter("test")
        f.cached_clean_fields({}, exclude=["test1"], sync_filter=sync_filter)
        mock_clean_fields.assert_called_once_with(
            exclude=["test1", "parent"], sync_filter=sync_filter
        )

    @mock.patch("morango.models.core.SyncableModel.clean_fields")
    def test_deferred_clean_fields(self, mock_clean_fields):
        f = Facility(name="test")
        sync_filter = Filter("test")
        f.deferred_clean_fields(exclude=["test1"], sync_filter=sync_filter)
        mock_clean_fields.assert_called_once_with(exclude=["test1"], sync_filter=sync_filter)


class StoreDeserializationErrorTestCase(TestCase):
    """Tests for Store.set_deserialization_error() and Store.unset_deserialization_error()"""

    def test_set_deserialization_error_with_validation_error(self):
        """Test that set_deserialization_error correctly captures ValidationError."""
        from django.core.exceptions import ValidationError

        store = StoreFactory(
            id=uuid.uuid4().hex,
            partition="test",
            serialized="{}",
            last_saved_instance="a" * 32,
            last_saved_counter=1,
        )
        exc = ValidationError("Invalid data")
        store.set_deserialization_error(exc)

        # ValidationError str() returns a list representation like "['Invalid data']"
        self.assertIn("Invalid data", store.deserialization_error)
        self.assertEqual(
            store.deserialization_exception,
            "django.core.exceptions.ValidationError",
        )

    def test_set_deserialization_error_with_integrity_error(self):
        """Test that set_deserialization_error correctly captures IntegrityError."""
        from django.db.utils import IntegrityError

        store = StoreFactory(
            id=uuid.uuid4().hex,
            partition="test",
            serialized="{}",
            last_saved_instance="a" * 32,
            last_saved_counter=1,
        )
        exc = IntegrityError("UNIQUE constraint failed")
        store.set_deserialization_error(exc)

        self.assertEqual(store.deserialization_error, "UNIQUE constraint failed")
        self.assertEqual(
            store.deserialization_exception,
            "django.db.utils.IntegrityError",
        )

    def test_set_deserialization_error_with_value_error(self):
        """Test that set_deserialization_error correctly captures ValueError."""
        store = StoreFactory(
            id=uuid.uuid4().hex,
            partition="test",
            serialized="{}",
            last_saved_instance="a" * 32,
            last_saved_counter=1,
        )
        exc = ValueError("Invalid value")
        store.set_deserialization_error(exc)

        self.assertEqual(store.deserialization_error, "Invalid value")
        self.assertEqual(
            store.deserialization_exception,
            "builtins.ValueError",
        )

    def test_unset_deserialization_error(self):
        """Test that unset_deserialization_error clears both fields."""
        store = StoreFactory(
            id=uuid.uuid4().hex,
            partition="test",
            serialized="{}",
            last_saved_instance="a" * 32,
            last_saved_counter=1,
            deserialization_error="some error",
            deserialization_exception="module.Error",
        )

        store.unset_deserialization_error()

        self.assertIsNone(store.deserialization_error)
        self.assertIsNone(store.deserialization_exception)

    def test_set_and_unset_deserialization_error(self):
        """Test that set_deserialization_error followed by unset_deserialization_error works correctly."""
        from django.core.exceptions import ValidationError

        store = StoreFactory(
            id=uuid.uuid4().hex,
            partition="test",
            serialized="{}",
            last_saved_instance="a" * 32,
            last_saved_counter=1,
        )

        exc = ValidationError("Test error")
        store.set_deserialization_error(exc)
        # ValidationError str() returns a list representation like "['Test error']"
        self.assertIn("Test error", store.deserialization_error)
        self.assertEqual(
            store.deserialization_exception,
            "django.core.exceptions.ValidationError",
        )

        store.unset_deserialization_error()
        self.assertIsNone(store.deserialization_error)
        self.assertIsNone(store.deserialization_exception)
