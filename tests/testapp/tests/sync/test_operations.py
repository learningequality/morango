import json
import uuid

import factory
from django.db import connection
from django.test import TestCase
from django.test import override_settings
from django.utils import timezone
from facility_profile.models import Facility
import mock

from ..helpers import create_buffer_and_store_dummy_data
from ..helpers import create_dummy_store_data
from morango.constants import transfer_statuses
from morango.models.core import Buffer
from morango.models.core import DatabaseIDModel
from morango.models.core import InstanceIDModel
from morango.models.core import RecordMaxCounter
from morango.models.core import RecordMaxCounterBuffer
from morango.models.core import Store
from morango.models.core import SyncSession
from morango.models.core import TransferSession
from morango.sync.backends.utils import load_backend
from morango.sync.context import LocalSessionContext
from morango.sync.controller import MorangoProfileController
from morango.sync.controller import SessionController
from morango.sync.operations import _dequeue_into_store
from morango.sync.operations import _queue_into_buffer
from morango.sync.operations import CleanupOperation
from morango.sync.operations import ReceiverDequeueOperation
from morango.sync.operations import ProducerDequeueOperation
from morango.sync.operations import ReceiverDeserializeOperation
from morango.sync.operations import InitializeOperation
from morango.sync.operations import ProducerQueueOperation
from morango.sync.operations import ReceiverQueueOperation
from morango.sync.syncsession import TransferClient

DBBackend = load_backend(connection).SQLWrapper()


class FacilityModelFactory(factory.DjangoModelFactory):
    class Meta:
        model = Facility

    name = factory.Sequence(lambda n: "Fac %d" % n)


@override_settings(MORANGO_SERIALIZE_BEFORE_QUEUING=False)
class QueueStoreIntoBufferTestCase(TestCase):
    def setUp(self):
        super(QueueStoreIntoBufferTestCase, self).setUp()
        self.data = create_dummy_store_data()
        self.transfer_session = self.data["sc"].current_transfer_session
        # self.transfer_session.filter = "abc123"
        self.context = mock.Mock(
            spec=LocalSessionContext,
            transfer_session=self.transfer_session,
            sync_session=self.transfer_session.sync_session,
            filter=self.transfer_session.get_filter(),
            is_push=self.transfer_session.push,
            is_server=False,
        )

    def assertRecordsBuffered(self, records):
        buffer_ids = Buffer.objects.values_list("model_uuid", flat=True)
        rmcb_ids = RecordMaxCounterBuffer.objects.values_list("model_uuid", flat=True)
        # ensure all store and buffer records are buffered
        for i in records:
            self.assertIn(i.id, buffer_ids)
            self.assertIn(i.id, rmcb_ids)

    def assertRecordsNotBuffered(self, records):
        buffer_ids = Buffer.objects.values_list("model_uuid", flat=True)
        rmcb_ids = RecordMaxCounterBuffer.objects.values_list("model_uuid", flat=True)
        # ensure all store and buffer records are buffered
        for i in records:
            self.assertNotIn(i.id, buffer_ids)
            self.assertNotIn(i.id, rmcb_ids)

    def test_all_fsics(self):
        fsics = {self.data["group1_id"].id: 1, self.data["group2_id"].id: 1}
        self.transfer_session.client_fsic = json.dumps(fsics)
        _queue_into_buffer(self.transfer_session)
        # ensure all store and buffer records are buffered
        self.assertRecordsBuffered(self.data["group1_c1"])
        self.assertRecordsBuffered(self.data["group1_c2"])
        self.assertRecordsBuffered(self.data["group2_c1"])

    def test_fsic_specific_id(self):
        fsics = {self.data["group2_id"].id: 1}
        self.transfer_session.client_fsic = json.dumps(fsics)
        _queue_into_buffer(self.transfer_session)
        # ensure only records modified with 2nd instance id are buffered
        self.assertRecordsNotBuffered(self.data["group1_c1"])
        self.assertRecordsNotBuffered(self.data["group1_c2"])
        self.assertRecordsBuffered(self.data["group2_c1"])

    def test_fsic_counters(self):
        counter = InstanceIDModel.objects.get(id=self.data["group1_id"].id).counter
        fsics = {self.data["group1_id"].id: counter - 1}
        self.transfer_session.client_fsic = json.dumps(fsics)
        fsics[self.data["group1_id"].id] = 0
        self.transfer_session.server_fsic = json.dumps(fsics)
        _queue_into_buffer(self.transfer_session)
        # ensure only records with updated 1st instance id are buffered
        self.assertRecordsBuffered(self.data["group1_c1"])
        self.assertRecordsBuffered(self.data["group1_c2"])
        self.assertRecordsNotBuffered(self.data["group2_c1"])

    def test_fsic_counters_too_high(self):
        fsics = {self.data["group1_id"].id: 100, self.data["group2_id"].id: 100}
        self.transfer_session.client_fsic = json.dumps(fsics)
        self.transfer_session.server_fsic = json.dumps(fsics)
        _queue_into_buffer(self.transfer_session)
        # ensure no records are buffered
        self.assertFalse(Buffer.objects.all())
        self.assertFalse(RecordMaxCounterBuffer.objects.all())

    def test_partition_filter_buffering(self):
        fsics = {self.data["group2_id"].id: 1}
        filter_prefixes = "{}:user:summary\n{}:user:interaction".format(
            self.data["user3"].id, self.data["user3"].id
        )
        self.transfer_session.filter = filter_prefixes
        self.transfer_session.client_fsic = json.dumps(fsics)
        _queue_into_buffer(self.transfer_session)
        # ensure records with different partition values are buffered
        self.assertRecordsNotBuffered([self.data["user2"]])
        self.assertRecordsBuffered(self.data["user3_sumlogs"])
        self.assertRecordsBuffered(self.data["user3_interlogs"])

    def test_partition_prefix_buffering(self):
        fsics = {self.data["group2_id"].id: 1}
        filter_prefixes = "{}".format(self.data["user2"].id)
        self.transfer_session.filter = filter_prefixes
        self.transfer_session.client_fsic = json.dumps(fsics)
        _queue_into_buffer(self.transfer_session)
        # ensure only records with user2 partition are buffered
        self.assertRecordsBuffered([self.data["user2"]])
        self.assertRecordsBuffered(self.data["user2_sumlogs"])
        self.assertRecordsBuffered(self.data["user2_interlogs"])
        self.assertRecordsNotBuffered([self.data["user3"]])

    def test_partition_and_fsic_buffering(self):
        filter_prefixes = "{}:user:summary".format(self.data["user1"].id)
        fsics = {self.data["group1_id"].id: 1}
        self.transfer_session.filter = filter_prefixes
        self.transfer_session.client_fsic = json.dumps(fsics)
        _queue_into_buffer(self.transfer_session)
        # ensure records updated with 1st instance id and summarylog partition are buffered
        self.assertRecordsBuffered(self.data["user1_sumlogs"])
        self.assertRecordsNotBuffered(self.data["user2_sumlogs"])
        self.assertRecordsNotBuffered(self.data["user3_sumlogs"])

    def test_valid_fsic_but_invalid_partition(self):
        filter_prefixes = "{}:user:summary".format(self.data["user1"].id)
        fsics = {self.data["group2_id"].id: 1}
        self.transfer_session.filter = filter_prefixes
        self.transfer_session.client_fsic = json.dumps(fsics)
        _queue_into_buffer(self.transfer_session)
        # ensure that record with valid fsic but invalid partition is not buffered
        self.assertRecordsNotBuffered([self.data["user4"]])

    def test_local_initialize_operation__server(self):
        self.transfer_session.active = False
        self.transfer_session.save()
        self.context.transfer_session = None
        id = uuid.uuid4().hex
        client_fsic = '{"abc123": 456}'
        records_total = 10
        self.context.request.data.get.side_effect = [
            id,
            records_total,
            client_fsic,
        ]
        self.context.filter = [self.transfer_session.get_filter()]
        operation = InitializeOperation()
        self.assertEqual(transfer_statuses.COMPLETED, operation.handle(self.context))
        self.context.update.assert_called_once()
        transfer_session = self.context.update.call_args_list[0][1].get("transfer_session")
        self.assertEqual(id, transfer_session.id)
        self.assertEqual(records_total, transfer_session.records_total)
        self.assertEqual(client_fsic, transfer_session.client_fsic)

    def test_local_initialize_operation__not_server(self):
        self.context.transfer_session = None
        self.context.request = None
        self.context.is_server = False
        self.context.filter = [self.transfer_session.get_filter()]
        operation = InitializeOperation()
        self.assertEqual(transfer_statuses.COMPLETED, operation.handle(self.context))
        self.context.update.assert_called_once()

    def test_local_initialize_operation__resume(self):
        self.context.transfer_session = None
        operation = InitializeOperation()
        self.assertEqual(transfer_statuses.COMPLETED, operation.handle(self.context))
        self.context.update.assert_called_once_with(transfer_session=self.transfer_session)

    def test_local_queue_operation(self):
        fsics = {self.data["group1_id"].id: 1, self.data["group2_id"].id: 1}
        self.transfer_session.client_fsic = json.dumps(fsics)

        self.assertEqual(0, self.transfer_session.records_total or 0)
        operation = ProducerQueueOperation()
        self.assertEqual(transfer_statuses.COMPLETED, operation.handle(self.context))
        self.assertNotEqual(0, self.transfer_session.records_total)

        # ensure all store and buffer records are buffered
        self.assertRecordsBuffered(self.data["group1_c1"])
        self.assertRecordsBuffered(self.data["group1_c2"])
        self.assertRecordsBuffered(self.data["group2_c1"])

    @mock.patch("morango.sync.operations._queue_into_buffer")
    def test_local_queue_operation__noop(self, mock_queue):
        fsics = {self.data["group1_id"].id: 1, self.data["group2_id"].id: 1}
        self.transfer_session.client_fsic = json.dumps(fsics)

        # as server, for push, operation should not queue into buffer
        self.context.is_push = True
        self.context.is_server = True

        operation = ReceiverQueueOperation()
        self.assertEqual(transfer_statuses.COMPLETED, operation.handle(self.context))
        mock_queue.assert_not_called()


@override_settings(MORANGO_DESERIALIZE_AFTER_DEQUEUING=False)
class DequeueBufferIntoStoreTestCase(TestCase):
    def setUp(self):
        super(DequeueBufferIntoStoreTestCase, self).setUp()
        self.data = {}
        DatabaseIDModel.objects.create()
        (self.current_id, _) = InstanceIDModel.get_or_create_current_instance()

        # create controllers for app/store/buffer operations
        conn = mock.Mock(spec="morango.sync.syncsession.NetworkSyncConnection")
        conn.server_info = dict(capabilities=[])
        self.data["mc"] = MorangoProfileController("facilitydata")
        self.data["sc"] = TransferClient(conn, "host", SessionController.build())
        session = SyncSession.objects.create(
            id=uuid.uuid4().hex, profile="", last_activity_timestamp=timezone.now()
        )
        self.data["sc"].current_transfer_session = TransferSession.objects.create(
            id=uuid.uuid4().hex,
            sync_session=session,
            push=True,
            last_activity_timestamp=timezone.now(),
        )
        self.transfer_session = self.data["sc"].current_transfer_session
        self.data.update(
            create_buffer_and_store_dummy_data(
                self.data["sc"].current_transfer_session.id
            )
        )
        self.context = mock.Mock(
            spec=LocalSessionContext,
            transfer_session=self.transfer_session,
            sync_session=self.transfer_session.sync_session,
            is_server=True,
        )

    def assert_store_records_tagged_with_last_session(self, store_ids):
        session_id = self.data["sc"].current_transfer_session.id
        for store_id in store_ids:
            assert Store.objects.get(id=store_id).last_transfer_session_id == session_id

    def assert_store_records_not_tagged_with_last_session(self, store_ids):
        session_id = self.data["sc"].current_transfer_session.id
        for store_id in store_ids:
            try:
                assert Store.objects.get(id=store_id).last_transfer_session_id != session_id
            except Store.DoesNotExist:
                pass

    def test_dequeuing_sets_last_session(self):
        store_ids = [self.data[key] for key in ["model2", "model3", "model4", "model5", "model7"]]
        self.assert_store_records_not_tagged_with_last_session(store_ids)
        _dequeue_into_store(self.data["sc"].current_transfer_session)
        # this one is a reverse fast forward, so it doesn't modify the store record and shouldn't be tagged
        self.assert_store_records_not_tagged_with_last_session([self.data["model1"]])
        self.assert_store_records_tagged_with_last_session(store_ids)
        tagged_actual = set(self.data["sc"].current_transfer_session.get_touched_record_ids_for_model("facility"))
        tagged_expected = set(store_ids)
        assert tagged_actual == tagged_expected

    def test_dequeuing_delete_rmcb_records(self):
        for i in self.data["model1_rmcb_ids"]:
            self.assertTrue(
                RecordMaxCounterBuffer.objects.filter(
                    instance_id=i, model_uuid=self.data["model1"]
                ).exists()
            )
        with connection.cursor() as cursor:
            DBBackend._dequeuing_delete_rmcb_records(cursor, self.transfer_session.id)
        for i in self.data["model1_rmcb_ids"]:
            self.assertFalse(
                RecordMaxCounterBuffer.objects.filter(
                    instance_id=i, model_uuid=self.data["model1"]
                ).exists()
            )
        # ensure other records were not deleted
        for i in self.data["model2_rmcb_ids"]:
            self.assertTrue(
                RecordMaxCounterBuffer.objects.filter(
                    instance_id=i, model_uuid=self.data["model2"]
                ).exists()
            )

    def test_dequeuing_delete_buffered_records(self):
        self.assertTrue(Buffer.objects.filter(model_uuid=self.data["model1"]).exists())
        with connection.cursor() as cursor:
            DBBackend._dequeuing_delete_buffered_records(
                cursor, self.transfer_session.id
            )
        self.assertFalse(Buffer.objects.filter(model_uuid=self.data["model1"]).exists())
        # ensure other records were not deleted
        self.assertTrue(Buffer.objects.filter(model_uuid=self.data["model2"]).exists())

    def test_dequeuing_merge_conflict_rmcb_greater_than_rmc(self):
        rmc = RecordMaxCounter.objects.get(
            instance_id=self.data["model2_rmc_ids"][0],
            store_model_id=self.data["model2"],
        )
        rmcb = RecordMaxCounterBuffer.objects.get(
            instance_id=self.data["model2_rmc_ids"][0], model_uuid=self.data["model2"]
        )
        self.assertNotEqual(rmc.counter, rmcb.counter)
        self.assertGreaterEqual(rmcb.counter, rmc.counter)
        with connection.cursor() as cursor:
            DBBackend._dequeuing_merge_conflict_rmcb(cursor, self.transfer_session.id)
        rmc = RecordMaxCounter.objects.get(
            instance_id=self.data["model2_rmc_ids"][0],
            store_model_id=self.data["model2"],
        )
        rmcb = RecordMaxCounterBuffer.objects.get(
            instance_id=self.data["model2_rmc_ids"][0], model_uuid=self.data["model2"]
        )
        self.assertEqual(rmc.counter, rmcb.counter)

    def test_dequeuing_merge_conflict_rmcb_less_than_rmc(self):
        rmc = RecordMaxCounter.objects.get(
            instance_id=self.data["model5_rmc_ids"][0],
            store_model_id=self.data["model5"],
        )
        rmcb = RecordMaxCounterBuffer.objects.get(
            instance_id=self.data["model5_rmc_ids"][0], model_uuid=self.data["model5"]
        )
        self.assertNotEqual(rmc.counter, rmcb.counter)
        self.assertGreaterEqual(rmc.counter, rmcb.counter)
        with connection.cursor() as cursor:
            DBBackend._dequeuing_merge_conflict_rmcb(cursor, self.transfer_session.id)
        rmc = RecordMaxCounter.objects.get(
            instance_id=self.data["model5_rmc_ids"][0],
            store_model_id=self.data["model5"],
        )
        rmcb = RecordMaxCounterBuffer.objects.get(
            instance_id=self.data["model5_rmc_ids"][0], model_uuid=self.data["model5"]
        )
        self.assertNotEqual(rmc.counter, rmcb.counter)
        self.assertGreaterEqual(rmc.counter, rmcb.counter)

    def test_dequeuing_merge_conflict_buffer_rmcb_greater_than_rmc(self):
        store = Store.objects.get(id=self.data["model2"])
        self.assertNotEqual(store.last_saved_instance, self.current_id.id)
        self.assertEqual(store.conflicting_serialized_data, "store")
        self.assertFalse(store.deleted)
        with connection.cursor() as cursor:
            current_id = InstanceIDModel.get_current_instance_and_increment_counter()
            DBBackend._dequeuing_merge_conflict_buffer(
                cursor, current_id, self.transfer_session.id
            )
        store = Store.objects.get(id=self.data["model2"])
        self.assertEqual(store.last_saved_instance, current_id.id)
        self.assertEqual(store.last_saved_counter, current_id.counter)
        self.assertEqual(store.conflicting_serialized_data, "buffer\nstore")
        self.assertTrue(store.deleted)

    def test_dequeuing_merge_conflict_buffer_rmcb_less_rmc(self):
        store = Store.objects.get(id=self.data["model5"])
        self.assertNotEqual(store.last_saved_instance, self.current_id.id)
        self.assertEqual(store.conflicting_serialized_data, "store")
        with connection.cursor() as cursor:
            current_id = InstanceIDModel.get_current_instance_and_increment_counter()
            DBBackend._dequeuing_merge_conflict_buffer(
                cursor, current_id, self.transfer_session.id
            )
        store = Store.objects.get(id=self.data["model5"])
        self.assertEqual(store.last_saved_instance, current_id.id)
        self.assertEqual(store.last_saved_counter, current_id.counter)
        self.assertEqual(store.conflicting_serialized_data, "buffer\nstore")

    def test_dequeuing_merge_conflict_hard_delete(self):
        store = Store.objects.get(id=self.data["model7"])
        self.assertEqual(store.serialized, "store")
        self.assertEqual(store.conflicting_serialized_data, "store")
        with connection.cursor() as cursor:
            current_id = InstanceIDModel.get_current_instance_and_increment_counter()
            DBBackend._dequeuing_merge_conflict_buffer(
                cursor, current_id, self.transfer_session.id
            )
        store.refresh_from_db()
        self.assertEqual(store.serialized, "")
        self.assertEqual(store.conflicting_serialized_data, "")

    def test_dequeuing_update_rmcs_last_saved_by(self):
        self.assertFalse(
            RecordMaxCounter.objects.filter(instance_id=self.current_id.id).exists()
        )
        with connection.cursor() as cursor:
            current_id = InstanceIDModel.get_current_instance_and_increment_counter()
            DBBackend._dequeuing_update_rmcs_last_saved_by(
                cursor, current_id, self.transfer_session.id
            )
        self.assertTrue(
            RecordMaxCounter.objects.filter(instance_id=current_id.id).exists()
        )

    def test_dequeuing_delete_mc_buffer(self):
        self.assertTrue(Buffer.objects.filter(model_uuid=self.data["model2"]).exists())
        with connection.cursor() as cursor:
            DBBackend._dequeuing_delete_mc_buffer(cursor, self.transfer_session.id)
        self.assertFalse(Buffer.objects.filter(model_uuid=self.data["model2"]).exists())
        # ensure other records were not deleted
        self.assertTrue(Buffer.objects.filter(model_uuid=self.data["model3"]).exists())

    def test_dequeuing_delete_mc_rmcb(self):
        self.assertTrue(
            RecordMaxCounterBuffer.objects.filter(
                model_uuid=self.data["model2"],
                instance_id=self.data["model2_rmcb_ids"][0],
            ).exists()
        )
        with connection.cursor() as cursor:
            DBBackend._dequeuing_delete_mc_rmcb(cursor, self.transfer_session.id)
        self.assertFalse(
            RecordMaxCounterBuffer.objects.filter(
                model_uuid=self.data["model2"],
                instance_id=self.data["model2_rmcb_ids"][0],
            ).exists()
        )
        self.assertTrue(
            RecordMaxCounterBuffer.objects.filter(
                model_uuid=self.data["model2"],
                instance_id=self.data["model2_rmcb_ids"][1],
            ).exists()
        )
        # ensure other records were not deleted
        self.assertTrue(
            RecordMaxCounterBuffer.objects.filter(
                model_uuid=self.data["model3"],
                instance_id=self.data["model3_rmcb_ids"][0],
            ).exists()
        )

    def test_dequeuing_insert_remaining_buffer(self):
        self.assertNotEqual(
            Store.objects.get(id=self.data["model3"]).serialized, "buffer"
        )
        self.assertFalse(Store.objects.filter(id=self.data["model4"]).exists())
        with connection.cursor() as cursor:
            DBBackend._dequeuing_insert_remaining_buffer(
                cursor, self.transfer_session.id
            )
        self.assertEqual(Store.objects.get(id=self.data["model3"]).serialized, "buffer")
        self.assertTrue(Store.objects.filter(id=self.data["model4"]).exists())

    def test_dequeuing_insert_remaining_rmcb(self):
        for i in self.data["model4_rmcb_ids"]:
            self.assertFalse(
                RecordMaxCounter.objects.filter(
                    instance_id=i, store_model_id=self.data["model4"]
                ).exists()
            )
        with connection.cursor() as cursor:
            DBBackend._dequeuing_insert_remaining_buffer(
                cursor, self.transfer_session.id
            )
            DBBackend._dequeuing_insert_remaining_rmcb(cursor, self.transfer_session.id)
        for i in self.data["model4_rmcb_ids"]:
            self.assertTrue(
                RecordMaxCounter.objects.filter(
                    instance_id=i, store_model_id=self.data["model4"]
                ).exists()
            )

    def test_dequeuing_delete_remaining_rmcb(self):
        self.assertTrue(
            RecordMaxCounterBuffer.objects.filter(
                transfer_session_id=self.transfer_session.id
            ).exists()
        )
        with connection.cursor() as cursor:
            DBBackend._dequeuing_delete_remaining_rmcb(cursor, self.transfer_session.id)
        self.assertFalse(
            RecordMaxCounterBuffer.objects.filter(
                transfer_session_id=self.transfer_session.id
            ).exists()
        )

    def test_dequeuing_delete_remaining_buffer(self):
        self.assertTrue(
            Buffer.objects.filter(transfer_session_id=self.transfer_session.id).exists()
        )
        with connection.cursor() as cursor:
            DBBackend._dequeuing_delete_remaining_buffer(
                cursor, self.transfer_session.id
            )
        self.assertFalse(
            Buffer.objects.filter(transfer_session_id=self.transfer_session.id).exists()
        )

    def test_dequeue_into_store(self):
        _dequeue_into_store(self.transfer_session)
        # ensure a record with different transfer session id is not affected
        self.assertTrue(
            Buffer.objects.filter(transfer_session_id=self.data["tfs_id"]).exists()
        )
        self.assertFalse(Store.objects.filter(id=self.data["model6"]).exists())
        self.assertFalse(
            RecordMaxCounter.objects.filter(
                store_model_id=self.data["model6"],
                instance_id__in=self.data["model6_rmcb_ids"],
            ).exists()
        )

        # ensure reverse fast forward records are not modified
        self.assertNotEqual(
            Store.objects.get(id=self.data["model1"]).serialized, "buffer"
        )
        self.assertFalse(
            RecordMaxCounter.objects.filter(
                instance_id=self.data["model1_rmcb_ids"][1]
            ).exists()
        )

        # ensure records with merge conflicts are modified
        self.assertEqual(
            Store.objects.get(id=self.data["model2"]).conflicting_serialized_data,
            "buffer\nstore",
        )  # conflicting field is overwritten
        self.assertEqual(
            Store.objects.get(id=self.data["model5"]).conflicting_serialized_data,
            "buffer\nstore",
        )
        self.assertTrue(
            RecordMaxCounter.objects.filter(
                instance_id=self.data["model2_rmcb_ids"][1]
            ).exists()
        )
        self.assertTrue(
            RecordMaxCounter.objects.filter(
                instance_id=self.data["model5_rmcb_ids"][1]
            ).exists()
        )
        self.assertEqual(
            Store.objects.get(id=self.data["model2"]).last_saved_instance,
            InstanceIDModel.get_or_create_current_instance()[0].id,
        )
        self.assertEqual(
            Store.objects.get(id=self.data["model5"]).last_saved_instance,
            InstanceIDModel.get_or_create_current_instance()[0].id,
        )

        # ensure fast forward records are modified
        self.assertEqual(
            Store.objects.get(id=self.data["model3"]).serialized, "buffer"
        )  # serialized field is overwritten
        self.assertTrue(
            RecordMaxCounter.objects.filter(
                instance_id=self.data["model3_rmcb_ids"][1]
            ).exists()
        )
        self.assertEqual(
            Store.objects.get(id=self.data["model3"]).last_saved_instance,
            self.data["model3_rmcb_ids"][1],
        )  # last_saved_by is updated
        self.assertEqual(
            RecordMaxCounter.objects.get(
                instance_id=self.data["model3_rmcb_ids"][0],
                store_model_id=self.data["model3"],
            ).counter,
            3,
        )

        # ensure all buffer and rmcb records were deleted for this transfer session id
        self.assertFalse(
            Buffer.objects.filter(transfer_session_id=self.transfer_session.id).exists()
        )
        self.assertFalse(
            RecordMaxCounterBuffer.objects.filter(
                transfer_session_id=self.transfer_session.id
            ).exists()
        )

    def test_local_dequeue_operation(self):
        self.transfer_session.records_transferred = 1
        self.context.filter = [self.transfer_session.filter]
        operation = ReceiverDequeueOperation()
        self.assertEqual(transfer_statuses.COMPLETED, operation.handle(self.context))
        self.assertFalse(
            Buffer.objects.filter(transfer_session_id=self.transfer_session.id).exists()
        )

    @mock.patch("morango.sync.operations._dequeue_into_store")
    def test_local_dequeue_operation__noop(self, mock_dequeue):
        self.context.is_server = False
        operation = ProducerDequeueOperation()
        self.assertEqual(transfer_statuses.COMPLETED, operation.handle(self.context))
        mock_dequeue.assert_not_called()

    @mock.patch("morango.sync.operations._dequeue_into_store")
    def test_local_dequeue_operation__noop__nothing_transferred(self, mock_dequeue):
        self.transfer_session.records_transferred = 0
        operation = ReceiverDequeueOperation()
        self.assertEqual(transfer_statuses.COMPLETED, operation.handle(self.context))
        mock_dequeue.assert_not_called()

    @mock.patch("morango.sync.operations.DatabaseMaxCounter.update_fsics")
    def test_local_deserialize_operation(self, mock_update_fsics):
        self.transfer_session.records_transferred = 1
        self.context.filter = [self.transfer_session.filter]
        operation = ReceiverDeserializeOperation()
        self.assertEqual(transfer_statuses.COMPLETED, operation.handle(self.context))
        mock_update_fsics.assert_called()

    def test_local_cleanup(self):
        self.context.is_server = False
        self.context.is_push = True
        operation = CleanupOperation()
        self.assertTrue(self.transfer_session.active)
        self.assertTrue(
            Buffer.objects.filter(transfer_session_id=self.transfer_session.id).exists()
        )
        self.assertTrue(
            RecordMaxCounterBuffer.objects.filter(transfer_session_id=self.transfer_session.id).exists()
        )
        self.assertEqual(transfer_statuses.COMPLETED, operation.handle(self.context))
        self.assertFalse(self.transfer_session.active)
        self.assertFalse(
            Buffer.objects.filter(transfer_session_id=self.transfer_session.id).exists()
        )
        self.assertFalse(
            RecordMaxCounterBuffer.objects.filter(transfer_session_id=self.transfer_session.id).exists()
        )
