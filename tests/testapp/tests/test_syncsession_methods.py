import json
import uuid

import factory
from django.conf import settings
from django.db import connection
from django.test import TestCase
from django.utils import timezone
from facility_profile.models import Facility
from morango.controller import MorangoProfileController
from morango.models import (Buffer, DatabaseIDModel, InstanceIDModel,
                            RecordMaxCounter, RecordMaxCounterBuffer, Store,
                            SyncSession, TransferSession)
from morango.syncsession import SyncClient
from morango.utils.sync_utils import _dequeue_into_store, _queue_into_buffer
from morango.utils.backends.utils import load_backend

from .helpers import (create_buffer_and_store_dummy_data,
                      create_dummy_store_data)

DBBackend = load_backend(connection).SQLWrapper()

class FacilityModelFactory(factory.DjangoModelFactory):

    class Meta:
        model = Facility

    name = factory.Sequence(lambda n: "Fac %d" % n)


class QueueStoreIntoBufferTestCase(TestCase):

    def setUp(self):
        settings.MORANGO_SERIALIZE_BEFORE_QUEUING = False
        self.data = create_dummy_store_data()

    def assertRecordsBuffered(self, records):
        buffer_ids = Buffer.objects.values_list('model_uuid', flat=True)
        rmcb_ids = RecordMaxCounterBuffer.objects.values_list('model_uuid', flat=True)
        # ensure all store and buffer records are buffered
        for i in records:
            self.assertIn(i.id, buffer_ids)
            self.assertIn(i.id, rmcb_ids)

    def assertRecordsNotBuffered(self, records):
        buffer_ids = Buffer.objects.values_list('model_uuid', flat=True)
        rmcb_ids = RecordMaxCounterBuffer.objects.values_list('model_uuid', flat=True)
        # ensure all store and buffer records are buffered
        for i in records:
            self.assertNotIn(i.id, buffer_ids)
            self.assertNotIn(i.id, rmcb_ids)

    def test_all_fsics(self):
        fsics = {self.data['group1_id'].id: 1, self.data['group2_id'].id: 1}
        self.data['sc'].current_transfer_session.client_fsic = json.dumps(fsics)
        _queue_into_buffer(self.data['sc'].current_transfer_session)
        # ensure all store and buffer records are buffered
        self.assertRecordsBuffered(self.data['group1_c1'])
        self.assertRecordsBuffered(self.data['group1_c2'])
        self.assertRecordsBuffered(self.data['group2_c1'])

    def test_fsic_specific_id(self):
        fsics = {self.data['group2_id'].id: 1}
        self.data['sc'].current_transfer_session.client_fsic = json.dumps(fsics)
        _queue_into_buffer(self.data['sc'].current_transfer_session)
        # ensure only records modified with 2nd instance id are buffered
        self.assertRecordsNotBuffered(self.data['group1_c1'])
        self.assertRecordsNotBuffered(self.data['group1_c2'])
        self.assertRecordsBuffered(self.data['group2_c1'])

    def test_fsic_counters(self):
        counter = InstanceIDModel.objects.get(id=self.data['group1_id'].id).counter
        fsics = {self.data['group1_id'].id: counter - 1}
        self.data['sc'].current_transfer_session.client_fsic = json.dumps(fsics)
        fsics[self.data['group1_id'].id] = 0
        self.data['sc'].current_transfer_session.server_fsic = json.dumps(fsics)
        _queue_into_buffer(self.data['sc'].current_transfer_session)
        # ensure only records with updated 1st instance id are buffered
        self.assertRecordsBuffered(self.data['group1_c1'])
        self.assertRecordsBuffered(self.data['group1_c2'])
        self.assertRecordsNotBuffered(self.data['group2_c1'])

    def test_fsic_counters_too_high(self):
        fsics = {self.data['group1_id'].id: 100, self.data['group2_id'].id: 100}
        self.data['sc'].current_transfer_session.client_fsic = json.dumps(fsics)
        self.data['sc'].current_transfer_session.server_fsic = json.dumps(fsics)
        _queue_into_buffer(self.data['sc'].current_transfer_session)
        # ensure no records are buffered
        self.assertFalse(Buffer.objects.all())
        self.assertFalse(RecordMaxCounterBuffer.objects.all())

    def test_partition_filter_buffering(self):
        fsics = {self.data['group2_id'].id: 1}
        filter_prefixes = '{}:user:summary\n{}:user:interaction'.format(self.data['user3'].id, self.data['user3'].id)
        self.data['sc'].current_transfer_session.filter = filter_prefixes
        self.data['sc'].current_transfer_session.client_fsic = json.dumps(fsics)
        _queue_into_buffer(self.data['sc'].current_transfer_session)
        # ensure records with different partition values are buffered
        self.assertRecordsNotBuffered([self.data['user2']])
        self.assertRecordsBuffered(self.data['user3_sumlogs'])
        self.assertRecordsBuffered(self.data['user3_interlogs'])

    def test_partition_prefix_buffering(self):
        fsics = {self.data['group2_id'].id: 1}
        filter_prefixes = '{}'.format(self.data['user2'].id)
        self.data['sc'].current_transfer_session.filter = filter_prefixes
        self.data['sc'].current_transfer_session.client_fsic = json.dumps(fsics)
        _queue_into_buffer(self.data['sc'].current_transfer_session)
        # ensure only records with user2 partition are buffered
        self.assertRecordsBuffered([self.data['user2']])
        self.assertRecordsBuffered(self.data['user2_sumlogs'])
        self.assertRecordsBuffered(self.data['user2_interlogs'])
        self.assertRecordsNotBuffered([self.data['user3']])

    def test_partition_and_fsic_buffering(self):
        filter_prefixes = '{}:user:summary'.format(self.data['user1'].id)
        fsics = {self.data['group1_id'].id: 1}
        self.data['sc'].current_transfer_session.filter = filter_prefixes
        self.data['sc'].current_transfer_session.client_fsic = json.dumps(fsics)
        _queue_into_buffer(self.data['sc'].current_transfer_session)
        # ensure records updated with 1st instance id and summarylog partition are buffered
        self.assertRecordsBuffered(self.data['user1_sumlogs'])
        self.assertRecordsNotBuffered(self.data['user2_sumlogs'])
        self.assertRecordsNotBuffered(self.data['user3_sumlogs'])

    def test_valid_fsic_but_invalid_partition(self):
        filter_prefixes = '{}:user:summary'.format(self.data['user1'].id)
        fsics = {self.data['group2_id'].id: 1}
        self.data['sc'].current_transfer_session.filter = filter_prefixes
        self.data['sc'].current_transfer_session.client_fsic = json.dumps(fsics)
        _queue_into_buffer(self.data['sc'].current_transfer_session)
        # ensure that record with valid fsic but invalid partition is not buffered
        self.assertRecordsNotBuffered([self.data['user4']])


class BufferIntoStoreTestCase(TestCase):

    def setUp(self):
        settings.MORANGO_DESERIALIZE_AFTER_DEQUEUING = False
        self.data = {}
        DatabaseIDModel.objects.create()
        (self.current_id, _) = InstanceIDModel.get_or_create_current_instance()

        # create controllers for app/store/buffer operations
        self.data['mc'] = MorangoProfileController('facilitydata')
        self.data['sc'] = SyncClient(None, 'host')
        session = SyncSession.objects.create(id=uuid.uuid4().hex, profile="", last_activity_timestamp=timezone.now())
        self.data['sc'].current_transfer_session = TransferSession.objects.create(id=uuid.uuid4().hex, sync_session=session, push=True, last_activity_timestamp=timezone.now())
        self.data.update(create_buffer_and_store_dummy_data(self.data['sc'].current_transfer_session.id))

    def test_dequeuing_delete_rmcb_records(self):
        for i in self.data['model1_rmcb_ids']:
            self.assertTrue(RecordMaxCounterBuffer.objects.filter(instance_id=i, model_uuid=self.data['model1']).exists())
        with connection.cursor() as cursor:
            DBBackend._dequeuing_delete_rmcb_records(cursor, self.data['sc'].current_transfer_session.id)
        for i in self.data['model1_rmcb_ids']:
            self.assertFalse(RecordMaxCounterBuffer.objects.filter(instance_id=i, model_uuid=self.data['model1']).exists())
        # ensure other records were not deleted
        for i in self.data['model2_rmcb_ids']:
            self.assertTrue(RecordMaxCounterBuffer.objects.filter(instance_id=i, model_uuid=self.data['model2']).exists())

    def test_dequeuing_delete_buffered_records(self):
        self.assertTrue(Buffer.objects.filter(model_uuid=self.data['model1']).exists())
        with connection.cursor() as cursor:
            DBBackend._dequeuing_delete_buffered_records(cursor, self.data['sc'].current_transfer_session.id)
        self.assertFalse(Buffer.objects.filter(model_uuid=self.data['model1']).exists())
        # ensure other records were not deleted
        self.assertTrue(Buffer.objects.filter(model_uuid=self.data['model2']).exists())

    def test_dequeuing_merge_conflict_rmcb_greater_than_rmc(self):
        rmc = RecordMaxCounter.objects.get(instance_id=self.data['model2_rmc_ids'][0], store_model_id=self.data['model2'])
        rmcb = RecordMaxCounterBuffer.objects.get(instance_id=self.data['model2_rmc_ids'][0], model_uuid=self.data['model2'])
        self.assertNotEqual(rmc.counter, rmcb.counter)
        self.assertGreaterEqual(rmcb.counter, rmc.counter)
        with connection.cursor() as cursor:
            DBBackend._dequeuing_merge_conflict_rmcb(cursor, self.data['sc'].current_transfer_session.id)
        rmc = RecordMaxCounter.objects.get(instance_id=self.data['model2_rmc_ids'][0], store_model_id=self.data['model2'])
        rmcb = RecordMaxCounterBuffer.objects.get(instance_id=self.data['model2_rmc_ids'][0], model_uuid=self.data['model2'])
        self.assertEqual(rmc.counter, rmcb.counter)

    def test_dequeuing_merge_conflict_rmcb_less_than_rmc(self):
        rmc = RecordMaxCounter.objects.get(instance_id=self.data['model5_rmc_ids'][0], store_model_id=self.data['model5'])
        rmcb = RecordMaxCounterBuffer.objects.get(instance_id=self.data['model5_rmc_ids'][0], model_uuid=self.data['model5'])
        self.assertNotEqual(rmc.counter, rmcb.counter)
        self.assertGreaterEqual(rmc.counter, rmcb.counter)
        with connection.cursor() as cursor:
            DBBackend._dequeuing_merge_conflict_rmcb(cursor, self.data['sc'].current_transfer_session.id)
        rmc = RecordMaxCounter.objects.get(instance_id=self.data['model5_rmc_ids'][0], store_model_id=self.data['model5'])
        rmcb = RecordMaxCounterBuffer.objects.get(instance_id=self.data['model5_rmc_ids'][0], model_uuid=self.data['model5'])
        self.assertNotEqual(rmc.counter, rmcb.counter)
        self.assertGreaterEqual(rmc.counter, rmcb.counter)

    def test_dequeuing_merge_conflict_buffer_rmcb_greater_than_rmc(self):
        store = Store.objects.get(id=self.data['model2'])
        self.assertNotEqual(store.last_saved_instance, self.current_id.id)
        self.assertEqual(store.conflicting_serialized_data, "store")
        self.assertFalse(store.deleted)
        with connection.cursor() as cursor:
            current_id = InstanceIDModel.get_current_instance_and_increment_counter()
            DBBackend._dequeuing_merge_conflict_buffer(cursor, current_id, self.data['sc'].current_transfer_session.id)
        store = Store.objects.get(id=self.data['model2'])
        self.assertEqual(store.last_saved_instance, current_id.id)
        self.assertEqual(store.last_saved_counter, current_id.counter)
        self.assertEqual(store.conflicting_serialized_data, "buffer\nstore")
        self.assertTrue(store.deleted)

    def test_dequeuing_merge_conflict_buffer_rmcb_less_rmc(self):
        store = Store.objects.get(id=self.data['model5'])
        self.assertNotEqual(store.last_saved_instance, self.current_id.id)
        self.assertEqual(store.conflicting_serialized_data, "store")
        with connection.cursor() as cursor:
            current_id = InstanceIDModel.get_current_instance_and_increment_counter()
            DBBackend._dequeuing_merge_conflict_buffer(cursor, current_id, self.data['sc'].current_transfer_session.id)
        store = Store.objects.get(id=self.data['model5'])
        self.assertEqual(store.last_saved_instance, current_id.id)
        self.assertEqual(store.last_saved_counter, current_id.counter)
        self.assertEqual(store.conflicting_serialized_data, "buffer\nstore")

    def test_dequeuing_merge_conflict_hard_delete(self):
        store = Store.objects.get(id=self.data['model7'])
        self.assertEqual(store.serialized, "store")
        self.assertEqual(store.conflicting_serialized_data, "store")
        with connection.cursor() as cursor:
            current_id = InstanceIDModel.get_current_instance_and_increment_counter()
            DBBackend._dequeuing_merge_conflict_buffer(cursor, current_id, self.data['sc'].current_transfer_session.id)
        store.refresh_from_db()
        self.assertEqual(store.serialized, "")
        self.assertEqual(store.conflicting_serialized_data, "")

    def test_dequeuing_update_rmcs_last_saved_by(self):
        self.assertFalse(RecordMaxCounter.objects.filter(instance_id=self.current_id.id).exists())
        with connection.cursor() as cursor:
            current_id = InstanceIDModel.get_current_instance_and_increment_counter()
            DBBackend._dequeuing_update_rmcs_last_saved_by(cursor, current_id, self.data['sc'].current_transfer_session.id)
        self.assertTrue(RecordMaxCounter.objects.filter(instance_id=current_id.id).exists())

    def test_dequeuing_delete_mc_buffer(self):
        self.assertTrue(Buffer.objects.filter(model_uuid=self.data['model2']).exists())
        with connection.cursor() as cursor:
            DBBackend._dequeuing_delete_mc_buffer(cursor, self.data['sc'].current_transfer_session.id)
        self.assertFalse(Buffer.objects.filter(model_uuid=self.data['model2']).exists())
        # ensure other records were not deleted
        self.assertTrue(Buffer.objects.filter(model_uuid=self.data['model3']).exists())

    def test_dequeuing_delete_mc_rmcb(self):
        self.assertTrue(RecordMaxCounterBuffer.objects.filter(model_uuid=self.data['model2'], instance_id=self.data['model2_rmcb_ids'][0]).exists())
        with connection.cursor() as cursor:
            DBBackend._dequeuing_delete_mc_rmcb(cursor, self.data['sc'].current_transfer_session.id)
        self.assertFalse(RecordMaxCounterBuffer.objects.filter(model_uuid=self.data['model2'], instance_id=self.data['model2_rmcb_ids'][0]).exists())
        self.assertTrue(RecordMaxCounterBuffer.objects.filter(model_uuid=self.data['model2'], instance_id=self.data['model2_rmcb_ids'][1]).exists())
        # ensure other records were not deleted
        self.assertTrue(RecordMaxCounterBuffer.objects.filter(model_uuid=self.data['model3'], instance_id=self.data['model3_rmcb_ids'][0]).exists())

    def test_dequeuing_insert_remaining_buffer(self):
        self.assertNotEqual(Store.objects.get(id=self.data['model3']).serialized, "buffer")
        self.assertFalse(Store.objects.filter(id=self.data['model4']).exists())
        with connection.cursor() as cursor:
            DBBackend._dequeuing_insert_remaining_buffer(cursor, self.data['sc'].current_transfer_session.id)
        self.assertEqual(Store.objects.get(id=self.data['model3']).serialized, "buffer")
        self.assertTrue(Store.objects.filter(id=self.data['model4']).exists())

    def test_dequeuing_insert_remaining_rmcb(self):
        for i in self.data['model4_rmcb_ids']:
            self.assertFalse(RecordMaxCounter.objects.filter(instance_id=i, store_model_id=self.data['model4']).exists())
        with connection.cursor() as cursor:
            DBBackend._dequeuing_insert_remaining_buffer(cursor, self.data['sc'].current_transfer_session.id)
            DBBackend._dequeuing_insert_remaining_rmcb(cursor, self.data['sc'].current_transfer_session.id)
        for i in self.data['model4_rmcb_ids']:
            self.assertTrue(RecordMaxCounter.objects.filter(instance_id=i, store_model_id=self.data['model4']).exists())

    def test_dequeuing_delete_remaining_rmcb(self):
        self.assertTrue(RecordMaxCounterBuffer.objects.filter(transfer_session_id=self.data['sc'].current_transfer_session.id).exists())
        with connection.cursor() as cursor:
            DBBackend._dequeuing_delete_remaining_rmcb(cursor, self.data['sc'].current_transfer_session.id)
        self.assertFalse(RecordMaxCounterBuffer.objects.filter(transfer_session_id=self.data['sc'].current_transfer_session.id).exists())

    def test_dequeuing_delete_remaining_buffer(self):
        self.assertTrue(Buffer.objects.filter(transfer_session_id=self.data['sc'].current_transfer_session.id).exists())
        with connection.cursor() as cursor:
            DBBackend._dequeuing_delete_remaining_buffer(cursor, self.data['sc'].current_transfer_session.id)
        self.assertFalse(Buffer.objects.filter(transfer_session_id=self.data['sc'].current_transfer_session.id).exists())

    def test_dequeue_into_store(self):
        _dequeue_into_store(self.data['sc'].current_transfer_session)
        # ensure a record with different transfer session id is not affected
        self.assertTrue(Buffer.objects.filter(transfer_session_id=self.data['tfs_id']).exists())
        self.assertFalse(Store.objects.filter(id=self.data['model6']).exists())
        self.assertFalse(RecordMaxCounter.objects.filter(store_model_id=self.data['model6'], instance_id__in=self.data['model6_rmcb_ids']).exists())

        # ensure reverse fast forward records are not modified
        self.assertNotEqual(Store.objects.get(id=self.data['model1']).serialized, "buffer")
        self.assertFalse(RecordMaxCounter.objects.filter(instance_id=self.data['model1_rmcb_ids'][1]).exists())

        # ensure records with merge conflicts are modified
        self.assertEqual(Store.objects.get(id=self.data['model2']).conflicting_serialized_data, "buffer\nstore")  # conflicting field is overwritten
        self.assertEqual(Store.objects.get(id=self.data['model5']).conflicting_serialized_data, "buffer\nstore")
        self.assertTrue(RecordMaxCounter.objects.filter(instance_id=self.data['model2_rmcb_ids'][1]).exists())
        self.assertTrue(RecordMaxCounter.objects.filter(instance_id=self.data['model5_rmcb_ids'][1]).exists())
        self.assertEqual(Store.objects.get(id=self.data['model2']).last_saved_instance, InstanceIDModel.get_or_create_current_instance()[0].id)
        self.assertEqual(Store.objects.get(id=self.data['model5']).last_saved_instance, InstanceIDModel.get_or_create_current_instance()[0].id)

        # ensure fast forward records are modified
        self.assertEqual(Store.objects.get(id=self.data['model3']).serialized, "buffer")  # serialized field is overwritten
        self.assertTrue(RecordMaxCounter.objects.filter(instance_id=self.data['model3_rmcb_ids'][1]).exists())
        self.assertEqual(Store.objects.get(id=self.data['model3']).last_saved_instance, self.data['model3_rmcb_ids'][1])  # last_saved_by is updated
        self.assertEqual(RecordMaxCounter.objects.get(instance_id=self.data['model3_rmcb_ids'][0], store_model_id=self.data['model3']).counter, 3)

        # ensure all buffer and rmcb records were deleted for this transfer session id
        self.assertFalse(Buffer.objects.filter(transfer_session_id=self.data['sc'].current_transfer_session.id).exists())
        self.assertFalse(RecordMaxCounterBuffer.objects.filter(transfer_session_id=self.data['sc'].current_transfer_session.id).exists())
