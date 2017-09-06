import factory

from django.db import connection
from django.test import TestCase
from facility_profile.models import Facility
from morango.controller import MorangoProfileController
from morango.syncsession import SyncClient
from morango.models import Buffer, DatabaseIDModel, InstanceIDModel, Store, RecordMaxCounter, RecordMaxCounterBuffer
from .helpers import create_dummy_store_data, create_buffer_and_store_dummy_data

class FacilityModelFactory(factory.DjangoModelFactory):

    class Meta:
        model = Facility

    name = factory.Sequence(lambda n: "Fac %d" % n)


class QueueStoreIntoBufferTestCase(TestCase):

    def setUp(self):
        self.data = create_dummy_store_data()
        self.filter_prefixes = []
        self.fsics = {}

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
        self.fsics = {self.data['group1_id'].id: 0, self.data['group2_id'].id: 0}
        self.data['sc']._queue_into_buffer(self.filter_prefixes, self.fsics)
        # ensure all store and buffer records are buffered
        self.assertRecordsBuffered(self.data['group1_c1'])
        self.assertRecordsBuffered(self.data['group1_c2'])
        self.assertRecordsBuffered(self.data['group2_c1'])

    def test_fsic_specific_id(self):
        self.fsics = {self.data['group2_id'].id: 0}
        self.data['sc']._queue_into_buffer(self.filter_prefixes, self.fsics)
        # ensure only records modified with 2nd instance id are buffered
        self.assertRecordsNotBuffered(self.data['group1_c1'])
        self.assertRecordsNotBuffered(self.data['group1_c2'])
        self.assertRecordsBuffered(self.data['group2_c1'])

    def test_fsic_counters(self):
        counter = InstanceIDModel.objects.get(id=self.data['group1_id'].id).counter
        self.fsics = {self.data['group1_id'].id: counter - 1}
        self.data['sc']._queue_into_buffer(self.filter_prefixes, self.fsics)
        # ensure only records with updated 1st instance id are buffered
        self.assertRecordsNotBuffered(self.data['group1_c1'])
        self.assertRecordsBuffered(self.data['group1_c2'])
        self.assertRecordsNotBuffered(self.data['group2_c1'])

    def test_fsic_counters_too_high(self):
        self.fsics = {self.data['group1_id'].id: 100, self.data['group2_id'].id: 100}
        self.data['sc']._queue_into_buffer(self.filter_prefixes, self.fsics)
        # ensure no records are buffered
        self.assertFalse(Buffer.objects.all())
        self.assertFalse(RecordMaxCounterBuffer.objects.all())

    def test_partition_filter_buffering(self):
        self.filter_prefixes = ['{}:user:summary'.format(self.data['user3'].id),
                                '{}:user:interaction'.format(self.data['user3'].id)]
        self.data['sc']._queue_into_buffer(self.filter_prefixes, self.fsics)
        # ensure records with different partition values are buffered
        self.assertRecordsNotBuffered([self.data['user2']])
        self.assertRecordsBuffered(self.data['user3_sumlogs'])
        self.assertRecordsBuffered(self.data['user3_interlogs'])

    def test_partition_prefix_buffering(self):
        self.filter_prefixes = ['{}'.format(self.data['user2'].id)]
        self.data['sc']._queue_into_buffer(self.filter_prefixes, self.fsics)
        # ensure only records with user2 partition are buffered
        self.assertRecordsBuffered([self.data['user2']])
        self.assertRecordsBuffered(self.data['user2_sumlogs'])
        self.assertRecordsBuffered(self.data['user2_interlogs'])
        self.assertRecordsNotBuffered([self.data['user3']])

    def test_partition_and_fsic_buffering(self):
        self.filter_prefixes = ['{}:user:summary'.format(self.data['user1'].id)]
        self.fsics = {self.data['group1_id'].id: 1}
        self.data['sc']._queue_into_buffer(self.filter_prefixes, self.fsics)
        # ensure records updated with 1st instance id and summarylog partition are buffered
        self.assertRecordsBuffered(self.data['user1_sumlogs'])
        self.assertRecordsNotBuffered(self.data['user2_sumlogs'])
        self.assertRecordsNotBuffered(self.data['user3_sumlogs'])

    def test_valid_fsic_but_invalid_partition(self):
        self.filter_prefixes = ['{}:user:summary'.format(self.data['user1'].id)]
        self.fsics = {self.data['group2_id'].id: 1}
        # ensure that record with valid fsic but invalid partition is not buffered
        self.assertRecordsNotBuffered([self.data['user4']])


class BufferIntoStoreTestCase(TestCase):

    def setUp(self):
        self.data = {}
        DatabaseIDModel.objects.create()
        (self.current_id, _) = InstanceIDModel.get_or_create_current_instance()

        # create controllers for app/store/buffer operations
        self.data['mc'] = MorangoProfileController('facilitydata')
        self.data['sc'] = SyncClient('host', 'facilitydata')

        self.data.update(create_buffer_and_store_dummy_data(self.data['sc'].transfer_session_id))

    def test_dequeuing_delete_rmcb_records(self):
        for i in self.data['model1_rmcb_ids']:
            self.assertTrue(RecordMaxCounterBuffer.objects.filter(instance_id=i, model_uuid=self.data['model1']).exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_delete_rmcb_records(cursor)
        for i in self.data['model1_rmcb_ids']:
            self.assertFalse(RecordMaxCounterBuffer.objects.filter(instance_id=i, model_uuid=self.data['model1']).exists())
        # ensure other records were not deleted
        for i in self.data['model2_rmcb_ids']:
            self.assertTrue(RecordMaxCounterBuffer.objects.filter(instance_id=i, model_uuid=self.data['model2']).exists())

    def test_dequeuing_delete_buffered_records(self):
        self.assertTrue(Buffer.objects.filter(model_uuid=self.data['model1']).exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_delete_buffered_records(cursor)
        self.assertFalse(Buffer.objects.filter(model_uuid=self.data['model1']).exists())
        # ensure other records were not deleted
        self.assertTrue(Buffer.objects.filter(model_uuid=self.data['model2']).exists())

    def test_dequeuing_merge_conflict_rmcb_greater_than_rmc(self):
        rmc = RecordMaxCounter.objects.get(instance_id=self.data['model2_rmc_ids'][0], store_model_id=self.data['model2'])
        rmcb = RecordMaxCounterBuffer.objects.get(instance_id=self.data['model2_rmc_ids'][0], model_uuid=self.data['model2'])
        self.assertNotEqual(rmc.counter, rmcb.counter)
        self.assertGreaterEqual(rmcb.counter, rmc.counter)
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_merge_conflict_rmcb(cursor)
        rmc = RecordMaxCounter.objects.get(instance_id=self.data['model2_rmc_ids'][0], store_model_id=self.data['model2'])
        rmcb = RecordMaxCounterBuffer.objects.get(instance_id=self.data['model2_rmc_ids'][0], model_uuid=self.data['model2'])
        self.assertEqual(rmc.counter, rmcb.counter)

    def test_dequeuing_merge_conflict_rmcb_less_than_rmc(self):
        rmc = RecordMaxCounter.objects.get(instance_id=self.data['model5_rmc_ids'][0], store_model_id=self.data['model5'])
        rmcb = RecordMaxCounterBuffer.objects.get(instance_id=self.data['model5_rmc_ids'][0], model_uuid=self.data['model5'])
        self.assertNotEqual(rmc.counter, rmcb.counter)
        self.assertGreaterEqual(rmc.counter, rmcb.counter)
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_merge_conflict_rmcb(cursor)
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
            self.data['sc']._dequeuing_merge_conflict_buffer(cursor, current_id)
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
            self.data['sc']._dequeuing_merge_conflict_buffer(cursor, current_id)
        store = Store.objects.get(id=self.data['model5'])
        self.assertEqual(store.last_saved_instance, current_id.id)
        self.assertEqual(store.last_saved_counter, current_id.counter)
        self.assertEqual(store.conflicting_serialized_data, "buffer\nstore")

    def test_dequeuing_update_rmcs_last_saved_by(self):
        self.assertFalse(RecordMaxCounter.objects.filter(instance_id=self.current_id.id).exists())
        with connection.cursor() as cursor:
            current_id = InstanceIDModel.get_current_instance_and_increment_counter()
            self.data['sc']._dequeuing_update_rmcs_last_saved_by(cursor, current_id)
        self.assertTrue(RecordMaxCounter.objects.filter(instance_id=current_id.id).exists())

    def test_dequeuing_delete_mc_buffer(self):
        self.assertTrue(Buffer.objects.filter(model_uuid=self.data['model2']).exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_delete_mc_buffer(cursor)
        self.assertFalse(Buffer.objects.filter(model_uuid=self.data['model2']).exists())
        # ensure other records were not deleted
        self.assertTrue(Buffer.objects.filter(model_uuid=self.data['model3']).exists())

    def test_dequeuing_delete_mc_rmcb(self):
        self.assertTrue(RecordMaxCounterBuffer.objects.filter(model_uuid=self.data['model2'], instance_id=self.data['model2_rmcb_ids'][0]).exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_delete_mc_rmcb(cursor)
        self.assertFalse(RecordMaxCounterBuffer.objects.filter(model_uuid=self.data['model2'], instance_id=self.data['model2_rmcb_ids'][0]).exists())
        self.assertTrue(RecordMaxCounterBuffer.objects.filter(model_uuid=self.data['model2'], instance_id=self.data['model2_rmcb_ids'][1]).exists())
        # ensure other records were not deleted
        self.assertTrue(RecordMaxCounterBuffer.objects.filter(model_uuid=self.data['model3'], instance_id=self.data['model3_rmcb_ids'][0]).exists())

    def test_dequeuing_insert_remaining_buffer(self):
        self.assertNotEqual(Store.objects.get(id=self.data['model3']).serialized, "buffer")
        self.assertFalse(Store.objects.filter(id=self.data['model4']).exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_insert_remaining_buffer(cursor)
        self.assertEqual(Store.objects.get(id=self.data['model3']).serialized, "buffer")
        self.assertTrue(Store.objects.filter(id=self.data['model4']).exists())

    def test_dequeuing_insert_remaining_rmcb(self):
        for i in self.data['model4_rmcb_ids']:
            self.assertFalse(RecordMaxCounter.objects.filter(instance_id=i, store_model_id=self.data['model4']).exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_insert_remaining_rmcb(cursor)
        for i in self.data['model4_rmcb_ids']:
            self.assertTrue(RecordMaxCounter.objects.filter(instance_id=i, store_model_id=self.data['model4']).exists())

    def test_dequeuing_delete_remaining_rmcb(self):
        self.assertTrue(RecordMaxCounterBuffer.objects.filter(transfer_session_id=self.data['sc'].transfer_session_id).exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_delete_remaining_rmcb(cursor)
        self.assertFalse(RecordMaxCounterBuffer.objects.filter(transfer_session_id=self.data['sc'].transfer_session_id).exists())

    def test_dequeuing_delete_remaining_buffer(self):
        self.assertTrue(Buffer.objects.filter(transfer_session_id=self.data['sc'].transfer_session_id).exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_delete_remaining_buffer(cursor)
        self.assertFalse(Buffer.objects.filter(transfer_session_id=self.data['sc'].transfer_session_id).exists())

    def test_integrate_into_store(self):
        self.data['sc']._integrate_into_store()
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
        self.assertFalse(Buffer.objects.filter(transfer_session_id=self.data['sc'].transfer_session_id).exists())
        self.assertFalse(RecordMaxCounterBuffer.objects.filter(transfer_session_id=self.data['sc'].transfer_session_id).exists())
