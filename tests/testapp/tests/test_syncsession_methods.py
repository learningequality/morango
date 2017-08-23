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

    def assertRecordsBuffered(self, filter_prefixes, fsics, records):
        self.data['sc']._queue_into_buffer(filter_prefixes, fsics)
        buffer_ids = Buffer.objects.values_list('model_uuid', flat=True)
        rmcb_ids = RecordMaxCounterBuffer.objects.values_list('model_uuid', flat=True)
        # ensure all store and buffer records are buffered
        for i in records:
            self.assertIn(i.id, buffer_ids)
            self.assertIn(i.id, rmcb_ids)

    def assertRecordsNotBuffered(self, filter_prefixes, fsics, records):
        self.data['sc']._queue_into_buffer(filter_prefixes, fsics)
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
        self.assertRecordsBuffered(self.filter_prefixes, self.fsics, self.data['group1_c1'])
        self.assertRecordsBuffered(self.filter_prefixes, self.fsics, self.data['group1_c2'])
        self.assertRecordsBuffered(self.filter_prefixes, self.fsics, self.data['group2_c1'])

    def test_fsic_specific_id(self):
        self.fsics = {self.data['group2_id'].id: 0}
        # ensure only records modified with 2nd instance id are buffered
        self.assertRecordsNotBuffered(self.filter_prefixes, self.fsics, self.data['group1_c1'])
        self.assertRecordsNotBuffered(self.filter_prefixes, self.fsics, self.data['group1_c2'])
        self.assertRecordsBuffered(self.filter_prefixes, self.fsics, self.data['group2_c1'])

    def test_fsic_counters(self):
        counter = InstanceIDModel.objects.get(id=self.data['group1_id'].id).counter
        self.fsics = {self.data['group1_id'].id: counter - 1}
        # ensure only records with updated 1st instance id are buffered
        self.assertRecordsNotBuffered(self.filter_prefixes, self.fsics, self.data['group1_c1'])
        self.assertRecordsBuffered(self.filter_prefixes, self.fsics, self.data['group1_c2'])
        self.assertRecordsNotBuffered(self.filter_prefixes, self.fsics, self.data['group2_c1'])

    def test_fsic_counters_too_high(self):
        self.fsics = {self.data['group1_id'].id: 100, self.data['group2_id'].id: 100}
        self.data['sc']._queue_into_buffer(self.filter_prefixes, self.fsics)
        # ensure no records are buffered
        self.assertFalse(Buffer.objects.all())
        self.assertFalse(RecordMaxCounterBuffer.objects.all())

    def test_partition_filter_buffering(self):
        self.filter_prefixes = ['{}:user:summary'.format(self.data['user3'].id),
                                '{}:user:interaction'.format(self.data['user3'].id)]
        # ensure records with different partition values are buffered
        self.assertRecordsNotBuffered(self.filter_prefixes, self.fsics, [self.data['user2']])
        self.assertRecordsBuffered(self.filter_prefixes, self.fsics, self.data['user3_sumlogs'])
        self.assertRecordsBuffered(self.filter_prefixes, self.fsics, self.data['user3_interlogs'])

    def test_partition_prefix_buffering(self):
        self.filter_prefixes = ['{}'.format(self.data['user2'].id)]
        # ensure only records with user2 partition are buffered
        self.assertRecordsBuffered(self.filter_prefixes, self.fsics, [self.data['user2']])
        self.assertRecordsBuffered(self.filter_prefixes, self.fsics, self.data['user2_sumlogs'])
        self.assertRecordsBuffered(self.filter_prefixes, self.fsics, self.data['user2_interlogs'])
        self.assertRecordsNotBuffered(self.filter_prefixes, self.fsics, [self.data['user3']])

    def test_partition_and_fsic_buffering(self):
        self.filter_prefixes = ['{}:user:summary'.format(self.data['user1'].id)]
        self.fsics = {self.data['group1_id'].id: 1}
        # ensure records updated with 1st instance id and summarylog partition are buffered
        self.assertRecordsBuffered(self.filter_prefixes, self.fsics, self.data['user1_sumlogs'])
        self.assertRecordsNotBuffered(self.filter_prefixes, self.fsics, self.data['user2_sumlogs'])
        self.assertRecordsNotBuffered(self.filter_prefixes, self.fsics, self.data['user3_sumlogs'])

    def test_valid_fsic_but_invalid_partition(self):
        self.filter_prefixes = ['{}:user:summary'.format(self.data['user1'].id)]
        self.fsics = {self.data['group2_id'].id: 1}
        # ensure that record with valid fsic but invalid partition is not buffered
        self.assertRecordsNotBuffered(self.filter_prefixes, self.fsics, [self.data['user4']])


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

    def test_dequeuing_delete_buffered_records(self):
        self.assertTrue(Buffer.objects.filter(model_uuid=self.data['model1']).exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_delete_buffered_records(cursor)
        self.assertFalse(Buffer.objects.filter(model_uuid=self.data['model1']).exists())

    def test_dequeuing_merge_conflict_rmc(self):
        rmc = RecordMaxCounter.objects.get(instance_id=self.data['model2_rmc_ids'][0], store_model_id=self.data['model2'])
        rmcb = RecordMaxCounterBuffer.objects.get(instance_id=self.data['model2_rmc_ids'][0], model_uuid=self.data['model2'])
        self.assertNotEqual(rmc.counter, rmcb.counter)
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_merge_conflict_rmc(cursor)
        rmc = RecordMaxCounter.objects.get(instance_id=self.data['model2_rmc_ids'][0], store_model_id=self.data['model2'])
        rmcb = RecordMaxCounterBuffer.objects.get(instance_id=self.data['model2_rmc_ids'][0], model_uuid=self.data['model2'])
        self.assertEqual(rmc.counter, rmcb.counter)

    def test_dequeuing_merge_conflict_buffer(self):
        store = Store.objects.get(id=self.data['model2'])
        self.assertNotEqual(store.last_saved_instance, self.current_id.id)
        self.assertEqual(store.conflicting_serialized_data, "store")
        with connection.cursor() as cursor:
            current_id = InstanceIDModel.get_and_update_current_instance()
            self.data['sc']._dequeuing_merge_conflict_buffer(cursor, current_id)
        store = Store.objects.get(id=self.data['model2'])
        self.assertEqual(store.last_saved_instance, current_id.id)
        self.assertEqual(store.last_saved_counter, current_id.counter)
        self.assertEqual(store.conflicting_serialized_data, "buffer\nstore")

    def test_dequeuing_update_rmcs_last_saved_by(self):
        self.assertFalse(RecordMaxCounter.objects.filter(instance_id=self.current_id.id).exists())
        with connection.cursor() as cursor:
            current_id = InstanceIDModel.get_and_update_current_instance()
            self.data['sc']._dequeuing_update_rmcs_last_saved_by(cursor, current_id)
        self.assertTrue(RecordMaxCounter.objects.filter(instance_id=current_id.id).exists())

    def test_dequeuing_delete_mc_buffer(self):
        self.assertTrue(Buffer.objects.filter(model_uuid=self.data['model2']).exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_delete_mc_buffer(cursor)
        self.assertFalse(Buffer.objects.filter(model_uuid=self.data['model2']).exists())

    def test_dequeuing_delete_mc_rmcb(self):
        self.assertTrue(RecordMaxCounterBuffer.objects.filter(model_uuid=self.data['model2'], instance_id=self.data['model2_rmcb_ids'][0]).exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_delete_mc_rmcb(cursor)
        self.assertFalse(RecordMaxCounterBuffer.objects.filter(model_uuid=self.data['model2'], instance_id=self.data['model2_rmcb_ids'][0]).exists())

    def test_dequeuing_ff_rmc(self):
        # this only transfers rmcs that have the same instance id (does not handle instance ids it has not seen before)
        rmc = RecordMaxCounter.objects.get(instance_id=self.data['model3_rmc_ids'][0], store_model_id=self.data['model3'])
        rmcb = RecordMaxCounterBuffer.objects.get(instance_id=self.data['model3_rmcb_ids'][0], model_uuid=self.data['model3'])
        self.assertNotEqual(rmc.counter, rmcb.counter)
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_ff_rmc(cursor)
        rmc = RecordMaxCounter.objects.get(instance_id=self.data['model3_rmc_ids'][0], store_model_id=self.data['model3'])
        rmcb = RecordMaxCounterBuffer.objects.get(instance_id=self.data['model3_rmcb_ids'][0], model_uuid=self.data['model3'])
        self.assertEqual(rmc.counter, rmcb.counter)

    def test_dequeuing_ff_store(self):
        store_model = Store.objects.get(id=self.data['model3'])
        buffer_model = Buffer.objects.get(model_uuid=self.data['model3'])
        self.assertNotEqual(store_model.last_saved_instance, buffer_model.last_saved_instance)
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_ff_store(cursor)
        store_model = Store.objects.get(id=self.data['model3'])
        buffer_model = Buffer.objects.get(model_uuid=self.data['model3'])
        self.assertEqual(store_model.last_saved_instance, buffer_model.last_saved_instance)

    def test_dequeuing_delete_ff_buffer(self):
        self.assertTrue(Buffer.objects.filter(model_uuid=self.data['model3']).exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_delete_ff_buffer(cursor)
        self.assertFalse(Buffer.objects.filter(model_uuid=self.data['model3']).exists())

    def test_dequeuing_delete_ff_rmcb(self):
        self.assertTrue(RecordMaxCounterBuffer.objects.filter(instance_id=self.data['model3_rmcb_ids'][0], model_uuid=self.data['model3']).exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_delete_ff_rmcb(cursor)
        self.assertFalse(RecordMaxCounterBuffer.objects.filter(instance_id=self.data['model3_rmcb_ids'][0], model_uuid=self.data['model3']).exists())

    def test_dequeuing_insert_remaining_buffer(self):
        self.assertFalse(Store.objects.filter(id=self.data['model4']).exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_insert_remaining_buffer(cursor)
        self.assertTrue(Store.objects.filter(id=self.data['model4']).exists())

    def test_dequeuing_insert_remaining_rmcb(self):
        for i in self.data['model4_rmcb_ids']:
            self.assertFalse(RecordMaxCounter.objects.filter(instance_id=i, store_model_id=self.data['model4']).exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_insert_remaining_rmcb(cursor)
        for i in self.data['model4_rmcb_ids']:
            self.assertTrue(RecordMaxCounter.objects.filter(instance_id=i, store_model_id=self.data['model4']).exists())

    def test_dequeuing_delete_remaining_rmcb(self):
        self.assertTrue(RecordMaxCounterBuffer.objects.exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_delete_remaining_rmcb(cursor)
        self.assertFalse(RecordMaxCounterBuffer.objects.exists())

    def test_dequeuing_delete_remaining_buffer(self):
        self.assertTrue(Buffer.objects.exists())
        with connection.cursor() as cursor:
            self.data['sc']._dequeuing_delete_remaining_buffer(cursor)
        self.assertFalse(Buffer.objects.exists())
