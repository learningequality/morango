import factory

from django.test import TestCase
from facility_profile.models import Facility
from morango.models import Buffer, InstanceIDModel, Store, RecordMaxCounter, RecordMaxCounterBuffer
from .helpers import create_dummy_store_data, create_dummy_buffer_data

class FacilityModelFactory(factory.DjangoModelFactory):

    class Meta:
        model = Facility

    name = factory.Sequence(lambda n: "Fac %d" % n)


class QueueStoreIntoBufferTestCase(TestCase):

    def setUp(self):
        self.data = create_dummy_store_data()
        self.filter_prefixes = []
        self.fsics = {}

    def test_all_fsics(self):
        self.fsics = {self.data['group1_id'].id: 0, self.data['group2_id'].id: 0}
        self.data['sc']._queue_into_buffer(self.filter_prefixes, self.fsics)
        buffer_ids = Buffer.objects.values_list('model_uuid', flat=True)
        rmcb_ids = RecordMaxCounterBuffer.objects.values_list('model_uuid', flat=True)
        for i in self.data['group1_c1']:
            self.assertIn(i.id, buffer_ids)
            self.assertIn(i.id, rmcb_ids)
        for i in self.data['group1_c2']:
            self.assertIn(i.id, buffer_ids)
            self.assertIn(i.id, rmcb_ids)
        for i in self.data['group2_c1']:
            self.assertIn(i.id, buffer_ids)
            self.assertIn(i.id, rmcb_ids)

    def test_fsic_specific_id(self):
        self.fsics = {self.data['group2_id'].id: 0}
        self.data['sc']._queue_into_buffer(self.filter_prefixes, self.fsics)
        buffer_ids = Buffer.objects.values_list('model_uuid', flat=True)
        rmcb_ids = RecordMaxCounterBuffer.objects.values_list('model_uuid', flat=True)
        for i in self.data['group1_c1']:
            self.assertNotIn(i.id, buffer_ids)
            self.assertNotIn(i.id, rmcb_ids)
        for i in self.data['group1_c2']:
            self.assertNotIn(i.id, buffer_ids)
            self.assertNotIn(i.id, rmcb_ids)
        for i in self.data['group2_c1']:
            self.assertIn(i.id, buffer_ids)
            self.assertIn(i.id, rmcb_ids)

    def test_fsic_counters(self):
        counter = InstanceIDModel.objects.get(id=self.data['group1_id'].id).counter
        self.fsics = {self.data['group1_id'].id: counter - 1}
        self.data['sc']._queue_into_buffer(self.filter_prefixes, self.fsics)
        buffer_ids = Buffer.objects.values_list('model_uuid', flat=True)
        rmcb_ids = RecordMaxCounterBuffer.objects.values_list('model_uuid', flat=True)
        for i in self.data['group1_c1']:
            self.assertNotIn(i.id, buffer_ids)
            self.assertNotIn(i.id, rmcb_ids)
        for i in self.data['group1_c2']:
            self.assertIn(i.id, buffer_ids)
            self.assertIn(i.id, rmcb_ids)
        for i in self.data['group2_c1']:
            self.assertNotIn(i.id, buffer_ids)
            self.assertNotIn(i.id, rmcb_ids)

    def test_fsic_counters_too_high(self):
        self.fsics = {self.data['group1_id'].id: 100, self.data['group2_id'].id: 100}
        self.data['sc']._queue_into_buffer(self.filter_prefixes, self.fsics)
        self.assertFalse(Buffer.objects.all())
        self.assertFalse(RecordMaxCounterBuffer.objects.all())

    def test_partition_filter_buffering(self):
        self.filter_prefixes = ['{}:user:summary'.format(self.data['user3'].id),
                                '{}:user:interaction'.format(self.data['user3'].id)]
        self.data['sc']._queue_into_buffer(self.filter_prefixes, self.fsics)
        buffer_ids = Buffer.objects.values_list('model_uuid', flat=True)
        rmcb_ids = RecordMaxCounterBuffer.objects.values_list('model_uuid', flat=True)
        self.assertNotIn(self.data['user2'].id, buffer_ids)
        self.assertNotIn(self.data['user2'].id, rmcb_ids)
        for i in self.data['user3_sumlogs']:
            self.assertIn(i.id, buffer_ids)
            self.assertIn(i.id, rmcb_ids)
        for i in self.data['user3_interlogs']:
            self.assertIn(i.id, buffer_ids)
            self.assertIn(i.id, rmcb_ids)

    def test_partition_prefix_buffering(self):
        self.filter_prefixes = ['{}'.format(self.data['user2'].id)]
        self.data['sc']._queue_into_buffer(self.filter_prefixes, self.fsics)
        buffer_ids = Buffer.objects.values_list('model_uuid', flat=True)
        rmcb_ids = RecordMaxCounterBuffer.objects.values_list('model_uuid', flat=True)
        self.assertIn(self.data['user2'].id, buffer_ids)
        self.assertIn(self.data['user2'].id, rmcb_ids)
        for i in self.data['user2_sumlogs']:
            self.assertIn(i.id, buffer_ids)
            self.assertIn(i.id, rmcb_ids)
        for i in self.data['user2_interlogs']:
            self.assertIn(i.id, buffer_ids)
            self.assertIn(i.id, rmcb_ids)
        self.assertNotIn(self.data['user3'].id, buffer_ids)
        self.assertNotIn(self.data['user3'].id, rmcb_ids)

    def test_partition_and_fsic_buffering(self):
        self.filter_prefixes = ['{}:user:summary'.format(self.data['user1'].id)]
        self.fsics = {self.data['group1_id']: 1}
        self.data['sc']._queue_into_buffer(self.filter_prefixes, self.fsics)
        buffer_ids = Buffer.objects.values_list('model_uuid', flat=True)
        rmcb_ids = RecordMaxCounterBuffer.objects.values_list('model_uuid', flat=True)
        for i in self.data['user1_sumlogs']:
            self.assertIn(i.id, buffer_ids)
            self.assertIn(i.id, rmcb_ids)
        for i in self.data['user2_sumlogs']:
            self.assertNotIn(i.id, buffer_ids)
            self.assertNotIn(i.id, rmcb_ids)
        for i in self.data['user3_sumlogs']:
            self.assertNotIn(i.id, buffer_ids)
            self.assertNotIn(i.id, rmcb_ids)
