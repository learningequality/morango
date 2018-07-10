import json
import uuid

import factory
from django.test import TestCase
from django.utils import timezone
from django.utils.six import iteritems
from facility_profile.models import SummaryLog
from morango.api.serializers import BufferSerializer
from morango.certificates import Filter
from morango.models import (Buffer, DatabaseMaxCounter, SyncSession,
                            TransferSession, RecordMaxCounterBuffer)


class DatabaseMaxCounterFactory(factory.DjangoModelFactory):

    class Meta:
        model = DatabaseMaxCounter


class FilterMaxCounterTestCase(TestCase):

    def setUp(self):
        self.instance_a = "a" * 32
        self.prefix_a = "AAA"
        self.user_prefix_a = "AAA:user_id:joe"

        self.instance_b = "b" * 32
        self.prefix_b = "BBB"
        self.user_prefix_b = "BBB:user_id:rick"
        self.user2_prefix_b = "BBB:user_id:emily"

        # instance A dmc
        DatabaseMaxCounterFactory(instance_id=self.instance_a, partition=self.prefix_a, counter=15)
        DatabaseMaxCounterFactory(instance_id=self.instance_a, partition=self.user_prefix_a, counter=20)
        DatabaseMaxCounterFactory(instance_id=self.instance_a, partition=self.user2_prefix_b, counter=17)

        # instance B dmc
        DatabaseMaxCounterFactory(instance_id=self.instance_b, partition=self.user_prefix_a, counter=10)
        DatabaseMaxCounterFactory(instance_id=self.instance_b, partition=self.prefix_b, counter=12)
        DatabaseMaxCounterFactory(instance_id=self.instance_b, partition=self.user_prefix_b, counter=5)
        DatabaseMaxCounterFactory(instance_id=self.instance_b, partition=self.user2_prefix_b, counter=2)

    def test_filter_not_in_dmc(self):
        fmcs = DatabaseMaxCounter.calculate_filter_max_counters(Filter("ZZZ"))
        self.assertEqual(fmcs, {})

    def test_instances_for_one_partition_but_not_other(self):
        fmcs = DatabaseMaxCounter.calculate_filter_max_counters(Filter(self.user_prefix_a + "\n" + self.user_prefix_b))
        self.assertEqual(fmcs[self.instance_b], 10)

    def test_insufficient_instances_for_all_partitions(self):
        user_with_prefix = self.prefix_b + "user_id:richard"
        fmcs = DatabaseMaxCounter.calculate_filter_max_counters(Filter(self.prefix_a + "\n" + user_with_prefix))
        self.assertFalse(fmcs)

    def test_single_partition_with_all_instances(self):
        fmcs = DatabaseMaxCounter.calculate_filter_max_counters(Filter(self.user_prefix_a))
        self.assertEqual(fmcs[self.instance_a], 20)
        self.assertEqual(fmcs[self.instance_b], 10)

    def test_all_partitions_have_all_instances(self):
        fmcs = DatabaseMaxCounter.calculate_filter_max_counters(Filter(self.user_prefix_a + "\n" + self.user2_prefix_b))
        self.assertEqual(fmcs[self.instance_a], 17)
        self.assertEqual(fmcs[self.instance_b], 10)


class DatabaseMaxCounterUpdateCalculation(TestCase):

    def setUp(self):
        self.filter = "filter"

    def test_update_all_fsics(self):
        client_fsic = {'a'*32: 2, 'b'*32: 2, 'c'*32: 2}
        server_fsic = {'a'*32: 1, 'b'*32: 1, 'c'*32: 1}
        self.assertFalse(DatabaseMaxCounter.objects.filter(counter=2).exists())
        for instance_id, counter in iteritems(server_fsic):
            DatabaseMaxCounter.objects.create(instance_id=instance_id, counter=counter, partition=self.filter)
        DatabaseMaxCounter.update_fsics(client_fsic, Filter(self.filter))
        self.assertTrue(DatabaseMaxCounter.objects.filter(counter=2).exists())
        self.assertFalse(DatabaseMaxCounter.objects.filter(counter=1).exists())

    def test_update_some_fsics(self):
        client_fsic = {'a'*32: 1, 'e'*32: 2, 'c'*32: 1}
        server_fsic = {'a'*32: 2, 'b'*32: 1, 'c'*32: 2}
        self.assertFalse(DatabaseMaxCounter.objects.filter(instance_id='e'*32).exists())
        for instance_id, counter in iteritems(server_fsic):
            DatabaseMaxCounter.objects.create(instance_id=instance_id, counter=counter, partition=self.filter)
        DatabaseMaxCounter.update_fsics(client_fsic, Filter(self.filter))
        self.assertTrue(DatabaseMaxCounter.objects.filter(instance_id='e'*32).exists())

    def test_no_fsics_get_updated(self):
        client_fsic = {'a'*32: 1, 'b'*32: 1, 'c'*32: 1}
        server_fsic = {'a'*32: 2, 'b'*32: 2, 'c'*32: 2}
        self.assertFalse(DatabaseMaxCounter.objects.filter(counter=1).exists())
        for instance_id, counter in iteritems(server_fsic):
            DatabaseMaxCounter.objects.create(instance_id=instance_id, counter=counter, partition=self.filter)
        DatabaseMaxCounter.update_fsics(client_fsic, Filter(self.filter))
        self.assertFalse(DatabaseMaxCounter.objects.filter(counter=1).exists())


class TransferSessionDeletion(TestCase):

    def setUp(self):
        self.syncsession = SyncSession.objects.create(id=uuid.uuid4().hex, profile="facilitydata", last_activity_timestamp=timezone.now())
        self.transfersession = TransferSession.objects.create(id=uuid.uuid4().hex, sync_session=self.syncsession, filter='partition',
                                                              push=True, last_activity_timestamp=timezone.now(), records_total=100)
        self.build_buffer_items()

    def build_buffer_items(self, **kwargs):

        data = {
            "profile": kwargs.get("profile", 'facilitydata'),
            "serialized": kwargs.get("serialized", '{"test": 99}'),
            "deleted": kwargs.get("deleted", False),
            "last_saved_instance": kwargs.get("last_saved_instance", uuid.uuid4().hex),
            "last_saved_counter": kwargs.get("last_saved_counter", 179),
            "partition": kwargs.get("partition", 'partition'),
            "source_id": kwargs.get("source_id", uuid.uuid4().hex),
            "model_name": kwargs.get("model_name", "contentsummarylog"),
            "conflicting_serialized_data": kwargs.get("conflicting_serialized_data", ""),
            "model_uuid": kwargs.get("model_uuid", None),
            "transfer_session": self.transfersession,
        }

        for i in range(3):
            data['source_id'] = uuid.uuid4().hex
            data["model_uuid"] = SummaryLog.compute_namespaced_id(data["partition"], data["source_id"], data["model_name"])
            Buffer.objects.create(**data)
            RecordMaxCounterBuffer.objects.create(
                transfer_session=self.transfersession,
                model_uuid=data["model_uuid"],
                instance_id=uuid.uuid4().hex,
                counter=i * 3 + 1,
            )

    def test_model_soft_deletion(self):
        self.assertEqual(TransferSession.objects.count(), 1)
        self.assertTrue(Buffer.objects.count() > 0)
        self.assertTrue(RecordMaxCounterBuffer.objects.count() > 0)
        self.transfersession.delete(soft=True)
        self.assertEqual(TransferSession.objects.count(), 1)
        self.assertTrue(Buffer.objects.count() == 0)
        self.assertTrue(RecordMaxCounterBuffer.objects.count() == 0)

    def test_model_hard_deletion(self):
        self.assertTrue(Buffer.objects.count() > 0)
        self.assertTrue(RecordMaxCounterBuffer.objects.count() > 0)
        self.assertEqual(TransferSession.objects.count(), 1)
        self.transfersession.delete(soft=False)
        self.assertEqual(TransferSession.objects.count(), 0)
        self.assertTrue(Buffer.objects.count() == 0)
        self.assertTrue(RecordMaxCounterBuffer.objects.count() == 0)

    def test_queryset_soft_deletion(self):
        self.assertTrue(Buffer.objects.count() > 0)
        self.assertTrue(RecordMaxCounterBuffer.objects.count() > 0)
        self.assertEqual(TransferSession.objects.count(), 1)
        TransferSession.objects.delete(soft=True)
        self.assertEqual(TransferSession.objects.count(), 1)
        self.assertTrue(Buffer.objects.count() == 0)
        self.assertTrue(RecordMaxCounterBuffer.objects.count() == 0)

    def test_queryset_hard_deletion(self):
        self.assertTrue(Buffer.objects.count() > 0)
        self.assertTrue(RecordMaxCounterBuffer.objects.count() > 0)
        self.assertEqual(TransferSession.objects.count(), 1)
        TransferSession.objects.delete(soft=False)
        self.assertEqual(TransferSession.objects.count(), 0)
        self.assertTrue(Buffer.objects.count() == 0)
        self.assertTrue(RecordMaxCounterBuffer.objects.count() == 0)
