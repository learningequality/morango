import factory

from django.test import TestCase
from morango.models import DatabaseMaxCounter
from morango.certificates import Filter


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
