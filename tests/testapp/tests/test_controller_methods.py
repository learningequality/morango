import factory
import json
import mock
import uuid

from django.test import TestCase
from morango.utils.controller import MorangoProfileController
from facility_profile.models import Facility, MyUser
from morango.models import DatabaseIDModel, InstanceIDModel, RecordMaxCounter, Store


class FacilityModelFactory(factory.DjangoModelFactory):

    class Meta:
        model = Facility

    name = factory.Sequence(lambda n: "Fac %d" % n)


class SerializeIntoStoreTestCase(TestCase):

    def setUp(self):
        DatabaseIDModel.objects.create()
        InstanceIDModel.get_or_create_current_instance()
        self.range = 10
        self.mc = MorangoProfileController('facilitydata')

    def test_all_models_get_serialized(self):
        [FacilityModelFactory() for _ in range(self.range)]
        self.mc._serialize_into_store()

        self.assertTrue(Facility.objects.all())
        self.assertEqual(len(Store.objects.all()), self.range)

    def test_no_models_get_serialized(self):
        # set dirty bit off on new models created
        [FacilityModelFactory.build().save(update_dirty_bit_to=False) for _ in range(self.range)]
        # only models with dirty bit on should be serialized
        self.mc._serialize_into_store()
        self.assertFalse(Store.objects.all())

    def test_dirty_bit_gets_set(self):
        [FacilityModelFactory() for _ in range(self.range)]
        # dirty bit should be on
        for facility in Facility.objects.all():
            self.assertTrue(facility._morango_dirty_bit)

        self.mc._serialize_into_store()
        # dirty bit should have been toggled off
        for facility in Facility.objects.all():
            self.assertFalse(facility._morango_dirty_bit)

    def test_store_models_get_updated(self):
        original_name = "ralphie"
        FacilityModelFactory(name=original_name)
        self.mc._serialize_into_store()
        store_facility = Store.objects.first()
        deserialized_model = json.loads(store_facility.serialized)
        self.assertEqual(deserialized_model['name'], original_name)

        new_name = "rafael"
        Facility.objects.update(name=new_name)
        self.mc._serialize_into_store()
        store_facility = Store.objects.first()
        deserialized_model = json.loads(store_facility.serialized)
        self.assertEqual(deserialized_model['name'], new_name)

    def test_last_saved_counter_updates(self):
        original_name = "ralphie"
        FacilityModelFactory(name=original_name)
        self.mc._serialize_into_store()
        old_counter = Store.objects.first().last_saved_counter

        new_name = "rafael"
        Facility.objects.all().update(name=new_name)
        self.mc._serialize_into_store()
        new_counter = Store.objects.first().last_saved_counter

        self.assertEqual(old_counter + 1, new_counter)

    def test_last_saved_instance_updates(self):
        original_name = "ralphie"
        FacilityModelFactory(name=original_name)
        self.mc._serialize_into_store()
        old_instance_id = Store.objects.first().last_saved_instance

        with mock.patch('platform.platform', return_value='Windows'):
            (new_id, _) = InstanceIDModel.get_or_create_current_instance()

        new_name = "rafael"
        Facility.objects.all().update(name=new_name)
        self.mc._serialize_into_store()
        new_instance_id = Store.objects.first().last_saved_instance

        self.assertNotEqual(old_instance_id, new_instance_id)
        self.assertEqual(new_instance_id, new_id.id)

    def test_extra_fields_dont_get_overwritten(self):
        serialized = """{"username": "deadbeef", "height": 6.0, "weight": 100}"""
        MyUser.objects.create(username='deadbeef')
        self.mc._serialize_into_store()
        Store.objects.update(serialized=serialized)

        MyUser.objects.update(username='alivebeef')
        self.mc._serialize_into_store()
        serialized = json.loads(Store.objects.first().serialized)
        self.assertIn('height', serialized)


class RecordMaxCounterUpdatesDuringSerialization(TestCase):

    def setUp(self):
        DatabaseIDModel.objects.create()
        (self.current_id, _) = InstanceIDModel.get_or_create_current_instance()
        self.mc = MorangoProfileController('facilitydata')
        self.fac1 = FacilityModelFactory(name='school')
        self.mc._serialize_into_store()
        self.old_rmc = RecordMaxCounter.objects.first()

    def test_new_rmc_for_existing_model(self):
        with mock.patch('platform.platform', return_value='Windows'):
            (new_id, _) = InstanceIDModel.get_or_create_current_instance()

        Facility.objects.update(name='facility')
        self.mc._serialize_into_store()
        new_rmc = RecordMaxCounter.objects.get(instance_id=new_id.id, store_model_id=self.fac1.id)
        new_store_record = Store.objects.get(id=self.fac1.id)

        self.assertEqual(new_rmc.counter, new_store_record.last_saved_counter)
        self.assertEqual(new_rmc.instance_id, new_store_record.last_saved_instance)

    def test_update_rmc_for_existing_model(self):
        Facility.objects.update(name='facility')
        self.mc._serialize_into_store()

        # there should only be 1 RecordMaxCounter for a specific instance_id and a specific model (unique_together)
        self.assertEqual(RecordMaxCounter.objects.filter(instance_id=self.current_id.id, store_model_id=self.fac1.id).count(), 1)

        new_rmc = RecordMaxCounter.objects.get(instance_id=self.current_id.id, store_model_id=self.fac1.id)
        new_store_record = Store.objects.get(id=self.fac1.id)

        self.assertEqual(self.old_rmc.counter + 1, new_rmc.counter)
        self.assertEqual(new_rmc.counter, new_store_record.last_saved_counter)
        self.assertEqual(new_rmc.instance_id, new_store_record.last_saved_instance)

    def test_new_rmc_for_non_existent_model(self):
        with mock.patch('platform.platform', return_value='Windows'):
            (new_id, _) = InstanceIDModel.get_or_create_current_instance()

        new_fac = FacilityModelFactory(name='college')
        self.mc._serialize_into_store()
        new_rmc = RecordMaxCounter.objects.get(instance_id=new_id.id, store_model_id=new_fac.id)
        new_store_record = Store.objects.get(id=new_fac.id)

        self.assertNotEqual(new_id.id, self.current_id.id)
        self.assertEqual(new_store_record.last_saved_instance, new_rmc.instance_id)
        self.assertEqual(new_store_record.last_saved_counter, new_rmc.counter)
