import factory
import json
import mock
import uuid

from django.test import TestCase
from morango.controller import MorangoProfileController
from morango.controller import _self_referential_fk
from facility_profile.models import Facility, MyUser, SummaryLog
from morango.certificates import Filter
from morango.models import DeletedModels, InstanceIDModel, RecordMaxCounter, Store, HardDeletedModels

from .helpers import serialized_facility_factory


class FacilityModelFactory(factory.DjangoModelFactory):

    class Meta:
        model = Facility

    name = factory.Sequence(lambda n: "Fac %d" % n)


class StoreModelFacilityFactory(factory.DjangoModelFactory):

    class Meta:
        model = Store

    model_name = "facility"
    profile = "facilitydata"
    last_saved_instance = uuid.uuid4().hex
    last_saved_counter = 1
    dirty_bit = True


class SerializeIntoStoreTestCase(TestCase):

    def setUp(self):
        InstanceIDModel.get_or_create_current_instance()
        self.range = 10
        self.mc = MorangoProfileController('facilitydata')
        self.original_name = "ralphie"
        self.new_name = "rafael"

    def test_all_models_get_serialized(self):
        [FacilityModelFactory() for _ in range(self.range)]
        self.mc.serialize_into_store()
        self.assertEqual(len(Store.objects.all()), self.range)

    def test_no_models_get_serialized(self):
        # set dirty bit off on new models created
        [FacilityModelFactory.build().save(update_dirty_bit_to=False) for _ in range(self.range)]
        # only models with dirty bit on should be serialized
        self.mc.serialize_into_store()
        self.assertFalse(Store.objects.exists())

    def test_dirty_bit_gets_set(self):
        [FacilityModelFactory() for _ in range(self.range)]
        # dirty bit should be on
        for facility in Facility.objects.all():
            self.assertTrue(facility._morango_dirty_bit)

        self.mc.serialize_into_store()
        # dirty bit should have been toggled off
        for facility in Facility.objects.all():
            self.assertFalse(facility._morango_dirty_bit)

    def test_store_models_get_updated(self):
        FacilityModelFactory(name=self.original_name)
        self.mc.serialize_into_store()
        store_facility = Store.objects.first()
        deserialized_model = json.loads(store_facility.serialized)
        self.assertEqual(deserialized_model['name'], self.original_name)

        Facility.objects.update(name=self.new_name)
        self.mc.serialize_into_store()
        store_facility = Store.objects.first()
        deserialized_model = json.loads(store_facility.serialized)
        self.assertEqual(deserialized_model['name'], self.new_name)

    def test_last_saved_counter_updates(self):
        FacilityModelFactory(name=self.original_name)
        self.mc.serialize_into_store()
        old_counter = Store.objects.first().last_saved_counter

        Facility.objects.all().update(name=self.new_name)
        self.mc.serialize_into_store()
        new_counter = Store.objects.first().last_saved_counter

        self.assertEqual(old_counter + 1, new_counter)

    def test_last_saved_instance_updates(self):
        FacilityModelFactory(name=self.original_name)
        self.mc.serialize_into_store()
        old_instance_id = Store.objects.first().last_saved_instance

        with mock.patch('platform.platform', return_value='Windows'):
            (new_id, _) = InstanceIDModel.get_or_create_current_instance()

        Facility.objects.all().update(name=self.new_name)
        self.mc.serialize_into_store()
        new_instance_id = Store.objects.first().last_saved_instance

        self.assertNotEqual(old_instance_id, new_instance_id)
        self.assertEqual(new_instance_id, new_id.id)

    def test_extra_fields_dont_get_overwritten(self):
        serialized = """{"username": "deadbeef", "height": 6.0, "weight": 100}"""
        MyUser.objects.create(username='deadbeef')
        self.mc.serialize_into_store()
        Store.objects.update(serialized=serialized)

        MyUser.objects.update(username='alivebeef')
        self.mc.serialize_into_store()
        serialized = json.loads(Store.objects.first().serialized)
        self.assertIn('height', serialized)

    def test_updates_store_deleted_flag(self):
        fac = FacilityModelFactory()
        fac_id = fac.id
        self.mc.serialize_into_store()
        self.assertFalse(Store.objects.get(pk=fac_id).deleted)
        fac.delete()
        self.assertTrue(DeletedModels.objects.exists())
        self.mc.serialize_into_store()
        self.assertFalse(DeletedModels.objects.exists())
        self.assertTrue(Store.objects.get(pk=fac_id).deleted)

    def test_cascading_delete_updates_store_deleted_flag(self):
        fac = FacilityModelFactory()
        child = FacilityModelFactory(parent_id=fac.id)
        child_id = child.id
        self.mc.serialize_into_store()
        self.assertFalse(Store.objects.get(pk=child_id).deleted)
        fac.delete()
        self.mc.serialize_into_store()
        self.assertTrue(Store.objects.get(pk=child_id).deleted)

    def test_conflicting_data_appended(self):
        self.maxDiff = None
        serialized = json.dumps({"username": "deadb\neef"})
        conflicting = []
        user = MyUser.objects.create(username="user")
        self.mc.serialize_into_store()

        # add serialized fields to conflicting data
        conflicting.insert(0, serialized)
        conflicting.insert(0, json.dumps(user.serialize()))

        # set store record and app record dirty bits to true to force serialization merge conflict
        Store.objects.update(conflicting_serialized_data=serialized, dirty_bit=True)
        user.username = "user1"
        user.save(update_dirty_bit_to=True)
        self.mc.serialize_into_store()

        # assert we have placed serialized object into store's serialized field
        st = Store.objects.get(id=user.id)
        self.assertEqual(json.loads(st.serialized), user.serialize())

        # assert store serialized field is moved to conflicting data
        conflicting_serialized_data = st.conflicting_serialized_data.split('\n')
        for x in range(len(conflicting)):
            self.assertEqual(conflicting[x], conflicting_serialized_data[x])

    def test_filtered_serialization_single_filter(self):
        fac = FacilityModelFactory()
        user = MyUser.objects.create(username='deadbeef')
        log = SummaryLog.objects.create(user=user)
        self.mc.serialize_into_store(filter=Filter(user._morango_partition))
        self.assertFalse(Store.objects.filter(id=fac.id).exists())
        self.assertTrue(Store.objects.filter(id=user.id).exists())
        self.assertTrue(Store.objects.filter(id=log.id).exists())

    def test_filtered_serialization_multiple_filter(self):
        fac = FacilityModelFactory()
        user = MyUser.objects.create(username='deadbeef')
        user2 = MyUser.objects.create(username='alivebeef')
        log = SummaryLog.objects.create(user=user)
        self.mc.serialize_into_store(filter=Filter(user._morango_partition + "\n" + user2._morango_partition))
        self.assertFalse(Store.objects.filter(id=fac.id).exists())
        self.assertTrue(Store.objects.filter(id=user2.id).exists())
        self.assertTrue(Store.objects.filter(id=user.id).exists())
        self.assertTrue(Store.objects.filter(id=log.id).exists())

    def test_self_ref_fk_class_adds_value_to_store(self):
        root = FacilityModelFactory()
        child = FacilityModelFactory(parent=root)
        self.mc.serialize_into_store()
        self.assertEqual(Store.objects.get(id=child.id)._self_ref_fk, root.id)

    def test_regular_class_leaves_value_blank_in_store(self):
        log = SummaryLog.objects.create(user=MyUser.objects.create(username='user'))
        self.mc.serialize_into_store()
        self.assertEqual(Store.objects.get(id=log.id)._self_ref_fk, '')

    def test_previously_deleted_store_flag_resets(self):
        # create and delete object
        user = MyUser.objects.create(username='user')
        self.mc.serialize_into_store()
        MyUser.objects.all().delete()
        self.mc.serialize_into_store()
        self.assertTrue(Store.objects.get(id=user.id).deleted)
        # recreate object with same id
        user = MyUser.objects.create(username='user')
        # ensure deleted flag is updated after recreation
        self.mc.serialize_into_store()
        self.assertFalse(Store.objects.get(id=user.id).deleted)

    def test_hard_delete_wipes_serialized(self):
        user = MyUser.objects.create(username='user')
        log = SummaryLog.objects.create(user=user)
        self.mc.serialize_into_store()
        Store.objects.update(conflicting_serialized_data='store')
        st = Store.objects.get(id=log.id)
        self.assertNotEqual(st.serialized, '')
        self.assertNotEqual(st.conflicting_serialized_data, '')
        user.delete(hard_delete=True)  # cascade hard delete
        self.mc.serialize_into_store()
        st.refresh_from_db()
        self.assertEqual(st.serialized, '')
        self.assertEqual(st.conflicting_serialized_data, '')

    def test_in_app_hard_delete_propagates(self):
        user = MyUser.objects.create(username='user')
        log_id = uuid.uuid4().hex
        log = SummaryLog(user=user, id=log_id)
        StoreModelFacilityFactory(model_name="user", id=user.id, serialized=json.dumps(user.serialize()))
        store_log = StoreModelFacilityFactory(model_name="contentsummarylog", id=log.id, serialized=json.dumps(log.serialize()))
        user.delete(hard_delete=True)
        # preps log to be hard_deleted
        self.mc.deserialize_from_store()
        # updates store log to be hard_deleted
        self.mc.serialize_into_store()
        store_log.refresh_from_db()
        self.assertTrue(store_log.hard_delete)
        self.assertEqual(store_log.serialized, '')

    def test_store_hard_delete_propagates(self):
        user = MyUser(username='user')
        user.save(update_dirty_bit_to=False)
        log = SummaryLog(user=user)
        log.save(update_dirty_bit_to=False)
        StoreModelFacilityFactory(model_name="user", id=user.id, serialized=json.dumps(user.serialize()), hard_delete=True, deleted=True)
        # make sure hard_delete propogates to related models even if they are not hard_deleted
        self.mc.deserialize_from_store()
        self.assertTrue(HardDeletedModels.objects.filter(id=log.id).exists())


class RecordMaxCounterUpdatesDuringSerialization(TestCase):

    def setUp(self):
        (self.current_id, _) = InstanceIDModel.get_or_create_current_instance()
        self.mc = MorangoProfileController('facilitydata')
        self.fac1 = FacilityModelFactory(name='school')
        self.mc.serialize_into_store()
        self.old_rmc = RecordMaxCounter.objects.first()

    def test_new_rmc_for_existing_model(self):
        with mock.patch('platform.platform', return_value='Windows'):
            (new_id, _) = InstanceIDModel.get_or_create_current_instance()

        Facility.objects.update(name='facility')
        self.mc.serialize_into_store()
        new_rmc = RecordMaxCounter.objects.get(instance_id=new_id.id, store_model_id=self.fac1.id)
        new_store_record = Store.objects.get(id=self.fac1.id)

        self.assertEqual(new_rmc.counter, new_store_record.last_saved_counter)
        self.assertEqual(new_rmc.instance_id, new_store_record.last_saved_instance)

    def test_update_rmc_for_existing_model(self):
        Facility.objects.update(name='facility')
        self.mc.serialize_into_store()

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
        self.mc.serialize_into_store()
        new_rmc = RecordMaxCounter.objects.get(instance_id=new_id.id, store_model_id=new_fac.id)
        new_store_record = Store.objects.get(id=new_fac.id)

        self.assertNotEqual(new_id.id, self.current_id.id)
        self.assertEqual(new_store_record.last_saved_instance, new_rmc.instance_id)
        self.assertEqual(new_store_record.last_saved_counter, new_rmc.counter)


class DeserializationFromStoreIntoAppTestCase(TestCase):

    def setUp(self):
        (self.current_id, _) = InstanceIDModel.get_or_create_current_instance()
        self.range = 10
        self.mc = MorangoProfileController('facilitydata')
        for i in range(self.range):
            self.ident = uuid.uuid4().hex
            StoreModelFacilityFactory(pk=self.ident, serialized=serialized_facility_factory(self.ident))

    def test_dirty_store_records_are_deserialized(self):
        self.assertFalse(Facility.objects.all().exists())
        self.mc.deserialize_from_store()
        self.assertEqual(len(Facility.objects.all()), self.range)

    def test_clean_store_records_do_not_get_deserialized(self):
        self.assertFalse(Facility.objects.exists())
        Store.objects.update(dirty_bit=False)
        self.mc.deserialize_from_store()
        self.assertFalse(Facility.objects.exists())

    def test_deleted_models_do_not_get_deserialized(self):
        Store.objects.update_or_create(defaults={'deleted': True}, id=self.ident)
        self.mc.deserialize_from_store()
        self.assertFalse(Facility.objects.filter(id=self.ident).exists())

    def test_deleted_models_deletes_them_in_app(self):
        # put models in app layer
        self.mc.deserialize_from_store()

        # deleted flag on store should delete model in app layer
        Store.objects.update_or_create(defaults={'deleted': True, 'dirty_bit': True}, id=self.ident)
        self.mc.deserialize_from_store()
        self.assertFalse(Facility.objects.filter(id=self.ident).exists())

    def test_update_app_with_newer_data_from_store(self):
        name = 'test'
        fac = FacilityModelFactory(id=self.ident, name=name)
        fac.save(update_dirty_bit_to=False)
        self.assertEqual(fac.name, name)

        self.mc.deserialize_from_store()
        fac = Facility.objects.get(id=self.ident)
        self.assertNotEqual(fac.name, name)

    def test_handle_extra_field_deserialization(self):
        # modify a store record by adding extra serialized field
        store_model = Store.objects.get(id=self.ident)
        serialized = json.loads(store_model.serialized)
        serialized.update({'wacky': True})
        store_model.serialized = json.dumps(serialized)
        store_model.save()

        # deserialize records
        self.mc.deserialize_from_store()

        # by this point no errors should have occurred but we check list of fields anyways
        fac = Facility.objects.get(id=self.ident)
        self.assertNotIn('wacky', fac.__dict__)

    def test_store_dirty_bit_resets(self):
        self.assertTrue(Store.objects.filter(dirty_bit=True))
        self.mc.deserialize_from_store()
        self.assertFalse(Store.objects.filter(dirty_bit=True))

    def test_record_with_dirty_bit_off_doesnt_deserialize(self):
        st = Store.objects.first()
        st.dirty_bit = False
        st.save()
        self.mc.deserialize_from_store()
        self.assertFalse(Facility.objects.filter(id=st.id).exists())

    def test_broken_fk_leaves_store_dirty_bit(self):
        serialized = """{"user_id": "40de9a3fded95d7198f200c78e559353", "id": "bd205b5ee5bc42da85925d24c61341a8"}"""
        st = StoreModelFacilityFactory(id=uuid.uuid4().hex, serialized=serialized, model_name="contentsummarylog")
        self.mc.deserialize_from_store()
        st.refresh_from_db()
        self.assertTrue(st.dirty_bit)

    def test_invalid_model_leaves_store_dirty_bit(self):
        user = MyUser.objects.create(username='a'*21)
        st = StoreModelFacilityFactory(model_name="user", id=user.id, serialized=json.dumps(user.serialize()))
        self.mc.deserialize_from_store()
        st.refresh_from_db()
        self.assertTrue(st.dirty_bit)


class SelfReferentialFKDeserializationTestCase(TestCase):

    def setUp(self):
        (self.current_id, _) = InstanceIDModel.get_or_create_current_instance()
        self.mc = MorangoProfileController('facilitydata')

    def test_self_ref_fk(self):
        self.assertEqual(_self_referential_fk(Facility), 'parent_id')
        self.assertEqual(_self_referential_fk(MyUser), None)

    def test_delete_model_in_store_deletes_models_in_app(self):
        root = FacilityModelFactory()
        child1 = FacilityModelFactory(parent=root)
        child2 = FacilityModelFactory(parent=root)
        self.mc.serialize_into_store()
        # simulate a node being deleted and synced
        Store.objects.filter(id=child2.id).update(deleted=True)
        Store.objects.update(dirty_bit=True)
        grandchild1 = FacilityModelFactory(parent=child2)
        grandchild2 = FacilityModelFactory(parent=child2)

        self.mc.deserialize_from_store()
        # ensure tree structure in app layer is correct
        child1 = Facility.objects.filter(id=child1.id)
        self.assertTrue(child1.exists())
        self.assertEqual(child1[0].parent_id, root.id)
        self.assertFalse(Facility.objects.filter(id=child2.id).exists())
        self.assertFalse(Facility.objects.filter(id=grandchild1.id).exists())
        self.assertFalse(Facility.objects.filter(id=grandchild2.id).exists())

    def test_models_created_successfully(self):
        root = FacilityModelFactory()
        child1 = FacilityModelFactory(parent=root)
        child2 = FacilityModelFactory(parent=root)
        self.mc.serialize_into_store()
        Facility.objects.all().delete()
        DeletedModels.objects.all().delete()
        Store.objects.update(dirty_bit=True, deleted=False)

        self.mc.deserialize_from_store()
        # ensure tree structure in app layer is correct
        self.assertTrue(Facility.objects.filter(id=root.id).exists())
        child1 = Facility.objects.filter(id=child1.id)
        self.assertTrue(child1.exists())
        self.assertEqual(child1[0].parent_id, root.id)
        child2 = Facility.objects.filter(id=child2.id)
        self.assertTrue(child2.exists())
        self.assertEqual(child2[0].parent_id, root.id)
