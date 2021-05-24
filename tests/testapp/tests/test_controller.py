import json
import uuid
import contextlib

import factory
import mock
from django.test import SimpleTestCase
from django.test import TestCase
from facility_profile.models import Facility
from facility_profile.models import MyUser
from facility_profile.models import SummaryLog
from test.support import EnvironmentVarGuard

from .helpers import serialized_facility_factory
from morango.constants import transfer_stage
from morango.constants import transfer_status
from morango.models.certificates import Filter
from morango.models.core import DeletedModels
from morango.models.core import HardDeletedModels
from morango.models.core import InstanceIDModel
from morango.models.core import RecordMaxCounter
from morango.models.core import Store
from morango.sync.context import SessionContext
from morango.sync.controller import _self_referential_fk
from morango.sync.controller import MorangoProfileController
from morango.sync.controller import SessionController


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
        self.mc = MorangoProfileController("facilitydata")
        self.original_name = "ralphie"
        self.new_name = "rafael"

    def test_all_models_get_serialized(self):
        [FacilityModelFactory() for _ in range(self.range)]
        self.mc.serialize_into_store()
        self.assertEqual(len(Store.objects.all()), self.range)

    def test_no_models_get_serialized(self):
        # set dirty bit off on new models created
        [
            FacilityModelFactory.build().save(update_dirty_bit_to=False)
            for _ in range(self.range)
        ]
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
        self.assertEqual(deserialized_model["name"], self.original_name)

        Facility.objects.update(name=self.new_name)
        self.mc.serialize_into_store()
        store_facility = Store.objects.first()
        deserialized_model = json.loads(store_facility.serialized)
        self.assertEqual(deserialized_model["name"], self.new_name)

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

        with EnvironmentVarGuard() as env:
            env['MORANGO_SYSTEM_ID'] = 'new_sys_id'
            (new_id, _) = InstanceIDModel.get_or_create_current_instance(clear_cache=True)

            Facility.objects.all().update(name=self.new_name)
            self.mc.serialize_into_store()
            new_instance_id = Store.objects.first().last_saved_instance

        self.assertNotEqual(old_instance_id, new_instance_id)
        self.assertEqual(new_instance_id, new_id.id)

    def test_extra_fields_dont_get_overwritten(self):
        serialized = """{"username": "deadbeef", "height": 6.0, "weight": 100}"""
        MyUser.objects.create(username="deadbeef")
        self.mc.serialize_into_store()
        Store.objects.update(serialized=serialized)

        MyUser.objects.update(username="alivebeef")
        self.mc.serialize_into_store()
        serialized = json.loads(Store.objects.first().serialized)
        self.assertIn("height", serialized)

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
        conflicting_serialized_data = st.conflicting_serialized_data.split("\n")
        for x in range(len(conflicting)):
            self.assertEqual(conflicting[x], conflicting_serialized_data[x])

    def test_filtered_serialization_single_filter(self):
        fac = FacilityModelFactory()
        user = MyUser.objects.create(username="deadbeef")
        log = SummaryLog.objects.create(user=user)
        self.mc.serialize_into_store(filter=Filter(user._morango_partition))
        self.assertFalse(Store.objects.filter(id=fac.id).exists())
        self.assertTrue(Store.objects.filter(id=user.id).exists())
        self.assertTrue(Store.objects.filter(id=log.id).exists())

    def test_filtered_serialization_multiple_filter(self):
        fac = FacilityModelFactory()
        user = MyUser.objects.create(username="deadbeef")
        user2 = MyUser.objects.create(username="alivebeef")
        log = SummaryLog.objects.create(user=user)
        self.mc.serialize_into_store(
            filter=Filter(user._morango_partition + "\n" + user2._morango_partition)
        )
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
        log = SummaryLog.objects.create(user=MyUser.objects.create(username="user"))
        self.mc.serialize_into_store()
        self.assertEqual(Store.objects.get(id=log.id)._self_ref_fk, "")

    def test_previously_deleted_store_flag_resets(self):
        # create and delete object
        user = MyUser.objects.create(username="user")
        user_id = user.id
        self.mc.serialize_into_store()
        MyUser.objects.all().delete()
        self.mc.serialize_into_store()
        self.assertTrue(Store.objects.get(id=user_id).deleted)
        # recreate object with same id
        user = MyUser.objects.create(username="user")
        # ensure deleted flag is updated after recreation
        self.mc.serialize_into_store()
        self.assertFalse(Store.objects.get(id=user_id).deleted)

    def test_previously_hard_deleted_store_flag_resets(self):
        # create and delete object
        user = MyUser.objects.create(username="user")
        user_id = user.id
        self.mc.serialize_into_store()
        user.delete(hard_delete=True)
        self.mc.serialize_into_store()
        self.assertTrue(Store.objects.get(id=user_id).hard_deleted)
        # recreate object with same id
        user = MyUser.objects.create(username="user")
        # ensure hard deleted flag is updated after recreation
        self.mc.serialize_into_store()
        self.assertFalse(Store.objects.get(id=user_id).hard_deleted)

    def test_hard_delete_wipes_serialized(self):
        user = MyUser.objects.create(username="user")
        log = SummaryLog.objects.create(user=user)
        self.mc.serialize_into_store()
        Store.objects.update(conflicting_serialized_data="store")
        st = Store.objects.get(id=log.id)
        self.assertNotEqual(st.serialized, "")
        self.assertNotEqual(st.conflicting_serialized_data, "")
        user.delete(hard_delete=True)  # cascade hard delete
        self.mc.serialize_into_store()
        st.refresh_from_db()
        self.assertEqual(st.serialized, "{}")
        self.assertEqual(st.conflicting_serialized_data, "")

    def test_in_app_hard_delete_propagates(self):
        user = MyUser.objects.create(username="user")
        log_id = uuid.uuid4().hex
        log = SummaryLog(user=user, id=log_id)
        StoreModelFacilityFactory(
            model_name="user", id=user.id, serialized=json.dumps(user.serialize())
        )
        store_log = StoreModelFacilityFactory(
            model_name="contentsummarylog",
            id=log.id,
            serialized=json.dumps(log.serialize()),
        )
        user.delete(hard_delete=True)
        # preps log to be hard_deleted
        self.mc.deserialize_from_store()
        # updates store log to be hard_deleted
        self.mc.serialize_into_store()
        store_log.refresh_from_db()
        self.assertTrue(store_log.hard_deleted)
        self.assertEqual(store_log.serialized, "{}")

    def test_store_hard_delete_propagates(self):
        user = MyUser(username="user")
        user.save(update_dirty_bit_to=False)
        log = SummaryLog(user=user)
        log.save(update_dirty_bit_to=False)
        StoreModelFacilityFactory(
            model_name="user",
            id=user.id,
            serialized=json.dumps(user.serialize()),
            hard_deleted=True,
            deleted=True,
        )
        # make sure hard_deleted propagates to related models even if they are not hard_deleted
        self.mc.deserialize_from_store()
        self.assertTrue(HardDeletedModels.objects.filter(id=log.id).exists())


class RecordMaxCounterUpdatesDuringSerialization(TestCase):
    def setUp(self):
        (self.current_id, _) = InstanceIDModel.get_or_create_current_instance()
        self.mc = MorangoProfileController("facilitydata")
        self.fac1 = FacilityModelFactory(name="school")
        self.mc.serialize_into_store()
        self.old_rmc = RecordMaxCounter.objects.first()

    def test_new_rmc_for_existing_model(self):
        with EnvironmentVarGuard() as env:
            env['MORANGO_SYSTEM_ID'] = 'new_sys_id'
            (new_id, _) = InstanceIDModel.get_or_create_current_instance(clear_cache=True)

            Facility.objects.update(name="facility")
            self.mc.serialize_into_store()

        new_rmc = RecordMaxCounter.objects.get(
            instance_id=new_id.id, store_model_id=self.fac1.id
        )
        new_store_record = Store.objects.get(id=self.fac1.id)

        self.assertEqual(new_rmc.counter, new_store_record.last_saved_counter)
        self.assertEqual(new_rmc.instance_id, new_store_record.last_saved_instance)

    def test_update_rmc_for_existing_model(self):
        Facility.objects.update(name="facility")
        self.mc.serialize_into_store()

        # there should only be 1 RecordMaxCounter for a specific instance_id and a specific model (unique_together)
        self.assertEqual(
            RecordMaxCounter.objects.filter(
                instance_id=self.current_id.id, store_model_id=self.fac1.id
            ).count(),
            1,
        )

        new_rmc = RecordMaxCounter.objects.get(
            instance_id=self.current_id.id, store_model_id=self.fac1.id
        )
        new_store_record = Store.objects.get(id=self.fac1.id)

        self.assertEqual(self.old_rmc.counter + 1, new_rmc.counter)
        self.assertEqual(new_rmc.counter, new_store_record.last_saved_counter)
        self.assertEqual(new_rmc.instance_id, new_store_record.last_saved_instance)

    def test_new_rmc_for_non_existent_model(self):
        with EnvironmentVarGuard() as env:
            env['MORANGO_SYSTEM_ID'] = 'new_sys_id'
            (new_id, _) = InstanceIDModel.get_or_create_current_instance(clear_cache=True)

            new_fac = FacilityModelFactory(name="college")
            self.mc.serialize_into_store()

        new_rmc = RecordMaxCounter.objects.get(
            instance_id=new_id.id, store_model_id=new_fac.id
        )
        new_store_record = Store.objects.get(id=new_fac.id)

        self.assertNotEqual(new_id.id, self.current_id.id)
        self.assertEqual(new_store_record.last_saved_instance, new_rmc.instance_id)
        self.assertEqual(new_store_record.last_saved_counter, new_rmc.counter)


class DeserializationFromStoreIntoAppTestCase(TestCase):
    def setUp(self):
        (self.current_id, _) = InstanceIDModel.get_or_create_current_instance()
        self.range = 10
        self.mc = MorangoProfileController("facilitydata")
        for i in range(self.range):
            self.ident = uuid.uuid4().hex
            StoreModelFacilityFactory(
                pk=self.ident, serialized=serialized_facility_factory(self.ident)
            )

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
        Store.objects.update_or_create(defaults={"deleted": True}, id=self.ident)
        self.mc.deserialize_from_store()
        self.assertFalse(Facility.objects.filter(id=self.ident).exists())

    def test_deleted_models_deletes_them_in_app(self):
        # put models in app layer
        self.mc.deserialize_from_store()

        # deleted flag on store should delete model in app layer
        Store.objects.update_or_create(
            defaults={"deleted": True, "dirty_bit": True}, id=self.ident
        )
        self.mc.deserialize_from_store()
        self.assertFalse(Facility.objects.filter(id=self.ident).exists())

    def test_update_app_with_newer_data_from_store(self):
        name = "test"
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
        serialized.update({"wacky": True})
        store_model.serialized = json.dumps(serialized)
        store_model.save()

        # deserialize records
        self.mc.deserialize_from_store()

        # by this point no errors should have occurred but we check list of fields anyways
        fac = Facility.objects.get(id=self.ident)
        self.assertNotIn("wacky", fac.__dict__)

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
        st = StoreModelFacilityFactory(
            id=uuid.uuid4().hex, serialized=serialized, model_name="contentsummarylog"
        )
        self.mc.deserialize_from_store()
        st.refresh_from_db()
        self.assertTrue(st.dirty_bit)

    def test_invalid_model_leaves_store_dirty_bit(self):
        user = MyUser(username="a" * 21)
        st = StoreModelFacilityFactory(
            model_name="user",
            id=uuid.uuid4().hex,
            serialized=json.dumps(user.serialize()),
        )
        self.mc.deserialize_from_store()
        st.refresh_from_db()
        self.assertTrue(st.dirty_bit)

    def test_deleted_model_propagates_to_store_record(self):
        """
        It could be the case that we have two store records, one that is deleted and the other that has a fk pointing to the deleted record.
        When we deserialize, we want to ensure that the record with the fk pointer also gets the deleted flag set, while also not
        deserializing the data into a model.
        """
        # user will be deleted
        user = MyUser(username="user")
        user.save(update_dirty_bit_to=False)
        # log may be synced in from other device
        log = SummaryLog(user_id=user.id)
        log.id = log.calculate_uuid()
        StoreModelFacilityFactory(
            model_name="user",
            id=user.id,
            serialized=json.dumps(user.serialize()),
            deleted=True,
        )
        StoreModelFacilityFactory(
            model_name="contentsummarylog",
            id=log.id,
            serialized=json.dumps(log.serialize()),
        )
        # make sure delete propagates to store due to deleted foreign key
        self.mc.deserialize_from_store()
        # have to serialize to update deleted models
        self.mc.serialize_into_store()
        self.assertFalse(SummaryLog.objects.filter(id=log.id).exists())
        self.assertTrue(Store.objects.get(id=log.id).deleted)

    def test_hard_deleted_model_propagates_to_store_record(self):
        """
        It could be the case that we have two store records, one that is hard deleted and the other that has a fk pointing to the hard deleted record.
        When we deserialize, we want to ensure that the record with the fk pointer also gets the hard deleted flag set, while also not
        deserializing the data into a model.
        """
        # user will be deleted
        user = MyUser(username="user")
        user.save(update_dirty_bit_to=False)
        # log may be synced in from other device
        log = SummaryLog(user_id=user.id)
        log.id = log.calculate_uuid()
        StoreModelFacilityFactory(
            model_name="user",
            id=user.id,
            serialized=json.dumps(user.serialize()),
            deleted=True,
            hard_deleted=True,
        )
        StoreModelFacilityFactory(
            model_name="contentsummarylog",
            id=log.id,
            serialized=json.dumps(log.serialize()),
        )
        # make sure delete propagates to store due to deleted foreign key
        self.mc.deserialize_from_store()
        # have to serialize to update deleted models
        self.mc.serialize_into_store()
        self.assertFalse(SummaryLog.objects.filter(id=log.id).exists())
        self.assertTrue(Store.objects.get(id=log.id).hard_deleted)

    def _create_two_users_to_deserialize(self):
        user = MyUser(username="test", password="password")
        user2 = MyUser(username="test2", password="password")
        user.save()
        user2.save()
        self.mc.serialize_into_store()
        user.username = "changed"
        user2.username = "changed2"
        Store.objects.filter(id=user.id).update(serialized=json.dumps(user.serialize()), dirty_bit=True)
        Store.objects.filter(id=user2.id).update(serialized=json.dumps(user2.serialize()), dirty_bit=True)
        return user, user2

    def test_regular_model_deserialization(self):
        # deserialization should be able to handle multiple records
        user, user2 = self._create_two_users_to_deserialize()
        self.mc.deserialize_from_store()
        self.assertFalse(MyUser.objects.filter(username="test").exists())
        self.assertFalse(MyUser.objects.filter(username="test2").exists())
        self.assertTrue(MyUser.objects.filter(username="changed").exists())
        self.assertTrue(MyUser.objects.filter(username="changed2").exists())

    def test_filtered_deserialization(self):
        # filtered deserialization only impacts specific records
        user, user2 = self._create_two_users_to_deserialize()
        self.mc.deserialize_from_store(filter=Filter(user._morango_partition))
        self.assertFalse(MyUser.objects.filter(username="test").exists())
        self.assertTrue(MyUser.objects.filter(username="test2").exists())
        self.assertTrue(MyUser.objects.filter(username="changed").exists())
        self.assertFalse(MyUser.objects.filter(username="changed2").exists())


class SelfReferentialFKDeserializationTestCase(TestCase):
    def setUp(self):
        (self.current_id, _) = InstanceIDModel.get_or_create_current_instance()
        self.mc = MorangoProfileController("facilitydata")

    def test_self_ref_fk(self):
        self.assertEqual(_self_referential_fk(Facility), "parent_id")
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

    def test_deserialization_of_model_with_missing_parent(self):
        self._test_deserialization_of_model_with_missing_parent(correct_self_ref_fk=True)

    def test_deserialization_of_model_with_mismatched_self_ref_fk(self):
        self._test_deserialization_of_model_with_missing_parent(correct_self_ref_fk=False)

    def _test_deserialization_of_model_with_missing_parent(self, correct_self_ref_fk):
        root = FacilityModelFactory()
        child1 = FacilityModelFactory(parent=root)
        self.mc.serialize_into_store()

        new_child = Store.objects.get(id=child1.id)
        data = json.loads(new_child.serialized)
        new_child.id = data["id"] = "a" * 32
        data["parent_id"] = "b" * 32
        if correct_self_ref_fk:
            new_child._self_ref_fk = data["parent_id"]
        new_child.serialized = json.dumps(data)
        new_child.dirty_bit = True
        new_child.save()

        self.mc.deserialize_from_store()

        new_child.refresh_from_db()
        self.assertTrue(new_child.dirty_bit)
        self.assertIn("exist", new_child.deserialization_error)

    def test_deserialization_of_model_with_missing_foreignkey_referent(self):

        user = MyUser.objects.create(username="penguin")
        log = SummaryLog.objects.create(user=user)
        self.mc.serialize_into_store()

        new_log = Store.objects.get(id=log.id)
        data = json.loads(new_log.serialized)
        new_log.id = data["id"] = "f" * 32
        data["user_id"] = "e" * 32
        new_log.serialized = json.dumps(data)
        new_log.dirty_bit = True
        new_log.save()

        self.mc.deserialize_from_store()

        new_log.refresh_from_db()
        self.assertTrue(new_log.dirty_bit)
        self.assertIn("exist", new_log.deserialization_error)


class SessionControllerTestCase(SimpleTestCase):
    def setUp(self):
        super(SessionControllerTestCase, self).setUp()
        self.middleware = [
            mock.Mock(related_stage=stage)
            for stage, _ in transfer_stage.CHOICES
        ]
        self.context = SessionContext()
        self.controller = SessionController(self.middleware, self.context, False)

    @contextlib.contextmanager
    def _mock_invoke(self):
        with mock.patch('morango.sync.controller.SessionController._invoke_middleware') as invoke:
            yield invoke

    def test_proceed_to__passed_stage(self):
        self.context.update(stage=transfer_stage.CLEANUP)
        result = self.controller.proceed_to(transfer_stage.TRANSFERRING)
        self.assertEqual(transfer_status.COMPLETED, result)

    def test_proceed_to__in_progress(self):
        self.context.update(stage=transfer_stage.TRANSFERRING, stage_status=transfer_status.STARTED)
        result = self.controller.proceed_to(transfer_stage.TRANSFERRING)
        self.assertEqual(transfer_status.STARTED, result)

    def test_proceed_to__errored(self):
        self.context.update(stage=transfer_stage.TRANSFERRING, stage_status=transfer_status.ERRORED)
        result = self.controller.proceed_to(transfer_stage.TRANSFERRING)
        self.assertEqual(transfer_status.ERRORED, result)

    def test_proceed_to__executes_middleware__incrementally(self):
        self.context.update(stage=transfer_stage.SERIALIZING, stage_status=transfer_status.COMPLETED)
        with self._mock_invoke() as invoke:
            invoke.return_value = transfer_status.STARTED
            result = self.controller.proceed_to(transfer_stage.QUEUING)
            self.assertEqual(transfer_status.STARTED, result)
            self.assertEqual(1, len(invoke.call_args_list))
            call = invoke.call_args[0]
            self.assertEqual(transfer_stage.QUEUING, call[0].related_stage)

    def test_proceed_to__executes_middleware__all(self):
        self.context.update(stage=transfer_stage.SERIALIZING, stage_status=transfer_status.COMPLETED)
        with self._mock_invoke() as invoke:
            invoke.return_value = transfer_status.COMPLETED
            result = self.controller.proceed_to(transfer_stage.CLEANUP)
            self.assertEqual(transfer_status.COMPLETED, result)
            self.assertEqual(5, len(invoke.call_args_list))

    def test_invoke_middleware(self):
        pass
