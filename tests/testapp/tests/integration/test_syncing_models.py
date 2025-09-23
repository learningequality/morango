import json

from django.test import TestCase
from facility_profile.models import MyUser
from facility_profile.models import TestModel

from morango.models.core import Store
from morango.models.manager import SyncableModelManager
from morango.models.query import SyncableModelQuerySet
from morango.sync.controller import MorangoProfileController


class SyncingModelsTestCase(TestCase):

    def setUp(self):
        MyUser.objects.create(username='beans')

    def test_syncable_manager_inheritance(self):
        self.assertTrue(isinstance(MyUser.objects, SyncableModelManager))

    def test_syncable_qs_inheritance(self):
        self.assertTrue(isinstance(MyUser.objects.all(), SyncableModelQuerySet))

    def test_syncable_manager_update(self):
        self.assertTrue(MyUser.objects.first()._morango_dirty_bit)
        MyUser.objects.update(update_dirty_bit_to=False)
        self.assertFalse(MyUser.objects.first()._morango_dirty_bit)
        MyUser.objects.update(update_dirty_bit_to=None)
        self.assertFalse(MyUser.objects.first()._morango_dirty_bit)
        MyUser.objects.update(update_dirty_bit_to=True)
        self.assertTrue(MyUser.objects.first()._morango_dirty_bit)
        MyUser.objects.update(update_dirty_bit_to=False)
        self.assertFalse(MyUser.objects.first()._morango_dirty_bit)
        MyUser.objects.update()
        self.assertTrue(MyUser.objects.first()._morango_dirty_bit)
        MyUser.objects.update(update_dirty_bit_to=None)
        self.assertTrue(MyUser.objects.first()._morango_dirty_bit)

    def test_syncable_qs_update(self):
        self.assertTrue(MyUser.objects.first()._morango_dirty_bit)
        MyUser.objects.all().update(update_dirty_bit_to=False)
        self.assertFalse(MyUser.objects.first()._morango_dirty_bit)
        MyUser.objects.all().update(update_dirty_bit_to=None)
        self.assertFalse(MyUser.objects.first()._morango_dirty_bit)
        MyUser.objects.all().update(update_dirty_bit_to=True)
        self.assertTrue(MyUser.objects.first()._morango_dirty_bit)
        MyUser.objects.all().update(update_dirty_bit_to=False)
        self.assertFalse(MyUser.objects.first()._morango_dirty_bit)
        MyUser.objects.all().update()
        self.assertTrue(MyUser.objects.first()._morango_dirty_bit)
        MyUser.objects.all().update(update_dirty_bit_to=None)
        self.assertTrue(MyUser.objects.first()._morango_dirty_bit)

    def test_syncable_save(self):
        user = MyUser.objects.first()
        self.assertTrue(user._morango_dirty_bit)
        user.save(update_dirty_bit_to=False)
        self.assertFalse(MyUser.objects.first()._morango_dirty_bit)
        user.save(update_dirty_bit_to=None)
        self.assertFalse(MyUser.objects.first()._morango_dirty_bit)
        user.save(update_dirty_bit_to=True)
        self.assertTrue(MyUser.objects.first()._morango_dirty_bit)
        user.save(update_dirty_bit_to=False)
        self.assertFalse(MyUser.objects.first()._morango_dirty_bit)
        user.save()
        self.assertTrue(MyUser.objects.first()._morango_dirty_bit)
        user.save(update_dirty_bit_to=None)
        self.assertTrue(MyUser.objects.first()._morango_dirty_bit)

    def test_syncing_objects_manager_with_custom_default_manager(self):
        """Test that syncing_objects manager includes all objects even when default manager filters them out"""
        # Create some test objects
        TestModel.objects.create(name="visible", hidden=False)
        hidden_obj = TestModel(name="hidden", hidden=True)
        # Use save() to bypass the manager filter during creation
        hidden_obj.save()

        # Verify that the default objects manager filters out hidden objects
        self.assertEqual(TestModel.objects.count(), 1)
        self.assertEqual(TestModel.objects.first().name, "visible")

        # Verify that syncing_objects manager includes all objects for syncing
        self.assertEqual(TestModel.syncing_objects.count(), 2)
        syncing_names = set(TestModel.syncing_objects.values_list('name', flat=True))
        self.assertEqual(syncing_names, {"visible", "hidden"})

    def test_hidden_models_serialization_into_store(self):
        """Test that hidden models (filtered by default manager) still get serialized into Store"""
        controller = MorangoProfileController(TestModel.morango_profile)

        # Create both visible and hidden objects
        visible_obj = TestModel.objects.create(name="visible", hidden=False)
        hidden_obj = TestModel(name="hidden", hidden=True)
        # Use save() to bypass the manager filter during creation
        hidden_obj.save()

        # Serialize into store
        controller.serialize_into_store()

        # Verify both objects (visible and hidden) are serialized into Store
        store_records = Store.objects.filter(model_name=TestModel.morango_model_name)
        self.assertEqual(store_records.count(), 2)

        # Verify both objects are present in store by checking serialized data
        serialized_names = set()
        for store_record in store_records:
            serialized_data = json.loads(store_record.serialized)
            serialized_names.add(serialized_data['name'])

        self.assertEqual(serialized_names, {"visible", "hidden"})

        # Verify the store records have the correct IDs
        store_ids = set(store_records.values_list('id', flat=True))
        expected_ids = {str(visible_obj.id), str(hidden_obj.id)}
        self.assertEqual(store_ids, expected_ids)

    def test_hidden_models_deletion_during_deserialization(self):
        """Test that hidden models can be deleted during deserialization using syncing_objects"""
        controller = MorangoProfileController(TestModel.morango_profile)

        # Create and serialize a hidden object
        hidden_obj = TestModel(name="hidden", hidden=True)
        hidden_obj.save()
        controller.serialize_into_store()

        # Verify object exists in Store
        store_record = Store.objects.get(id=hidden_obj.id)
        self.assertFalse(store_record.deleted)

        # Mark the store record as deleted (simulating deletion from another device)
        # Also set dirty_bit=True so it gets processed during deserialization
        store_record.deleted = True
        store_record.dirty_bit = True
        store_record.save()

        # Deserialize from store - this should delete the hidden object
        controller.deserialize_from_store()

        # Verify the hidden object was deleted from the database
        self.assertFalse(TestModel.syncing_objects.filter(id=hidden_obj.id).exists())
        self.assertFalse(TestModel.objects.filter(id=hidden_obj.id).exists())

        # The store record should still exist but marked as deleted
        self.assertTrue(Store.objects.filter(id=hidden_obj.id, deleted=True).exists())
