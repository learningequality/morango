from django.test import TestCase
from facility_profile.models import MyUser
from morango.models import SyncableModelManager, SyncableModelQuerySet


class SyncingModelsTestCase(TestCase):

    def setUp(self):
        MyUser.objects.create(username='beans')

    def test_syncable_manager_inheritance(self):
        self.assertTrue(isinstance(MyUser.objects, SyncableModelManager))

    def test_syncable_qs_inheritance(self):
        self.assertTrue(isinstance(MyUser.objects.all(), SyncableModelQuerySet))

    def test_syncable_manager_update(self):
        self.assertTrue(MyUser.objects.first()._dirty_bit)
        MyUser.objects.update(update_dirty_bit_to=False)
        self.assertFalse(MyUser.objects.first()._dirty_bit)
        MyUser.objects.update(update_dirty_bit_to=None)
        self.assertFalse(MyUser.objects.first()._dirty_bit)
        MyUser.objects.update(update_dirty_bit_to=True)
        self.assertTrue(MyUser.objects.first()._dirty_bit)

    def test_syncable_qs_update(self):
        self.assertTrue(MyUser.objects.first()._dirty_bit)
        MyUser.objects.all().update(update_dirty_bit_to=False)
        self.assertFalse(MyUser.objects.first()._dirty_bit)
        MyUser.objects.all().update(update_dirty_bit_to=None)
        self.assertFalse(MyUser.objects.first()._dirty_bit)
        MyUser.objects.all().update(update_dirty_bit_to=True)
        self.assertTrue(MyUser.objects.first()._dirty_bit)

    def test_syncable_save(self):
        user = MyUser.objects.first()
        self.assertTrue(user._dirty_bit)
        user.save(update_dirty_bit_to=False)
        self.assertFalse(MyUser.objects.first()._dirty_bit)
        user.save(update_dirty_bit_to=None)
        self.assertFalse(MyUser.objects.first()._dirty_bit)
        user.save(update_dirty_bit_to=True)
        self.assertTrue(MyUser.objects.first()._dirty_bit)