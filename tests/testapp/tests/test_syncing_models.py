from django.test import TestCase
from facility_profile.models import MyUser
from morango.manager import SyncableModelManager
from morango.query import SyncableModelQuerySet


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
