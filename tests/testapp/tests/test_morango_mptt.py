from django.test import TestCase
from facility_profile.models import Facility, MyUser
from morango.utils.morango_mptt import MorangoMPTTTreeManager, MorangoTreeQuerySet


class MorangoMPTTModelTestCase(TestCase):

    def setUp(self):
        Facility.objects.create(name='beans')

    def test_mptt_manager_inheritance(self):
        self.assertTrue(isinstance(Facility.objects, MorangoMPTTTreeManager))
        self.assertFalse(isinstance(MyUser.objects, MorangoMPTTTreeManager))

    def test_mptt_qs_inheritance(self):
        self.assertTrue(isinstance(Facility.objects.all(), MorangoTreeQuerySet))
        self.assertFalse(isinstance(MyUser.objects.all(), MorangoTreeQuerySet))

    def test_mptt_manager_update(self):
        self.assertTrue(Facility.objects.first()._morango_dirty_bit)
        Facility.objects.update(update_dirty_bit_to=False)
        self.assertFalse(Facility.objects.first()._morango_dirty_bit)
        Facility.objects.update(update_dirty_bit_to=None)
        self.assertFalse(Facility.objects.first()._morango_dirty_bit)
        Facility.objects.update(update_dirty_bit_to=True)
        self.assertTrue(Facility.objects.first()._morango_dirty_bit)
        Facility.objects.update(update_dirty_bit_to=False)
        self.assertFalse(Facility.objects.first()._morango_dirty_bit)
        Facility.objects.update()
        self.assertTrue(Facility.objects.first()._morango_dirty_bit)
        Facility.objects.update(update_dirty_bit_to=None)
        self.assertTrue(Facility.objects.first()._morango_dirty_bit)

    def test_mptt_qs_update(self):
        self.assertTrue(Facility.objects.first()._morango_dirty_bit)
        Facility.objects.all().update(update_dirty_bit_to=False)
        self.assertFalse(Facility.objects.first()._morango_dirty_bit)
        Facility.objects.all().update(update_dirty_bit_to=None)
        self.assertFalse(Facility.objects.first()._morango_dirty_bit)
        Facility.objects.all().update(update_dirty_bit_to=True)
        self.assertTrue(Facility.objects.first()._morango_dirty_bit)
        Facility.objects.all().update(update_dirty_bit_to=False)
        self.assertFalse(Facility.objects.first()._morango_dirty_bit)
        Facility.objects.all().update()
        self.assertTrue(Facility.objects.first()._morango_dirty_bit)
        Facility.objects.all().update(update_dirty_bit_to=None)
        self.assertTrue(Facility.objects.first()._morango_dirty_bit)

    def test_mptt_save(self):
        fac = Facility.objects.first()
        self.assertTrue(fac._morango_dirty_bit)
        fac.save(update_dirty_bit_to=False)
        self.assertFalse(Facility.objects.first()._morango_dirty_bit)
        fac.save(update_dirty_bit_to=None)
        self.assertFalse(Facility.objects.first()._morango_dirty_bit)
        fac.save(update_dirty_bit_to=True)
        self.assertTrue(Facility.objects.first()._morango_dirty_bit)
        fac.save(update_dirty_bit_to=False)
        self.assertFalse(Facility.objects.first()._morango_dirty_bit)
        fac.save()
        self.assertTrue(Facility.objects.first()._morango_dirty_bit)
        fac.save(update_dirty_bit_to=None)
        self.assertTrue(Facility.objects.first()._morango_dirty_bit)

    def test_new_mptt_update(self):
        self.assertTrue(Facility.objects.first()._morango_dirty_bit)
        Facility.objects.rebuild()  # calls _mptt_update
        self.assertTrue(Facility.objects.first()._morango_dirty_bit)
        Facility.objects.update(update_dirty_bit_to=False)
        self.assertFalse(Facility.objects.first()._morango_dirty_bit)
        Facility.objects.rebuild()  # calls _mptt_update
        self.assertFalse(Facility.objects.first()._morango_dirty_bit)
