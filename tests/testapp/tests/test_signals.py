import factory
from django.test import TestCase
from facility_profile.models import Facility
from morango.models import DeletedModels, InstanceIDModel
from morango.controller import MorangoProfileController


class FacilityModelFactory(factory.DjangoModelFactory):

    class Meta:
        model = Facility

    name = factory.Sequence(lambda n: "Fac %d" % n)


class PostDeleteSignalsTestCase(TestCase):

    def setUp(self):
        InstanceIDModel.get_or_create_current_instance()
        [FacilityModelFactory() for _ in range(10)]
        self.mc = MorangoProfileController('facilitydata')
        self.mc.serialize_into_store()

    def test_deleted_flag_gets_set(self):
        facility = Facility.objects.first()
        deleted_id = facility.id
        facility.delete()
        self.assertTrue(DeletedModels.objects.filter(id=deleted_id))

    def test_cascading_delete(self):
        facility = Facility.objects.first()
        child = FacilityModelFactory(parent=facility)
        deleted_child_id = child.id
        facility.delete()
        self.assertTrue(DeletedModels.objects.filter(id=deleted_child_id))
