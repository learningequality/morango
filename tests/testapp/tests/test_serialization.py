from django.db import models
from django.test import TestCase
from facility_profile.models import Facility
from morango.utils.register_models import _profile_models


class SerializationTestCase(TestCase):

    def setUp(self):
        self.bob = Facility.objects.create(name="bob")
        self.student = Facility.objects.create(name="student")
        self.bob_dict = self.bob.serialize()
        self.student_dict = self.student.serialize()

    def test_serialization(self):
        self.assertEqual(self.bob_dict['name'], 'bob')
        self.assertEqual(self.bob.morango_model_name, Facility.morango_model_name)

    def test_field_deserialization(self):
        class_model = _profile_models['facilitydata'][self.bob.morango_model_name]
        self.bob_copy = class_model.deserialize(self.bob_dict)
        for f in Facility._meta.concrete_fields:
            # we remove DateTimeField (for now) from this test because serializing and deserializing loses units of precision
            if isinstance(f, models.DateTimeField) or \
                    f.attname in class_model._internal_mptt_fields_not_to_serialize or \
                    f.attname in class_model._morango_internal_fields_not_to_serialize:
                continue
            self.assertEqual(getattr(self.bob, f.attname), getattr(self.bob_copy, f.attname))

    def test_serializing_different_models(self):
        self.assertNotEqual(self.bob_dict['id'], self.student_dict['id'])
        self.assertNotEqual(self.bob_dict['name'], self.student_dict['name'])

    def test_fields_not_to_serialize(self):
        self.assertTrue('now_date' in self.bob_dict)
        Facility._fields_not_to_serialize = ("now_date",)
        self.bob_dict = self.bob.serialize()
        self.assertFalse('now_data' in self.bob_dict)
