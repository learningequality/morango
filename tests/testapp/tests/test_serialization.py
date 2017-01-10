from django.db import models
from django.test import TestCase
from facility_profile.models import Facility
from morango.utils.syncing_utils import _syncing_models


class SerializationTestCase(TestCase):

    def setUp(self):
        self.bob = Facility.objects.create(name="bob")
        self.dob = Facility.objects.create(name="dob")
        self.bob.id = self.bob.id.hex
        self.dob.id = self.dob.id.hex
        self.bob_dict = self.bob.serialize()
        self.dob_dict = self.dob.serialize()

    def test_serialization(self):
        self.assertEqual(self.bob_dict['name'], 'bob')
        self.assertEqual(self.bob.morango_model_name, Facility.morango_model_name)

    def test_field_deserialization(self):
        class_model = _syncing_models[self.bob.morango_model_name]
        self.bob_copy = class_model.deserialize(self.bob_dict)
        for f in Facility._meta.concrete_fields:
            if isinstance(f, models.DateTimeField):
                continue
            self.assertEqual(getattr(self.bob, f.attname), getattr(self.bob_copy, f.attname))

    def test_serializing_different_models(self):
        self.assertNotEqual(self.bob_dict['id'], self.dob_dict['id'])
        self.assertNotEqual(self.bob_dict['name'], self.dob_dict['name'])

    def test_fields_not_to_serialize(self):
        self.assertTrue('now_date' in self.bob_dict)
        Facility._fields_not_to_serialize = ("now_date",)
        self.bob_dict = self.bob.serialize()
        self.assertFalse('now_data' in self.bob_dict)
