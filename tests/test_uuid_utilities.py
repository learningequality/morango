import mock

from django.test import TestCase
from morango.utils.uuids import DatabaseIDModel, InstanceIDModel, UUIDModelMixin


class MorangoUUIDUtilsTestCase(TestCase):

    def test_creating_same_instance_ID_model(self):
        DatabaseIDModel.objects.create()
        InstanceIDModel.get_or_create_current_instance()
        firstIDModel = InstanceIDModel.objects.first()
        (secondIDModel, _) = InstanceIDModel.get_or_create_current_instance()
        self.assertEqual(firstIDModel, secondIDModel)
        self.assertEqual(InstanceIDModel.objects.count(), 1)

    def test_creating_different_instance_ID_model(self):
        DatabaseIDModel.objects.create()
        InstanceIDModel.get_or_create_current_instance()
        # change system state
        with mock.patch('platform.platform', return_value='platform'):
            with mock.patch('uuid.getnode', return_value=9999999999999):  # fake (random) address
                (IDModel, _) = InstanceIDModel.get_or_create_current_instance()
        self.assertEqual(InstanceIDModel.objects.count(), 2)
        self.assertEqual(IDModel.macaddress, '')  # assert that macaddress was not added

    def test_uuid_model_mixin(self):
        # create random object to inherit from UUIDModelMixin
        childClass = type('ChildClass', (UUIDModelMixin,), {'__module__': 'morango', 'dummy': 1})
        child = childClass()

        child.uuid_input_fields = 'RANDOM'
        with mock.patch('uuid.uuid4', return_value='random'):
            self.assertEqual(child.calculate_uuid(), 'random')

        child.uuid_input_fields = []
        with self.assertRaises(AssertionError):
            child.calculate_uuid()

        child.uuid_input_fields = ()
        with mock.patch('uuid.uuid4', return_value='random'):
            self.assertEqual(child.calculate_uuid(), 'random')

        child.uuid_input_fields = ('dummy',)
        with mock.patch('uuid.uuid5', return_value='DEADBEEF'):
            self.assertEqual(child.calculate_uuid(), 'DEADBEEF')
