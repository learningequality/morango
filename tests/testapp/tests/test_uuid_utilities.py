import hashlib
import sys
import uuid

import mock
from django.test import TestCase
from facility_profile.models import Facility
from facility_profile.models import InteractionLog
from facility_profile.models import MyUser

from .compat import EnvironmentVarGuard
from morango.errors import InvalidMorangoSourceId
from morango.models.core import DatabaseIDModel
from morango.models.core import InstanceIDModel
from morango.models.fields.uuids import sha2_uuid
from morango.models.utils import _calculate_0_4_uuid
from morango.models.utils import get_0_4_system_parameters
from morango.models.utils import get_0_5_mac_address
from morango.models.utils import get_0_5_system_id


class UUIDModelMixinTestCase(TestCase):
    def setUp(self):
        self.fac = Facility(name="bob")

    def test_calculate_uuid(self):
        log_with_random_id = InteractionLog(user=MyUser.objects.create(username="Test"))
        with mock.patch(
            "uuid.uuid4", return_value=uuid.UUID("12345678123456781234567812345678")
        ):
            target_uuid = sha2_uuid(
                log_with_random_id.calculate_partition(),
                "12345678123456781234567812345678",
                log_with_random_id.morango_model_name,
            )
            self.assertEqual(log_with_random_id.calculate_uuid(), target_uuid)

    def test_calculate_uuid__empty_source_id(self):
        with self.assertRaises(InvalidMorangoSourceId):
            # facility source calculated from name
            Facility.objects.create(name="")

    def test_save_with_id(self):
        ID = "11111111111111111111111111111111"
        self.fac.id = ID
        self.fac.calculate_uuid = mock.Mock()
        self.fac.save()

        self.assertFalse(self.fac.calculate_uuid.called)
        self.assertEqual(ID, Facility.objects.first().id)

    def test_save_without_id(self):
        ID = "40ce9a3fded95d7198f200c78e559353"
        self.fac.calculate_uuid = mock.Mock(return_value=ID)
        self.fac.save()

        self.assertTrue(self.fac.calculate_uuid.called)
        self.assertEqual(Facility.objects.first().id, ID)


class InstanceIDModelTestCase(TestCase):
    def setUp(self):
        InstanceIDModel.get_or_create_current_instance()

    def test_creating_same_instance_ID_model(self):
        firstIDModel = InstanceIDModel.objects.first()
        (secondIDModel, _) = InstanceIDModel.get_or_create_current_instance()

        self.assertEqual(firstIDModel, secondIDModel)
        self.assertEqual(InstanceIDModel.objects.count(), 1)

    def test_only_one_current_instance_ID(self):
        with mock.patch("platform.platform", return_value="platform"):
            InstanceIDModel.get_or_create_current_instance()
        self.assertEqual(len(InstanceIDModel.objects.filter(current=True)), 1)

    def test_same_node_id(self):
        with mock.patch(
            "uuid.getnode", return_value=67002173923623
        ):  # fake (random) address
            (IDModel, _) = InstanceIDModel.get_or_create_current_instance()
            ident = IDModel.id

        with mock.patch(
            "uuid.getnode", return_value=69002173923623
        ):  # fake (random) address
            (IDModel, _) = InstanceIDModel.get_or_create_current_instance()

        with mock.patch(
            "uuid.getnode", return_value=67002173923623
        ):  # fake (random) address
            (IDModel, _) = InstanceIDModel.get_or_create_current_instance()

        self.assertFalse(
            InstanceIDModel.objects.exclude(id=ident).filter(current=True).exists()
        )
        self.assertTrue(InstanceIDModel.objects.get(id=ident).current)

    @mock.patch("uuid.getnode", return_value=24359248572014)
    @mock.patch("platform.platform", return_value="Windows 3.1")
    @mock.patch("platform.node", return_value="myhost")
    @mock.patch("morango.models.utils._get_database_path", return_value="<dummypath>")
    def test_consistent_with_0_4_instance_id_calculation(self, *args):
        """
        This test ensures that we don't accidentally make changes that impact how we calculate
        the instance ID, in a way that would cause instance IDs to change when they shouldn't.
        """

        from morango.models.utils import _get_database_path

        sys.version = "2.7.333"

        DatabaseIDModel.objects.all().update(current=False)
        database_id = DatabaseIDModel.objects.create(
            id="6fe445b75cea11858c00fb97bdee8878", current=True
        ).id

        node_id = hashlib.sha1(
            "{}:{}".format(database_id, 24359248572014).encode("utf-8")
        ).hexdigest()[:20]

        target = {
            "platform": "Windows 3.1",
            "hostname": "myhost",
            "sysversion": "2.7.333",
            "node_id": node_id,
            "database_id": database_id,
            "db_path": _get_database_path(),
        }

        result = get_0_4_system_parameters(database_id)

        self.assertEqual(target, result)

        calculated_id = _calculate_0_4_uuid(result)

        self.assertEqual(calculated_id, "4480fda04236975d0895c0048b767647")

        InstanceIDModel.objects.all().delete()

        InstanceIDModel.objects.create(current=True, id=calculated_id, **result)

        instance, _ = InstanceIDModel.get_or_create_current_instance()

        self.assertEqual(calculated_id, instance.id)

    @mock.patch(
        "ifcfg.interfaces",
        return_value={"eth0": {"device": "eth0", "ether": "a0:aa:aa:aa:aa"}},
    )
    def test_consistent_0_5_instance_id(self, *args):
        """
        If this test fails, it means we've changed the way Instance IDs are calculated in an undesirable way.
        """

        with EnvironmentVarGuard() as env:
            env["MORANGO_SYSTEM_ID"] = "magicsysid"

            DatabaseIDModel.objects.all().update(current=False)
            DatabaseIDModel.objects.create(
                id="7fe445b75cea11858c00fb97bdee8878", current=True
            )

            self.assertEqual(get_0_5_system_id(), "54940f560a55bbf7d86b")
            self.assertEqual(get_0_5_mac_address(), "804f4c20d3b2b5a29b95")

            instance, _ = InstanceIDModel.get_or_create_current_instance(clear_cache=True)

            self.assertEqual(instance.id, "e45c06595d820f4581e0c82930359592")

    @mock.patch(
        "ifcfg.interfaces",
        return_value={"eth0": {"device": "eth0", "ether": "a0:aa:aa:aa:aa"}},
    )
    def test_envvar_overrides(self, *args):

        with EnvironmentVarGuard() as env:
            env["MORANGO_SYSTEM_ID"] = "magicsysid"
            env["MORANGO_NODE_ID"] = "magicnodeid"

            DatabaseIDModel.objects.all().update(current=False)
            database_id = DatabaseIDModel.objects.create(
                id="7fe445b75cea11858c00fb97bdee8878", current=True
            ).id

            system_id = get_0_5_system_id()
            node_id = get_0_5_mac_address()

            self.assertEqual(system_id, "54940f560a55bbf7d86b")
            self.assertEqual(node_id, "9ed21d0fb4dacfa4009d")

            instance, _ = InstanceIDModel.get_or_create_current_instance(clear_cache=True)

            self.assertEqual(instance.id, "9033c0cec24d8a8d906dcba416f77625")

            expected_id = sha2_uuid(database_id, system_id, node_id)

            self.assertEqual(instance.id, expected_id)

    @mock.patch(
        "ifcfg.interfaces",
        return_value={"eth0": {"device": "eth0", "ether": "a0:aa:aa:aa:aa"}},
    )
    def test_instance_id_caching(self, *args):
        """
        Ensure that the cache works but that clearing it works as well.
        """

        with EnvironmentVarGuard() as env:

            env["MORANGO_SYSTEM_ID"] = "oldmagicsysid"

            old_instance, created = InstanceIDModel.get_or_create_current_instance(clear_cache=True)
            self.assertTrue(created)

            env["MORANGO_SYSTEM_ID"] = "newmagicsysid"

            cached_instance, created = InstanceIDModel.get_or_create_current_instance()
            self.assertFalse(created)

            uncached_instance, created = InstanceIDModel.get_or_create_current_instance(clear_cache=True)
            self.assertTrue(created)

            recached_instance, created = InstanceIDModel.get_or_create_current_instance()
            self.assertFalse(created)

            self.assertEqual(old_instance.id, cached_instance.id)
            self.assertNotEqual(old_instance.id, uncached_instance.id)
            self.assertEqual(uncached_instance.id, recached_instance.id)


class DatabaseIDModelTestCase(TestCase):
    def setUp(self):
        self.ID = "40ce9a3fded95d7198f200c78e559353"

    def test_save(self):
        [DatabaseIDModel().save() for _ in range(10)]
        current_id = DatabaseIDModel()
        current_id.calculate_uuid = mock.Mock(return_value=self.ID)
        current_id.save()

        db_models = DatabaseIDModel.objects.filter(current=True)
        self.assertTrue(len(db_models), 1)
        self.assertTrue(db_models[0].id, self.ID)

    def test_manager_create(self):
        [DatabaseIDModel.objects.create() for _ in range(10)]
        DatabaseIDModel.objects.create(id=self.ID)

        db_models = DatabaseIDModel.objects.filter(current=True)
        self.assertTrue(len(db_models), 1)
        self.assertTrue(db_models[0].id, self.ID)
