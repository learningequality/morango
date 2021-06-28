import json
import pytest

from django.db import connections
from facility_profile.models import SummaryLog
from facility_profile.models import InteractionLog
from facility_profile.models import MyUser
from test.support import EnvironmentVarGuard

from ..helpers import FacilityFactory
from ..helpers import BaseTransferClientTestCase
from morango.models.certificates import Certificate
from morango.models.certificates import Filter
from morango.models.certificates import Key
from morango.models.certificates import ScopeDefinition
from morango.models.core import Buffer
from morango.models.core import InstanceIDModel
from morango.models.core import TransferSession
from morango.sync.controller import MorangoProfileController
from morango.sync.controller import SessionController
from morango.sync.syncsession import TransferClient
from morango.sync.syncsession import PullClient
from morango.sync.syncsession import PushClient


SECOND_SYSTEM_ID = "new_sys_id_2"


# def env_decorator(callable):
#     def wrapper(*args, **kwargs):
#         with EnvironmentVarGuard() as env:
#             env["MORANGO_SYSTEM_ID"] = SECOND_SYSTEM_ID
#             instance2, _ = InstanceIDModel.get_or_create_current_instance(clear_cache=True)
#             result = callable(*args, **kwargs)
#         instance1, _ = InstanceIDModel.get_or_create_current_instance(clear_cache=True)
#         assert instance1.id != instance2.id
#         return result
#
#     return wrapper


@pytest.mark.skip("Needs two separate databases")
class PushPullClientTestCase(BaseTransferClientTestCase):
    @classmethod
    def setUpClass(cls):
        with EnvironmentVarGuard() as env:
            connections.close_all()
            env["MORANGO_SYSTEM_ID"] = SECOND_SYSTEM_ID
            env["DATABASE_NAME"] = "testapp2"
            super(PushPullClientTestCase, cls).setUpClass()

    def setUp(self):
        super(PushPullClientTestCase, self).setUp()
        self.profile_controller = MorangoProfileController(self.profile)
        self.root_scope_def = ScopeDefinition.objects.create(
            id="rootcert",
            profile=self.profile,
            version=1,
            primary_scope_param_key="user",
            description="Root cert for ${user}.",
            read_filter_template="",
            write_filter_template="",
            read_write_filter_template="${user}",
        )

        self.subset_scope_def = ScopeDefinition.objects.create(
            id="subcert",
            profile=self.profile,
            version=1,
            primary_scope_param_key="",
            description="Subset cert under ${user} for :${sub}.",
            read_filter_template="${user}",
            write_filter_template="${user}:${sub}",
            read_write_filter_template="",
        )

        self.root_cert = Certificate.generate_root_certificate(self.root_scope_def.id)

        self.my_user = MyUser.objects.create(id=self.root_cert.id, username="bob")
        self.subset_cert = Certificate(
            parent=self.root_cert,
            profile=self.profile,
            scope_definition=self.subset_scope_def,
            scope_version=self.subset_scope_def.version,
            scope_params=json.dumps(
                {"user": self.my_user.id, "sub": "user"}
            ),
            private_key=Key(),
        )
        self.root_cert.sign_certificate(self.subset_cert)
        self.subset_cert.save()
        self.filter = Filter("{}:user".format(self.my_user.id))

        self.session.client_certificate = self.subset_cert
        self.session.server_certificate = self.root_cert
        self.session.save()

        self.transfer_session.active = False
        self.transfer_session.save()

        self.facility = FacilityFactory()
        for _ in range(5):
            SummaryLog.objects.create(user=self.my_user)
            InteractionLog.objects.create(user=self.my_user)

        self.profile_controller.serialize_into_store(self.filter)

        # with EnvironmentVarGuard() as env:
        #     env["MORANGO_SYSTEM_ID"] = SECOND_SYSTEM_ID
        #     InstanceIDModel.get_or_create_current_instance(clear_cache=True)
        #     FacilityFactory()
        #     bob2 = MyUser.objects.create(username="bob2")
        #     bob3 = MyUser.objects.create(username="bob3")
        #
        #     for i in range(5):
        #         SummaryLog.objects.create(user=bob2)
        #         InteractionLog.objects.create(user=bob2)
        #         SummaryLog.objects.create(user=bob3)
        #         InteractionLog.objects.create(user=bob3)
        #
        #     self.profile_controller.serialize_into_store(self.filter)

    @classmethod
    def _create_server_thread(cls, connections_override):
        # connections_override[DEFAULT_DB_ALIAS] = connections["second_instance"]
        return super(PushPullClientTestCase, cls)._create_server_thread(connections_override)

    def build_client(self, client_class=TransferClient, controller=None, update_context=False):
        controller = controller or SessionController.build()
        client = super(PushPullClientTestCase, self).build_client(client_class=client_class, controller=controller, update_context=update_context)
        return client

    def test_push(self):
        client = self.build_client(client_class=PushClient)
        self.assertEqual(0, TransferSession.objects.filter(active=True).count())
        client.initialize(self.filter)
        self.assertEqual(1, TransferSession.objects.filter(active=True).count())
        transfer_session = client.local_context.transfer_session
        self.assertNotEqual(0, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        self.assertLessEqual(1, Buffer.objects.filter(transfer_session=transfer_session).count())
        client.run()
        self.assertNotEqual(0, transfer_session.records_transferred)
        client.finalize()
        self.assertEqual(0, Buffer.objects.filter(transfer_session=transfer_session).count())
        self.assertEqual(0, TransferSession.objects.filter(active=True).count())

    def test_pull(self):
        client = self.build_client(client_class=PullClient)
        self.assertEqual(0, TransferSession.objects.filter(active=True).count())
        client.initialize(self.filter)
        self.assertEqual(1, TransferSession.objects.filter(active=True).count())
        transfer_session = client.local_context.transfer_session
        self.assertNotEqual(0, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        self.assertLessEqual(1, Buffer.objects.filter(transfer_session=transfer_session).count())
        client.run()
        self.assertNotEqual(0, transfer_session.records_transferred)
        client.finalize()
        self.assertEqual(0, Buffer.objects.filter(transfer_session=transfer_session).count())
        self.assertEqual(1, TransferSession.objects.filter(active=True).count())
