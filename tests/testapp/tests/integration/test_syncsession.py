import json
import contextlib
import pytest
import mock

from django.conf import settings
from django.db import connections
from django.test.testcases import LiveServerTestCase
from facility_profile.models import SummaryLog
from facility_profile.models import InteractionLog
from facility_profile.models import MyUser
from requests.exceptions import Timeout
from test.support import EnvironmentVarGuard

from morango.errors import MorangoError
from morango.models.certificates import Certificate
from morango.models.certificates import Filter
from morango.models.certificates import Key
from morango.models.certificates import ScopeDefinition
from morango.models.core import Buffer
from morango.models.core import InstanceIDModel
from morango.models.core import TransferSession
from morango.sync.controller import MorangoProfileController


SECOND_TEST_DATABASE = "default2"
SECOND_SYSTEM_ID = "default2"


@contextlib.contextmanager
def second_environment():
    with EnvironmentVarGuard() as env:
        env["MORANGO_TEST_DATABASE"] = SECOND_TEST_DATABASE
        env["MORANGO_SYSTEM_ID"] = SECOND_SYSTEM_ID
        instance2, _ = InstanceIDModel.get_or_create_current_instance(clear_cache=True)
        yield
    instance1, _ = InstanceIDModel.get_or_create_current_instance(clear_cache=True)
    assert instance1.id != instance2.id


@pytest.mark.skipif(
    getattr(settings, "MORANGO_TEST_POSTGRESQL", False), reason="Not supported"
)
class PushPullClientTestCase(LiveServerTestCase):
    multi_db = True
    profile = "facilitydata"

    def setUp(self):
        super(PushPullClientTestCase, self).setUp()
        self.profile_controller = MorangoProfileController(self.profile)
        self.conn = self.profile_controller.create_network_connection(
            self.live_server_url
        )
        self.conn.chunk_size = 3

        self.remote_user = self._setUpServer()
        self.filter = Filter("{}:user".format(self.remote_user.id))
        self.client = self._setUpClient(self.remote_user.id)
        self.session = self.client.sync_session
        self.last_session_activity = self.session.last_activity_timestamp
        self.last_transfer_activity = None

        self.local_user = MyUser.objects.create(
            id=self.remote_user.id, username="bob", is_superuser=True
        )

    def _setUpCertScopes(self):
        root_scope = ScopeDefinition.objects.create(
            id="root_scope",
            profile=self.profile,
            version=1,
            primary_scope_param_key="user",
            description="Root cert for ${user}.",
            read_filter_template="",
            write_filter_template="",
            read_write_filter_template="${user}",
        )

        subset_scope = ScopeDefinition.objects.create(
            id="subset_scope",
            profile=self.profile,
            version=1,
            primary_scope_param_key="",
            description="Subset cert under ${user} for :${sub}.",
            read_filter_template="${user}",
            write_filter_template="${user}:${sub}",
            read_write_filter_template="",
        )
        return root_scope, subset_scope

    def _setUpServer(self):
        with second_environment():
            root_scope, subset_scope = self._setUpCertScopes()
            root_cert = Certificate.generate_root_certificate(root_scope.id)

            remote_user = MyUser.objects.create(
                id=root_cert.id, username="bob", is_superuser=True
            )
            remote_user.set_password("password")
            remote_user.save()

            subset_cert = Certificate(
                parent=root_cert,
                profile=self.profile,
                scope_definition=subset_scope,
                scope_version=subset_scope.version,
                scope_params=json.dumps({"user": remote_user.id, "sub": "user"}),
                private_key=Key(),
            )
            root_cert.sign_certificate(subset_cert)
            subset_cert.save()
        return remote_user

    def _setUpClient(self, primary_partition):
        root_scope, subset_scope = self._setUpCertScopes()

        server_certs = self.conn.get_remote_certificates(
            primary_partition, root_scope.id
        )
        server_cert = server_certs[0]
        client_cert = self.conn.certificate_signing_request(
            server_cert,
            subset_scope.id,
            {"user": primary_partition, "sub": "user"},
            userargs="bob",
            password="password",
        )
        return self.conn.create_sync_session(client_cert, server_cert)

    @classmethod
    def _create_server_thread(cls, connections_override):
        # override default to point to second environment database
        connections_override["default"] = connections["default2"]
        return super(PushPullClientTestCase, cls)._create_server_thread(
            connections_override
        )

    def assertLastActivityUpdate(self, transfer_session=None):
        """A signal callable that asserts `last_activity_timestamp`s are updated"""
        if self.last_transfer_activity is not None:
            self.assertLess(
                self.last_transfer_activity, transfer_session.last_activity_timestamp
            )
            self.assertLess(
                self.last_session_activity,
                transfer_session.sync_session.last_activity_timestamp,
            )
        self.last_transfer_activity = transfer_session.last_activity_timestamp
        self.last_session_activity = (
            transfer_session.sync_session.last_activity_timestamp
        )

    def test_push(self):
        for _ in range(5):
            SummaryLog.objects.create(user=self.local_user)
            InteractionLog.objects.create(user=self.local_user)
        self.profile_controller.serialize_into_store(self.filter)

        with second_environment():
            self.assertEqual(
                0, SummaryLog.objects.filter(user=self.remote_user).count()
            )
            self.assertEqual(
                0, InteractionLog.objects.filter(user=self.remote_user).count()
            )

        client = self.client.get_push_client()
        client.signals.queuing.completed.connect(self.assertLastActivityUpdate)
        client.signals.transferring.in_progress.connect(self.assertLastActivityUpdate)
        client.signals.dequeuing.completed.connect(self.assertLastActivityUpdate)

        self.assertEqual(0, TransferSession.objects.filter(active=True).count())
        client.initialize(self.filter)
        self.assertEqual(1, TransferSession.objects.filter(active=True).count())
        transfer_session = client.local_context.transfer_session
        self.assertNotEqual(0, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        self.assertLessEqual(
            1, Buffer.objects.filter(transfer_session=transfer_session).count()
        )
        client.run()
        self.assertNotEqual(0, transfer_session.records_transferred)
        client.finalize()
        self.assertEqual(
            0, Buffer.objects.filter(transfer_session=transfer_session).count()
        )
        self.assertEqual(0, TransferSession.objects.filter(active=True).count())

        with second_environment():
            self.assertEqual(
                5, SummaryLog.objects.filter(user=self.remote_user).count()
            )
            self.assertEqual(
                5, InteractionLog.objects.filter(user=self.remote_user).count()
            )

    def test_pull(self):
        with second_environment():
            for _ in range(5):
                SummaryLog.objects.create(user=self.remote_user)
                InteractionLog.objects.create(user=self.remote_user)
            self.profile_controller.serialize_into_store(self.filter)

        self.assertEqual(0, SummaryLog.objects.filter(user=self.local_user).count())
        self.assertEqual(0, InteractionLog.objects.filter(user=self.local_user).count())

        client = self.client.get_pull_client()
        client.signals.queuing.completed.connect(self.assertLastActivityUpdate)
        client.signals.transferring.in_progress.connect(self.assertLastActivityUpdate)
        client.signals.dequeuing.completed.connect(self.assertLastActivityUpdate)

        self.assertEqual(0, TransferSession.objects.filter(active=True).count())
        client.initialize(self.filter)
        self.assertEqual(1, TransferSession.objects.filter(active=True).count())
        transfer_session = client.local_context.transfer_session
        self.assertNotEqual(0, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        client.run()
        self.assertNotEqual(0, transfer_session.records_transferred)
        self.assertLessEqual(
            1, Buffer.objects.filter(transfer_session=transfer_session).count()
        )
        client.finalize()
        self.assertEqual(
            0, Buffer.objects.filter(transfer_session=transfer_session).count()
        )
        self.assertEqual(0, TransferSession.objects.filter(active=True).count())

        self.assertEqual(5, SummaryLog.objects.filter(user=self.local_user).count())
        self.assertEqual(5, InteractionLog.objects.filter(user=self.local_user).count())

    def test_resume(self):
        # create data
        for _ in range(5):
            SummaryLog.objects.create(user=self.local_user)
            InteractionLog.objects.create(user=self.local_user)
        self.profile_controller.serialize_into_store(self.filter)

        with second_environment():
            self.assertEqual(0, SummaryLog.objects.filter(user=self.remote_user).count())
            self.assertEqual(0, InteractionLog.objects.filter(user=self.remote_user).count())

        # use client to start a sync
        client = self.client.get_push_client()
        self.assertEqual(0, TransferSession.objects.filter(active=True).count())
        client.initialize(self.filter)
        self.assertEqual(1, TransferSession.objects.filter(active=True).count())
        transfer_session = client.local_context.transfer_session
        self.assertNotEqual(0, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        self.assertLessEqual(1, Buffer.objects.filter(transfer_session=transfer_session).count())

        # simulate timeout
        with mock.patch("morango.sync.operations.NetworkOperation.put_buffers") as mock_put_buffers:
            mock_put_buffers.side_effect = Timeout("Network disconnected")
            with self.assertRaises(MorangoError):
                client.run()

        self.assertEqual(0, transfer_session.records_transferred)

        # get resume client and retry
        resume_client = self.conn.resume_sync_session(client.sync_session.id).get_push_client()
        self.assertEqual(1, TransferSession.objects.filter(active=True).count())
        resume_client.initialize(self.filter)
        self.assertEqual(1, TransferSession.objects.filter(active=True).count())
        transfer_session = resume_client.local_context.transfer_session
        self.assertNotEqual(0, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        self.assertLessEqual(1, Buffer.objects.filter(transfer_session=transfer_session).count())
        self.assertEqual(0, transfer_session.records_transferred)
        resume_client.run()
        self.assertNotEqual(0, transfer_session.records_transferred)

        resume_client.finalize()
        self.assertEqual(0, Buffer.objects.filter(transfer_session=transfer_session).count())
        self.assertEqual(0, TransferSession.objects.filter(active=True).count())

        with second_environment():
            self.assertEqual(5, SummaryLog.objects.filter(user=self.remote_user).count())
            self.assertEqual(5, InteractionLog.objects.filter(user=self.remote_user).count())
