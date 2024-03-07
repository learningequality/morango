import contextlib
import json
import os
import socket
import subprocess
import sys
import time

import mock
import pytest
import requests
from django.conf import settings
from django.test.testcases import TransactionTestCase
from facility_profile.models import InteractionLog
from facility_profile.models import MyUser
from facility_profile.models import SummaryLog
from requests.exceptions import RequestException
from requests.exceptions import Timeout
from testapp.settings import BASE_DIR

from ..compat import EnvironmentVarGuard
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


def get_free_tcp_port():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(("", 0))
    addr, port = tcp.getsockname()
    tcp.close()
    return port


class LiveServer:
    def __init__(self):
        self.env = os.environ.copy()
        self.env["MORANGO_SYSTEM_ID"] = SECOND_SYSTEM_ID
        self.port = get_free_tcp_port()
        self.host = "127.0.0.1"
        self.start()

    @property
    def baseurl(self):
        return f"http://{self.host}:{self.port}/"

    def start(self):
        manage_py_path = os.path.join(BASE_DIR, "manage.py")
        self._instance = subprocess.Popen(
            [sys.executable, manage_py_path, "runserver", "--nothreading", "--noreload", "--settings", "testapp.server2_settings", f"{self.host}:{self.port}"],
            env=self.env,
        )
        self._wait_for_server_start()

    def _wait_for_server_start(self, timeout=20):
        for i in range(timeout * 2):
            try:
                resp = requests.get(self.baseurl, timeout=3)
                if resp.status_code > 0:
                    return
            except RequestException:
                pass
            time.sleep(0.5)

        raise Exception("Server did not start within {} seconds".format(timeout))

    def kill(self):
        try:
            self._instance.kill()
        except OSError:
            pass


@pytest.mark.skipif(
    getattr(settings, "MORANGO_TEST_POSTGRESQL", False), reason="Not supported"
)
class PushPullClientTestCase(TransactionTestCase):
    profile = "facilitydata"
    databases = ["default", SECOND_TEST_DATABASE]

    @classmethod
    def setUpClass(cls):
        super(TransactionTestCase, cls).setUpClass()
        cls.server = LiveServer()

    @classmethod
    def tearDownClass(cls):
        # There may not be a 'server' attribute if setUpClass() for some
        # reasons has raised an exception.
        if hasattr(cls, 'server'):
            # Terminate the live server's thread
            cls.server.kill()
            super(TransactionTestCase, cls).tearDownClass()

    def setUp(self):
        super(PushPullClientTestCase, self).setUp()
        self.profile_controller = MorangoProfileController(self.profile)
        self.conn = self.profile_controller.create_network_connection(
            self.server.baseurl
        )
        self.conn.chunk_size = 3

        self.remote_user, self.root_cert_id = self._setUpServer()
        self.filter = Filter("{}:user".format(self.remote_user.id))
        self.client = self._setUpClient(self.root_cert_id)
        self.session = self.client.sync_session
        self.last_session_activity = self.session.last_activity_timestamp
        self.last_transfer_activity = None

        # perform an initial sync to ensure the user is on both sides
        client = self.client.get_pull_client()
        client.initialize(self.filter)
        client.run()
        client.finalize()

        self.local_user = MyUser.objects.first()

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
            self.server_root_scope, self.server_subset_scope = self._setUpCertScopes()
            root_cert = Certificate.generate_root_certificate(self.server_root_scope.id)

            remote_user = MyUser.objects.create(
                _morango_source_id=root_cert.id, username="bob", is_superuser=True
            )
            remote_user.set_password("password")
            remote_user.save()

            subset_cert = Certificate(
                parent=root_cert,
                profile=self.profile,
                scope_definition=self.server_subset_scope,
                scope_version=self.server_subset_scope.version,
                scope_params=json.dumps({"user": remote_user.id, "sub": "user"}),
                private_key=Key(),
            )
            root_cert.sign_certificate(subset_cert)
            subset_cert.save()
        return remote_user, root_cert.id

    def _setUpClient(self, primary_partition):
        self.client_root_scope, self.client_subset_scope = self._setUpCertScopes()

        server_certs = self.conn.get_remote_certificates(
            primary_partition, self.client_root_scope.id
        )
        server_cert = server_certs[0]
        client_cert = self.conn.certificate_signing_request(
            server_cert,
            self.client_subset_scope.id,
            {"user": primary_partition, "sub": "user"},
            userargs="bob",
            password="password",
        )
        return self.conn.create_sync_session(client_cert, server_cert)

    def assertLastActivityUpdate(self, transfer_session=None):
        """A signal callable that asserts `last_activity_timestamp`s are updated"""
        if self.last_transfer_activity is not None:
            self.assertLessEqual(
                self.last_transfer_activity, transfer_session.last_activity_timestamp
            )
            self.assertLessEqual(
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
        transfer_session = client.context.transfer_session
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

        self.assertEqual(0, SummaryLog.objects.filter(user=self.local_user).count())
        self.assertEqual(0, InteractionLog.objects.filter(user=self.local_user).count())

        client = self.client.get_pull_client()
        client.signals.queuing.completed.connect(self.assertLastActivityUpdate)
        client.signals.transferring.in_progress.connect(self.assertLastActivityUpdate)
        client.signals.dequeuing.completed.connect(self.assertLastActivityUpdate)

        self.assertEqual(0, TransferSession.objects.filter(active=True).count())
        client.initialize(self.filter)
        self.assertEqual(1, TransferSession.objects.filter(active=True).count())
        transfer_session = client.context.transfer_session
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

    def test_full_flow_and_repeat(self):
        with second_environment():
            for _ in range(5):
                SummaryLog.objects.create(user=self.remote_user)
                InteractionLog.objects.create(user=self.remote_user)

        self.assertEqual(0, SummaryLog.objects.filter(user=self.local_user).count())
        self.assertEqual(0, InteractionLog.objects.filter(user=self.local_user).count())

        # first pull
        pull_client = self.client.get_pull_client()
        pull_client.initialize(self.filter)
        transfer_session = pull_client.context.transfer_session
        self.assertNotEqual(0, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        pull_client.run()
        self.assertNotEqual(0, transfer_session.records_transferred)
        pull_client.finalize()

        # sanity check pull worked
        self.assertEqual(5, SummaryLog.objects.filter(user=self.local_user).count())
        self.assertEqual(5, InteractionLog.objects.filter(user=self.local_user).count())

        # now do a push after pull, but nothing to actually transfer
        push_client = self.client.get_push_client()
        push_client.initialize(self.filter)
        transfer_session = push_client.context.transfer_session
        self.assertEqual(0, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        push_client.run()
        self.assertEqual(0, transfer_session.records_transferred)
        push_client.finalize()

        # second pass for pull, only do initialize to make sure nothing gets queued for sync
        second_pull_client = self.client.get_pull_client()
        second_pull_client.initialize(self.filter)
        transfer_session = second_pull_client.context.transfer_session
        self.assertEqual(0, transfer_session.records_total)

    def test_second_pull_with_instance_id_no_longer_in_store(self):
        with second_environment():
            SummaryLog.objects.create(user=self.remote_user)
            summ_log_id = SummaryLog.objects.first().id

        self.assertEqual(0, SummaryLog.objects.filter(id=summ_log_id).count())

        # first pull
        pull_client = self.client.get_pull_client()
        pull_client.initialize(self.filter)
        transfer_session = pull_client.context.transfer_session
        self.assertEqual(1, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        pull_client.run()
        self.assertEqual(1, transfer_session.records_transferred)
        pull_client.finalize()

        # sanity check pull worked
        self.assertEqual(1, SummaryLog.objects.filter(id=summ_log_id).count())

        # update the log record locally
        SummaryLog.objects.filter(id=summ_log_id).update(content_id="a" * 32)

        # now start a push, to serialize the local record, but don't actually push
        second_push_client = self.client.get_push_client()
        second_push_client.initialize(self.filter)

        # now do another pull, which shouldn't have anything new to bring down
        second_pull_client = self.client.get_pull_client()
        second_pull_client.initialize(self.filter)
        transfer_session = second_pull_client.context.transfer_session
        self.assertEqual(0, transfer_session.records_total)

    def test_resume(self):
        # create data
        for _ in range(5):
            SummaryLog.objects.create(user=self.local_user)
            InteractionLog.objects.create(user=self.local_user)

        with second_environment():
            self.assertEqual(0, SummaryLog.objects.filter(user=self.remote_user).count())
            self.assertEqual(0, InteractionLog.objects.filter(user=self.remote_user).count())

        # use client to start a sync
        client = self.client.get_push_client()
        self.assertEqual(0, TransferSession.objects.filter(active=True).count())
        client.initialize(self.filter)
        self.assertEqual(1, TransferSession.objects.filter(active=True).count())
        transfer_session = client.context.transfer_session
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
        transfer_session = resume_client.context.transfer_session
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

    def test_create_sync_delete_sync_recreate_sync(self):
        with second_environment():
            SummaryLog.objects.create(user=self.remote_user)
            summ_log = SummaryLog.objects.first()
            summ_log_id = summ_log.id
            content_id = summ_log.content_id

        self.assertEqual(0, SummaryLog.objects.filter(id=summ_log_id).count())

        # first pull
        pull_client = self.client.get_pull_client()
        pull_client.initialize(self.filter)
        transfer_session = pull_client.context.transfer_session
        self.assertEqual(1, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        pull_client.run()
        self.assertEqual(1, transfer_session.records_transferred)
        pull_client.finalize()

        # sanity check pull worked
        self.assertEqual(1, SummaryLog.objects.filter(id=summ_log_id).count())

        with second_environment():
            SummaryLog.objects.get(id=summ_log_id).delete()

        # now do another pull, which should pull in the deletion
        second_pull_client = self.client.get_pull_client()
        second_pull_client.initialize(self.filter)
        second_pull_client.run()
        second_pull_client.finalize()
        self.assertEqual(0, SummaryLog.objects.filter(id=summ_log_id).count())

        with second_environment():
            sum_log = SummaryLog.objects.create(user=self.remote_user, content_id=content_id)
            self.assertEqual(sum_log.id, summ_log_id)

        # now do another pull, which should pull in the recreation
        third_pull_client = self.client.get_pull_client()
        third_pull_client.initialize(self.filter)
        third_pull_client.run()
        third_pull_client.finalize()
        self.assertEqual(1, SummaryLog.objects.filter(id=summ_log_id).count())
