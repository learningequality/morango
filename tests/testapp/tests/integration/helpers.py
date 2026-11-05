import contextlib
import json
import os
import subprocess
import sys
import time

import pytest
import requests
from django.conf import settings
from django.test.testcases import TransactionTestCase
from facility_profile.models import MyUser
from requests.exceptions import RequestException
from testapp.settings import BASE_DIR

from morango.models.certificates import Certificate
from morango.models.certificates import Filter
from morango.models.certificates import Key
from morango.models.certificates import ScopeDefinition
from morango.models.core import InstanceIDModel
from morango.sync.controller import MorangoProfileController

from ..compat import EnvironmentVarGuard
from ..helpers import get_free_tcp_port

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
            [
                sys.executable,
                manage_py_path,
                "runserver",
                "--nothreading",
                "--noreload",
                "--settings",
                "testapp.server2_settings",
                f"{self.host}:{self.port}",
            ],
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


@pytest.mark.skipif(getattr(settings, "MORANGO_TEST_POSTGRESQL", False), reason="Not supported")
class BasePushPullClientTestCase(TransactionTestCase):
    """
    Runs a second morango instance as a live server, and sets up a sync session with it
    """

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
        if hasattr(cls, "server"):
            # Terminate the live server's thread
            cls.server.kill()
            super(TransactionTestCase, cls).tearDownClass()

    def setUp(self):
        super(BasePushPullClientTestCase, self).setUp()
        self.profile_controller = MorangoProfileController(self.profile)
        self.conn = self.profile_controller.create_network_connection(self.server.baseurl)
        self.conn.chunk_size = 3

        self.remote_user, self.root_cert_id = self._setUpServer()
        self.filter = Filter("{}:user".format(self.remote_user.id))
        self.client = self._setUpClient(self.root_cert_id)
        self.session = self.client.sync_session

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
