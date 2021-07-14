import json
import uuid

import mock
from django.test.testcases import LiveServerTestCase
from django.test.utils import override_settings
from requests.exceptions import HTTPError

from ..helpers import BaseClientTestCase
from ..helpers import BaseTransferClientTestCase
from morango.api.serializers import CertificateSerializer
from morango.constants import transfer_stages
from morango.constants import transfer_statuses
from morango.constants.capabilities import ALLOW_CERTIFICATE_PUSHING
from morango.errors import CertificateSignatureInvalid
from morango.errors import MorangoServerDoesNotAllowNewCertPush
from morango.errors import MorangoResumeSyncError
from morango.errors import MorangoError
from morango.models.certificates import Certificate
from morango.models.certificates import Filter
from morango.models.certificates import Key
from morango.models.certificates import ScopeDefinition
from morango.models.core import SyncSession
from morango.models.fields.crypto import SharedKey
from morango.sync.controller import MorangoProfileController
from morango.sync.context import LocalSessionContext
from morango.sync.context import NetworkSessionContext
from morango.sync.session import SessionWrapper
from morango.sync.syncsession import NetworkSyncConnection
from morango.sync.syncsession import TransferClient
from morango.sync.syncsession import PullClient
from morango.sync.syncsession import PushClient
from morango.sync.syncsession import SyncSessionClient


def mock_patch_decorator(func):
    def wrapper(*args, **kwargs):
        mock_object = mock.Mock(
            status_code=200,
            content=b"""{"id": "abc"}""",
            data={"signature": "sig", "client_fsic": "{}", "server_fsic": "{}"},
        )
        mock_object.json.return_value = {}
        with mock.patch.object(SessionWrapper, "request", return_value=mock_object):
            with mock.patch.object(Certificate, "verify", return_value=True):
                return func(*args, **kwargs)

    return wrapper


class NetworkSyncConnectionTestCase(LiveServerTestCase):
    def setUp(self):
        super(NetworkSyncConnectionTestCase, self).setUp()
        self.profile = "facilitydata"

        self.root_scope_def = ScopeDefinition.objects.create(
            id="rootcert",
            profile=self.profile,
            version=1,
            primary_scope_param_key="mainpartition",
            description="Root cert for ${mainpartition}.",
            read_filter_template="",
            write_filter_template="",
            read_write_filter_template="${mainpartition}",
        )

        self.subset_scope_def = ScopeDefinition.objects.create(
            id="subcert",
            profile=self.profile,
            version=1,
            primary_scope_param_key="",
            description="Subset cert under ${mainpartition} for ${subpartition}.",
            read_filter_template="${mainpartition}",
            write_filter_template="${mainpartition}:${subpartition}",
            read_write_filter_template="",
        )

        self.root_cert = Certificate.generate_root_certificate(self.root_scope_def.id)

        self.subset_cert = Certificate(
            parent=self.root_cert,
            profile=self.profile,
            scope_definition=self.subset_scope_def,
            scope_version=self.subset_scope_def.version,
            scope_params=json.dumps(
                {"mainpartition": self.root_cert.id, "subpartition": "abracadabra"}
            ),
            private_key=Key(),
        )
        self.root_cert.sign_certificate(self.subset_cert)
        self.subset_cert.save()

        self.unsaved_cert = Certificate(
            parent=self.root_cert,
            profile=self.profile,
            scope_definition=self.subset_scope_def,
            scope_version=self.subset_scope_def.version,
            scope_params=json.dumps(
                {"mainpartition": self.root_cert.id, "subpartition": "other"}
            ),
            public_key=Key(),
        )
        self.root_cert.sign_certificate(self.unsaved_cert)

        self.controller = MorangoProfileController("facilitydata")
        self.network_connection = self.controller.create_network_connection(
            self.live_server_url
        )
        self.key = SharedKey.get_or_create_shared_key()

    @mock.patch.object(SyncSession.objects, "create", return_value=None)
    def test_creating_sync_session_successful(self, mock_object):
        self.assertEqual(SyncSession.objects.filter(active=True).count(), 0)
        self.network_connection.create_sync_session(self.subset_cert, self.root_cert)
        self.assertEqual(SyncSession.objects.filter(active=True).count(), 1)

    @mock.patch.object(NetworkSyncConnection, "_create_sync_session")
    @mock.patch.object(Certificate, "verify", return_value=False)
    def test_creating_sync_session_cert_fails_to_verify(self, mock_verify, mock_create):
        mock_create.return_value.json.return_value = {}
        with self.assertRaises(CertificateSignatureInvalid):
            self.network_connection.create_sync_session(
                self.subset_cert, self.root_cert
            )

    def test_get_remote_certs(self):
        certs = self.subset_cert.get_ancestors(include_self=True)
        remote_certs = self.network_connection.get_remote_certificates(
            self.root_cert.id
        )
        self.assertSetEqual(set(certs), set(remote_certs))

    @mock.patch.object(SessionWrapper, "request")
    def test_csr(self, mock_request):
        # mock a "signed" cert being returned by server
        cert_serialized = json.dumps(CertificateSerializer(self.subset_cert).data)
        mock_request.return_value.json.return_value = json.loads(cert_serialized)
        self.subset_cert.delete()

        # we only want to make sure the "signed" cert is saved
        with mock.patch.object(
            Key,
            "get_private_key_string",
            return_value=self.subset_cert.private_key.get_private_key_string(),
        ):
            self.network_connection.certificate_signing_request(self.root_cert, "", "")
        self.assertTrue(
            Certificate.objects.filter(id=json.loads(cert_serialized)["id"]).exists()
        )

    @override_settings(ALLOW_CERTIFICATE_PUSHING=True)
    def test_push_signed_client_certificate_chain(self):
        self.network_connection.capabilities = [ALLOW_CERTIFICATE_PUSHING]
        cert = self.network_connection.push_signed_client_certificate_chain(
            self.root_cert,
            self.subset_scope_def.id,
            {"mainpartition": self.root_cert.id, "subpartition": "abracadabra"},
        )
        self.assertEqual(cert.private_key, None)
        self.assertTrue(Certificate.objects.filter(id=cert.id).exists())

    @override_settings(ALLOW_CERTIFICATE_PUSHING=True)
    def test_push_signed_client_certificate_chain_publickey_error(self):
        self.network_connection.capabilities = [ALLOW_CERTIFICATE_PUSHING]
        with mock.patch.object(NetworkSyncConnection, "_get_public_key"):
            NetworkSyncConnection._get_public_key.return_value.json.return_value = [
                {"public_key": Key().get_public_key_string()}
            ]
            with self.assertRaises(HTTPError) as e:
                self.network_connection.push_signed_client_certificate_chain(
                    self.root_cert,
                    self.subset_scope_def.id,
                    {"mainpartition": self.root_cert.id, "subpartition": "abracadabra"},
                )
            self.assertEqual(e.exception.response.status_code, 400)

    @override_settings(ALLOW_CERTIFICATE_PUSHING=True)
    def test_push_signed_client_certificate_chain_bad_cert(self):
        self.network_connection.capabilities = [ALLOW_CERTIFICATE_PUSHING]
        with self.assertRaises(HTTPError) as e:
            self.network_connection.push_signed_client_certificate_chain(
                self.root_cert, self.subset_scope_def.id, {"bad": "scope_params"}
            )
        self.assertEqual(e.exception.response.status_code, 400)

    @override_settings(ALLOW_CERTIFICATE_PUSHING=True)
    @mock.patch.object(NetworkSyncConnection, "_get_nonce")
    def test_push_signed_client_certificate_chain_nonce_error(self, mock_nonce):
        self.network_connection.capabilities = [ALLOW_CERTIFICATE_PUSHING]
        mock_nonce.return_value.json.return_value = {"id": uuid.uuid4().hex}
        with self.assertRaises(HTTPError) as e:
            self.network_connection.push_signed_client_certificate_chain(
                self.root_cert,
                self.subset_scope_def.id,
                {"mainpartition": self.root_cert.id, "subpartition": "abracadabra"},
            )
        self.assertEqual(e.exception.response.status_code, 403)

    def test_push_signed_client_certificate_chain_not_allowed(self):
        with self.assertRaises(MorangoServerDoesNotAllowNewCertPush) as e:
            self.network_connection.push_signed_client_certificate_chain(
                self.root_cert,
                self.subset_scope_def.id,
                {"mainpartition": self.root_cert.id, "subpartition": "abracadabra"},
            )
            self.assertEqual(e.exception.response.status_code, 403)

    def test_get_cert_chain(self):
        response = self.network_connection._get_certificate_chain(
            params={"ancestors_of": self.subset_cert.id}
        )
        data = response.json()
        self.assertEqual(len(data), Certificate.objects.count())
        self.assertEqual(data[0]["id"], self.root_cert.id)
        self.assertEqual(data[1]["id"], self.subset_cert.id)

    @mock.patch.object(SyncSession.objects, "create")
    def test_close_sync_session(self, mock_create):
        mock_session = mock.Mock(spec=SyncSession)

        def create(**data):
            mock_session.id = data.get("id")
            return mock_session

        mock_create.side_effect = create
        self.assertEqual(SyncSession.objects.filter(active=True).count(), 0)
        client = self.network_connection.create_sync_session(
            self.subset_cert, self.root_cert
        )
        self.assertEqual(SyncSession.objects.filter(active=True).count(), 1)

        self.network_connection.close_sync_session(client.sync_session)
        self.assertEqual(SyncSession.objects.filter(active=True).count(), 0)

    @mock.patch.object(SyncSession.objects, "create")
    def test_resume_sync_session(self, mock_create):
        def create(**data):
            """Trickery to get around same DB being used for both client and server"""
            return SyncSession.objects.get(pk=data.get("id"))

        mock_create.side_effect = create

        # first create a session
        client = self.network_connection.create_sync_session(self.subset_cert, self.root_cert)
        # reset process ID
        sync_session = client.sync_session
        sync_session.process_id = 123
        sync_session.save()
        resume_client = self.network_connection.resume_sync_session(sync_session.id)
        self.assertEqual(sync_session.id, resume_client.sync_session.id)

    @mock.patch.object(SyncSession.objects, "create")
    def test_resume_sync_session__still_running(self, mock_create):
        def create(**data):
            """Trickery to get around same DB being used for both client and server"""
            return SyncSession.objects.get(pk=data.get("id"))

        mock_create.side_effect = create

        # first create a session
        client = self.network_connection.create_sync_session(self.subset_cert, self.root_cert)
        # reset process ID
        sync_session = client.sync_session
        sync_session.process_id = 123
        sync_session.save()

        with mock.patch("morango.sync.syncsession.psutil.pid_exists") as mock_pid_exists:
            mock_pid_exists.return_value = True
            with mock.patch("morango.sync.syncsession.os.getpid") as mock_getpid:
                mock_getpid.return_value = 245
                with self.assertRaises(MorangoResumeSyncError):
                    self.network_connection.resume_sync_session(sync_session.id)


class SyncSessionClientTestCase(BaseClientTestCase):
    def test_get_pull_client(self):
        client = self.client.get_pull_client()
        self.assertIsInstance(client, PullClient)
        self.assertEqual(self.client.sync_connection, client.sync_connection)
        self.assertEqual(self.client.sync_session, client.sync_session)

    def test_get_push_client(self):
        client = self.client.get_push_client()
        self.assertIsInstance(client, PushClient)
        self.assertEqual(self.client.sync_connection, client.sync_connection)
        self.assertEqual(self.client.sync_session, client.sync_session)

    @mock.patch("morango.sync.syncsession.PullClient")
    def test_initiate_pull(self, MockPullClient):
        """
        TODO: should eventually be removed as this method is deprecated
        """
        mock_pull_client = mock.Mock(spec=PullClient)
        MockPullClient.return_value = mock_pull_client

        filter = Filter("abc123")
        self.client.initiate_pull(filter)
        MockPullClient.assert_called_with(
            self.conn, self.session, self.client.controller
        )

        mock_pull_client.initialize.assert_called_once_with(filter)
        mock_pull_client.run.assert_called_once()
        mock_pull_client.finalize.assert_called_once()

        self.assertEqual(self.client.signals, mock_pull_client.signals)

    @mock.patch("morango.sync.syncsession.PushClient")
    def test_initiate_push(self, MockPushClient):
        """
        TODO: should eventually be removed as this method is deprecated
        """
        mock_pull_client = mock.Mock(spec=PushClient)
        MockPushClient.return_value = mock_pull_client

        sync_filter = Filter("abc123")
        self.client.initiate_push(sync_filter)
        MockPushClient.assert_called_with(
            self.conn, self.session, self.client.controller
        )

        mock_pull_client.initialize.assert_called_once_with(sync_filter)
        mock_pull_client.run.assert_called_once()
        mock_pull_client.finalize.assert_called_once()

        self.assertEqual(self.client.signals, mock_pull_client.signals)

    def test_close_sync_session(self):
        """
        TODO: should eventually be removed as this method is deprecated
        """
        conn = mock.Mock(spec=NetworkSyncConnection)
        client = SyncSessionClient(conn, self.session)
        client.close_sync_session()
        conn.close.assert_called_once()


class TransferClientTestCase(BaseTransferClientTestCase):
    def build_client(self, client_class=TransferClient, controller=None, update_context=False):
        self.controller = controller or mock.Mock(spec="morango.sync.controller.SessionController")()
        return super(TransferClientTestCase, self).build_client(client_class=client_class, controller=self.controller, update_context=update_context)

    def test_init(self):
        self.assertIsInstance(self.client, TransferClient)
        self.assertEqual(self.session, self.client.sync_session)
        self.assertEqual(self.conn, self.client.sync_connection)
        self.assertEqual(self.controller, self.client.controller)
        self.assertIsInstance(self.client.local_context, LocalSessionContext)
        self.assertEqual(self.session, self.client.local_context.sync_session)
        self.assertIsInstance(self.client.remote_context, NetworkSessionContext)
        self.assertEqual(self.session, self.client.remote_context.sync_session)
        self.assertEqual(self.conn, self.client.remote_context.connection)

    def test_proceed_to_and_wait_for(self):
        mock_proceed = self.controller.proceed_to_and_wait_for
        mock_proceed.return_value = transfer_statuses.COMPLETED
        self.client.proceed_to_and_wait_for(transfer_stages.QUEUING)
        mock_proceed_calls = mock_proceed.call_args_list
        self.assertEqual(2, len(mock_proceed_calls))
        self.assertEqual(transfer_stages.QUEUING, mock_proceed_calls[0][0][0])
        self.assertEqual(self.client.remote_context, mock_proceed_calls[0][1].get("context"))
        self.assertEqual(transfer_stages.QUEUING, mock_proceed_calls[1][0][0])
        self.assertEqual(self.client.local_context, mock_proceed_calls[1][1].get("context"))

    def test_proceed_to_and_wait_for__error(self):
        self.controller.last_error = Exception("Oops")
        self.controller.proceed_to_and_wait_for.return_value = transfer_statuses.ERRORED
        with self.assertRaises(MorangoError):
            self.client.proceed_to_and_wait_for(transfer_stages.QUEUING)
        self.controller.proceed_to_and_wait_for.assert_called_once_with(
            transfer_stages.QUEUING, context=self.client.remote_context
        )

    @mock.patch("morango.sync.syncsession.TransferClient.proceed_to_and_wait_for")
    def test_initialize(self, mock_proceed):
        session_started_handler = mock.Mock()
        self.client.signals.session.started.connect(session_started_handler)
        queuing_handler = mock.Mock()
        self.client.signals.session.connect(queuing_handler)
        self.client.local_context.transfer_session = self.transfer_session

        sync_filter = self.transfer_session.get_filter()
        self.client.initialize(sync_filter)
        self.assertEqual(sync_filter, self.client.local_context.filter)
        self.assertEqual(sync_filter, self.client.remote_context.filter)
        self.controller.proceed_to_and_wait_for.assert_any_call(
            transfer_stages.INITIALIZING, context=self.client.local_context
        )
        self.assertEqual(self.transfer_session, self.client.remote_context.transfer_session)
        session_started_handler.assert_called_once_with(transfer_session=self.transfer_session)
        mock_proceed.assert_any_call(transfer_stages.QUEUING)
        queuing_handler.assert_any_call(transfer_session=self.transfer_session)

    @mock.patch("morango.sync.syncsession.TransferClient._transfer")
    def test_run(self, mock_transfer):
        mock_start = mock.Mock()
        mock_end = mock.Mock()
        self.client.signals.transferring.started.connect(mock_start)
        self.client.signals.transferring.completed.connect(mock_end)
        self.client.run()
        mock_transfer.assert_called_once()
        mock_start.assert_called_once()
        mock_end.assert_called_once()

    @mock.patch("morango.sync.syncsession.TransferClient.proceed_to_and_wait_for")
    def test_finalize(self, mock_proceed):
        mock_start = mock.Mock()
        mock_end = mock.Mock()
        mock_session_end = mock.Mock()
        self.client.signals.dequeuing.started.connect(mock_start)
        self.client.signals.dequeuing.completed.connect(mock_end)
        self.client.signals.session.completed.connect(mock_session_end)
        self.client.finalize()
        mock_proceed.assert_any_call(transfer_stages.DESERIALIZING)
        mock_proceed.assert_any_call(transfer_stages.CLEANUP)
        mock_start.assert_called_once()
        mock_end.assert_called_once()

    def test_transfer(self):
        mock_callback = mock.Mock()
        self.controller.proceed_to.side_effect = [
            transfer_statuses.PENDING,
            transfer_statuses.COMPLETED,
        ]
        self.client._transfer(callback=mock_callback)
        self.controller.proceed_to.assert_any_call(
            transfer_stages.TRANSFERRING, context=self.client.remote_context
        )
        self.assertEqual(2, len(mock_callback.call_args_list))

    def test_transfer__error(self):
        mock_callback = mock.Mock()
        self.controller.last_error = Exception("Oops")
        self.controller.proceed_to.side_effect = [
            transfer_statuses.PENDING,
            transfer_statuses.ERRORED,
        ]

        with self.assertRaises(MorangoError):
            self.client._transfer(callback=mock_callback)
        self.controller.proceed_to.assert_any_call(
            transfer_stages.TRANSFERRING, context=self.client.remote_context
        )
        self.assertEqual(2, len(mock_callback.call_args_list))


