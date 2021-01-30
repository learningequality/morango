import json
import uuid

import mock
from django.test import TestCase
from django.test.testcases import LiveServerTestCase
from django.test.utils import override_settings
from django.utils import timezone
from facility_profile.models import SummaryLog
from requests.exceptions import HTTPError
from requests.exceptions import RequestException
from requests.exceptions import Timeout

from morango.api.serializers import BufferSerializer
from morango.api.serializers import CertificateSerializer
from morango.constants.capabilities import ALLOW_CERTIFICATE_PUSHING
from morango.errors import CertificateSignatureInvalid
from morango.errors import MorangoServerDoesNotAllowNewCertPush
from morango.models.certificates import Certificate
from morango.models.certificates import Filter
from morango.models.certificates import Key
from morango.models.certificates import ScopeDefinition
from morango.models.core import Buffer
from morango.models.core import InstanceIDModel
from morango.models.core import SyncSession
from morango.models.core import TransferSession
from morango.models.fields.crypto import SharedKey
from morango.sync.controller import MorangoProfileController
from morango.sync.session import _length_of_headers
from morango.sync.session import SessionWrapper
from morango.sync.syncsession import NetworkSyncConnection
from morango.sync.syncsession import BaseSyncClient
from morango.sync.syncsession import PullClient
from morango.sync.syncsession import PushClient
from morango.sync.syncsession import SyncSessionClient


def mock_patch_decorator(func):
    def wrapper(*args, **kwargs):
        mock_object = mock.Mock(
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

    @mock.patch.object(Certificate, "verify", return_value=True)
    @mock.patch.object(SyncSession.objects, "create", return_value=None)
    @mock.patch("morango.sync.syncsession.NetworkSyncConnection._create_sync_session")
    @mock.patch.object(SyncSession.objects, "get", return_value=None)
    def test_creating_sync_session_allow_resume(self, mock_get, mock_create_other_session, mock_create, mock_verify):
        response = mock.Mock()
        mock_create_other_session.return_value = response
        response.json.return_value = dict(id="abc123", allow_resume=True, signature="sig")

        mock_get.return_value = mock.MagicMock(spec=SyncSession)
        self.network_connection.create_sync_session(self.subset_cert, self.root_cert, allow_resume=True)
        mock_get.assert_called_once_with(
            id="abc123",
            active=True,
            is_server=False,
            client_certificate=self.subset_cert,
            server_certificate=self.root_cert,
            profile=self.subset_cert.profile,
            connection_kind="network",
            connection_path=self.live_server_url,
            client_ip=mock.ANY,
            server_ip=mock.ANY,
            allow_resume=True,
        )

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


class SyncClientTestCase(LiveServerTestCase):
    def setUp(self):
        self.session = SyncSession.objects.create(
            id=uuid.uuid4().hex,
            profile="facilitydata",
            last_activity_timestamp=timezone.now(),
            allow_resume=False,
        )
        self.transfer_session = TransferSession.objects.create(
            id=uuid.uuid4().hex,
            sync_session=self.session,
            filter="partition",
            push=True,
            last_activity_timestamp=timezone.now(),
            records_total=3,
        )
        self.chunk_size = 3
        self.conn = NetworkSyncConnection(base_url=self.live_server_url)
        self.syncclient = self.build_client(BaseSyncClient)
        InstanceIDModel.get_or_create_current_instance()

    def build_client(self, client_class):
        client = client_class(self.conn, self.session, self.chunk_size)
        client.current_transfer_session = self.transfer_session
        self.transferring_mock = mock.Mock()
        client.signals.transferring.connect(self.transferring_mock)
        return client

    def build_buffer_items(self, transfer_session, **kwargs):

        data = {
            "profile": kwargs.get("profile", "facilitydata"),
            "serialized": kwargs.get("serialized", '{"test": 99}'),
            "deleted": kwargs.get("deleted", False),
            "last_saved_instance": kwargs.get("last_saved_instance", uuid.uuid4().hex),
            "last_saved_counter": kwargs.get("last_saved_counter", 179),
            "partition": kwargs.get("partition", "partition"),
            "source_id": kwargs.get("source_id", uuid.uuid4().hex),
            "model_name": kwargs.get("model_name", "contentsummarylog"),
            "conflicting_serialized_data": kwargs.get(
                "conflicting_serialized_data", ""
            ),
            "model_uuid": kwargs.get("model_uuid", None),
            "transfer_session": transfer_session,
        }

        for i in range(self.chunk_size):
            data["source_id"] = uuid.uuid4().hex
            data["model_uuid"] = SummaryLog.compute_namespaced_id(
                data["partition"], data["source_id"], data["model_name"]
            )
            Buffer.objects.create(**data)

        buffered_items = Buffer.objects.filter(
            transfer_session=self.syncclient.current_transfer_session
        )
        serialized_records = BufferSerializer(buffered_items, many=True)
        return json.dumps(serialized_records.data)

    def test_get_pull_client(self):
        session_client = self.build_client(SyncSessionClient)
        client = session_client.get_pull_client()
        self.assertIsInstance(client, PullClient)
        self.assertEqual(session_client.sync_connection, client.sync_connection)
        self.assertEqual(session_client.sync_session, client.sync_session)

    def test_get_push_client(self):
        session_client = self.build_client(SyncSessionClient)
        client = session_client.get_push_client()
        self.assertIsInstance(client, PushClient)
        self.assertEqual(session_client.sync_connection, client.sync_connection)
        self.assertEqual(session_client.sync_session, client.sync_session)

    @mock_patch_decorator
    def test_push_records(self):
        client = self.build_client(PushClient)
        self.build_buffer_items(client.current_transfer_session)
        self.assertEqual(client.current_transfer_session.records_transferred, 0)
        with client.signals.transferring.send(
            transfer_session=client.current_transfer_session
        ) as in_progress:
            client._push_records(in_progress.fire)
        self.assertEqual(
            client.current_transfer_session.records_transferred, self.chunk_size,
        )
        self.assertGreaterEqual(client.current_transfer_session.bytes_received, 150)
        self.transferring_mock.assert_called_with(
            transfer_session=client.current_transfer_session
        )

    @mock_patch_decorator
    def test_pull_records(self):
        self.transfer_session.push = False
        self.transfer_session.save()
        client = self.build_client(PullClient)

        resp = self.build_buffer_items(client.current_transfer_session)
        SessionWrapper.request.return_value.json.return_value = json.loads(resp)
        Buffer.objects.filter(transfer_session=client.current_transfer_session).delete()
        self.assertEqual(
            Buffer.objects.filter(
                transfer_session=client.current_transfer_session
            ).count(),
            0,
        )
        self.assertEqual(client.current_transfer_session.records_transferred, 0)
        with client.signals.transferring.send(
            transfer_session=client.current_transfer_session
        ) as status:
            client._pull_records(status.in_progress.fire)
        self.assertEqual(
            Buffer.objects.filter(
                transfer_session=client.current_transfer_session
            ).count(),
            self.chunk_size,
        )
        self.assertEqual(
            client.current_transfer_session.records_transferred, self.chunk_size,
        )
        self.assertGreaterEqual(client.current_transfer_session.bytes_received, 150)
        self.transferring_mock.assert_called_with(
            transfer_session=client.current_transfer_session
        )

    @mock_patch_decorator
    def test_create_transfer_session_push(self):
        self.syncclient.current_transfer_session.active = False
        self.syncclient.current_transfer_session.save()
        self.syncclient.current_transfer_session = None

        self.assertEqual(TransferSession.objects.filter(active=True).count(), 0)
        self.syncclient._create_transfer_session("filter", push=True)
        self.assertEqual(TransferSession.objects.filter(active=True, push=True).count(), 1)

    @mock_patch_decorator
    def test_create_transfer_session__resume__nothing(self):
        self.syncclient.sync_session.allow_resume = True
        self.syncclient.current_transfer_session.active = False
        self.syncclient.current_transfer_session.save()
        self.syncclient.current_transfer_session = None

        self.assertEqual(TransferSession.objects.filter(active=True).count(), 0)

        with mock.patch("morango.sync.syncsession.TransferSession.objects.filter") as mock_filter:
            results = mock.Mock()
            mock_filter.return_value = results
            results.first.return_value = None
            self.syncclient._create_transfer_session("filter", push=True)

        self.assertEqual(TransferSession.objects.filter(active=True, push=True).count(), 1)

    @mock_patch_decorator
    def test_create_transfer_session__resume__something(self):
        self.syncclient.sync_session.allow_resume = True
        self.syncclient.current_transfer_session = None
        last_activity = self.transfer_session.last_activity_timestamp

        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)

        with mock.patch("morango.sync.syncsession.TransferSession.objects.filter") as mock_filter:
            results = mock.Mock()
            mock_filter.return_value = results
            results.first.return_value = self.transfer_session
            self.syncclient._create_transfer_session("filter", push=True)

        self.assertGreaterEqual(self.transfer_session.last_activity_timestamp, last_activity)
        self.assertEqual(TransferSession.objects.filter(active=True, push=True).count(), 1)

    @mock_patch_decorator
    def test_close_transfer_session_push(self):
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)
        self.syncclient._close_transfer_session()
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 0)

    @mock.patch(
        "morango.sync.syncsession.BaseSyncClient._close_server_transfer_session"
    )
    @mock_patch_decorator
    def test_close_transfer_session_disallow_timeout(self, mocked_close):
        mocked_close.side_effect = Timeout()

        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)
        with self.assertRaises(Timeout):
            self.syncclient._close_transfer_session()

        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)

    @mock.patch(
        "morango.sync.syncsession.BaseSyncClient._close_server_transfer_session"
    )
    @mock_patch_decorator
    def test_close_transfer_session_allow_timeout(self, mocked_close):
        mocked_close.side_effect = Timeout()

        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)
        self.syncclient._close_transfer_session(allow_server_timeout=True)
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 0)

    @mock.patch("morango.sync.syncsession.PullClient")
    def test_initiate_pull(self, MockPullClient):
        """
        TODO: should eventually be removed as this method is deprecated
        """
        mock_pull_client = mock.Mock(spec=PullClient)
        MockPullClient.return_value = mock_pull_client
        client = self.build_client(SyncSessionClient)

        filter = Filter("abc123")
        client.initiate_pull(filter)
        MockPullClient.assert_called_with(
            self.conn, self.session, chunk_size=self.chunk_size
        )

        mock_pull_client.initialize.assert_called_once_with(filter)
        mock_pull_client.run.assert_called_once()
        mock_pull_client.finalize.assert_called_once()

        self.assertEqual(client.signals, mock_pull_client.signals)

    @mock.patch("morango.sync.syncsession.PushClient")
    def test_initiate_push(self, MockPushClient):
        """
        TODO: should eventually be removed as this method is deprecated
        """
        mock_pull_client = mock.Mock(spec=PushClient)
        MockPushClient.return_value = mock_pull_client
        client = self.build_client(SyncSessionClient)

        sync_filter = Filter("abc123")
        client.initiate_push(sync_filter)
        MockPushClient.assert_called_with(
            self.conn, self.session, chunk_size=self.chunk_size
        )

        mock_pull_client.initialize.assert_called_once_with(sync_filter)
        mock_pull_client.run.assert_called_once()
        mock_pull_client.finalize.assert_called_once()

        self.assertEqual(client.signals, mock_pull_client.signals)

    def test_close_sync_session(self):
        """
        TODO: should eventually be removed as this method is deprecated
        """
        conn = mock.Mock(spec=NetworkSyncConnection)
        client = SyncSessionClient(conn, self.session)
        client.close_sync_session()
        conn.close.assert_called_once()

    @mock.patch(
        "morango.sync.syncsession.NetworkSyncConnection._update_transfer_session"
    )
    @mock.patch("morango.sync.syncsession._queue_into_buffer")
    @mock.patch("morango.sync.syncsession.BaseSyncClient._create_transfer_session")
    @mock.patch("morango.sync.syncsession._serialize_into_store")
    @mock.patch("morango.sync.syncsession.settings")
    def test_push_client__initialize(
        self,
        mock_settings,
        mock_serialize,
        mock_parent_create,
        mock_queue,
        mock_transfer_update,
    ):
        mock_handler = mock.Mock()
        client = self.build_client(PushClient)
        client.signals.queuing.connect(mock_handler)
        sync_filter = Filter("abc123")
        setattr(mock_settings, "MORANGO_SERIALIZE_BEFORE_QUEUING", True)

        client.initialize(sync_filter)
        mock_serialize.assert_called_with(self.session.profile, filter=sync_filter)
        mock_parent_create.assert_called_with(sync_filter, push=True)
        mock_queue.assert_called_with(client.current_transfer_session)
        mock_transfer_update.assert_called_once_with(
            {"records_total": client.current_transfer_session.records_total},
            client.current_transfer_session,
        )

        mock_handler.assert_any_call(transfer_session=client.current_transfer_session)

    @mock.patch("morango.sync.syncsession.PushClient._push_records")
    def test_push_client__run(self, mock_push):
        client = self.build_client(PushClient)
        client.current_transfer_session.records_total = 1
        client.run()

        mock_push.assert_called_once()
        self.transferring_mock.assert_called_with(
            transfer_session=client.current_transfer_session
        )

    @mock.patch("morango.sync.syncsession.PushClient._push_records")
    def test_push_client__run__no_records(self, mock_push):
        client = self.build_client(PushClient)
        client.current_transfer_session.records_total = 0
        client.run()
        mock_push.assert_not_called()

    @mock.patch("morango.sync.syncsession.BaseSyncClient._close_transfer_session")
    def test_push_client__finalize(self, mock_close):
        mock_handler = mock.Mock()
        client = self.build_client(PushClient)
        client.signals.dequeuing.connect(mock_handler)
        client.finalize()

        mock_handler.assert_any_call(transfer_session=client.current_transfer_session)
        mock_close.assert_called_once()

    @mock.patch("morango.sync.syncsession.BaseSyncClient._create_transfer_session")
    def test_pull_client__initialize(self, mock_parent_create):
        client = self.build_client(PullClient)
        sync_filter = Filter("abc123")

        client.initialize(sync_filter)
        self.assertEqual(sync_filter, client.sync_filter)
        mock_parent_create.assert_called_with(sync_filter, push=False)

    @mock.patch("morango.sync.syncsession.PullClient._pull_records")
    def test_pull_client__run(self, mock_pull):
        client = self.build_client(PullClient)
        client.current_transfer_session.records_total = 1
        client.run()

        mock_pull.assert_called_once()
        self.transferring_mock.assert_called_with(
            transfer_session=client.current_transfer_session
        )

    @mock.patch("morango.sync.syncsession.PullClient._pull_records")
    def test_pull_client__run__no_records(self, mock_pull):
        client = self.build_client(PullClient)
        client.current_transfer_session.records_total = 0
        client.run()
        mock_pull.assert_not_called()

    @mock.patch("morango.sync.syncsession.BaseSyncClient._close_transfer_session")
    @mock.patch("morango.sync.syncsession._dequeue_into_store")
    def test_pull_client__finalize(self, mock_dequeue, mock_close):
        mock_handler = mock.Mock()
        client = self.build_client(PullClient)
        client.signals.dequeuing.connect(mock_handler)
        client.sync_filter = Filter("abc123")
        client.current_transfer_session.server_fsic = "{}"
        client.finalize()

        mock_dequeue.assert_called_with(client.current_transfer_session)
        mock_handler.assert_any_call(transfer_session=client.current_transfer_session)
        mock_close.assert_called_once()

    @mock.patch(
        "morango.sync.syncsession.BaseSyncClient._initialize_server_transfer_session"
    )
    def test_pull_client__initialize_server_transfer_session(
        self, mock_parent_initialize
    ):
        mock_handler = mock.Mock()
        mock_response = mock.Mock()

        client = self.build_client(PullClient)
        client.signals.queuing.connect(mock_handler)

        mock_parent_initialize.return_value = mock_response
        mock_response.json.return_value = dict(records_total=101)

        self.assertEqual(mock_response, client._initialize_server_transfer_session())
        self.assertEqual(client.current_transfer_session.records_total, 101)
        mock_handler.assert_any_call(transfer_session=client.current_transfer_session)


class SessionWrapperTestCase(TestCase):
    @mock.patch("morango.sync.session.Session.request")
    def test_request(self, mocked_super_request):
        headers = {"Content-Length": 1024}
        expected = mocked_super_request.return_value = mock.Mock(
            headers=headers, raise_for_status=mock.Mock(), status_code=200, reason="OK"
        )

        wrapper = SessionWrapper()
        actual = wrapper.request("GET", "test_url", is_test=True)
        mocked_super_request.assert_called_once_with("GET", "test_url", is_test=True)
        self.assertEqual(expected, actual)

        head_length = len("HTTP/1.1 200 OK") + _length_of_headers(headers)
        self.assertEqual(wrapper.bytes_received, 1024 + head_length)

    @mock.patch("morango.sync.session.logger")
    @mock.patch("morango.sync.session.Session.request")
    def test_request__not_ok(self, mocked_super_request, mocked_logger):
        raise_for_status = mock.Mock()
        expected = mocked_super_request.return_value = mock.Mock(
            headers={"Content-Length": 1024},
            raise_for_status=raise_for_status,
            content="Connection timeout",
        )

        raise_for_status.side_effect = HTTPError(response=expected)

        wrapper = SessionWrapper()

        with self.assertRaises(HTTPError):
            wrapper.request("GET", "test_url", is_test=True)

        mocked_super_request.assert_called_once_with("GET", "test_url", is_test=True)
        mocked_logger.error.assert_called_once_with(
            "HTTPError Reason: Connection timeout"
        )

    @mock.patch("morango.sync.session.logger")
    @mock.patch("morango.sync.session.Session.request")
    def test_request__really_not_ok(self, mocked_super_request, mocked_logger):
        raise_for_status = mock.Mock()
        mocked_super_request.return_value = mock.Mock(
            headers={"Content-Length": 1024}, raise_for_status=raise_for_status,
        )

        raise_for_status.side_effect = RequestException()

        wrapper = SessionWrapper()

        with self.assertRaises(RequestException):
            wrapper.request("GET", "test_url", is_test=True)

        mocked_super_request.assert_called_once_with("GET", "test_url", is_test=True)
        mocked_logger.error.assert_called_once_with(
            "RequestException Reason: (no response)"
        )

    @mock.patch("morango.sync.session.Session.prepare_request")
    def test_prepare_request(self, mocked_super_prepare_request):
        headers = {"Content-Length": 256}
        expected = mocked_super_prepare_request.return_value = mock.Mock(
            headers=headers,
        )

        request = mock.Mock(url="http://test_app/path/to/resource", method="GET")
        wrapper = SessionWrapper()
        actual = wrapper.prepare_request(request)
        mocked_super_prepare_request.assert_called_once_with(request)

        self.assertEqual(expected, actual)
        head_length = len("GET /path/to/resource HTTP/1.1") + _length_of_headers(
            headers
        )
        self.assertEqual(wrapper.bytes_sent, 256 + head_length)
