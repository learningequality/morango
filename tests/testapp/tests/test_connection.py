import json
import uuid

import mock
from django.test.testcases import LiveServerTestCase
from django.test.utils import override_settings
from django.utils import timezone
from facility_profile.models import SummaryLog
from requests.exceptions import HTTPError

from morango.api.serializers import BufferSerializer
from morango.api.serializers import CertificateSerializer
from morango.certificates import Certificate
from morango.certificates import Key
from morango.certificates import ScopeDefinition
from morango.constants.capabilities import ALLOW_CERTIFICATE_PUSHING
from morango.controller import MorangoProfileController
from morango.crypto import SharedKey
from morango.errors import CertificateSignatureInvalid
from morango.errors import MorangoServerDoesNotAllowNewCertPush
from morango.models import Buffer
from morango.models import InstanceIDModel
from morango.models import SyncSession
from morango.models import TransferSession
from morango.session import SessionWrapper
from morango.syncsession import NetworkSyncConnection
from morango.syncsession import SyncClient


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


class SyncClientTestCase(LiveServerTestCase):
    def setUp(self):
        session = SyncSession.objects.create(
            id=uuid.uuid4().hex,
            profile="facilitydata",
            last_activity_timestamp=timezone.now(),
        )
        transfer_session = TransferSession.objects.create(
            id=uuid.uuid4().hex,
            sync_session=session,
            filter="partition",
            push=True,
            last_activity_timestamp=timezone.now(),
            records_total=3,
        )
        conn = NetworkSyncConnection(base_url=self.live_server_url)
        self.syncclient = SyncClient(conn, session)
        self.syncclient.current_transfer_session = transfer_session
        self.chunk_size = 3
        InstanceIDModel.get_or_create_current_instance()

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

    @mock_patch_decorator
    def test_push_records(self):
        self.build_buffer_items(self.syncclient.current_transfer_session)
        self.assertEqual(
            self.syncclient.current_transfer_session.records_transferred, 0
        )
        self.syncclient._push_records()
        self.assertEqual(
            self.syncclient.current_transfer_session.records_transferred,
            self.chunk_size,
        )

    @mock_patch_decorator
    def test_pull_records(self):
        self.syncclient.current_transfer_session.push = False
        self.syncclient.current_transfer_session.save()
        resp = self.build_buffer_items(self.syncclient.current_transfer_session)
        SessionWrapper.request.return_value.json.return_value = json.loads(resp)
        Buffer.objects.filter(
            transfer_session=self.syncclient.current_transfer_session
        ).delete()
        self.assertEqual(
            Buffer.objects.filter(
                transfer_session=self.syncclient.current_transfer_session
            ).count(),
            0,
        )
        self.assertEqual(
            self.syncclient.current_transfer_session.records_transferred, 0
        )
        self.syncclient._pull_records()
        self.assertEqual(
            Buffer.objects.filter(
                transfer_session=self.syncclient.current_transfer_session
            ).count(),
            self.chunk_size,
        )
        self.assertEqual(
            self.syncclient.current_transfer_session.records_transferred,
            self.chunk_size,
        )

    @mock_patch_decorator
    def test_create_transfer_session_push(self):
        self.syncclient.current_transfer_session.active = False
        self.syncclient.current_transfer_session.save()
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 0)
        self.syncclient._create_transfer_session(True, "filter")
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)

    @mock_patch_decorator
    def test_close_transfer_session_push(self):
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)
        self.syncclient._close_transfer_session()
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 0)

    @mock_patch_decorator
    def test_close_sync_session(self):
        self.assertEqual(SyncSession.objects.filter(active=True).count(), 1)
        self.syncclient._close_transfer_session()
        self.syncclient.close_sync_session()
        self.assertEqual(SyncSession.objects.filter(active=True).count(), 0)
