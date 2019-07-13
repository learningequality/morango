import json
import uuid

import mock
from django.test.testcases import LiveServerTestCase
from django.test.utils import override_settings
from django.utils import timezone
from facility_profile.models import InteractionLog
from facility_profile.models import MyUser
from facility_profile.models import SummaryLog
from requests.exceptions import HTTPError
from rest_framework.exceptions import ValidationError

from morango.api.serializers import BufferSerializer
from morango.api.serializers import CertificateSerializer
from morango.api.serializers import InstanceIDSerializer
from morango.constants import transfer_status
from morango.constants.capabilities import ALLOW_CERTIFICATE_PUSHING
from morango.constants.capabilities import GZIP_BUFFER_POST
from morango.errors import CertificateSignatureInvalid
from morango.errors import MorangoError
from morango.errors import MorangoServerDoesNotAllowNewCertPush
from morango.models.certificates import Certificate
from morango.models.certificates import Key
from morango.models.certificates import ScopeDefinition
from morango.models.core import Buffer
from morango.models.core import InstanceIDModel
from morango.models.core import Store
from morango.models.core import SyncSession
from morango.models.core import TransferSession
from morango.models.fields.crypto import SharedKey
from morango.sync.controller import MorangoProfileController
from morango.sync.session import SessionWrapper
from morango.sync.syncsession import NetworkSyncConnection
from morango.sync.syncsession import SyncClient


def mock_session_request(func):
    def wrapper(*args, **kwargs):
        mock_object = mock.Mock()
        mock_object.json.return_value = {}
        with mock.patch.object(SessionWrapper, "request", return_value=mock_object):
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

        for i in range(3):
            data["source_id"] = uuid.uuid4().hex
            data["model_uuid"] = SummaryLog.compute_namespaced_id(
                data["partition"], data["source_id"], data["model_name"]
            )
            Buffer.objects.create(**data)

        buffered_items = Buffer.objects.filter(transfer_session=transfer_session)
        serialized_records = BufferSerializer(buffered_items, many=True)
        return serialized_records.data

    @mock.patch.object(Certificate.objects, "filter", return_value=mock.Mock())
    def test_retrieve_server_cert_if_needed(self, mock_filter):
        Certificate.objects.filter().exists.return_value = False
        with mock.patch.object(Certificate, "save_certificate_chain"):
            self.network_connection._retrieve_server_cert_if_needed(self.root_cert)
            Certificate.save_certificate_chain.assert_called()

    def test_push_record_chunk(self):
        session = SyncSession.objects.create(
            id=uuid.uuid4().hex,
            profile="facilitydata",
            last_activity_timestamp=timezone.now(),
            client_certificate=self.subset_cert,
            server_certificate=self.root_cert,
            server_instance=json.dumps(
                InstanceIDSerializer(
                    InstanceIDModel.get_or_create_current_instance()[0]
                ).data
            ),
        )
        # test with gzip enabled
        ts = TransferSession.objects.create(
            id=uuid.uuid4().hex, push=True, sync_session=session, filter="partition"
        )
        data = self.build_buffer_items(ts)
        Buffer.objects.all().delete()
        self.network_connection._push_record_chunk(data)
        self.assertEqual(len(data), Buffer.objects.count())
        Buffer.objects.all().delete()
        # test with gzip disabled
        self.network_connection.capabilities.remove(GZIP_BUFFER_POST)
        self.network_connection._push_record_chunk(data)
        self.assertEqual(len(data), Buffer.objects.count())

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

    @mock_session_request
    def test_resuming_sync_session(self):
        session = SyncSession.objects.create(
            id=uuid.uuid4().hex,
            profile="facilitydata",
            last_activity_timestamp=timezone.now(),
            client_certificate=self.subset_cert,
            server_certificate=self.root_cert,
            server_instance=json.dumps(
                InstanceIDSerializer(
                    InstanceIDModel.get_or_create_current_instance()[0]
                ).data
            ),
        )
        SessionWrapper.request.return_value.json.return_value = {"active": True}
        client = self.network_connection.create_sync_session(
            self.subset_cert, self.root_cert
        )
        self.assertEqual(session.id, client.sync_session.id)
        self.assertEqual(SyncSession.objects.count(), 1)

    @mock_session_request
    @mock.patch.object(Certificate, "verify", return_value=True)
    def test_not_resuming_sync_session(self, mock_verify):
        session = SyncSession.objects.create(
            id=uuid.uuid4().hex,
            profile="facilitydata",
            last_activity_timestamp=timezone.now(),
            client_certificate=self.subset_cert,
            server_certificate=self.root_cert,
            server_instance=json.dumps(
                InstanceIDSerializer(
                    InstanceIDModel.get_or_create_current_instance()[0]
                ).data
            ),
        )
        SessionWrapper.request.return_value.json.return_value = {
            "active": False,
            "id": "abc",
        }
        client = self.network_connection.create_sync_session(
            self.subset_cert, self.root_cert
        )
        self.assertNotEqual(session.id, client.sync_session.id)
        self.assertEqual(SyncSession.objects.count(), 2)

    @mock_session_request
    @mock.patch.object(Certificate, "verify", return_value=True)
    def test_not_resuming_sync_session_404(self, mock_verify):
        session = SyncSession.objects.create(
            id=uuid.uuid4().hex,
            profile="facilitydata",
            last_activity_timestamp=timezone.now(),
            client_certificate=self.subset_cert,
            server_certificate=self.root_cert,
            server_instance=json.dumps(
                InstanceIDSerializer(
                    InstanceIDModel.get_or_create_current_instance()[0]
                ).data
            ),
        )
        SessionWrapper.request.return_value.json.return_value = {"id": "abc"}
        with mock.patch.object(
            NetworkSyncConnection,
            "_get_sync_session",
            side_effect=HTTPError(response=mock.Mock(status_code=404)),
        ):
            client = self.network_connection.create_sync_session(
                self.subset_cert, self.root_cert
            )
        self.assertNotEqual(session.id, client.sync_session.id)
        self.assertEqual(SyncSession.objects.count(), 2)

    @mock_session_request
    @mock.patch.object(Certificate, "verify", return_value=True)
    def test_not_resuming_sync_session_http_error(self, mock_verify):
        SyncSession.objects.create(
            id=uuid.uuid4().hex,
            profile="facilitydata",
            last_activity_timestamp=timezone.now(),
            client_certificate=self.subset_cert,
            server_certificate=self.root_cert,
            server_instance=json.dumps(
                InstanceIDSerializer(
                    InstanceIDModel.get_or_create_current_instance()[0]
                ).data
            ),
        )
        with self.assertRaises(HTTPError):
            with mock.patch.object(
                NetworkSyncConnection,
                "_get_sync_session",
                side_effect=HTTPError(response=mock.Mock(status_code=403)),
            ):
                self.network_connection.create_sync_session(
                    self.subset_cert, self.root_cert
                )

    @mock_session_request
    @mock.patch.object(Certificate, "verify", return_value=True)
    def test_not_resuming_sync_session_not_same_server(self, mock_verify):
        session = SyncSession.objects.create(
            id=uuid.uuid4().hex,
            profile="facilitydata",
            last_activity_timestamp=timezone.now(),
            client_certificate=self.subset_cert,
            server_certificate=self.root_cert,
            server_instance=json.dumps({"id": "123"}),
        )
        SessionWrapper.request.return_value.json.return_value = {
            "active": True,
            "id": "abc",
        }
        client = self.network_connection.create_sync_session(
            self.subset_cert, self.root_cert
        )
        self.assertNotEqual(session.id, client.sync_session.id)
        self.assertEqual(SyncSession.objects.count(), 2)

    def test_get_cert_chain(self):
        response = self.network_connection._get_certificate_chain(
            params={"ancestors_of": self.subset_cert.id}
        )
        data = response.json()
        self.assertEqual(len(data), Certificate.objects.count())
        self.assertEqual(data[0]["id"], self.root_cert.id)
        self.assertEqual(data[1]["id"], self.subset_cert.id)

    def test_get_remote_certs(self):
        certs = self.subset_cert.get_ancestors(include_self=True)
        remote_certs = self.network_connection.get_remote_certificates(
            self.root_cert.id
        )
        self.assertSetEqual(set(certs), set(remote_certs))
        # test scope_def_id
        remote_certs = self.network_connection.get_remote_certificates(
            self.root_cert.id, scope_def_id=self.root_scope_def.id
        )
        self.assertEqual(set([self.root_cert]), set(remote_certs))
        # test scope_params json
        remote_certs = self.network_connection.get_remote_certificates(
            self.root_cert.id, scope_params=self.subset_cert.scope_params
        )
        self.assertEqual(set([self.subset_cert]), set(remote_certs))
        # test scope_params dict
        remote_certs = self.network_connection.get_remote_certificates(
            self.root_cert.id, scope_params=json.loads(self.subset_cert.scope_params)
        )
        self.assertEqual(set([self.subset_cert]), set(remote_certs))

    @mock_session_request
    def test_csr(self):
        # mock a "signed" cert being returned by server
        cert_serialized = json.dumps(CertificateSerializer(self.subset_cert).data)
        SessionWrapper.request.return_value.json.return_value = json.loads(
            cert_serialized
        )
        self.subset_cert.delete()

        # we only want to make sure the "signed" cert is saved
        with mock.patch.object(
            Key,
            "get_private_key_string",
            return_value=self.subset_cert.private_key.get_private_key_string(),
        ):
            self.network_connection.certificate_signing_request(
                self.root_cert, "", "", userargs={"username": "fakeuser"}
            )
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


class SyncClientTestCase(LiveServerTestCase):
    def setUp(self):
        self.syncsession = SyncSession.objects.create(
            id=uuid.uuid4().hex,
            profile="facilitydata",
            last_activity_timestamp=timezone.now(),
        )
        conn = NetworkSyncConnection(base_url=self.live_server_url)
        self.syncclient = SyncClient(conn, self.syncsession, chunk_size=3)
        InstanceIDModel.get_or_create_current_instance()
        # create some dummy data
        self.user = MyUser.objects.create(username="testuser")
        self.filter = self.user.id
        SummaryLog.objects.create(user=self.user)
        InteractionLog.objects.create(user=self.user)

        self.transfersession = TransferSession.objects.create(
            id=uuid.uuid4().hex,
            sync_session=self.syncsession,
            filter=self.filter,
            push=True,
            last_activity_timestamp=timezone.now(),
            records_total=3,
        )
        self.syncclient.current_transfer_session = self.transfersession

    @mock_session_request
    def test_not_resuming_start_transfer_session_inactive(self):
        self.syncclient.current_transfer_session = None
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)
        SessionWrapper.request.return_value.json.return_value = {
            "active": False,
            "push": True,
        }
        self.syncclient._starting_transfer_session(self.filter, True)
        # new transfer session should be created
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 2)
        self.assertNotEqual(
            self.syncclient.current_transfer_session, self.transfersession
        )

    @mock_session_request
    def test_resuming_start_transfer_session_records_transferred(self):
        records_transferred = 100
        self.assertEqual(
            self.syncclient.current_transfer_session.records_transferred, 0
        )
        self.syncclient.current_transfer_session.transfer_stage = (
            transfer_status.PUSHING
        )
        self.syncclient.current_transfer_session.save()
        self.syncclient.current_transfer_session = None
        SessionWrapper.request.return_value.json.return_value = {
            "active": True,
            "push": True,
            "records_transferred": records_transferred,
        }
        self.syncclient._starting_transfer_session(self.filter, True)
        self.assertEqual(self.syncclient.current_transfer_session, self.transfersession)
        self.assertEqual(
            self.syncclient.current_transfer_session.records_transferred,
            records_transferred,
        )

    @mock_session_request
    def test_not_resuming_start_transfer_session_404(self):
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)
        with mock.patch.object(
            NetworkSyncConnection,
            "_get_transfer_session",
            side_effect=HTTPError(response=mock.Mock(status_code=404)),
        ):
            self.syncclient._starting_transfer_session(self.filter, True)
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 2)
        self.assertNotEqual(
            self.syncclient.current_transfer_session, self.transfersession
        )

    @mock_session_request
    def test_not_resuming_start_transfer_session_403(self):
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)
        with self.assertRaises(HTTPError):
            with mock.patch.object(
                NetworkSyncConnection,
                "_get_transfer_session",
                side_effect=HTTPError(response=mock.Mock(status_code=403)),
            ):
                self.syncclient._starting_transfer_session(self.filter, True)
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)

    @mock_session_request
    def test_close_transfer_session(self):
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)
        self.syncclient._close_transfer_session()
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 0)
        self.assertEqual(
            TransferSession.objects.filter(
                transfer_stage=transfer_status.COMPLETED
            ).count(),
            1,
        )

    @mock_session_request
    def test_close_transfer_session_http_error(self):
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)
        with self.assertRaises(HTTPError):
            with mock.patch.object(
                NetworkSyncConnection,
                "_close_transfer_session",
                side_effect=HTTPError(),
            ):
                self.syncclient._close_transfer_session()
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 0)
        self.assertEqual(
            TransferSession.objects.filter(
                transfer_stage=transfer_status.COMPLETED
            ).count(),
            0,
        )

    @mock_session_request
    def test_close_sync_session(self):
        self.assertEqual(SyncSession.objects.filter(active=True).count(), 1)
        self.syncclient._close_transfer_session()
        self.syncclient.close_sync_session()
        self.assertEqual(SyncSession.objects.filter(active=True).count(), 0)

    @mock_session_request
    def test_close_sync_session_with_transfer_session(self):
        self.assertEqual(SyncSession.objects.filter(active=True).count(), 1)
        with self.assertRaises(MorangoError):
            self.syncclient.close_sync_session()
        self.assertEqual(SyncSession.objects.filter(active=True).count(), 1)


class SyncClientPushTestCase(LiveServerTestCase):
    def setUp(self):
        self.syncsession = SyncSession.objects.create(
            id=uuid.uuid4().hex,
            profile="facilitydata",
            last_activity_timestamp=timezone.now(),
        )
        conn = NetworkSyncConnection(base_url=self.live_server_url)
        self.syncclient = SyncClient(conn, self.syncsession, chunk_size=3)
        self.instance_id = InstanceIDModel.get_or_create_current_instance()[0]
        # create some dummy data
        self.user = MyUser.objects.create(username="testuser")
        self.filter = self.user.id
        SummaryLog.objects.create(user=self.user)
        InteractionLog.objects.create(user=self.user)

        self.transfersession = TransferSession.objects.create(
            id=uuid.uuid4().hex,
            sync_session=self.syncsession,
            filter=self.filter,
            push=True,
            last_activity_timestamp=timezone.now(),
            records_total=3,
            client_fsic=json.dumps({self.instance_id.id: 1}),
        )
        self.syncclient.current_transfer_session = self.transfersession

    def build_buffer_items(self, transfer_session, **kwargs):

        data = {
            "profile": kwargs.get("profile", "facilitydata"),
            "serialized": kwargs.get("serialized", '{"test": 99}'),
            "deleted": kwargs.get("deleted", False),
            "last_saved_instance": kwargs.get("last_saved_instance", uuid.uuid4().hex),
            "last_saved_counter": kwargs.get("last_saved_counter", 179),
            "partition": kwargs.get("partition", self.filter),
            "source_id": kwargs.get("source_id", uuid.uuid4().hex),
            "model_name": kwargs.get("model_name", "contentsummarylog"),
            "conflicting_serialized_data": kwargs.get(
                "conflicting_serialized_data", ""
            ),
            "model_uuid": kwargs.get("model_uuid", None),
            "transfer_session": transfer_session,
        }

        for i in range(self.syncclient.chunk_size):
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

    @mock_session_request
    def test_starting_transfer_session(self):
        self.syncclient.current_transfer_session.delete(soft=False)
        self.assertEqual(TransferSession.objects.count(), 0)
        self.syncclient._starting_transfer_session(self.filter, True)
        # data should have been serialized
        self.assertTrue(Store.objects.all().exists())
        self.assertEqual(
            self.syncclient.current_transfer_session.transfer_stage,
            transfer_status.QUEUING,
        )
        self.assertEqual(
            TransferSession.objects.filter(active=True, push=True).count(), 1
        )

    @mock_session_request
    def test_resuming_start_transfer_session(self):
        # create random transfer session tied to same sync session
        TransferSession.objects.create(
            id=uuid.uuid4().hex,
            sync_session=self.syncsession,
            filter=self.filter,
            push=False,
            last_activity_timestamp=timezone.now(),
            records_total=1,
        )
        self.syncclient.current_transfer_session = None
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 2)
        SessionWrapper.request.return_value.json.return_value = {
            "active": True,
            "push": True,
        }
        self.syncclient._starting_transfer_session(self.filter, True)
        # ensure other transfer session attached to sync session is turned off
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)
        self.assertEqual(self.syncclient.current_transfer_session, self.transfersession)

    @mock_session_request
    def test_queuing(self):
        self.syncclient.current_transfer_session.delete(soft=False)
        self.syncclient._starting_transfer_session(self.filter, True)
        self.syncclient.current_transfer_session.client_fsic = json.dumps(
            {self.instance_id.id: 1}
        )
        self.syncclient._queuing(data=None, push=True)
        self.assertTrue(Buffer.objects.exists())
        self.assertEqual(
            self.syncclient.current_transfer_session.records_total,
            Buffer.objects.count(),
        )
        self.assertEqual(
            self.syncclient.current_transfer_session.transfer_stage,
            transfer_status.PUSHING,
        )

    def test_push_records(self):
        # build up records to be patched onto return value
        self.build_buffer_items(self.syncclient.current_transfer_session)
        buffered_records = list(
            Buffer.objects.filter(
                transfer_session=self.syncclient.current_transfer_session
            )
        )
        Buffer.objects.all().delete()
        self.syncclient.current_transfer_session.records_total = len(buffered_records)
        self.syncclient.current_transfer_session.save()
        with mock.patch.object(Buffer.objects, "filter", return_value=mock.Mock()):
            Buffer.objects.filter().order_by.return_value = buffered_records
            self.syncclient._push_records()
        self.assertEqual(
            self.syncclient.current_transfer_session.records_transferred,
            Buffer.objects.count(),
        )

    def test_push_records_resume_records_transferred(self):
        self.build_buffer_items(self.syncclient.current_transfer_session)
        buffered_records = list(
            Buffer.objects.filter(
                transfer_session=self.syncclient.current_transfer_session
            )
        )
        Buffer.objects.last().delete()
        self.syncclient.current_transfer_session.records_transferred = (
            len(buffered_records) - 1
        )
        with mock.patch.object(Buffer.objects, "filter", return_value=mock.Mock()):
            Buffer.objects.filter().order_by.return_value = buffered_records
            self.syncclient._push_records()
        self.assertEqual(
            self.syncclient.current_transfer_session.records_transferred,
            Buffer.objects.count(),
        )

    def test_push_records_http_error(self):
        with self.assertRaises(HTTPError):
            with mock.patch.object(
                NetworkSyncConnection, "_push_record_chunk", side_effect=HTTPError()
            ):
                self.syncclient._push_records()
        self.assertIsNone(self.syncclient.current_transfer_session)
        self.assertEqual(TransferSession.objects.filter(active=False).count(), 1)

    def test_pushing_update_records_total(self):
        self.syncclient.current_transfer_session.records_total = 100
        with mock.patch.object(SyncClient, "_push_records"):
            self.syncclient._pushing()
        self.syncclient.current_transfer_session.refresh_from_db()
        self.assertEqual(self.syncclient.current_transfer_session.records_total, 100)

    @mock_session_request
    def test_pushing_http_error(self):
        with self.assertRaises(HTTPError):
            with mock.patch.object(
                NetworkSyncConnection,
                "_update_transfer_session",
                side_effect=HTTPError(),
            ):
                self.syncclient._pushing()
        self.assertIsNone(self.syncclient.current_transfer_session)
        self.assertEqual(TransferSession.objects.filter(active=False).count(), 1)

    @mock_session_request
    def test_pushing(self):
        self.build_buffer_items(self.syncclient.current_transfer_session)
        with mock.patch.object(SyncClient, "_push_records"):
            self.syncclient._pushing()
        self.assertFalse(Buffer.objects.exists())
        self.assertTrue(
            self.syncclient.current_transfer_session.transfer_stage,
            transfer_status.DEQUEUING,
        )

    @mock_session_request
    def test_dequeuing(self):
        ts = self.syncclient.current_transfer_session
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)
        self.syncclient._dequeuing(push=True)
        ts.refresh_from_db()
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 0)
        self.assertEqual(ts.transfer_stage, transfer_status.COMPLETED)

    @mock.patch.object(SyncClient, "_dequeuing")
    @mock.patch.object(SyncClient, "_pushing")
    @mock.patch.object(SyncClient, "_queuing")
    def test_resume_queuing_stage(self, mock_queuing, mock_pushing, mock_dequeuing):
        self.syncclient.current_transfer_session.transfer_stage = (
            transfer_status.QUEUING
        )
        self.syncclient.current_transfer_session.save()
        mock_queuing.side_effect = lambda *args, **kwargs: setattr(
            self.syncclient.current_transfer_session,
            "transfer_stage",
            transfer_status.PUSHING,
        )
        mock_pushing.side_effect = lambda *args, **kwargs: setattr(
            self.syncclient.current_transfer_session,
            "transfer_stage",
            transfer_status.DEQUEUING,
        )
        self.syncclient.initiate_push(self.filter)
        mock_queuing.assert_called()
        mock_pushing.assert_called()
        mock_dequeuing.assert_called()

    @mock.patch.object(SyncClient, "_dequeuing")
    @mock.patch.object(SyncClient, "_pushing")
    @mock.patch.object(SyncClient, "_queuing")
    def test_resume_pushing_stage(self, mock_queuing, mock_pushing, mock_dequeuing):
        self.syncclient.current_transfer_session.transfer_stage = (
            transfer_status.PUSHING
        )
        self.syncclient.current_transfer_session.save()
        mock_pushing.side_effect = lambda *args, **kwargs: setattr(
            self.syncclient.current_transfer_session,
            "transfer_stage",
            transfer_status.DEQUEUING,
        )
        self.syncclient.initiate_push(self.filter)
        mock_queuing.assert_not_called()
        mock_pushing.assert_called()
        mock_dequeuing.assert_called()

    @mock.patch.object(SyncClient, "_dequeuing")
    @mock.patch.object(SyncClient, "_pushing")
    @mock.patch.object(SyncClient, "_queuing")
    def test_resume_dequeuing_stage(self, mock_queuing, mock_pushing, mock_dequeuing):
        self.syncclient.current_transfer_session.transfer_stage = (
            transfer_status.DEQUEUING
        )
        self.syncclient.current_transfer_session.save()
        self.syncclient.initiate_push(self.filter)
        mock_queuing.assert_not_called()
        mock_pushing.assert_not_called()
        mock_dequeuing.assert_called()


class SyncClientPullTestCase(LiveServerTestCase):
    def setUp(self):
        self.syncsession = SyncSession.objects.create(
            id=uuid.uuid4().hex,
            profile="facilitydata",
            last_activity_timestamp=timezone.now(),
        )
        conn = NetworkSyncConnection(base_url=self.live_server_url)
        self.syncclient = SyncClient(conn, self.syncsession, chunk_size=3)
        self.instance_id = InstanceIDModel.get_or_create_current_instance()[0]
        # create some dummy data
        self.user = MyUser.objects.create(username="testuser")
        self.filter = self.user.id
        SummaryLog.objects.create(user=self.user)
        InteractionLog.objects.create(user=self.user)

        self.transfersession = TransferSession.objects.create(
            id=uuid.uuid4().hex,
            sync_session=self.syncsession,
            filter=self.filter,
            push=False,
            last_activity_timestamp=timezone.now(),
            records_total=3,
        )
        self.syncclient.current_transfer_session = self.transfersession

    def build_buffer_items(self, transfer_session, **kwargs):

        data = {
            "profile": kwargs.get("profile", "facilitydata"),
            "serialized": kwargs.get("serialized", '{"test": 99}'),
            "deleted": kwargs.get("deleted", False),
            "last_saved_instance": kwargs.get("last_saved_instance", uuid.uuid4().hex),
            "last_saved_counter": kwargs.get("last_saved_counter", 179),
            "partition": kwargs.get("partition", self.filter),
            "source_id": kwargs.get("source_id", uuid.uuid4().hex),
            "model_name": kwargs.get("model_name", "contentsummarylog"),
            "conflicting_serialized_data": kwargs.get(
                "conflicting_serialized_data", ""
            ),
            "model_uuid": kwargs.get("model_uuid", None),
            "transfer_session": transfer_session,
        }

        for i in range(self.syncclient.chunk_size):
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

    @mock_session_request
    def test_resuming_start_transfer_session_pull(self):
        self.transfersession.push = False
        self.transfersession.save()
        self.syncclient.current_transfer_session = None
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)
        SessionWrapper.request.return_value.json.return_value = {
            "active": True,
            "push": False,
        }
        data = self.syncclient._starting_transfer_session(self.filter, False)
        # new transfer session should NOT be created
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)
        self.assertEqual(data["id"], self.transfersession.id)

    @mock_session_request
    def test_start_transfer_session_pull(self):
        self.syncclient.current_transfer_session.delete(soft=False)
        self.assertEqual(TransferSession.objects.count(), 0)
        self.syncclient._starting_transfer_session(self.filter, False)
        self.assertEqual(
            self.syncclient.current_transfer_session.transfer_stage,
            transfer_status.QUEUING,
        )
        self.assertEqual(
            TransferSession.objects.filter(active=True, push=False).count(), 1
        )

    @mock_session_request
    def test_queuing(self):
        self.build_buffer_items(self.syncclient.current_transfer_session)
        SessionWrapper.request.return_value.json.return_value = {
            "records_total": Buffer.objects.count()
        }
        self.syncclient._queuing(None, push=False)
        self.assertEqual(
            self.syncclient.current_transfer_session.records_total,
            Buffer.objects.count(),
        )
        self.assertEqual(
            self.syncclient.current_transfer_session.transfer_stage,
            transfer_status.PULLING,
        )

    def test_queuing_http_error(self):
        with self.assertRaises(HTTPError):
            with mock.patch.object(
                NetworkSyncConnection,
                "_create_transfer_session",
                side_effect=HTTPError(),
            ):
                self.syncclient._queuing(None, push=False)
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 0)

    @mock_session_request
    def test_pull_records(self):
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
            self.syncclient.chunk_size,
        )
        self.assertEqual(
            self.syncclient.current_transfer_session.records_transferred,
            self.syncclient.chunk_size,
        )

    def test_pull_records_resume_records_transferred(self):
        self.build_buffer_items(self.syncclient.current_transfer_session)
        buffered_records = list(
            Buffer.objects.filter(
                transfer_session=self.syncclient.current_transfer_session
            )
        )
        Buffer.objects.last().delete()
        self.syncclient.current_transfer_session.records_transferred = (
            len(buffered_records) - 1
        )
        with mock.patch.object(Buffer.objects, "filter", return_value=buffered_records):
            self.syncclient._pull_records()
        self.assertEqual(
            self.syncclient.current_transfer_session.records_transferred,
            Buffer.objects.count(),
        )

    def test_pull_records_http_error(self):
        with self.assertRaises(HTTPError):
            with mock.patch.object(
                NetworkSyncConnection, "_pull_record_chunk", side_effect=HTTPError()
            ):
                self.syncclient._pull_records()
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 0)

    @mock_session_request
    def test_pull_records_integrity_checks(self):
        data = [
            {"transfer_session": self.syncclient.current_transfer_session.id},
            {"transfer_session": uuid.uuid4().hex},
        ]
        SessionWrapper.request.return_value.json.return_value = data
        with self.assertRaises(ValidationError) as e:
            self.syncclient._pull_records()
        self.assertIn("same TransferSession", str(e.exception))
        data.pop(1)
        SessionWrapper.request.return_value.json.return_value = {"results": data}
        self.syncclient.current_transfer_session.id = "123"
        with self.assertRaises(ValidationError) as e:
            self.syncclient._pull_records()
        self.assertIn("does not match", str(e.exception))
        # fail on transfer session push
        TransferSession.objects.update(push=True)
        with self.assertRaises(ValidationError) as e:
            self.syncclient._pull_records()
        self.assertIn("does not allow pulling", str(e.exception))

    @mock_session_request
    def test_pulling(self):
        with mock.patch.object(SyncClient, "_pull_records"):
            self.syncclient._pulling()
        self.assertTrue(
            self.syncclient.current_transfer_session.transfer_stage,
            transfer_status.DEQUEUING,
        )

    @mock_session_request
    def test_dequeuing(self):
        ts = self.syncclient.current_transfer_session
        self.assertFalse(
            TransferSession.objects.filter(id=ts.id, active=False).exists()
        )
        self.syncclient._dequeuing(push=False)
        ts.refresh_from_db()
        self.assertTrue(TransferSession.objects.filter(id=ts.id, active=False).exists())
        self.assertEqual(ts.transfer_stage, transfer_status.COMPLETED)

    @mock.patch.object(SyncClient, "_dequeuing")
    @mock.patch.object(SyncClient, "_pulling")
    @mock.patch.object(SyncClient, "_queuing")
    def test_resume_queuing_stage(self, mock_queuing, mock_pulling, mock_dequeuing):
        self.syncclient.current_transfer_session.transfer_stage = (
            transfer_status.QUEUING
        )
        self.syncclient.current_transfer_session.save()
        mock_queuing.side_effect = lambda *args, **kwargs: setattr(
            self.syncclient.current_transfer_session,
            "transfer_stage",
            transfer_status.PULLING,
        )
        mock_pulling.side_effect = lambda *args, **kwargs: setattr(
            self.syncclient.current_transfer_session,
            "transfer_stage",
            transfer_status.DEQUEUING,
        )
        self.syncclient.initiate_pull(self.filter)
        mock_queuing.assert_called()
        mock_pulling.assert_called()
        mock_dequeuing.assert_called()

    @mock.patch.object(SyncClient, "_dequeuing")
    @mock.patch.object(SyncClient, "_pulling")
    @mock.patch.object(SyncClient, "_queuing")
    def test_resume_pulling_stage(self, mock_queuing, mock_pulling, mock_dequeuing):
        self.syncclient.current_transfer_session.transfer_stage = (
            transfer_status.PULLING
        )
        self.syncclient.current_transfer_session.save()
        mock_pulling.side_effect = lambda *args, **kwargs: setattr(
            self.syncclient.current_transfer_session,
            "transfer_stage",
            transfer_status.DEQUEUING,
        )
        self.syncclient.initiate_pull(self.filter)
        mock_queuing.assert_not_called()
        mock_pulling.assert_called()
        mock_dequeuing.assert_called()

    @mock.patch.object(SyncClient, "_dequeuing")
    @mock.patch.object(SyncClient, "_pulling")
    @mock.patch.object(SyncClient, "_queuing")
    def test_resume_dequeuing_stage(self, mock_queuing, mock_pulling, mock_dequeuing):
        self.syncclient.current_transfer_session.transfer_stage = (
            transfer_status.DEQUEUING
        )
        self.syncclient.current_transfer_session.save()
        self.syncclient.initiate_pull(self.filter)
        mock_queuing.assert_not_called()
        mock_pulling.assert_not_called()
        mock_dequeuing.assert_called()
