import json
import uuid

import mock
from django.test import TestCase
from django.utils import timezone
from facility_profile.models import SummaryLog
from morango.api.serializers import (BufferSerializer, CertificateSerializer,
                                     InstanceIDSerializer)
from morango.certificates import Certificate, Key, ScopeDefinition
from morango.constants import transfer_status
from morango.controller import MorangoProfileController
from morango.errors import CertificateSignatureInvalid
from morango.models import (Buffer, InstanceIDModel, SyncSession,
                            TransferSession)
from morango.syncsession import NetworkSyncConnection, SyncClient
from requests.exceptions import HTTPError


def mock_patch_decorator(func):

    def wrapper(*args, **kwargs):
        mock_object = mock.Mock(content=b"""{"id": "abc"}""", data={'signature': 'sig', 'client_fsic': '{}', 'server_fsic': '{}'})
        mock_object.json.return_value = {}
        with mock.patch.object(NetworkSyncConnection, '_request', return_value=mock_object):
            with mock.patch.object(Certificate, 'verify', return_value=True):
                    return func(*args, **kwargs)
    return wrapper


class NetworkSyncConnectionTestCase(TestCase):

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
            scope_params=json.dumps({"mainpartition": self.root_cert.id, "subpartition": "abracadabra"}),
            private_key=Key(),
        )
        self.root_cert.sign_certificate(self.subset_cert)
        self.subset_cert.save()

        self.controller = MorangoProfileController('facilitydata')
        self.network_connection = self.controller.create_network_connection('127.0.0.1')

    @mock_patch_decorator
    def test_creating_sync_session_successful(self):
        self.assertEqual(SyncSession.objects.filter(active=True).count(), 0)
        NetworkSyncConnection._request.return_value.json.return_value = {'signature': 'sig', 'local_fsic': '{}'}
        self.network_connection.create_sync_session(self.subset_cert, self.root_cert)
        self.assertEqual(SyncSession.objects.filter(active=True).count(), 1)

    @mock_patch_decorator
    def test_creating_sync_session_cert_fails_to_verify(self):
        Certificate.verify.return_value = False
        with self.assertRaises(CertificateSignatureInvalid):
            self.network_connection.create_sync_session(self.subset_cert, self.root_cert)

    @mock_patch_decorator
    def test_resuming_sync_session(self):
        session = SyncSession.objects.create(id=uuid.uuid4().hex,
                                             profile="facilitydata",
                                             last_activity_timestamp=timezone.now(),
                                             client_certificate_id=self.subset_cert.id,
                                             server_certificate_id=self.root_cert.id,
                                             server_instance=json.dumps(InstanceIDSerializer(InstanceIDModel.get_or_create_current_instance()[0]).data))
        NetworkSyncConnection._request.return_value.json.return_value = {'instance_id': InstanceIDModel.get_or_create_current_instance()[0].id, 'active': True}
        client = self.network_connection.create_sync_session(self.subset_cert, self.root_cert)
        self.assertEqual(session.id, client.sync_session.id)
        self.assertEqual(SyncSession.objects.count(), 1)

    @mock_patch_decorator
    def test_not_resuming_sync_session(self):
        session = SyncSession.objects.create(id=uuid.uuid4().hex,
                                             profile="facilitydata",
                                             last_activity_timestamp=timezone.now(),
                                             client_certificate_id=self.subset_cert.id,
                                             server_certificate_id=self.root_cert.id,
                                             server_instance=json.dumps(InstanceIDSerializer(InstanceIDModel.get_or_create_current_instance()[0]).data))
        NetworkSyncConnection._request.return_value.json.return_value = {'instance_id': InstanceIDModel.get_or_create_current_instance()[0].id, 'active': False}
        client = self.network_connection.create_sync_session(self.subset_cert, self.root_cert)
        self.assertNotEqual(session.id, client.sync_session.id)
        self.assertEqual(SyncSession.objects.count(), 2)

    @mock_patch_decorator
    def test_not_resuming_sync_session_404(self):
        session = SyncSession.objects.create(id=uuid.uuid4().hex,
                                             profile="facilitydata",
                                             last_activity_timestamp=timezone.now(),
                                             client_certificate_id=self.subset_cert.id,
                                             server_certificate_id=self.root_cert.id,
                                             server_instance=json.dumps(InstanceIDSerializer(InstanceIDModel.get_or_create_current_instance()[0]).data))
        NetworkSyncConnection._request.return_value.json.return_value = {'instance_id': InstanceIDModel.get_or_create_current_instance()[0].id}
        with mock.patch.object(NetworkSyncConnection, '_get_sync_session') as ss_mock:
            ss_mock.side_effect = HTTPError(response=mock.Mock(status_code=404))
            client = self.network_connection.create_sync_session(self.subset_cert, self.root_cert)
        self.assertNotEqual(session.id, client.sync_session.id)
        self.assertEqual(SyncSession.objects.count(), 2)

    @mock_patch_decorator
    def test_get_remote_certs(self):
        # mock certs being returned by server
        certs = self.subset_cert.get_ancestors(include_self=True)
        cert_serialized = json.dumps(CertificateSerializer(certs, many=True).data)
        NetworkSyncConnection._request.return_value.json.return_value = json.loads(cert_serialized)

        # we want to see if the models are created (not saved) successfully
        remote_certs = self.network_connection.get_remote_certificates('abc')
        self.assertSetEqual(set(certs), set(remote_certs))

    @mock_patch_decorator
    def test_csr(self):
        # mock a "signed" cert being returned by server
        cert_serialized = json.dumps(CertificateSerializer(self.subset_cert).data)
        NetworkSyncConnection._request.return_value.json.return_value = json.loads(cert_serialized)
        self.subset_cert.delete()

        # we only want to make sure the "signed" cert is saved
        with mock.patch.object(Key, "get_private_key_string", return_value=self.subset_cert.private_key.get_private_key_string()):
            self.network_connection.certificate_signing_request(self.root_cert, '', '')
        self.assertTrue(Certificate.objects.filter(id=json.loads(cert_serialized)['id']).exists())

    @mock_patch_decorator
    def test_get_cert_chain(self):
        # mock a cert chain being returned by server
        certs = self.subset_cert.get_ancestors(include_self=True)
        original_cert_count = certs.count()
        cert_serialized = json.dumps(CertificateSerializer(certs, many=True).data)
        NetworkSyncConnection._request.return_value.json.return_value = json.loads(cert_serialized)
        Certificate.objects.all().delete()

        # we only want to make sure the cert chain is saved
        self.network_connection._get_certificate_chain(certs[1])
        self.assertEqual(Certificate.objects.count(), original_cert_count)


class SyncClientTestCase(TestCase):

    def setUp(self):
        self.filter = 'partition'
        self.syncsession = SyncSession.objects.create(id=uuid.uuid4().hex, profile="facilitydata", last_activity_timestamp=timezone.now())
        self.transfersession = TransferSession.objects.create(id=uuid.uuid4().hex, sync_session=self.syncsession, filter=self.filter,
                                                              push=True, last_activity_timestamp=timezone.now(), records_total=100)
        self.conn = NetworkSyncConnection()
        self.chunk_size = 100
        self.syncclient = SyncClient(self.conn, self.syncsession, self.chunk_size)
        self.syncclient.current_transfer_session = self.transfersession
        InstanceIDModel.get_or_create_current_instance()

    def build_buffer_items(self, transfer_session, **kwargs):

        data = {
            "profile": kwargs.get("profile", 'facilitydata'),
            "serialized": kwargs.get("serialized", '{"test": 99}'),
            "deleted": kwargs.get("deleted", False),
            "last_saved_instance": kwargs.get("last_saved_instance", uuid.uuid4().hex),
            "last_saved_counter": kwargs.get("last_saved_counter", 179),
            "partition": kwargs.get("partition", 'partition'),
            "source_id": kwargs.get("source_id", uuid.uuid4().hex),
            "model_name": kwargs.get("model_name", "contentsummarylog"),
            "conflicting_serialized_data": kwargs.get("conflicting_serialized_data", ""),
            "model_uuid": kwargs.get("model_uuid", None),
            "transfer_session": transfer_session,
        }

        for i in range(self.chunk_size):
            data['source_id'] = uuid.uuid4().hex
            data["model_uuid"] = SummaryLog.compute_namespaced_id(data["partition"], data["source_id"], data["model_name"])
            Buffer.objects.create(**data)

        buffered_items = Buffer.objects.filter(transfer_session=self.syncclient.current_transfer_session)
        serialized_records = BufferSerializer(buffered_items, many=True)
        return json.dumps(serialized_records.data)

    @mock_patch_decorator
    def test_push_records(self):
        self.build_buffer_items(self.syncclient.current_transfer_session)
        self.assertEqual(self.syncclient.current_transfer_session.records_transferred, 0)
        self.syncclient._push_records()
        self.assertEqual(self.syncclient.current_transfer_session.records_transferred, self.chunk_size)

    @mock_patch_decorator
    def test_pull_records(self):
        resp = self.build_buffer_items(self.syncclient.current_transfer_session)
        NetworkSyncConnection._request.return_value.json.return_value = json.loads(resp)
        Buffer.objects.filter(transfer_session=self.syncclient.current_transfer_session).delete()
        self.assertEqual(Buffer.objects.filter(transfer_session=self.syncclient.current_transfer_session).count(), 0)
        self.assertEqual(self.syncclient.current_transfer_session.records_transferred, 0)
        self.syncclient._pull_records()
        self.assertEqual(Buffer.objects.filter(transfer_session=self.syncclient.current_transfer_session).count(), self.chunk_size)
        self.assertEqual(self.syncclient.current_transfer_session.records_transferred, self.chunk_size)

    @mock_patch_decorator
    def test_start_transfer_session_push(self):
        self.syncclient.current_transfer_session.delete(soft=False)
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 0)
        self.syncclient._starting_transfer_session(self.filter, True)
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)

    @mock_patch_decorator
    def test_start_transfer_session_resume(self):
        # create random transfer session tied to same sync session
        TransferSession.objects.create(id=uuid.uuid4().hex, sync_session=self.syncsession, filter=self.filter,
                                       push=False, last_activity_timestamp=timezone.now(), records_total=1)
        self.syncclient.current_transfer_session = None
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 2)
        NetworkSyncConnection._request.return_value.json.return_value = {'active': True, 'push': True}
        self.syncclient._starting_transfer_session(self.filter, True)
        # ensure other transfer session attached to sync session is turned off
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)
        self.assertEqual(self.syncclient.current_transfer_session, self.transfersession)

    @mock_patch_decorator
    def test_start_transfer_session_resume_404(self):
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 1)
        with mock.patch.object(NetworkSyncConnection, '_get_transfer_session') as ts_mock:
            ts_mock.side_effect = HTTPError(response=mock.Mock(status_code=404))
            self.syncclient._starting_transfer_session(self.filter, True)
        self.assertEqual(TransferSession.objects.filter(active=True).count(), 2)
        self.assertNotEqual(self.syncclient.current_transfer_session, self.transfersession)

    @mock_patch_decorator
    def test_start_transfer_session_resume_records_transferred(self):
        self.assertEqual(self.syncclient.current_transfer_session.records_transferred, 0)
        self.syncclient.current_transfer_session.transfer_stage = transfer_status.PUSHING
        self.syncclient.current_transfer_session.save()
        self.syncclient.current_transfer_session = None
        NetworkSyncConnection._request.return_value.json.return_value = {'active': True, 'push': True, 'records_transferred': 100}
        self.syncclient._starting_transfer_session(self.filter, True)
        self.assertEqual(self.syncclient.current_transfer_session, self.transfersession)
        self.assertEqual(self.syncclient.current_transfer_session.records_transferred, 100)

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
