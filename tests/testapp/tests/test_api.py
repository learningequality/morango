import base64
import json
import sys
import uuid

import mock
from django.core.urlresolvers import reverse
from django.test.utils import override_settings
from django.urls.exceptions import NoReverseMatch
from django.utils import timezone
from facility_profile.models import MyUser
from rest_framework.test import APITestCase as BaseTestCase
from test.support import EnvironmentVarGuard

from morango.api.serializers import BufferSerializer
from morango.api.serializers import CertificateSerializer
from morango.api.serializers import InstanceIDSerializer
from morango.constants import transfer_stages
from morango.constants import transfer_statuses
from morango.models.certificates import Certificate
from morango.models.certificates import Key
from morango.models.certificates import Nonce
from morango.models.certificates import ScopeDefinition
from morango.models.core import Buffer
from morango.models.core import DatabaseMaxCounter
from morango.models.core import InstanceIDModel
from morango.models.core import RecordMaxCounterBuffer
from morango.models.core import SyncSession
from morango.models.core import TransferSession
from morango.models.fields.crypto import SharedKey
from morango.registry import syncable_models
from morango.sync.syncsession import compress_string
from morango.sync.utils import validate_and_create_buffer_data

if sys.version_info >= (3,):
    from base64 import encodebytes as b64encode

    # A weird hack because of http://bugs.python.org/issue17866
    class APITestCase(BaseTestCase):
        def assertItemsEqual(self, *args, **kwargs):
            self.assertCountEqual(*args, **kwargs)


else:
    from base64 import encodestring as b64encode

    class APITestCase(BaseTestCase):
        pass


class CertificateTestCaseMixin(object):
    def setUp(self):

        self.user = MyUser(username="user")
        self.user.actual_password = "opensesame"
        self.user.set_password(self.user.actual_password)
        self.user.save()

        self.superuser = MyUser(username="superuser", is_superuser=True)
        self.superuser.actual_password = "opentahini"
        self.superuser.set_password(self.superuser.actual_password)
        self.superuser.save()

        self.fakeuser = MyUser(username="fakeuser")
        self.fakeuser.actual_password = "nosauce"

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
            read_filter_template="${mainpartition}:shared\n${mainpartition}:${subpartition}",
            write_filter_template="${mainpartition}:${subpartition}",
            read_write_filter_template="",
        )

        self.root_cert1_with_key = Certificate.generate_root_certificate(
            self.root_scope_def.id
        )

        self.subset_cert1_without_key = Certificate(
            parent=self.root_cert1_with_key,
            profile=self.profile,
            scope_definition=self.subset_scope_def,
            scope_version=self.subset_scope_def.version,
            scope_params=json.dumps(
                {
                    "mainpartition": self.root_cert1_with_key.id,
                    "subpartition": "abracadabra",
                }
            ),
            private_key=Key(),
        )
        self.root_cert1_with_key.sign_certificate(self.subset_cert1_without_key)
        self.subset_cert1_without_key.save()

        self.sub_subset_cert1_with_key = Certificate(
            parent=self.subset_cert1_without_key,
            profile=self.profile,
            scope_definition=self.subset_scope_def,
            scope_version=self.subset_scope_def.version,
            scope_params=self.subset_cert1_without_key.scope_params,
            private_key=Key(),
        )
        self.subset_cert1_without_key.sign_certificate(self.sub_subset_cert1_with_key)
        self.sub_subset_cert1_with_key.save()

        self.subset_cert1_without_key._private_key = None
        self.subset_cert1_without_key.save()

        self.root_cert2_without_key = Certificate.generate_root_certificate(
            self.root_scope_def.id
        )

        self.subset_cert2_with_key = Certificate(
            parent=self.root_cert2_without_key,
            profile=self.profile,
            scope_definition=self.subset_scope_def,
            scope_version=self.subset_scope_def.version,
            scope_params=json.dumps(
                {
                    "mainpartition": self.root_cert2_without_key.id,
                    "subpartition": "abracadabra",
                }
            ),
            private_key=Key(),
        )
        self.root_cert2_without_key.sign_certificate(self.subset_cert2_with_key)
        self.subset_cert2_with_key.save()
        self.root_cert2_without_key._private_key = None
        self.root_cert2_without_key.save()

        self.original_cert_count = Certificate.objects.count()

        self.sharedkey = SharedKey.get_or_create_shared_key()

        # create a root cert
        self.unsaved_root_cert = Certificate(
            scope_definition=self.root_scope_def,
            scope_version=self.root_scope_def.version,
            profile=self.profile,
            private_key=Key(),
        )
        self.unsaved_root_cert.id = self.unsaved_root_cert.calculate_uuid()
        self.unsaved_root_cert.scope_params = json.dumps(
            {self.root_scope_def.primary_scope_param_key: self.unsaved_root_cert.id}
        )
        self.unsaved_root_cert.sign_certificate(self.unsaved_root_cert)

        # create a child cert
        self.unsaved_subset_cert = Certificate(
            parent=self.unsaved_root_cert,
            profile=self.profile,
            scope_definition=self.subset_scope_def,
            scope_version=self.subset_scope_def.version,
            scope_params=json.dumps(
                {"mainpartition": self.unsaved_root_cert.id, "subpartition": "hooplah"}
            ),
            public_key=self.sharedkey.public_key,
        )

    def make_cert_endpoint_request(self, params={}, method="GET"):
        fn = getattr(self.client, method.lower())
        response = fn(reverse("certificates-list"), params, format="json")
        data = json.loads(response.content.decode())
        return (response, data)

    def perform_basic_authentication(self, user):
        basic_auth_header = b"Basic " + b64encode(
            ("username=%s:%s" % (user.username, user.actual_password)).encode()
        )
        self.client.credentials(HTTP_AUTHORIZATION=basic_auth_header)

    def create_syncsession(self, client_certificate=None, server_certificate=None):

        if not client_certificate:
            client_certificate = self.sub_subset_cert1_with_key

        if not server_certificate:
            server_certificate = self.root_cert1_with_key

        # fetch a nonce value to use in creating the syncsession
        response = self.client.post(reverse("nonces-list"), {}, format="json")
        nonce = json.loads(response.content.decode())["id"]

        # prepare the data to send in the syncsession creation request
        data = {
            "id": uuid.uuid4().hex,
            "server_certificate_id": server_certificate.id,
            "client_certificate_id": client_certificate.id,
            "profile": client_certificate.profile,
            "certificate_chain": json.dumps(
                CertificateSerializer(
                    client_certificate.get_ancestors(include_self=True), many=True
                ).data
            ),
            "connection_path": "http://127.0.0.1:8000",
            "instance": json.dumps(
                InstanceIDSerializer(
                    InstanceIDModel.get_or_create_current_instance()[0]
                ).data
            ),
            "nonce": nonce,
        }

        # sign the nonce/ID combo to attach to the request
        data["signature"] = client_certificate.sign("{nonce}:{id}".format(**data))

        # make the API call to create the SyncSession
        response = self.client.post(reverse("syncsessions-list"), data, format="json")
        self.assertEqual(response.status_code, 201)

        return SyncSession.objects.get(id=data["id"])

    def make_transfersession_creation_request(
        self,
        filter,
        push,
        syncsession=None,
        expected_status=201,
        expected_message=None,
        **kwargs
    ):

        if not syncsession:
            syncsession = self.create_syncsession()

        data = {
            "id": uuid.uuid4().hex,
            "filter": filter,
            "push": push,
            "records_total": 0,
            "sync_session_id": syncsession.id,
        }

        # make the API call to attempt to create the TransferSession
        response = self.client.post(
            reverse("transfersessions-list"), data, format="json"
        )
        self.assertEqual(response.status_code, expected_status)

        if expected_status == 201:
            # check that the syncsession was created
            transfersession = TransferSession.objects.get(
                id=json.loads(response.content.decode())["id"]
            )
            self.assertTrue(transfersession.active)
        else:
            # check that the syncsession was not created
            self.assertEqual(TransferSession.objects.count(), 0)

        if expected_message:
            self.assertIn(expected_message, response.content.decode())

        return response


class CertificateListingTestCase(CertificateTestCaseMixin, APITestCase):
    def test_certificate_filtering_by_primary_partition(self):

        # check that only the root cert and leaf cert are returned when they're the ones with a private key
        _, data = self.make_cert_endpoint_request(
            params={"primary_partition": self.root_cert1_with_key.id}
        )
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["id"], self.root_cert1_with_key.id)
        self.assertEqual(data[1]["id"], self.sub_subset_cert1_with_key.id)

        # check that only the subcert is returned when it's the one with a private key
        _, data = self.make_cert_endpoint_request(
            params={"primary_partition": self.root_cert2_without_key.id}
        )
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["id"], self.subset_cert2_with_key.id)

        # check that no certificates are returned when the partition doesn't exist
        _, data = self.make_cert_endpoint_request(
            params={"primary_partition": "a" * 32}
        )
        self.assertEqual(len(data), 0)

        # check that no certificates are returned when profile doesn't match
        _, data = self.make_cert_endpoint_request(
            params={
                "primary_partition": self.root_cert2_without_key.id,
                "profile": "namelessone",
            }
        )
        self.assertEqual(len(data), 0)

        # check that certificates are returned when profile does match
        _, data = self.make_cert_endpoint_request(
            params={
                "primary_partition": self.root_cert2_without_key.id,
                "profile": self.profile,
            }
        )
        self.assertEqual(len(data), 1)

    def test_certificate_filtering_by_ancestors_of(self):

        # check that only the root cert is returned when it's the one we're requesting ancestors of
        _, data = self.make_cert_endpoint_request(
            params={"ancestors_of": self.root_cert1_with_key.id}
        )
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["id"], self.root_cert1_with_key.id)

        # check that both the subcert and root cert are returned when we request ancestors of subcert
        _, data = self.make_cert_endpoint_request(
            params={"ancestors_of": self.subset_cert2_with_key.id}
        )
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["id"], self.root_cert2_without_key.id)
        self.assertEqual(data[1]["id"], self.subset_cert2_with_key.id)

        # check that no certificates are returned when the certificate ID doesn't exist
        _, data = self.make_cert_endpoint_request(params={"ancestors_of": "a" * 32})
        self.assertEqual(len(data), 0)

        # check that no certificates are returned when profile doesn't match
        _, data = self.make_cert_endpoint_request(
            params={
                "ancestors_of": self.subset_cert2_with_key.id,
                "profile": "namelessone",
            }
        )
        self.assertEqual(len(data), 0)

        # check that certificates are returned when profile does match
        _, data = self.make_cert_endpoint_request(
            params={
                "ancestors_of": self.subset_cert2_with_key.id,
                "profile": self.profile,
            }
        )
        self.assertEqual(len(data), 2)

    def test_certificate_full_list_request(self):

        # check that all the certs owned by the server (for which it has private keys) are returned
        _, data = self.make_cert_endpoint_request()
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]["id"], self.root_cert1_with_key.id)
        self.assertEqual(data[1]["id"], self.sub_subset_cert1_with_key.id)
        self.assertEqual(data[2]["id"], self.subset_cert2_with_key.id)

        # check that no certificates are returned when profile doesn't match
        _, data = self.make_cert_endpoint_request(params={"profile": "namelessone"})
        self.assertEqual(len(data), 0)

        # check that certificates are returned when profile does match
        _, data = self.make_cert_endpoint_request(params={"profile": self.profile})
        self.assertEqual(len(data), 3)


class CertificateCreationTestCase(CertificateTestCaseMixin, APITestCase):
    def make_csr(self, parent, **kwargs):
        key = Key()
        params = {
            "parent": parent.id,
            "profile": kwargs.get("profile", self.profile),
            "scope_definition": kwargs.get(
                "scope_definition", parent.scope_definition_id
            ),
            "scope_version": kwargs.get("scope_version", parent.scope_version),
            "scope_params": kwargs.get("scope_params", parent.scope_params),
            "public_key": kwargs.get("public_key", key.get_public_key_string()),
        }
        response, data = self.make_cert_endpoint_request(params=params, method="POST")
        return (response, data, key)

    def test_certificate_creation_works_correctly(self):
        self.perform_basic_authentication(self.superuser)
        response, data, key = self.make_csr(parent=self.root_cert1_with_key)
        self.assertEqual(response.status_code, 201)
        self.assertEqual(Certificate.objects.count(), self.original_cert_count + 1)

    def test_certificate_creation_fails_for_non_superuser(self):
        self.perform_basic_authentication(self.user)
        response, data, key = self.make_csr(parent=self.root_cert1_with_key)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(Certificate.objects.count(), self.original_cert_count)

    def test_certificate_creation_fails_without_credentials(self):
        response, data, key = self.make_csr(parent=self.root_cert1_with_key)
        self.assertEqual(response.status_code, 401)
        self.assertEqual(Certificate.objects.count(), self.original_cert_count)

    def test_certificate_creation_fails_with_bad_credentials(self):
        self.perform_basic_authentication(self.fakeuser)
        response, data, key = self.make_csr(parent=self.root_cert1_with_key)
        self.assertEqual(response.status_code, 401)
        self.assertEqual(Certificate.objects.count(), self.original_cert_count)

    def assert_certificate_creation_fails_with_bad_parameters(self, parent, **params):
        self.perform_basic_authentication(self.superuser)
        response, data, key = self.make_csr(parent=parent, **params)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(Certificate.objects.count(), self.original_cert_count)

    def test_certificate_creation_fails_for_wrong_profile(self):
        self.assert_certificate_creation_fails_with_bad_parameters(
            parent=self.root_cert1_with_key, profile="covfefe"
        )

    def test_certificate_creation_fails_for_parent_with_no_private_key(self):
        self.assert_certificate_creation_fails_with_bad_parameters(
            parent=self.root_cert2_without_key,
        )

    def test_certificate_creation_fails_for_non_subset_scope(self):
        self.assert_certificate_creation_fails_with_bad_parameters(
            parent=self.subset_cert2_with_key,
            scope_definition=self.root_cert2_without_key.scope_definition_id,
            scope_params=self.root_cert2_without_key.scope_params,
        )

    def test_certificate_creation_fails_for_invalid_scope_definition_id(self):
        self.assert_certificate_creation_fails_with_bad_parameters(
            parent=self.root_cert1_with_key,
            scope_definition="this-aint-no-scope-def",
        )

    @override_settings(ALLOW_CERTIFICATE_PUSHING=True)
    def test_certificate_chain_pushing(self):
        # attach nonce value to certificate salt
        response = self.client.post(reverse("nonces-list"), format="json")
        self.unsaved_subset_cert.salt = response.json()["id"]
        self.unsaved_subset_cert.id = self.unsaved_subset_cert.calculate_uuid()
        self.unsaved_root_cert.sign_certificate(self.unsaved_subset_cert)

        # push cert chain up to server
        data = json.dumps(
            CertificateSerializer(
                [self.unsaved_root_cert, self.unsaved_subset_cert], many=True
            ).data
        )
        response = self.client.post(
            reverse("certificatechain-list"), data=data, format="json"
        )
        self.assertEqual(response.status_code, 201)
        saved_subset_cert = Certificate.objects.get(id=self.unsaved_subset_cert.id)
        self.assertEqual(
            saved_subset_cert.private_key.get_private_key_string(),
            self.sharedkey.private_key.get_private_key_string(),
        )

    @override_settings(ALLOW_CERTIFICATE_PUSHING=True)
    def test_certificate_chain_pushing_fails_for_nonce(self):
        # use non-existent nonce value
        self.unsaved_subset_cert.salt = uuid.uuid4().hex
        self.unsaved_subset_cert.id = self.unsaved_subset_cert.calculate_uuid()
        self.unsaved_root_cert.sign_certificate(self.unsaved_subset_cert)

        # push cert chain up to server
        data = json.dumps(
            CertificateSerializer(
                [self.unsaved_root_cert, self.unsaved_subset_cert], many=True
            ).data
        )
        response = self.client.post(
            reverse("certificatechain-list"), data=data, format="json"
        )
        self.assertEqual(response.status_code, 403)

    @override_settings(ALLOW_CERTIFICATE_PUSHING=True)
    def test_certificate_chain_pushing_fails_for_public_key(self):
        # do not use shared public key value
        self.unsaved_subset_cert.public_key = Key()
        self.unsaved_subset_cert.id = self.unsaved_subset_cert.calculate_uuid()
        self.unsaved_root_cert.sign_certificate(self.unsaved_subset_cert)

        # push cert chain up to server
        data = json.dumps(
            CertificateSerializer(
                [self.unsaved_root_cert, self.unsaved_subset_cert], many=True
            ).data
        )
        response = self.client.post(
            reverse("certificatechain-list"), data=data, format="json"
        )
        self.assertEqual(response.status_code, 400)


class NonceCreationTestCase(APITestCase):
    def test_nonces_can_be_created(self):
        response = self.client.post(reverse("nonces-list"), {}, format="json")
        data = json.loads(response.content.decode())
        self.assertEqual(response.status_code, 201)
        nonces = Nonce.objects.all()
        self.assertEqual(nonces.count(), 1)
        self.assertEqual(nonces[0].id, data["id"])

    def test_nonces_list_cannot_be_read(self):
        response = self.client.get(reverse("nonces-list"), {}, format="json")
        self.assertEqual(response.status_code, 405)
        nonces = Nonce.objects.all()
        self.assertEqual(nonces.count(), 0)

    def test_nonces_item_cannot_be_read(self):
        # create the nonce
        response = self.client.post(reverse("nonces-list"), {}, format="json")
        data = json.loads(response.content.decode())
        # try to read the nonce
        with self.assertRaises(NoReverseMatch):
            response = self.client.get(
                reverse("nonces-detail", kwargs={"pk": data["id"]}), {}, format="json"
            )
            self.assertEqual(response.status_code, 403)


class SyncSessionEndpointTestCase(CertificateTestCaseMixin, APITestCase):
    def _get_nonce(self):
        response = self.client.post(reverse("nonces-list"), {}, format="json")
        return json.loads(response.content.decode())["id"]

    def get_initial_syncsession_data_for_request(self):
        # fetch a nonce value to use in creating the syncsession
        nonce = self._get_nonce()

        # prepare the data to send in the syncsession creation request
        data = {
            "id": uuid.uuid4().hex,
            "server_certificate_id": self.root_cert1_with_key.id,
            "client_certificate_id": self.sub_subset_cert1_with_key.id,
            "certificate_chain": json.dumps(
                CertificateSerializer(
                    self.sub_subset_cert1_with_key.get_ancestors(include_self=True),
                    many=True,
                ).data
            ),
            "connection_path": "http://127.0.0.1:8000",
            "instance": json.dumps(
                InstanceIDSerializer(
                    InstanceIDModel.get_or_create_current_instance()[0]
                ).data
            ),
            "nonce": nonce,
        }

        # sign the nonce/ID combo to attach to the request
        data["signature"] = self.sub_subset_cert1_with_key.sign(
            "{nonce}:{id}".format(**data)
        )

        return data

    def assertSyncSessionCreationFails(self, data, status_code=403):

        # make the API call to attempt to create the SyncSession, and make sure it was denied
        response = self.client.post(reverse("syncsessions-list"), data, format="json")
        self.assertEqual(response.status_code, status_code)

        # check that the syncsession was not created
        self.assertEqual(SyncSession.objects.count(), 0)

    @override_settings(MORANGO_INSTANCE_INFO={"this_is_a_test": "yes"})
    def test_syncsession_can_be_created(self):

        data = self.get_initial_syncsession_data_for_request()

        # delete two of the certs from the chain so we can make sure they get added back
        self.sub_subset_cert1_with_key.delete()
        self.subset_cert1_without_key.delete()
        self.assertEqual(Certificate.objects.count(), self.original_cert_count - 2)

        # make the API call to create the SyncSession
        response = self.client.post(reverse("syncsessions-list"), data, format="json")
        self.assertEqual(response.status_code, 201)
        server_instance_data = json.loads(response.data["server_instance"])
        self.assertIn("this_is_a_test", server_instance_data)
        self.assertEqual("yes", server_instance_data["this_is_a_test"])

        # check that the cert chain was deserialized
        self.assertEqual(Certificate.objects.count(), self.original_cert_count)

        # check that the syncsession was created
        syncsession = SyncSession.objects.get()
        self.assertEqual(syncsession.id, data["id"])
        self.assertEqual(
            syncsession.server_certificate_id, data["server_certificate_id"]
        )
        self.assertEqual(
            syncsession.client_certificate_id, data["client_certificate_id"]
        )
        self.assertTrue(syncsession.active)

    def test_syncsession_creation_fails_with_bad_signature(self):

        data = self.get_initial_syncsession_data_for_request()

        data["signature"] = self.sub_subset_cert1_with_key.sign("nonsense:id")

        self.assertSyncSessionCreationFails(data)

    def test_syncsession_creation_fails_with_client_cert_not_matching_cert_chain(self):

        data = self.get_initial_syncsession_data_for_request()

        data["certificate_chain"] = json.dumps(
            CertificateSerializer(
                self.subset_cert2_with_key.get_ancestors(include_self=True), many=True
            ).data
        )

        self.assertSyncSessionCreationFails(data)

    def test_syncsession_creation_fails_with_expired_nonce(self):

        data = self.get_initial_syncsession_data_for_request()

        Nonce.objects.all().update(
            timestamp=timezone.datetime(
                2000, 1, 1, tzinfo=timezone.get_current_timezone()
            )
        )

        self.assertSyncSessionCreationFails(data)

    def test_syncsession_creation_fails_with_nonexistent_nonce(self):

        data = self.get_initial_syncsession_data_for_request()

        Nonce.objects.all().delete()

        self.assertSyncSessionCreationFails(data)

    def test_syncsession_creation_fails_with_nonexistent_server_certificate(self):

        data = self.get_initial_syncsession_data_for_request()

        data["server_certificate_id"] = uuid.uuid4().hex

        self.assertSyncSessionCreationFails(data, status_code=400)

    def test_syncsession_can_be_deleted(self):

        self.test_syncsession_can_be_created()

        syncsession = SyncSession.objects.get()
        self.assertEqual(syncsession.active, True)

        response = self.client.delete(
            reverse("syncsessions-detail", kwargs={"pk": syncsession.id}), format="json"
        )
        self.assertEqual(response.status_code, 204)

        # check that the syncsession was "deleted" but not _deleted_
        self.assertEqual(SyncSession.objects.filter(active=True).count(), 0)
        self.assertEqual(SyncSession.objects.filter(active=False).count(), 1)

    def test_inactive_syncsession_cannot_be_deleted(self):

        self.test_syncsession_can_be_created()

        syncsession = SyncSession.objects.get()
        syncsession.active = False
        syncsession.save()

        response = self.client.delete(
            reverse("syncsessions-detail", kwargs={"pk": syncsession.id}), format="json"
        )
        self.assertEqual(response.status_code, 404)

    def test_get_sync_session(self):
        data = self.get_initial_syncsession_data_for_request()
        self.client.post(reverse("syncsessions-list"), data, format="json")

        response = self.client.get(
            reverse("syncsessions-detail", kwargs={"pk": data["id"]})
        )
        self.assertEqual(response.status_code, 200)


class TransferSessionEndpointTestCase(CertificateTestCaseMixin, APITestCase):
    def test_transfersession_can_be_created(self):

        self.make_transfersession_creation_request(
            filter=str(self.sub_subset_cert1_with_key.get_scope().write_filter),
            push=True,
        )

    def test_transfersession_can_be_created_with_smaller_subset_filter(self):

        self.make_transfersession_creation_request(
            filter=str(self.sub_subset_cert1_with_key.get_scope().read_filter).split()[
                0
            ],
            push=False,
        )

    def test_transfersession_creation_fails_for_push_when_filter_not_in_client_write_scope(
        self,
    ):

        self.make_transfersession_creation_request(
            filter=str(self.root_cert1_with_key.get_scope().read_filter),
            push=True,
            expected_status=403,
            expected_message="Client certificate scope does not permit pushing",
        )

    def test_transfersession_creation_fails_for_push_when_filter_not_in_server_read_scope(
        self,
    ):

        syncsession = self.create_syncsession(
            client_certificate=self.root_cert1_with_key,
            server_certificate=self.sub_subset_cert1_with_key,
        )

        self.make_transfersession_creation_request(
            filter=str(self.root_cert1_with_key.get_scope().write_filter),
            push=True,
            syncsession=syncsession,
            expected_status=403,
            expected_message="Server certificate scope does not permit receiving pushes",
        )

    def test_transfersession_creation_fails_for_pull_when_filter_not_in_client_read_scope(
        self,
    ):

        self.make_transfersession_creation_request(
            filter=str(self.root_cert1_with_key.get_scope().read_filter),
            push=False,
            expected_status=403,
            expected_message="Client certificate scope does not permit pulling",
        )

    def test_transfersession_creation_fails_for_pull_when_filter_not_in_server_write_scope(
        self,
    ):

        syncsession = self.create_syncsession(
            client_certificate=self.root_cert1_with_key,
            server_certificate=self.sub_subset_cert1_with_key,
        )

        self.make_transfersession_creation_request(
            filter=str(self.root_cert1_with_key.get_scope().write_filter),
            push=False,
            syncsession=syncsession,
            expected_status=403,
            expected_message="Server certificate scope does not permit responding to pulls",
        )

    def test_transfersession_creation_fails_for_expired_syncsession(self):

        syncsession = self.create_syncsession()

        syncsession.active = False
        syncsession.save()

        self.make_transfersession_creation_request(
            filter=str(self.sub_subset_cert1_with_key.get_scope().write_filter),
            push=True,
            expected_status=400,
            expected_message="Requested syncsession does not exist",
            syncsession=syncsession,
        )

    def test_transfersession_creation_fails_for_nonexistent_syncsession(self):

        syncsession = self.create_syncsession()

        syncsession.delete()

        self.make_transfersession_creation_request(
            filter=str(self.sub_subset_cert1_with_key.get_scope().write_filter),
            push=True,
            expected_status=400,
            expected_message="Requested syncsession does not exist",
            syncsession=syncsession,
        )

    def test_transfersession_can_be_deleted(self):

        self.test_transfersession_can_be_created()

        transfersession = TransferSession.objects.get()
        transfersession.update_state(
            stage=transfer_stages.DESERIALIZING, stage_status=transfer_statuses.COMPLETED
        )
        self.assertEqual(transfersession.active, True)

        self._delete_transfer_session(transfersession)

    def test_transfersession_with_null_records_total_can_be_deleted(self):

        self.test_transfersession_can_be_created()

        transfersession = TransferSession.objects.get()
        transfersession.records_total = None
        transfersession.update_state(
            stage=transfer_stages.DESERIALIZING, stage_status=transfer_statuses.COMPLETED
        )

        self._delete_transfer_session(transfersession)

    def _delete_transfer_session(self, transfersession):

        response = self.client.delete(
            reverse("transfersessions-detail", kwargs={"pk": transfersession.id}),
            format="json",
        )
        self.assertEqual(response.status_code, 204)

        # check that the transfersession was "deleted"
        self.assertEqual(TransferSession.objects.get().active, False)

        # check that we didn't create a DatabaseMaxCounter with empty partition in the process
        self.assertEqual(DatabaseMaxCounter.objects.filter(partition="").count(), 0)

    def test_inactive_transfersession_cannot_be_deleted(self):

        self.test_transfersession_can_be_created()

        transfersession = TransferSession.objects.get()
        transfersession.active = False
        transfersession.save()

        response = self.client.delete(
            reverse("transfersessions-detail", kwargs={"pk": transfersession.id}),
            format="json",
        )
        self.assertEqual(response.status_code, 404)


class BufferEndpointTestCase(CertificateTestCaseMixin, APITestCase):
    def setUp(self):
        super(BufferEndpointTestCase, self).setUp()
        self.default_push_filter = str(
            self.sub_subset_cert1_with_key.get_scope().write_filter
        )
        self.default_pull_filter = str(
            self.sub_subset_cert1_with_key.get_scope().read_filter
        )

    def build_buffer_item(self, **kwargs):

        if "transfer_session" not in kwargs:
            assert "filter" in kwargs and "push" in kwargs
            t_sess_req = self.make_transfersession_creation_request(**kwargs)
            t_sess_id = json.loads(t_sess_req.content.decode())["id"]
            kwargs["transfer_session"] = TransferSession.objects.get(id=t_sess_id)

        server_cert = kwargs["transfer_session"].sync_session.client_certificate
        push = kwargs["transfer_session"].push
        records_total = kwargs["transfer_session"].records_total or 0
        kwargs["transfer_session"].records_total = records_total + 1
        kwargs["transfer_session"].save()

        filt = (
            server_cert.get_scope().write_filter
            if push
            else server_cert.get_scope().read_filter
        )
        partition = filt._filter_tuple[0] + ":furthersubpart"

        data = {
            "profile": kwargs.get(
                "profile", kwargs["transfer_session"].sync_session.profile
            ),
            "serialized": kwargs.get("serialized", '{"test": 99}'),
            "deleted": kwargs.get("deleted", False),
            "last_saved_instance": kwargs.get("last_saved_instance", uuid.uuid4().hex),
            "last_saved_counter": kwargs.get("last_saved_counter", 179),
            "partition": kwargs.get("partition", partition),
            "source_id": kwargs.get("source_id", uuid.uuid4().hex),
            "model_name": kwargs.get("model_name", "contentsummarylog"),
            "conflicting_serialized_data": kwargs.get(
                "conflicting_serialized_data", ""
            ),
            "model_uuid": kwargs.get("model_uuid", None),
            "transfer_session": kwargs["transfer_session"],
        }

        if not data["model_uuid"]:
            Model = syncable_models.get_model(data["profile"], data["model_name"])
            data["model_uuid"] = Model.compute_namespaced_id(
                data["partition"], data["source_id"], data["model_name"]
            )

        buffermodel = Buffer.objects.create(**data)

        for i in range(3):
            RecordMaxCounterBuffer.objects.create(
                transfer_session=data["transfer_session"],
                model_uuid=data["model_uuid"],
                instance_id=uuid.uuid4().hex,
                counter=i * 3 + 1,
            )

        return buffermodel

    def make_buffer_post_request(self, buffers, expected_status=201, gzip=False):
        serialized_recs = BufferSerializer(buffers, many=True)

        # extract that data that is to be posted
        data = serialized_recs.data
        headers = {"format": "json"}

        # gzip the content before sending the request
        if gzip:
            new_data = json.dumps([dict(el) for el in data])
            data = compress_string(bytes(new_data.encode("utf-8")))
            headers["content_type"] = "application/gzip"
            headers["format"] = None

        # delete the records from the DB so we don't conflict when we POST
        Buffer.objects.all().delete()
        RecordMaxCounterBuffer.objects.all().delete()

        response = self.client.post(reverse("buffers-list"), data, **headers)
        self.assertEqual(response.status_code, expected_status)
        if expected_status == 201:
            # check that the buffer items were created
            self.assertEqual(Buffer.objects.count(), len(buffers))
        else:
            # check that the buffer items were not created
            self.assertEqual(Buffer.objects.count(), 0)

    def test_push_valid_gzipped_buffer_chunk(self):
        rec_1 = self.build_buffer_item(push=True, filter=self.default_push_filter)
        rec_2 = self.build_buffer_item(
            serialized=u"unicode", transfer_session=rec_1.transfer_session
        )
        rec_3 = self.build_buffer_item(transfer_session=rec_1.transfer_session)
        self.make_buffer_post_request(
            [rec_1, rec_2, rec_3], expected_status=201, gzip=True
        )

    def test_push_valid_buffer_chunk(self):
        rec_1 = self.build_buffer_item(push=True, filter=self.default_push_filter)
        rec_2 = self.build_buffer_item(transfer_session=rec_1.transfer_session)
        rec_3 = self.build_buffer_item(transfer_session=rec_1.transfer_session)
        self.make_buffer_post_request([rec_1, rec_2, rec_3], expected_status=201)

    def test_push_with_invalid_model_uuid(self):
        rec_1 = self.build_buffer_item(push=True, filter=self.default_push_filter)
        rec_2 = self.build_buffer_item(
            transfer_session=rec_1.transfer_session,
            model_uuid=uuid.uuid4().hex,
            model_name="facility",
        )
        rec_3 = self.build_buffer_item(transfer_session=rec_1.transfer_session)
        self.make_buffer_post_request([rec_1, rec_2, rec_3], expected_status=400)

    def test_push_with_partition_not_in_filter(self):
        rec_1 = self.build_buffer_item(push=True, filter=self.default_push_filter)
        rec_2 = self.build_buffer_item(transfer_session=rec_1.transfer_session)
        rec_3 = self.build_buffer_item(
            transfer_session=rec_1.transfer_session, partition=uuid.uuid4().hex
        )
        self.make_buffer_post_request([rec_1, rec_2, rec_3], expected_status=400)

    def test_push_fails_for_pull_transfersession(self):
        rec_1 = self.build_buffer_item(push=False, filter=self.default_push_filter)
        rec_2 = self.build_buffer_item(transfer_session=rec_1.transfer_session)
        rec_3 = self.build_buffer_item(transfer_session=rec_1.transfer_session)
        self.make_buffer_post_request([rec_1, rec_2, rec_3], expected_status=403)

    def create_records_for_pulling(self, count=3, **kwargs):

        assert count >= 1

        # update default transfer session arguments with provided kwargs
        transfer_session_kwargs = {
            "push": False,
            "filter": self.default_pull_filter,
        }
        transfer_session_kwargs.update(kwargs)

        # make the records we'll be querying
        records = [self.build_buffer_item(**transfer_session_kwargs)]
        transfer_session = records[0].transfer_session
        for i in range(count - 1):
            records.append(self.build_buffer_item(transfer_session=transfer_session))

        # also make some dummy records so we can make sure they don't get returned
        records.append(
            self.build_buffer_item(push=False, filter=self.default_pull_filter)
        )
        records.append(
            self.build_buffer_item(transfer_session=records[-1].transfer_session)
        )

        # save all the records to the database
        [rec.save() for rec in records]

        return records[0].transfer_session.id

    def make_buffer_get_request(
        self, expected_status=200, expected_count=None, **get_params
    ):
        """Make a GET request to the buffer endpoint. Warning: Deletes the local buffer instances before validating."""

        response = self.client.get(reverse("buffers-list"), get_params, format="json")

        self.assertEqual(response.status_code, expected_status)

        if expected_status == 200:

            t_id = get_params.get("transfer_session_id")

            if expected_count is None:
                expected_count = Buffer.objects.filter(transfer_session_id=t_id).count()

            # load the returned data from JSON
            data = json.loads(response.content.decode())

            # parse out the results from a paginated set, if needed
            if isinstance(data, dict) and "results" in data:
                data = data["results"]

            # load up the target model uuids
            model_uuids = [d["model_uuid"] for d in data]

            # delete "local" buffer records to avoid uniqueness constraint failures in validation
            Buffer.objects.filter(
                transfer_session_id=t_id, model_uuid__in=model_uuids
            ).delete()

            # run the validation logic to ensure no errors were returned
            errors = validate_and_create_buffer_data(
                data, TransferSession.objects.get(id=t_id)
            )
            self.assertFalse(errors)

            # check that the correct number of buffer items were created
            self.assertEqual(
                expected_count,
                Buffer.objects.filter(
                    transfer_session_id=t_id, model_uuid__in=model_uuids
                ).count(),
            )

            # check that the correct number of buffer items was returned
            self.assertEqual(expected_count, len(data))
            for record in data:
                self.assertEqual(3, len(record["rmcb_list"]))

            return data

    def test_pull_valid_buffer_list(self):

        transfer_session_id = self.create_records_for_pulling()

        self.make_buffer_get_request(transfer_session_id=transfer_session_id)

    def test_pull_fails_when_transfer_session_id_not_specified(self):

        self.create_records_for_pulling()

        self.make_buffer_get_request(
            expected_status=403,
        )

    def test_pull_fails_when_transfer_session_no_longer_active(self):

        transfer_session_id = self.create_records_for_pulling()

        TransferSession.objects.filter(id=transfer_session_id).update(active=False)

        self.make_buffer_get_request(
            transfer_session_id=transfer_session_id,
            expected_status=403,
        )

    def test_pull_fails_when_transfer_session_does_not_exist(self):

        transfer_session_id = self.create_records_for_pulling()

        TransferSession.objects.filter(id=transfer_session_id).delete()

        self.make_buffer_get_request(
            transfer_session_id=transfer_session_id,
            expected_status=403,
        )

    def test_pull_fails_when_transfer_session_is_for_pushing(self):

        transfer_session_id = self.create_records_for_pulling()

        TransferSession.objects.filter(id=transfer_session_id).update(push=True)

        self.make_buffer_get_request(
            transfer_session_id=transfer_session_id,
            expected_status=403,
        )

    def test_pull_by_page_works(self):

        transfer_session_id = self.create_records_for_pulling(count=5)

        self.make_buffer_get_request(
            transfer_session_id=transfer_session_id,
            limit=3,
            offset=0,
            expected_count=3,
        )

    def test_pull_by_page_offset_works(self):

        transfer_session_id = self.create_records_for_pulling(count=5)

        self.make_buffer_get_request(
            transfer_session_id=transfer_session_id,
            limit=3,
            offset=3,
            expected_count=2,
        )


class MorangoInfoTestCase(APITestCase):
    def setUp(self):
        InstanceIDModel.get_or_create_current_instance()
        self.m_info = self.client.get(
            reverse("morangoinfo-detail", kwargs={"pk": 1}), format="json"
        )

    def test_id_changes_id_hash_changes(self):
        old_id_hash = self.m_info.data["instance_hash"]
        with EnvironmentVarGuard() as env:
            env["MORANGO_SYSTEM_ID"] = "new_sys_id"
            InstanceIDModel.get_or_create_current_instance(clear_cache=True)
            m_info = self.client.get(
                reverse("morangoinfo-detail", kwargs={"pk": 1}), format="json"
            )
        self.assertNotEqual(m_info.data["instance_hash"], old_id_hash)

    @override_settings(MORANGO_INSTANCE_INFO={"this_is_a_test": "yes"})
    def test_custom_instance_info(self):
        self.m_info = self.client.get(
            reverse("morangoinfo-detail", kwargs={"pk": 1}), format="json"
        )
        self.assertIn("this_is_a_test", self.m_info.data)
        self.assertEqual(self.m_info.data["this_is_a_test"], "yes")


class PublicKeyTestCase(APITestCase):
    def setUp(self):
        self.key = SharedKey.get_or_create_shared_key()

    @override_settings(ALLOW_CERTIFICATE_PUSHING=True)
    def test_get_public_key(self):
        response = self.client.get(reverse("publickey-list"), format="json")
        self.assertEqual(
            response.data[0]["public_key"], self.key.public_key.get_public_key_string()
        )

    def test_can_not_get_public_key(self):
        response = self.client.get(reverse("publickey-list"), format="json")
        self.assertEqual(response.status_code, 403)
