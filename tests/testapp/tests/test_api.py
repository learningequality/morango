import base64
import json
import sys
import uuid

from django.core.urlresolvers import reverse
from django.utils import timezone

from rest_framework.test import APITestCase as BaseTestCase

from morango.api.serializers import CertificateSerializer, InstanceIDSerializer
from morango.certificates import Certificate, ScopeDefinition, Key, Nonce
from morango.errors import CertificateScopeNotSubset, CertificateSignatureInvalid, CertificateIDInvalid, CertificateProfileInvalid, CertificateRootScopeInvalid
from morango.models import InstanceIDModel, SyncSession
from facility_profile.models import MyUser


# A weird hack because of http://bugs.python.org/issue17866
if sys.version_info >= (3,):
    class APITestCase(BaseTestCase):
        def assertItemsEqual(self, *args, **kwargs):
            self.assertCountEqual(*args, **kwargs)
else:
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

        self.profile = "testprofile"

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

        self.root_cert1_with_key = Certificate.generate_root_certificate(self.root_scope_def.id)

        self.subset_cert1_without_key = Certificate(
            parent=self.root_cert1_with_key,
            profile=self.profile,
            scope_definition=self.subset_scope_def,
            scope_version=self.subset_scope_def.version,
            scope_params=json.dumps({"mainpartition": self.root_cert1_with_key.id, "subpartition": "abracadabra"}),
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

        self.root_cert2_without_key = Certificate.generate_root_certificate(self.root_scope_def.id)

        self.subset_cert2_with_key = Certificate(
            parent=self.root_cert2_without_key,
            profile=self.profile,
            scope_definition=self.subset_scope_def,
            scope_version=self.subset_scope_def.version,
            scope_params=json.dumps({"mainpartition": self.root_cert2_without_key.id, "subpartition": "abracadabra"}),
            private_key=Key(),
        )
        self.root_cert2_without_key.sign_certificate(self.subset_cert2_with_key)
        self.subset_cert2_with_key.save()
        self.root_cert2_without_key._private_key = None
        self.root_cert2_without_key.save()

        self.original_cert_count = Certificate.objects.count()

    def make_cert_endpoint_request(self, params={}, method="GET"):
        fn = getattr(self.client, method.lower())
        response = fn(reverse('certificates-list'), params, format='json')
        data = json.loads(response.content.decode())
        return (response, data)

    def perform_basic_authentication(self, user):
        basic_auth_header = b'Basic ' + base64.encodestring(b"username=%s:%s" % (user.username.encode(), user.actual_password.encode()))
        self.client.credentials(HTTP_AUTHORIZATION=basic_auth_header)


class CertificateListingTestCase(CertificateTestCaseMixin, APITestCase):

    def test_certificate_filtering_by_primary_partition(self):
        
        # check that only the root cert and leaf cert are returned when they're the ones with a private key
        _, data = self.make_cert_endpoint_request(params={'primary_partition': self.root_cert1_with_key.id})
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["id"], self.root_cert1_with_key.id)
        self.assertEqual(data[1]["id"], self.sub_subset_cert1_with_key.id)
        
        # check that only the subcert is returned when it's the one with a private key
        _, data = self.make_cert_endpoint_request(params={'primary_partition': self.root_cert2_without_key.id})
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["id"], self.subset_cert2_with_key.id)

        # check that no certificates are returned when the partition doesn't exist
        _, data = self.make_cert_endpoint_request(params={'primary_partition': "a" * 32})
        self.assertEqual(len(data), 0)

        # check that no certificates are returned when profile doesn't match
        _, data = self.make_cert_endpoint_request(params={'primary_partition': self.root_cert2_without_key.id, "profile": "namelessone"})
        self.assertEqual(len(data), 0)

        # check that certificates are returned when profile does match
        _, data = self.make_cert_endpoint_request(params={'primary_partition': self.root_cert2_without_key.id, "profile": self.profile})
        self.assertEqual(len(data), 1)
        
    def test_certificate_filtering_by_ancestors_of(self):
        
        # check that only the root cert is returned when it's the one we're requesting ancestors of
        _, data = self.make_cert_endpoint_request(params={'ancestors_of': self.root_cert1_with_key.id})
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["id"], self.root_cert1_with_key.id)
        
        # check that both the subcert and root cert are returned when we request ancestors of subcert
        _, data = self.make_cert_endpoint_request(params={'ancestors_of': self.subset_cert2_with_key.id})
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["id"], self.root_cert2_without_key.id)
        self.assertEqual(data[1]["id"], self.subset_cert2_with_key.id)

        # check that no certificates are returned when the certificate ID doesn't exist
        _, data = self.make_cert_endpoint_request(params={'ancestors_of': "a" * 32})
        self.assertEqual(len(data), 0)

        # check that no certificates are returned when profile doesn't match
        _, data = self.make_cert_endpoint_request(params={'ancestors_of': self.subset_cert2_with_key.id, "profile": "namelessone"})
        self.assertEqual(len(data), 0)

        # check that certificates are returned when profile does match
        _, data = self.make_cert_endpoint_request(params={'ancestors_of': self.subset_cert2_with_key.id, "profile": self.profile})
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
            "scope_definition": kwargs.get("scope_definition", parent.scope_definition_id),
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

    def assert_certificate_creation_fails_with_bad_parameters(self, parent, **params):
        self.perform_basic_authentication(self.superuser)
        response, data, key = self.make_csr(parent=parent, **params)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(Certificate.objects.count(), self.original_cert_count)

    def test_certificate_creation_fails_for_wrong_profile(self):
        self.assert_certificate_creation_fails_with_bad_parameters(
            parent=self.root_cert1_with_key,
            profile="covfefe"
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


class NonceCreationTestCase(APITestCase):

    def test_nonces_can_be_created(self):
        response = self.client.post(reverse('nonces-list'), {}, format='json')
        data = json.loads(response.content.decode())
        self.assertEqual(response.status_code, 201)
        nonces = Nonce.objects.all()
        self.assertEqual(nonces.count(), 1)
        self.assertEqual(nonces[0].id, data["id"])

    def test_nonces_list_cannot_be_read(self):
        response = self.client.get(reverse('nonces-list'), {}, format='json')
        self.assertEqual(response.status_code, 403)
        nonces = Nonce.objects.all()
        self.assertEqual(nonces.count(), 0)

    def test_nonces_item_cannot_be_read(self):
        # create the nonce
        response = self.client.post(reverse('nonces-list'), {}, format='json')
        data = json.loads(response.content.decode())
        # try to read the nonce
        response = self.client.get(reverse('nonces-detail', kwargs={"pk": data["id"]}), {}, format='json')
        self.assertEqual(response.status_code, 403)


class SyncSessionEndpointTestCase(CertificateTestCaseMixin, APITestCase):

    def get_initial_syncsession_data_for_request(self):

        # fetch a nonce value to use in creating the syncsession
        response = self.client.post(reverse('nonces-list'), {}, format='json')
        nonce = json.loads(response.content.decode())["id"]

        # prepare the data to send in the syncsession creation request
        data = {
            "id": uuid.uuid4().hex,
            "server_certificate_id": self.root_cert1_with_key.id,
            "client_certificate_id": self.sub_subset_cert1_with_key.id,
            "certificate_chain": json.dumps(CertificateSerializer(self.sub_subset_cert1_with_key.get_ancestors(include_self=True), many=True).data),
            "connection_path": "http://127.0.0.1:8000",
            "instance": json.dumps(InstanceIDSerializer(InstanceIDModel.get_or_create_current_instance()[0]).data),
            "nonce": nonce,
        }

        # sign the nonce/ID combo to attach to the request
        data["signature"] = self.sub_subset_cert1_with_key.sign("{nonce}:{id}".format(**data))

        return data

    def assertSyncSessionCreationFails(self, data, status_code=403):

        # make the API call to attempt to create the SyncSession, and make sure it was denied
        response = self.client.post(reverse('syncsessions-list'), data, format='json')
        self.assertEqual(response.status_code, status_code)

        # check that the syncsession was not created
        self.assertEqual(SyncSession.objects.count(), 0)

    def test_syncsession_can_be_created(self):

        data = self.get_initial_syncsession_data_for_request()

        # delete two of the certs from the chain so we can make sure they get added back
        self.sub_subset_cert1_with_key.delete()
        self.subset_cert1_without_key.delete()
        self.assertEqual(Certificate.objects.count(), self.original_cert_count - 2)

        # make the API call to create the SyncSession
        response = self.client.post(reverse('syncsessions-list'), data, format='json')
        self.assertEqual(response.status_code, 201)

        # check that the cert chain was deserialized
        self.assertEqual(Certificate.objects.count(), self.original_cert_count)

        # check that the syncsession was created
        syncsession = SyncSession.objects.get()
        self.assertEqual(syncsession.id, data["id"])
        self.assertEqual(syncsession.remote_certificate_id, data["client_certificate_id"])
        self.assertEqual(syncsession.local_certificate_id, data["server_certificate_id"])
        self.assertTrue(syncsession.active)

    def test_syncsession_creation_fails_with_bad_signature(self):

        data = self.get_initial_syncsession_data_for_request()

        data["signature"] = self.sub_subset_cert1_with_key.sign("nonsense:id")

        self.assertSyncSessionCreationFails(data)

    def test_syncsession_creation_fails_with_client_cert_not_matching_cert_chain(self):

        data = self.get_initial_syncsession_data_for_request()

        data["certificate_chain"] = json.dumps(CertificateSerializer(self.subset_cert2_with_key.get_ancestors(include_self=True), many=True).data)

        self.assertSyncSessionCreationFails(data)

    def test_syncsession_creation_fails_with_expired_nonce(self):

        data = self.get_initial_syncsession_data_for_request()

        Nonce.objects.all().update(timestamp=timezone.datetime(2000, 1, 1, tzinfo=timezone.get_current_timezone()))

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

        response = self.client.delete(reverse('syncsessions-detail', kwargs={"pk": syncsession.id}), format='json')
        self.assertEqual(response.status_code, 204)

        # check that the syncsession was "deleted"
        self.assertEqual(SyncSession.objects.get().active, False)

    def test_inactive_syncsession_cannot_be_deleted(self):

        self.test_syncsession_can_be_created()

        syncsession = SyncSession.objects.get()
        syncsession.active = False
        syncsession.save()

        response = self.client.delete(reverse('syncsessions-detail', kwargs={"pk": syncsession.id}), format='json')
        self.assertEqual(response.status_code, 404)
