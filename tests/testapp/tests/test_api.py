import base64
import json
import sys

from django.core.urlresolvers import reverse

from rest_framework.test import APITestCase as BaseTestCase

from morango.certificates import Certificate, ScopeDefinition, Key
from morango.errors import CertificateScopeNotSubset, CertificateSignatureInvalid, CertificateIDInvalid, CertificateProfileInvalid, CertificateRootScopeInvalid
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

    def make_cert_endpoint_request(self, params={}, method="GET"):
        fn = getattr(self.client, method.lower())
        response = fn(reverse('certificates-list'), params, format='json')
        data = json.loads(response.content)
        return (response, data)

    def perform_basic_authentication(self, user):
        basic_auth_header = 'Basic ' + base64.encodestring("username=%s:%s" % (user.username, user.actual_password))
        self.client.credentials(HTTP_AUTHORIZATION=basic_auth_header)


class CertificateListingTestCase(CertificateTestCaseMixin, APITestCase):

    def test_certificate_filtering_by_primary_partition(self):
        
        # check that only the root cert is returned when it's the one with a private key
        _, data = self.make_cert_endpoint_request(params={'primary_partition': self.root_cert1_with_key.id})
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["id"], self.root_cert1_with_key.id)
        
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

        # check that both the certs owned by the server (for which it has private keys) are returned
        _, data = self.make_cert_endpoint_request()
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["id"], self.root_cert1_with_key.id)
        self.assertEqual(data[1]["id"], self.subset_cert2_with_key.id)

        # check that no certificates are returned when profile doesn't match
        _, data = self.make_cert_endpoint_request(params={"profile": "namelessone"})
        self.assertEqual(len(data), 0)

        # check that certificates are returned when profile does match
        _, data = self.make_cert_endpoint_request(params={"profile": self.profile})
        self.assertEqual(len(data), 2)


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
        self.assertEqual(Certificate.objects.count(), 5)

    def test_certificate_creation_fails_for_non_superuser(self):
        self.perform_basic_authentication(self.user)
        response, data, key = self.make_csr(parent=self.root_cert1_with_key)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(Certificate.objects.count(), 4)

    def test_certificate_creation_fails_without_credentials(self):
        response, data, key = self.make_csr(parent=self.root_cert1_with_key)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(Certificate.objects.count(), 4)

    def assert_certificate_creation_fails_with_bad_parameters(self, parent, **params):
        self.perform_basic_authentication(self.superuser)
        response, data, key = self.make_csr(parent=parent, **params)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(Certificate.objects.count(), 4)

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

