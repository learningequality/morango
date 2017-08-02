import json

from django.test import TestCase

from morango.certificates import Certificate, ScopeDefinition, Key
from morango.errors import CertificateScopeNotSubset, CertificateSignatureInvalid, CertificateIDInvalid, CertificateProfileInvalid, CertificateRootScopeInvalid


class CertificateTestCaseMixin(object):

    def setUp(self):

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


class CertificateCheckingTestCase(CertificateTestCaseMixin, TestCase):

    def test_good_certificates_validate(self):

        self.root_cert.check_certificate()
        self.subset_cert.check_certificate()

    def test_bad_scope_subset_does_not_validate(self):

        bad_subset_cert = Certificate(
            parent=self.root_cert,
            profile=self.profile,
            scope_definition=self.subset_scope_def,
            scope_version=self.subset_scope_def.version,
            scope_params=json.dumps({"mainpartition": "a" * 32, "subpartition": "abracadabra"}),
            private_key=Key(),
        )
        self.root_cert.sign_certificate(bad_subset_cert)
        bad_subset_cert.save()

        with self.assertRaises(CertificateScopeNotSubset):
            bad_subset_cert.check_certificate()

    def test_bad_signature_does_not_validate(self):

        with self.assertRaises(CertificateSignatureInvalid):
            self.root_cert.signature = "bad" + self.root_cert.signature[3:]
            self.root_cert.check_certificate()

        with self.assertRaises(CertificateSignatureInvalid):
            self.subset_cert.signature = "bad" + self.subset_cert.signature[3:]
            self.subset_cert.check_certificate()

    def test_bad_uuid_does_not_validate(self):

        with self.assertRaises(CertificateIDInvalid):
            self.root_cert.id = "a" * 32
            self.root_cert.check_certificate()

        with self.assertRaises(CertificateIDInvalid):
            self.subset_cert.id = "a" * 32
            self.subset_cert.check_certificate()

    def test_different_profile_does_not_validate(self):

        with self.assertRaises(CertificateProfileInvalid):
            self.subset_cert.profile = "anotherprofile"
            self.subset_cert.id = self.subset_cert.calculate_uuid()
            self.subset_cert.check_certificate()

    def test_bad_root_scope_does_not_validate(self):

        with self.assertRaises(CertificateRootScopeInvalid):
            self.root_cert.scope_params = json.dumps({"mainpartition": "a" * 32})
            self.root_cert.check_certificate()


class CertificateSerializationTestCase(CertificateTestCaseMixin, TestCase):

    def setUp(self):
        super(CertificateSerializationTestCase, self).setUp()
        self.root_cert_deserialized = Certificate.deserialize(self.root_cert.serialized, self.root_cert.signature)
        self.subset_cert_deserialized = Certificate.deserialize(self.subset_cert.serialized, self.subset_cert.signature)

    def test_deserialized_certs_validate(self):
        self.subset_cert_deserialized.check_certificate()
        self.root_cert_deserialized.check_certificate()

        self.subset_cert.delete()  # to avoid "Certificate with this Id already exists" error
        self.subset_cert_deserialized.full_clean()

        self.root_cert.delete()  # to avoid "Certificate with this Id already exists" error
        self.root_cert_deserialized.full_clean()

    def test_deserialized_cert_signatures_verify(self):
        self.assertTrue(self.root_cert_deserialized.verify("testval", self.root_cert.sign("testval")))
        self.assertTrue(self.subset_cert_deserialized.verify("testval", self.subset_cert.sign("testval")))

    def test_deserialized_certs_can_be_saved(self):
        Certificate.objects.all().delete()
        self.root_cert_deserialized.save()
        self.subset_cert_deserialized.save()


class CertificateKeySettingTestCase(TestCase):

    def test_setting_private_key_sets_public_key(self):
        cert = Certificate()
        cert.private_key = Key()
        self.assertTrue(cert.public_key.verify("testval", cert.private_key.sign("testval")))

    def test_setting_public_key_does_not_set_private_key(self):
        cert = Certificate()
        cert.public_key = Key()
        self.assertEqual(cert.private_key, None)
