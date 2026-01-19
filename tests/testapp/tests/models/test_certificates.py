import json

from django.test import SimpleTestCase
from django.test import TestCase

from morango.errors import CertificateIDInvalid
from morango.errors import CertificateProfileInvalid
from morango.errors import CertificateRootScopeInvalid
from morango.errors import CertificateScopeNotSubset
from morango.errors import CertificateSignatureInvalid
from morango.models.certificates import Certificate
from morango.models.certificates import Filter
from morango.models.certificates import Key
from morango.models.certificates import ScopeDefinition


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


class FilterTestCase(SimpleTestCase):
    def test_init__with_string(self):
        f = Filter("test")
        self.assertEqual(f._template, "test")
        self.assertEqual(f._params, {})
        self.assertEqual(f._filter_tuple, ("test",))

    def test_init__with_params(self):
        f = Filter("test:${param}", {"param": "value"})
        self.assertEqual(f._template, "test:${param}")
        self.assertEqual(f._params, {"param": "value"})
        self.assertEqual(f._filter_tuple, ("test:value",))

    def test_init__with_json_params(self):
        f = Filter("test:${param}", '{"param": "value"}')
        self.assertEqual(f._template, "test:${param}")
        self.assertEqual(f._params, {"param": "value"})
        self.assertEqual(f._filter_tuple, ("test:value",))

    def test_is_subset_of(self):
        f1 = Filter("a\nb")
        f2 = Filter("a\nb\nc")
        f3 = Filter("a")
        f4 = Filter("a:2")
        self.assertTrue(f1.is_subset_of(f2))
        self.assertFalse(f2.is_subset_of(f1))
        self.assertTrue(f1.is_subset_of(f1))
        self.assertFalse(f1.is_subset_of(f3))
        self.assertTrue(f4.is_subset_of(f3))
        self.assertFalse(f3.is_subset_of(f4))

    def test_contains_partition(self):
        f = Filter("a\nb")
        self.assertTrue(f.contains_partition("a"))
        self.assertTrue(f.contains_partition("b"))
        self.assertFalse(f.contains_partition("c"))
        self.assertTrue(f.contains_partition("a:123"))
        self.assertFalse(f.contains_partition("c:123"))

    def test_le_operator(self):
        f1 = Filter("a\nb")
        f2 = Filter("a\nb\nc")
        self.assertTrue(f1 <= f2)
        self.assertFalse(f2 <= f1)

    def test_eq_operator(self):
        f1 = Filter("a\nb")
        f2 = Filter("b\na")
        f3 = Filter("a\nc")
        self.assertTrue(f1 == f2)
        self.assertFalse(f1 == f3)
        self.assertIsNone(f1)

    def test_contains_operator(self):
        f = Filter("a\nb")
        self.assertTrue("a" in f)
        self.assertTrue("a:2" in f)
        self.assertFalse("c" in f)

    def test_add_operator(self):
        f1 = Filter("a")
        f2 = Filter("b")
        f3 = f1 + f2
        self.assertEqual(f3._filter_tuple, ("a", "b"))

    def test_add_operator__duplicates(self):
        f1 = Filter("a")
        f2 = Filter("a\nb")
        f3 = f1 + f2
        self.assertEqual(f3._filter_tuple, ("a", "b"))

    def test_add_operator__with_none(self):
        f1 = Filter("a\nb")
        f2 = None
        f3 = f1 + f2
        self.assertEqual(f3._filter_tuple, ("a", "b"))

    def test_iter(self):
        f = Filter("a\nb")
        self.assertEqual(list(f), ["a", "b"])

    def test_str(self):
        f = Filter("a\nb")
        self.assertEqual(str(f), "a\nb")

    def test_len(self):
        f = Filter("a\nb")
        self.assertEqual(len(f), 2)
        f_empty = Filter("")
        self.assertEqual(len(f_empty), 1)  # empty string is a single partition

    def test_add(self):
        f1 = Filter("a")
        f2 = Filter("b")
        f3 = Filter.add(f1, f2)
        self.assertEqual(f3._filter_tuple, ("a", "b"))

    def test_add__with_none(self):
        f1 = None
        f2 = Filter("a\nb")
        f3 = Filter.add(f1, f2)
        self.assertEqual(f3._filter_tuple, ("a", "b"))
