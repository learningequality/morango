from __future__ import unicode_literals

import json
import string

import mptt.models
from django.core.management import call_command
from django.db import models
from django.db import transaction
from django.utils import timezone
from django.utils.six import string_types
from future.utils import python_2_unicode_compatible

from .fields.crypto import Key
from .fields.crypto import PrivateKeyField
from .fields.crypto import PublicKeyField
from .fields.uuids import UUIDModelMixin
from morango.errors import CertificateIDInvalid
from morango.errors import CertificateProfileInvalid
from morango.errors import CertificateRootScopeInvalid
from morango.errors import CertificateScopeNotSubset
from morango.errors import CertificateSignatureInvalid
from morango.errors import NonceDoesNotExist
from morango.errors import NonceExpired


@python_2_unicode_compatible
class Certificate(mptt.models.MPTTModel, UUIDModelMixin):

    uuid_input_fields = ("public_key", "profile", "salt")

    parent = models.ForeignKey("Certificate", blank=True, null=True)

    # the Morango profile with which this certificate is associated
    profile = models.CharField(max_length=20)

    # scope of this certificate, and version of the scope, along with associated params
    scope_definition = models.ForeignKey("ScopeDefinition")
    scope_version = models.IntegerField()
    scope_params = (
        models.TextField()
    )  # JSON dict of values to insert into scope definitions

    # track the certificate's public key so we can verify any certificates it signs
    public_key = PublicKeyField()

    # a salt value to include in the UUID calculation, to prevent CSR requests from forcing ID collisions
    salt = models.CharField(max_length=32, blank=True)

    # the JSON-serialized copy of all the fields above
    serialized = models.TextField()

    # signature from the private key of the parent certificate, of the "serialized" field text
    signature = models.TextField()

    # when we own a certificate, we'll have the private key for it (otherwise not)
    _private_key = PrivateKeyField(blank=True, null=True, db_column="private_key")

    @property
    def private_key(self):
        return self._private_key

    @private_key.setter
    def private_key(self, value):
        self._private_key = value
        if value and not self.public_key:
            self.public_key = Key(
                public_key_string=self._private_key.get_public_key_string()
            )

    @classmethod
    def generate_root_certificate(cls, scope_def_id, **extra_scope_params):

        # attempt to retrieve the requested scope definition object
        scope_def = ScopeDefinition.retrieve_by_id(scope_def_id)

        # create a certificate model instance
        cert = cls()

        # set the scope definition foreign key, and read some values off of the scope definition model
        cert.scope_definition = scope_def
        cert.scope_version = scope_def.version
        cert.profile = scope_def.profile
        primary_scope_param_key = scope_def.primary_scope_param_key
        assert (
            primary_scope_param_key
        ), "Root cert can only be created for ScopeDefinition that has primary_scope_param_key defined"

        # generate a key and extract the public key component
        cert.private_key = Key()
        cert.public_key = Key(
            public_key_string=cert.private_key.get_public_key_string()
        )

        # calculate the certificate's ID on the basis of the profile and public key
        cert.id = cert.calculate_uuid()

        # set the scope params to include the primary partition value and any additional params
        scope_params = {primary_scope_param_key: cert.id}
        scope_params.update(extra_scope_params)
        cert.scope_params = json.dumps(scope_params)

        # self-sign the certificate
        cert.sign_certificate(cert)

        # save and return the certificate
        cert.save()
        return cert

    def has_private_key(self):
        return self._private_key is not None

    def serialize(self):
        if not self.id:
            self.id = self.calculate_uuid()
        data = {
            "id": self.id,
            "parent_id": self.parent_id,
            "profile": self.profile,
            "salt": self.salt,
            "scope_definition_id": self.scope_definition_id,
            "scope_version": self.scope_version,
            "scope_params": self.scope_params,
            "public_key_string": self.public_key.get_public_key_string(),
        }
        return json.dumps(data)

    @classmethod
    def deserialize(cls, serialized, signature):
        data = json.loads(serialized)
        model = cls(
            id=data["id"],
            parent_id=data["parent_id"],
            profile=data["profile"],
            salt=data.get("salt") or "",
            scope_definition_id=data["scope_definition_id"],
            scope_version=data["scope_version"],
            scope_params=data["scope_params"],
            public_key=Key(public_key_string=data["public_key_string"]),
            serialized=serialized,
            signature=signature,
        )
        return model

    def _serialize_if_needed(self):
        if not self.serialized:
            self.serialized = self.serialize()

    def sign_certificate(self, cert_to_sign):
        cert_to_sign._serialize_if_needed()
        cert_to_sign.signature = self.sign(cert_to_sign.serialized)

    def check_certificate(self):

        # check that the certificate's ID is properly calculated
        if self.id != self.calculate_uuid():
            raise CertificateIDInvalid(
                "Certificate ID is {} but should be {}".format(
                    self.id, self.calculate_uuid()
                )
            )

        if not self.parent:  # self-signed root certificate
            # check that the certificate is properly self-signed
            if not self.verify(self.serialized, self.signature):
                raise CertificateSignatureInvalid()
            # check that the certificate scopes all start with the primary partition value
            scope = self.get_scope()
            for item in scope.read_filter + scope.write_filter:
                if not item.startswith(self.id):
                    raise CertificateRootScopeInvalid(
                        "Scope entry {} does not start with primary partition {}".format(
                            item, self.id
                        )
                    )
        else:  # non-root child certificate
            # check that the certificate is properly signed by its parent
            if not self.parent.verify(self.serialized, self.signature):
                raise CertificateSignatureInvalid()
            # check that certificate's scope is a subset of parent's scope
            if not self.get_scope().is_subset_of(self.parent.get_scope()):
                raise CertificateScopeNotSubset()
            # check that certificate is for same profile as parent
            if self.profile != self.parent.profile:
                raise CertificateProfileInvalid(
                    "Certificate profile is {} but parent's is {}".format(
                        self.profile, self.parent.profile
                    )
                )

    @classmethod
    def save_certificate_chain(cls, cert_chain, expected_last_id=None):

        # parse the chain from json if needed
        if isinstance(cert_chain, string_types):
            cert_chain = json.loads(cert_chain)

        # start from the bottom of the chain
        cert_data = cert_chain[-1]

        # create an in-memory instance of the cert from the serialized data and signature
        cert = cls.deserialize(cert_data["serialized"], cert_data["signature"])

        # verify the id of the cert matches the id of the outer serialized data
        assert cert_data["id"] == cert.id

        # check that the expected ID matches, if specified
        if expected_last_id:
            assert cert.id == expected_last_id

        # if cert already exists locally, it's already been verified, so no need to continue
        # (this also means we have the full cert chain for it, given the `parent` relations)
        try:
            return cls.objects.get(id=cert.id)
        except cls.DoesNotExist:
            pass

        # recurse up the certificate chain, until we hit a cert that exists or is the root
        if len(cert_chain) > 1:
            cls.save_certificate_chain(cert_chain[:-1], expected_last_id=cert.parent_id)
        else:
            assert (
                not cert.parent_id
            ), "First cert in chain must be a root cert (no parent)"

        # ensure the certificate checks out (now that we know its parent, if any, is saved)
        cert.check_certificate()

        # save the certificate, as it's now fully verified
        cert.save()

        return cert

    def sign(self, value):
        assert (
            self.private_key
        ), "Can only sign using certificates that have private keys"
        return self.private_key.sign(value)

    def verify(self, value, signature):
        return self.public_key.verify(value, signature)

    def get_scope(self):
        return self.scope_definition.get_scope(self.scope_params)

    def __str__(self):
        if self.scope_definition:
            return self.scope_definition.get_description(self.scope_params)


class Nonce(UUIDModelMixin):
    """
    Stores temporary nonce values used for cryptographic handshakes during syncing.
    These nonces are requested by the client, and then generated and stored by the server.
    When the client then goes to initiate a sync session, it signs the nonce value using
    the private key from the certificate it is using for the session, to prove to the
    server that it owns the certificate. The server checks that the nonce exists and hasn't
    expired, and then deletes it.
    """

    uuid_input_fields = "RANDOM"

    timestamp = models.DateTimeField(default=timezone.now)
    ip = models.CharField(max_length=100, blank=True)

    @classmethod
    def use_nonce(cls, nonce_value):
        with transaction.atomic():
            # try fetching the nonce
            try:
                nonce = cls.objects.get(id=nonce_value)
            except cls.DoesNotExist:
                raise NonceDoesNotExist()
            # check that the nonce hasn't expired
            if not (0 < (timezone.now() - nonce.timestamp).total_seconds() < 60):
                nonce.delete()
                raise NonceExpired()
            # now that we've used it, delete the nonce
            nonce.delete()


class ScopeDefinition(models.Model):

    # the identifier used to specify this scope within a certificate
    id = models.CharField(primary_key=True, max_length=20)

    # the Morango profile with which this scope is associated
    profile = models.CharField(max_length=20)

    # version number is incremented whenever scope definition is updated
    version = models.IntegerField()

    # the scope_param key that the primary partition value will be inserted into when generating a root cert
    # (if this is not set, then this scope definition cannot be used to generate a root cert)
    primary_scope_param_key = models.CharField(max_length=20, blank=True)

    # human-readable description
    # (can include string template refs to scope params e.g. "Allows syncing data for user ${username}")
    description = models.TextField()

    # filter templates, in the form of a newline-delimited list of colon-delimited partition strings
    # (can include string template refs to scope params e.g. "122211:singleuser:${user_id}")
    read_filter_template = models.TextField()
    write_filter_template = models.TextField()
    read_write_filter_template = models.TextField()

    @classmethod
    def retrieve_by_id(cls, scope_def_id):
        try:
            return cls.objects.get(id=scope_def_id)
        except ScopeDefinition.DoesNotExist:
            call_command("loaddata", "scopedefinitions")
            return cls.objects.get(id=scope_def_id)

    def get_scope(self, params):
        return Scope(definition=self, params=params)

    def get_description(self, params):
        if isinstance(params, string_types):
            params = json.loads(params)
        return string.Template(self.description).safe_substitute(params)


@python_2_unicode_compatible
class Filter(object):
    def __init__(self, template, params={}):
        # ensure params have been deserialized
        if isinstance(params, string_types):
            params = json.loads(params)
        self._template = template
        self._params = params
        self._filter_string = string.Template(template).safe_substitute(params)
        self._filter_tuple = tuple(self._filter_string.split())

    def is_subset_of(self, other):
        for partition in self._filter_tuple:
            if not partition.startswith(other._filter_tuple):
                return False
        return True

    def contains_partition(self, partition):
        return partition.startswith(self._filter_tuple)

    def __le__(self, other):
        return self.is_subset_of(other)

    def __eq__(self, other):
        for partition in self._filter_tuple:
            if partition not in other._filter_tuple:
                return False
        for partition in other._filter_tuple:
            if partition not in self._filter_tuple:
                return False
        return True

    def __contains__(self, partition):
        return self.contains_partition(partition)

    def __add__(self, other):
        return Filter(self._filter_string + "\n" + other._filter_string)

    def __iter__(self):
        return iter(self._filter_tuple)

    def __str__(self):
        return "\n".join(self._filter_tuple)

    def __len__(self):
        return len(self._filter_tuple)


class Scope(object):
    def __init__(self, definition, params):
        # turn the scope definition filter templates into Filter objects
        rw_filter = Filter(definition.read_write_filter_template, params)
        self.read_filter = rw_filter + Filter(definition.read_filter_template, params)
        self.write_filter = rw_filter + Filter(definition.write_filter_template, params)

    def is_subset_of(self, other):
        if not self.read_filter.is_subset_of(other.read_filter):
            return False
        if not self.write_filter.is_subset_of(other.write_filter):
            return False
        return True

    def __le__(self, other):
        return self.is_subset_of(other)

    def __eq__(self, other):
        return (
            self.read_filter == other.read_filter
            and self.write_filter == other.write_filter
        )
