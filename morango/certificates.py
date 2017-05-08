import base64
import json
import uuid
import mptt
import mptt.models

from django.db import models

from .utils.uuids import UUIDModelMixin

from .crypto import Key, PublicKeyField, PrivateKeyField
        

class Certificate(mptt.models.MPTTModel, UUIDModelMixin):

    uuid_input_fields = ("public_key",)

    parent = models.ForeignKey("Certificate", blank=True, null=True)
    
    # the Morango profile with which this certificate is associated
    profile = models.CharField(max_length=20)

    # scope of this certificate, and version of the scope, along with associated params
    scope_definition = models.ForeignKey("ScopeDefinition")
    scope_version = models.IntegerField()
    scope_params = models.TextField()  # JSON dict of values to insert into scope definitions

    # track the certificate's public key so we can verify any certificates it signs
    public_key = PublicKeyField()
    
    # the JSON-serialized copy of all the fields above
    serialized = models.TextField()

    # signature from the private key of the parent certificate, of the "serialized" field text
    signature = models.TextField()

    # when we own a certificate, we'll have the private key for it (otherwise not)
    private_key = PrivateKeyField(blank=True, null=True)

    def serialize(self):
        data = {
            "id": self.id,
            "parent_id": self.parent_id,
            "profile": self.profile,
            "scope_definition_id": self.scope_definition_id,
            "scope_version": self.scope_version,
            "scope_params": self.scope_params,
            "public_key_string": self.public_key.get_public_key_string(),
        }
        return json.dumps(data)

    @classmethod
    def deserialize(cls, serialized):
        data = json.loads(serialized)
        model = cls(
            id=data["id"],
            parent_id=data["parent_id"],
            profile=data["profile"],
            scope_definition_id=data["scope_definition_id"],
            scope_version=data["scope_version"],
            scope_params=data["scope_params"],
            public_key=Key(public_key_string=data["public_key_string"]),
            serialized=serialized,
        )
        return model

    def verify_cert_signature(self, self_signed=False):
        signer = self if self_signed else self.parent
        return signer.verify(self.serialized, self.signature)

    def sign(self, value):
        assert self.private_key, "Can only sign using certificates that have private keys"
        return self.private_key.sign(value)

    def verify(self, value, signature):
        return self.public_key.verify(value, signature)

    def save(self, *args, **kwargs):
        if not self.public_key:
            self.public_key = Key(public_key_string=self.private_key.get_public_key_string())
        if not self.serialized:
            self.serialized = self.serialize()
        super(Certificate, self).save(*args, **kwargs)

    def has_subset_scope_of(self, othercert):
        own_scope = self.scope_definition.get_scope(self.scope_params)
        other_scope = othercert.scope_definition.get_scope(othercert.scope_params)
        return own_scope.is_subset_of(other_scope)


class ScopeDefinition(models.Model):
    
    # the Morango profile with which this scope is associated
    profile = models.CharField(max_length=20)

    # version number is incremented whenever scope definition is updated
    version = models.IntegerField()

    # the identifier used to specify this scope within a certificate
    scope_id = models.CharField(primary_key=True, max_length=20)
    
    # human-readable description
    # (can include string template refs to scope params e.g. "Allows syncing data for user ${username}")
    description = models.TextField()
    
    # scope definition templates, in the form of a newline-delimited list of colon-delimited partition strings
    # (can include string template refs to scope params e.g. "122211:singleuser:${useruuid}")
    read_scope_def = models.TextField()
    write_scope_def = models.TextField()
    read_write_scope_def = models.TextField()
    
    # the JSON-serialized copy of all the fields above
    serialized = models.TextField()

    # signature from the private key of a trusted private key, of the "serialized" field text
    signature = models.TextField()
    key = models.ForeignKey("TrustedKey")

    def get_scope(self, params):
        return Scope(definition=self, params=params)


class ScopeIsNotSubset(Exception):
    pass


class Scope(object):

    def __init__(self, definition, params):
        # inflate the scope definition by filling in the template values from the params
        rw_scope = self._fill_in_scope_def(definition.read_scope_def, params)
        self.read_scope = rw_scope + self._fill_in_scope_def(definition.read_scope_def, params)
        self.write_scope = rw_scope + self._fill_in_scope_def(definition.write_scope_def, params)

    def _fill_in_scope_def(self, scope_def, params):
        return tuple(string.Template(scope_def).safe_substitute(params).split())

    def _verify_subset_for_field(self, scope, fieldname):
        s1 = getattr(self, fieldname)
        s2 = getattr(scope, fieldname)
        for partition in s1:
            if not s1.startswith(s2):
                raise ScopeIsNotSubset(
                    "No partition prefix found for {partition} in {scope} ({fieldname})!".format(
                        partition=partition,
                        scope=s2,
                        fieldname=fieldname,
                    )
                )

    def verify_subset_of(self, scope):
        self._verify_subset_for_field(scope, "read_scope")
        self._verify_subset_for_field(scope, "write_scope")

    def is_subset_of(self, scope):
        try:
            self.verify_subset_of(scope)
        except ScopeIsNotSubset:
            return False
        return True


class TrustedKey(UUIDModelMixin):
    
    public_key = PublicKeyField()
    notes = models.TextField(blank=True)

    revoked = models.BooleanField(default=False)

