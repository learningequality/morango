from rest_framework import serializers, exceptions

from .models import Certificate
from .crypto import Key


class PublicKeyField(serializers.Field):

    def to_representation(self, obj):
        return unicode(obj)

    def to_internal_value(self, data):
        return Key(public_key_string=data)


class CertificateSerializer(serializers.ModelSerializer):

    public_key = PublicKeyField()

    def validate_parent(self, parent):
        if not parent:
            raise exceptions.ValidationError("Parent certificate (to sign the requested certificate) must be specified!")
        if not parent.has_private_key():
            raise exceptions.ValidationError("Server does not have private key for requested parent certificate!")
        return parent

    class Meta:
        model = Certificate
        fields = ('id', 'parent', 'profile', 'scope_definition', 'scope_version', 'scope_params', 'public_key', 'serialized', 'signature', 'salt')
        read_only_fields = ('serialized', 'id', 'signature', 'salt')
