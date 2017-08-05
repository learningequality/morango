from rest_framework import serializers, exceptions

from .fields import PublicKeyField
from ..models import Certificate, Nonce, SyncSession, InstanceIDModel


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


class NonceSerializer(serializers.ModelSerializer):

    class Meta:
        model = Nonce
        fields = ('id', 'timestamp', 'ip')
        read_only_fields = fields


class SyncSessionSerializer(serializers.ModelSerializer):

    class Meta:
        model = SyncSession
        fields = ('id', 'start_timestamp', 'last_activity_timestamp', 'active', 'local_certificate', 'remote_certificate', 'connection_kind', 'connection_path', 'local_ip', 'remote_ip', 'local_instance', 'remote_instance')
        read_only_fields = ('start_timestamp', 'last_activity_timestamp', 'active', 'local_certificate', 'connection_kind', 'local_ip', 'remote_ip', 'local_instance',)


class InstanceIDSerializer(serializers.ModelSerializer):

    class Meta:
        model = InstanceIDModel
        fields = ('id', "platform", "hostname", "sysversion", "macaddress", "database", "db_path", "system_id")
        read_only_fields = fields
