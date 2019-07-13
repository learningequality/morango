from rest_framework import exceptions
from rest_framework import serializers

from ..models.certificates import Nonce
from ..models.core import Buffer
from ..models.core import Certificate
from ..models.core import InstanceIDModel
from ..models.core import RecordMaxCounterBuffer
from ..models.core import SyncSession
from ..models.core import TransferSession
from ..models.fields.crypto import SharedKey
from .fields import PublicKeyField


class CertificateSerializer(serializers.ModelSerializer):

    public_key = PublicKeyField()

    def validate_parent(self, parent):
        if not parent:
            raise exceptions.ValidationError(
                "Parent certificate (to sign the requested certificate) must be specified!"
            )
        if not parent.has_private_key():
            raise exceptions.ValidationError(
                "Server does not have private key for requested parent certificate!"
            )
        return parent

    class Meta:
        model = Certificate
        fields = (
            "id",
            "parent",
            "profile",
            "scope_definition",
            "scope_version",
            "scope_params",
            "public_key",
            "serialized",
            "signature",
            "salt",
        )
        read_only_fields = ("serialized", "id", "signature", "salt")


class SharedKeySerializer(serializers.ModelSerializer):
    class Meta:
        model = SharedKey
        fields = ("public_key",)


class NonceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Nonce
        fields = ("id", "timestamp", "ip")
        read_only_fields = fields


class SyncSessionSerializer(serializers.ModelSerializer):
    class Meta:
        model = SyncSession
        fields = (
            "id",
            "start_timestamp",
            "last_activity_timestamp",
            "active",
            "client_certificate",
            "server_certificate",
            "profile",
            "connection_kind",
            "connection_path",
            "client_ip",
            "server_ip",
            "client_instance",
            "server_instance",
        )
        read_only_fields = (
            "start_timestamp",
            "last_activity_timestamp",
            "active",
            "client_certificate",
            "connection_kind",
            "client_ip",
            "server_ip",
            "client_instance",
        )


class TransferSessionSerializer(serializers.ModelSerializer):
    class Meta:
        model = TransferSession
        fields = (
            "id",
            "start_timestamp",
            "last_activity_timestamp",
            "active",
            "filter",
            "push",
            "records_transferred",
            "records_total",
            "sync_session",
            "server_fsic",
            "client_fsic",
        )
        read_only_fields = ("start_timestamp", "last_activity_timestamp", "active")


class InstanceIDSerializer(serializers.ModelSerializer):
    class Meta:
        model = InstanceIDModel
        fields = (
            "id",
            "platform",
            "hostname",
            "sysversion",
            "node_id",
            "database",
            "db_path",
            "system_id",
        )
        read_only_fields = fields


class RecordMaxCounterBufferSerializer(serializers.ModelSerializer):
    class Meta:
        model = RecordMaxCounterBuffer
        fields = ("transfer_session", "model_uuid", "instance_id", "counter")
        read_only_fields = fields


class BufferSerializer(serializers.ModelSerializer):
    rmcb_list = RecordMaxCounterBufferSerializer(many=True)

    class Meta:
        model = Buffer
        fields = (
            "serialized",
            "deleted",
            "last_saved_instance",
            "last_saved_counter",
            "hard_deleted",
            "partition",
            "source_id",
            "model_name",
            "conflicting_serialized_data",
            "model_uuid",
            "transfer_session",
            "profile",
            "rmcb_list",
            "_self_ref_fk",
        )
        read_only_fields = fields
