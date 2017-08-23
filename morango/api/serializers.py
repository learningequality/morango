from rest_framework import serializers, exceptions

from .fields import PublicKeyField
from ..models import Certificate, Nonce, SyncSession, TransferSession, InstanceIDModel, Buffer, SyncableModel
from ..utils.register_models import _profile_models


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
        fields = ('id', 'start_timestamp', 'last_activity_timestamp', 'active', 'local_certificate', 'remote_certificate', 'profile', 'connection_kind', 'connection_path', 'local_ip', 'remote_ip', 'local_instance', 'remote_instance')
        read_only_fields = ('start_timestamp', 'last_activity_timestamp', 'active', 'local_certificate', 'connection_kind', 'local_ip', 'remote_ip', 'local_instance',)


class TransferSessionSerializer(serializers.ModelSerializer):

    class Meta:
        model = TransferSession
        fields = ('id', 'start_timestamp', 'last_activity_timestamp', 'active', 'filter', 'incoming', 'records_transferred', 'records_total', 'sync_session',)
        read_only_fields = ('start_timestamp', 'last_activity_timestamp', 'active', 'records_transferred',)


class InstanceIDSerializer(serializers.ModelSerializer):

    class Meta:
        model = InstanceIDModel
        fields = ('id', 'platform', 'hostname', 'sysversion', 'macaddress', 'database', 'db_path', 'system_id')
        read_only_fields = fields


class BufferSerializer(serializers.ModelSerializer):

    def validate(self, data):

        transfer_session = data["transfer_session"]

        # ensure the provided model_uuid matches the expected/computed id
        try:
            Model = _profile_models[data["profile"]][data["model_name"]]
        except KeyError:
            Model = SyncableModel
        expected_model_uuid = Model.compute_namespaced_id(data["partition"], data["source_id"], data["model_name"])
        if expected_model_uuid != data["model_uuid"]:
            raise serializers.ValidationError({"model_uuid": "Does not match results of calling {}.compute_namespaced_id".format(Model.__class__.__name__)})

        # ensure the profile is marked onto the buffer record
        data["profile"] = transfer_session.sync_session.profile

        # ensure the partition is within the transfer session's filter
        if not transfer_session.get_filter().contains_partition(data["partition"]):
            raise serializers.ValidationError({"partition": "Partition {} is not contained within filter for TransferSession ({})".format(data["partition"], transfer_session.filter)})

        return data

    class Meta:
        model = Buffer
        fields = ('serialized', 'deleted', 'last_saved_instance', 'last_saved_counter', 'partition', 'source_id', 'model_name', 'conflicting_serialized_data', 'model_uuid', 'transfer_session')
