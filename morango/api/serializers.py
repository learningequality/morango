from django.db import transaction
from rest_framework import serializers, exceptions
import json

from .fields import PublicKeyField
from ..models import Certificate, Nonce, SyncSession, TransferSession, InstanceIDModel, Buffer, SyncableModel, RecordMaxCounterBuffer
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
        fields = ('id', 'start_timestamp', 'last_activity_timestamp', 'active', 'client_certificate', 'server_certificate', 'profile', 'connection_kind', 'connection_path', 'client_ip', 'server_ip', 'client_instance', 'server_instance')
        read_only_fields = ('start_timestamp', 'last_activity_timestamp', 'active', 'client_certificate', 'connection_kind', 'client_ip', 'server_ip', 'client_instance',)


class TransferSessionSerializer(serializers.ModelSerializer):

    class Meta:
        model = TransferSession
        fields = ('id', 'start_timestamp', 'last_activity_timestamp', 'active', 'filter', 'push', 'records_transferred', 'records_total', 'sync_session', 'server_fsic', 'client_fsic',)
        read_only_fields = ('start_timestamp', 'last_activity_timestamp', 'active', 'records_transferred',)


class InstanceIDSerializer(serializers.ModelSerializer):

    class Meta:
        model = InstanceIDModel
        fields = ('id', 'platform', 'hostname', 'sysversion', 'node_id', 'database', 'db_path', 'system_id')
        read_only_fields = fields


class RecordMaxCounterBufferSerializer(serializers.ModelSerializer):

    class Meta:
        model = RecordMaxCounterBuffer
        fields = ('transfer_session', 'model_uuid', 'instance_id', 'counter')


class BufferSerializer(serializers.ModelSerializer):

    rmcb_list = RecordMaxCounterBufferSerializer(many=True)

    def validate(self, data):

        transfer_session = data["transfer_session"]

        # ensure the provided model_uuid matches the expected/computed id
        try:
            Model = _profile_models[data["profile"]][data["model_name"]]
        except KeyError:
            Model = SyncableModel
        expected_model_uuid = Model.compute_namespaced_id(data["partition"], data["source_id"], data["model_name"])
        if expected_model_uuid != data["model_uuid"]:
            # we sometimes calculate ids based on placeholders, so we recompute ids with those parameters
            model = Model.deserialize(json.loads(data['serialized']))
            expected_model_uuid = model.compute_namespaced_id(model.calculate_partition(), data['source_id'], data['model_name'])
            if expected_model_uuid != data['model_uuid']:
                raise serializers.ValidationError({"model_uuid": "Does not match results of calling {}.compute_namespaced_id".format(Model.__class__.__name__)})

        # ensure the profile is marked onto the buffer record
        data["profile"] = transfer_session.sync_session.profile

        # ensure the partition is within the transfer session's filter
        if not transfer_session.get_filter().contains_partition(data["partition"]):
            raise serializers.ValidationError({"partition": "Partition {} is not contained within filter for TransferSession ({})".format(data["partition"], transfer_session.filter)})

        # ensure that all nested RMCB models are properly associated with this record and transfer session
        for rmcb in data["rmcb_list"]:
            if rmcb["transfer_session"] != transfer_session:
                raise serializers.ValidationError({"rmcb_list": "Transfer session on RMCB ({}) does not match Buffer's TransferSession ({})".format(rmcb["transfer_session"], transfer_session)})
            if rmcb["model_uuid"] != data["model_uuid"]:
                raise serializers.ValidationError({"rmcb_list": "Model UUID on RMCB ({}) does not match Buffer's Model UUID ({})".format(rmcb["model_uuid"], data["model_uuid"])})

        return data

    def create(self, validated_data):
        rmcb_list = [RecordMaxCounterBuffer(**rmcb_data) for rmcb_data in validated_data.pop('rmcb_list')]
        with transaction.atomic():
            buffermodel = Buffer.objects.create(**validated_data)
            RecordMaxCounterBuffer.objects.bulk_create(rmcb_list)
        return buffermodel

    class Meta:
        model = Buffer
        fields = ('serialized', 'deleted', 'last_saved_instance', 'last_saved_counter', 'partition', 'source_id', 'model_name', 'conflicting_serialized_data', 'model_uuid', 'transfer_session', 'profile', 'rmcb_list', '_self_ref_fk')
