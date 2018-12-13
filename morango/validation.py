import copy
from django.db import transaction
from morango.models import Buffer, RecordMaxCounterBuffer, SyncableModel
from rest_framework.exceptions import ValidationError

from .utils.register_models import _profile_models


def validate_and_create_buffer_data(data, transfer_session):
    data = copy.deepcopy(data)
    rmcb_list = []
    buffer_list = []
    for record in data:
        # ensure the provided model_uuid matches the expected/computed id
        try:
            Model = _profile_models[record["profile"]][record["model_name"]]
        except KeyError:
            Model = SyncableModel

        partition = record['partition'].replace(record['model_uuid'], Model.ID_PLACEHOLDER)
        expected_model_uuid = Model.compute_namespaced_id(partition, record["source_id"], record["model_name"])
        if expected_model_uuid != record["model_uuid"]:
            raise ValidationError({"model_uuid": "Does not match results of calling {}.compute_namespaced_id".format(Model.__class__.__name__)})

        # ensure the profile is marked onto the buffer record
        record["profile"] = transfer_session.sync_session.profile

        # ensure the partition is within the transfer session's filter
        if not transfer_session.get_filter().contains_partition(record["partition"]):
            raise ValidationError({"partition": "Partition {} is not contained within filter for TransferSession ({})".format(record["partition"], transfer_session.filter)})

        # ensure that all nested RMCB models are properly associated with this record and transfer session
        for rmcb in record.pop('rmcb_list'):
            if rmcb["transfer_session"] != transfer_session.id:
                raise ValidationError({"rmcb_list": "Transfer session on RMCB ({}) does not match Buffer's TransferSession ({})".format(rmcb["transfer_session"], transfer_session)})
            if rmcb["model_uuid"] != record["model_uuid"]:
                raise ValidationError({"rmcb_list": "Model UUID on RMCB ({}) does not match Buffer's Model UUID ({})".format(rmcb["model_uuid"], record["model_uuid"])})
            rmcb['transfer_session_id'] = rmcb.pop('transfer_session')
            rmcb_list += [RecordMaxCounterBuffer(**rmcb)]

        record['transfer_session_id'] = record.pop('transfer_session')
        buffer_list += [Buffer(**record)]

    with transaction.atomic():
        transfer_session.records_transferred += len(data)
        transfer_session.save()
        Buffer.objects.bulk_create(buffer_list)
        RecordMaxCounterBuffer.objects.bulk_create(rmcb_list)
