"""
Helper functions for use across syncing related functionality.
"""

import factory
import mock
import uuid

from django.core.serializers.json import DjangoJSONEncoder
from facility_profile.models import Facility, MyUser, InteractionLog, SummaryLog
from morango.controller import MorangoProfileController
from morango.models import DatabaseIDModel, InstanceIDModel, AbstractStore, Store, Buffer, RecordMaxCounter, RecordMaxCounterBuffer
from morango.syncsession import SyncClient


class FacilityFactory(factory.DjangoModelFactory):

    class Meta:
        model = Facility

    name = factory.Sequence(lambda n: "Fac %d" % n)


class AbstractStoreFactory(factory.DjangoModelFactory):

    class Meta:
        model = AbstractStore

    model_name = 'facility'
    profile = 'facilitydata'

class BufferFactory(AbstractStoreFactory):

    class Meta:
        model = Buffer

class StoreFactory(AbstractStoreFactory):

    class Meta:
        model = Store

class RecordMaxCounterBufferFactory(factory.DjangoModelFactory):

    class Meta:
        model = RecordMaxCounterBuffer

class RecordMaxCounterFactory(factory.DjangoModelFactory):

    class Meta:
        model = RecordMaxCounter


def serialized_facility_factory(identifier):
    facility = Facility(name="Facility {}".format(identifier), id=identifier)
    return DjangoJSONEncoder().encode(facility.serialize())

def create_dummy_store_data():
    data = {}
    DatabaseIDModel.objects.create()
    (data['group1_id'], _) = InstanceIDModel.get_or_create_current_instance()  # counter is at 0

    # create controllers for app/store/buffer operations
    data['mc'] = MorangoProfileController('facilitydata')
    data['sc'] = SyncClient('host', 'facilitydata')

    # create group of facilities and first serialization
    data['group1_c1'] = [FacilityFactory() for _ in range(5)]
    data['mc'].serialize_into_store()  # counter is at 1

    # create group of facilities and second serialization
    data['group1_c2'] = [FacilityFactory() for _ in range(5)]

    # create users and logs associated with user
    data['user1'] = MyUser.objects.create(username='bob')
    data['user1_sumlogs'] = [SummaryLog.objects.create(user=data['user1']) for _ in range(5)]

    data['mc'].serialize_into_store()  # counter is at 2

    # create new instance id and group of facilities
    with mock.patch('platform.platform', return_value='plataforma'):
        (data['group2_id'], _) = InstanceIDModel.get_or_create_current_instance()  # new counter is at 0
    data['group2_c1'] = [FacilityFactory() for _ in range(5)]

    # create users and logs associated with user
    data['user2'] = MyUser.objects.create(username='rob')
    data['user2_sumlogs'] = [SummaryLog.objects.create(user=data['user2']) for _ in range(5)]
    data['user2_interlogs'] = [InteractionLog.objects.create(user=data['user2']) for _ in range(5)]

    data['user3'] = MyUser.objects.create(username='zob')
    data['user3_sumlogs'] = [SummaryLog.objects.create(user=data['user3']) for _ in range(5)]
    data['user3_interlogs'] = [InteractionLog.objects.create(user=data['user3']) for _ in range(5)]

    data['mc'].serialize_into_store()  # new counter is at 1

    data['user4'] = MyUser.objects.create(username='invalid', _morango_partition='badpartition')
    data['mc'].serialize_into_store()  # new counter is at 2

    return data

def setUpIds(common_id=True):

    if common_id:
        data = ['1' * 32]
    else:
        data = [uuid.uuid4().hex]
    data = data + [uuid.uuid4().hex for _ in range(3)]
    return data

def create_rmc_data(c1, c2, c3, c4, ids, model_id):
    for i, c in zip(ids, [c1, c2, c3, c4]):
        RecordMaxCounterFactory(instance_id=i, counter=c, store_model_id=model_id)

def create_rmcb_data(c1, c2, c3, c4, ids, model_id, ts):
    for i, c in zip(ids, [c1, c2, c3, c4]):
        RecordMaxCounterBufferFactory(instance_id=i, counter=c, model_uuid=model_id, transfer_session_id=ts)

def create_buffer_and_store_dummy_data(transfer_session_id):
    data = {}
    # example data for reverse ff
    data['model1'] = 'a' * 32
    data['model1_rmc_ids'] = setUpIds()
    # store1: last_saved => D: 3
    # RMCs A: 3, B: 1, C: 2, D: 3
    StoreFactory(serialized="store", last_saved_instance=data['model1_rmc_ids'][3], last_saved_counter=3, id=data['model1'])
    create_rmc_data(3, 1, 2, 3, data['model1_rmc_ids'], data['model1'])
    data['model1_rmcb_ids'] = setUpIds()
    # buffer1: last_saved => A: 1
    # RMCBs A: 1, F: 2, G: 3, H: 4
    BufferFactory(serialized="buffer", last_saved_instance=data['model1_rmcb_ids'][0], last_saved_counter=1, model_uuid=data['model1'],
                  transfer_session_id=transfer_session_id)
    create_rmcb_data(1, 2, 3, 4, data['model1_rmcb_ids'], data['model1'], transfer_session_id)

    # example data for merge conflict (rmcb.counter > rmc.counter)
    data['model2'] = 'b' * 32
    data['model2_rmc_ids'] = setUpIds()
    # store2: last_saved => C: 2
    # RMCs A: 1, B: 1, C: 2, D: 3
    StoreFactory(serialized="store", last_saved_instance=data['model2_rmc_ids'][2], last_saved_counter=2, id=data['model2'],
                 conflicting_serialized_data="store")
    create_rmc_data(1, 1, 2, 3, data['model2_rmc_ids'], data['model2'])
    data['model2_rmcb_ids'] = setUpIds()
    # buffer2: last_saved => F: 2
    # RMCBs A: 3, F: 2, G: 3, H: 4
    BufferFactory(serialized="buffer", last_saved_instance=data['model2_rmcb_ids'][1], last_saved_counter=2, model_uuid=data['model2'],
                  transfer_session_id=transfer_session_id, deleted=1)
    create_rmcb_data(3, 2, 3, 4, data['model2_rmcb_ids'], data['model2'], transfer_session_id)

    # example data for merge conflict (rmcb.counter <= rmc.counter)
    data['model5'] = 'e' * 32
    data['model5_rmc_ids'] = setUpIds()
    # store5: last_saved => C: 2
    # RMCs A: 3, B: 1, C: 2, D: 3
    StoreFactory(serialized="store", last_saved_instance=data['model5_rmc_ids'][2], last_saved_counter=2, id=data['model5'],
                 conflicting_serialized_data="store")
    create_rmc_data(3, 1, 2, 3, data['model5_rmc_ids'], data['model5'])
    data['model5_rmcb_ids'] = setUpIds()
    # buffer5: last_saved => F: 2
    # RMCBs A: 1, F: 2, G: 3, H: 4
    BufferFactory(serialized="buffer", last_saved_instance=data['model5_rmcb_ids'][1], last_saved_counter=2, model_uuid=data['model5'],
                  transfer_session_id=transfer_session_id)
    create_rmcb_data(1, 2, 3, 4, data['model5_rmcb_ids'], data['model5'], transfer_session_id)

    # example data for ff
    data['model3'] = 'c' * 32
    data['model3_rmc_ids'] = setUpIds()
    # store3: last_saved => A: 1
    # RMCs A: 1, B: 2, C: 3, D: 4
    StoreFactory(serialized="store", last_saved_instance=data['model3_rmc_ids'][0], last_saved_counter=1, id=data['model3'])
    create_rmc_data(1, 2, 3, 4, data['model3_rmc_ids'], data['model3'])
    data['model3_rmcb_ids'] = setUpIds()
    # buffer3: last_saved => F: 2
    # RMCBs A: 3, F: 2, G: 3, H: 4
    BufferFactory(serialized="buffer", last_saved_instance=data['model3_rmcb_ids'][1], last_saved_counter=2, model_uuid=data['model3'],
                  transfer_session_id=transfer_session_id)
    create_rmcb_data(3, 2, 3, 4, data['model3_rmcb_ids'], data['model3'], transfer_session_id)

    # example for missing store data
    data['model4'] = 'd' * 32
    data['model4_rmcb_ids'] = setUpIds()
    BufferFactory(serialized="buffer", last_saved_instance=data['model4_rmcb_ids'][0], last_saved_counter=1, model_uuid=data['model4'],
                  transfer_session_id=transfer_session_id)
    create_rmcb_data(1, 2, 3, 4, data['model4_rmcb_ids'], data['model4'], transfer_session_id)

    # buffer record with different transfer session id
    data['tfs_id'] = '9' * 32
    data['model6'] = 'f' * 32
    data['model6_rmcb_ids'] = setUpIds()
    BufferFactory(last_saved_instance=data['model6_rmcb_ids'][0], last_saved_counter=1, model_uuid=data['model6'], transfer_session_id=data['tfs_id'])
    create_rmcb_data(1, 2, 3, 4, data['model6_rmcb_ids'], data['model6'], data['tfs_id'])

    return data
