"""
Helper functions for use across syncing related functionality.
"""

import factory
import json
import mock
import uuid

from django.core.serializers.json import DjangoJSONEncoder
from facility_profile.models import Facility, MyUser, InteractionLog, SummaryLog
from morango.controller import MorangoProfileController
from morango.models import DatabaseIDModel, InstanceIDModel, AbstractStore, Store, Buffer, RecordMaxCounter, RecordMaxCounterBuffer
from morango.syncsession import SyncClient


class FacilityModelFactory(factory.DjangoModelFactory):

    class Meta:
        model = Facility

    name = factory.Sequence(lambda n: "Fac %d" % n)

class AbstractStoreFactory(factory.DjangoModelFactory):

    class Meta:
        model = AbstractStore

    model_name = 'facility'
    profile = 'facilitydata'

class BufferModelFactory(AbstractStoreFactory):

    class Meta:
        model = Buffer

class StoreModelFactory(AbstractStoreFactory):

    class Meta:
        model = Store

class RecordMaxCounterBufferModelFactory(factory.DjangoModelFactory):

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
    data['group1_c1'] = [FacilityModelFactory() for _ in range(5)]
    data['mc'].serialize_into_store()  # counter is at 1

    # create group of facilites and second serialization
    data['group1_c2'] = [FacilityModelFactory() for _ in range(5)]

    # create users and logs associated with user
    data['user1'] = MyUser.objects.create(username='bob')
    data['user1_sumlogs'] = [SummaryLog.objects.create(user=data['user1']) for _ in range(5)]

    data['mc'].serialize_into_store()  # counter is at 2

    # create new instance id and group of facilities
    with mock.patch('platform.platform', return_value='plataforma'):
        (data['group2_id'], _) = InstanceIDModel.get_or_create_current_instance()  # new counter is at 0
    data['group2_c1'] = [FacilityModelFactory() for _ in range(5)]

    # create users and logs associated with user
    data['user2'] = MyUser.objects.create(username='rob')
    data['user2_sumlogs'] = [SummaryLog.objects.create(user=data['user2']) for _ in range(5)]
    data['user2_interlogs'] = [InteractionLog.objects.create(user=data['user2']) for _ in range(5)]

    data['user3'] = MyUser.objects.create(username='zob')
    data['user3_sumlogs'] = [SummaryLog.objects.create(user=data['user3']) for _ in range(5)]
    data['user3_interlogs'] = [InteractionLog.objects.create(user=data['user3']) for _ in range(5)]

    data['mc'].serialize_into_store()  # new counter is at 1
    return data
