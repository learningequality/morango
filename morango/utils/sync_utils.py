import json
import functools

from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, transaction
from django.db.models import Q
from django.utils.six import iteritems
from morango.certificates import Filter
from morango.models import Buffer, DatabaseMaxCounter, DeletedModels, InstanceIDModel, RecordMaxCounter, RecordMaxCounterBuffer, Store
from morango.utils.register_models import _profile_models


def _join_with_logical_operator(lst, operator):
    op = ") {operator} (".format(operator=operator)
    return "(({items}))".format(items=op.join(lst))

def _self_referential_fk(klass_model):
    """
    Return whether this model has a self ref FK, and the name for the field
    """
    for f in klass_model._meta.concrete_fields:
        if f.related_model:
            if issubclass(klass_model, f.related_model):
                return f.attname
    return None

def _fsic_queuing_calc(fsic1, fsic2):
    """
    We set the lower counter between two same instance ids.
    If an instance_id exists in one fsic but not the other we want to give that counter a value of 0.

    :param fsic1: dictionary containing (instance_id, counter) pairs
    :param fsic2: dictionary containing (instance_id, counter) pairs
    :return ``dict`` of fsics to be used in queueing the correct records to the buffer
    """
    return {instance: fsic2.get(instance, 0) for instance, counter in iteritems(fsic1) if fsic2.get(instance, 0) < counter}

def _serialize_into_store(profile, filter=None):
    """
    Takes data from app layer and serializes the models into the store.
    """
    # ensure that we write and retrieve the counter in one go for consistency
    current_id = InstanceIDModel.get_current_instance_and_increment_counter()

    with transaction.atomic():
        # create Q objects for filtering by prefixes
        prefix_condition = None
        if filter:
            prefix_condition = functools.reduce(lambda x, y: x | y, [Q(_morango_partition__startswith=prefix) for prefix in filter])

        # filter through all models with the dirty bit turned on
        syncable_dict = _profile_models[profile]
        for (_, klass_model) in iteritems(syncable_dict):
            new_store_records = []
            new_rmc_records = []
            klass_queryset = klass_model.objects.filter(_morango_dirty_bit=True)
            if prefix_condition:
                klass_queryset = klass_queryset.filter(prefix_condition)
            for app_model in klass_queryset:
                try:
                    store_model = Store.objects.get(id=app_model.id)

                    # if store record dirty and app record dirty, append store serialized to conflicting data
                    if store_model.dirty_bit:
                        store_model.conflicting_serialized_data = store_model.serialized + "\n" + store_model.conflicting_serialized_data
                        store_model.dirty_bit = False

                    # set new serialized data on this store model
                    ser_dict = json.loads(store_model.serialized)
                    ser_dict.update(app_model.serialize())
                    store_model.serialized = DjangoJSONEncoder().encode(ser_dict)

                    # create or update instance and counter on the record max counter for this store model
                    RecordMaxCounter.objects.update_or_create(defaults={'counter': current_id.counter},
                                                              instance_id=current_id.id,
                                                              store_model_id=store_model.id)

                    # update last saved bys for this store model
                    store_model.last_saved_instance = current_id.id
                    store_model.last_saved_counter = current_id.counter
                    store_model.deleted = False

                    # update fields for this store model
                    store_model.save(update_fields=['serialized', 'last_saved_instance', 'last_saved_counter', 'conflicting_serialized_data', 'deleted'])

                except Store.DoesNotExist:
                    kwargs = {
                        'id': app_model.id,
                        'serialized': DjangoJSONEncoder().encode(app_model.serialize()),
                        'last_saved_instance': current_id.id,
                        'last_saved_counter': current_id.counter,
                        'model_name': app_model.morango_model_name,
                        'profile': app_model.morango_profile,
                        'partition': app_model._morango_partition,
                        'source_id': app_model._morango_source_id,
                    }
                    # check if model has FK pointing to itand add the value to a field on the store
                    self_ref_fk = _self_referential_fk(klass_model)
                    if self_ref_fk:
                        self_ref_fk_value = getattr(app_model, self_ref_fk)
                        kwargs.update({'_self_ref_fk': self_ref_fk_value or ''})
                    # create store model and record max counter for the app model
                    new_store_records.append(Store(**kwargs))
                    new_rmc_records.append(RecordMaxCounter(store_model_id=app_model.id, instance_id=current_id.id, counter=current_id.counter))

            # bulk create store and rmc records for this class
            Store.objects.bulk_create(new_store_records)
            RecordMaxCounter.objects.bulk_create(new_rmc_records)

            # set dirty bit to false for all instances of this model
            klass_queryset.update(update_dirty_bit_to=False)

        # get list of ids of deleted models
        deleted_ids = DeletedModels.objects.filter(profile=profile).values_list('id', flat=True)
        # update last_saved_bys and deleted flag of all deleted store model instances
        deleted_store_records = Store.objects.filter(id__in=deleted_ids)
        deleted_store_records.update(dirty_bit=False, deleted=True, last_saved_instance=current_id.id, last_saved_counter=current_id.counter)
        # update rmcs counters for deleted models that have our instance id
        RecordMaxCounter.objects.filter(instance_id=current_id.id, store_model_id__in=deleted_ids).update(counter=current_id.counter)
        # get a list of deleted model ids that don't have an rmc for our instance id
        new_rmc_ids = deleted_store_records.exclude(recordmaxcounter__instance_id=current_id.id).values_list("id", flat=True)
        # bulk create these new rmcs
        RecordMaxCounter.objects.bulk_create([RecordMaxCounter(store_model_id=r_id, instance_id=current_id.id, counter=current_id.counter) for r_id in new_rmc_ids])
        # clear deleted models table for this profile
        DeletedModels.objects.filter(profile=profile).delete()
        # update our own database max counters after serialization
        if not filter:
            DatabaseMaxCounter.objects.update_or_create(instance_id=current_id.id, partition="", defaults={'counter': current_id.counter})
        else:
            for f in filter:
                DatabaseMaxCounter.objects.update_or_create(instance_id=current_id.id, partition=f, defaults={'counter': current_id.counter})

def _deserialize_from_store(profile):
    """
    Takes data from the store and integrates into the application.
    """
    # we first serialize to avoid deserialization merge conflicts
    _serialize_into_store(profile)

    with transaction.atomic():
        syncable_dict = _profile_models[profile]
        # iterate through classes which are in foreign key dependency order
        for model_name, klass_model in iteritems(syncable_dict):
            # handle cases where a class has a single FK reference to itself
            self_ref_fk = _self_referential_fk(klass_model)
            query = Q(model_name=klass_model.morango_model_name)
            for klass in klass_model.morango_model_dependencies:
                query |= Q(model_name=klass.morango_model_name)
            if self_ref_fk:
                clean_parents = Store.objects.filter(dirty_bit=False, profile=profile).filter(query).values_list("id", flat=True)
                dirty_children = Store.objects.filter(dirty_bit=True, profile=profile) \
                                              .filter(Q(_self_ref_fk__in=clean_parents) | Q(_self_ref_fk='')).filter(query)

                # keep iterating until size of dirty_children is 0
                while len(dirty_children) > 0:
                    for store_model in dirty_children:
                        store_model._deserialize_store_model()
                        # we update a store model after we have deserialized it
                        store_model.dirty_bit = False
                        store_model.save(update_fields=['dirty_bit'])

                    # update lists with new clean parents and dirty children
                    clean_parents = Store.objects.filter(dirty_bit=False, profile=profile).filter(query).values_list("id", flat=True)
                    dirty_children = Store.objects.filter(dirty_bit=True, profile=profile, _self_ref_fk__in=clean_parents).filter(query)
            else:
                for store_model in Store.objects.filter(model_name=model_name, profile=profile, dirty_bit=True):
                    store_model._deserialize_store_model()

        # clear dirty bit for all store models for this profile
        Store.objects.filter(profile=profile, dirty_bit=True).update(dirty_bit=False)

def _queue_into_buffer(transfersession):
    """
    Takes a chunk of data from the store to be put into the buffer to be sent to another morango instance.
    """
    last_saved_by_conditions = []
    filter_prefixes = Filter(transfersession.filter)
    server_fsic = json.loads(transfersession.server_fsic)
    client_fsic = json.loads(transfersession.client_fsic)

    if transfersession.push:
        fsics = _fsic_queuing_calc(client_fsic, server_fsic)
    else:
        fsics = _fsic_queuing_calc(server_fsic, client_fsic)

    # if fsics are identical or receiving end has newer data, then there is nothing to queue
    if not fsics:
        return

    # create condition for all push FSICs where instance_ids are equal, but internal counters are higher than FSICs counters
    for instance, counter in iteritems(fsics):
        last_saved_by_conditions += ["(last_saved_instance = '{0}' AND last_saved_counter > {1})".format(instance, counter)]
    if fsics:
        last_saved_by_conditions = [_join_with_logical_operator(last_saved_by_conditions, 'OR')]

    partition_conditions = []
    # create condition for filtering by partitions
    for prefix in filter_prefixes:
        partition_conditions += ["partition LIKE '{}%'".format(prefix)]
    if filter_prefixes:
        partition_conditions = [_join_with_logical_operator(partition_conditions, 'OR')]

    # combine conditions
    fsic_and_partition_conditions = _join_with_logical_operator(last_saved_by_conditions + partition_conditions, 'AND')

    # filter by profile
    where_condition = _join_with_logical_operator([fsic_and_partition_conditions, "profile = '{}'".format(transfersession.sync_session.profile)], 'AND')

    # execute raw sql to take all records that match condition, to be put into buffer for transfer
    with connection.cursor() as cursor:
        queue_buffer = """INSERT INTO {outgoing_buffer}
                        (model_uuid, serialized, deleted, last_saved_instance, last_saved_counter,
                         model_name, profile, partition, source_id, conflicting_serialized_data, transfer_session_id, _self_ref_fk)
                        SELECT id, serialized, deleted, last_saved_instance, last_saved_counter,
                        model_name, profile, partition, source_id, conflicting_serialized_data, '{transfer_session_id}', _self_ref_fk
                        FROM {store}
                        WHERE {condition}""".format(outgoing_buffer=Buffer._meta.db_table,
                                                    transfer_session_id=transfersession.id,
                                                    condition=where_condition,
                                                    store=Store._meta.db_table)
        cursor.execute(queue_buffer)
        # take all record max counters that are foreign keyed onto store models, which were queued into the buffer
        queue_rmc_buffer = """INSERT INTO {outgoing_rmcb}
                            (instance_id, counter, transfer_session_id, model_uuid)
                            SELECT instance_id, counter, '{transfer_session_id}', store_model_id
                            FROM {record_max_counter} AS rmc LEFT JOIN {outgoing_buffer} AS buffer
                            WHERE buffer.model_uuid = rmc.store_model_id
                            AND buffer.transfer_session_id = '{transfer_session_id}'
                            """.format(outgoing_rmcb=RecordMaxCounterBuffer._meta.db_table,
                                       transfer_session_id=transfersession.id,
                                       record_max_counter=RecordMaxCounter._meta.db_table,
                                       outgoing_buffer=Buffer._meta.db_table)
        cursor.execute(queue_rmc_buffer)

# START of dequeuing methods
def _dequeuing_delete_rmcb_records(cursor, transfersession_id):
    # delete all RMCBs which are a reverse FF (store version newer than buffer version)
    delete_rmcb_records = """DELETE FROM {rmcb}
                             WHERE model_uuid IN
                             (SELECT rmcb.model_uuid FROM {store} as store, {buffer} as buffer, {rmc} as rmc, {rmcb} as rmcb
                             /*Scope to a single record*/
                             WHERE store.id = buffer.model_uuid
                             AND  store.id = rmc.store_model_id
                             AND  store.id = rmcb.model_uuid
                             /*Checks whether LSB of buffer or less is in RMC of store*/
                             AND buffer.last_saved_instance = rmc.instance_id
                             AND buffer.last_saved_counter <= rmc.counter
                             AND rmcb.transfer_session_id = '{transfer_session_id}'
                             AND buffer.transfer_session_id = '{transfer_session_id}')
                              """.format(buffer=Buffer._meta.db_table,
                                         store=Store._meta.db_table,
                                         rmc=RecordMaxCounter._meta.db_table,
                                         rmcb=RecordMaxCounterBuffer._meta.db_table,
                                         transfer_session_id=transfersession_id)

    cursor.execute(delete_rmcb_records)

def _dequeuing_delete_buffered_records(cursor, transfersession_id):
    # delete all buffer records which are a reverse FF (store version newer than buffer version)
    delete_buffered_records = """DELETE FROM {buffer}
                                 WHERE model_uuid in
                                 (SELECT buffer.model_uuid FROM {store} as store, {buffer} as buffer, {rmc} as rmc
                                 /*Scope to a single record*/
                                 WHERE store.id = buffer.model_uuid
                                 AND rmc.store_model_id = buffer.model_uuid
                                 /*Checks whether LSB of buffer or less is in RMC of store*/
                                 AND buffer.last_saved_instance = rmc.instance_id
                                 AND buffer.last_saved_counter <= rmc.counter
                                 AND buffer.transfer_session_id = '{transfer_session_id}')
                              """.format(buffer=Buffer._meta.db_table,
                                         store=Store._meta.db_table,
                                         rmc=RecordMaxCounter._meta.db_table,
                                         rmcb=RecordMaxCounterBuffer._meta.db_table,
                                         transfer_session_id=transfersession_id)
    cursor.execute(delete_buffered_records)

def _dequeuing_merge_conflict_rmcb(cursor, transfersession_id):
    # transfer record max counters for records with merge conflicts + perform max
    merge_conflict_rmc = """REPLACE INTO {rmc} (instance_id, counter, store_model_id)
                                SELECT rmcb.instance_id, rmcb.counter, rmcb.model_uuid
                                FROM {rmcb} AS rmcb, {store} AS store, {rmc} AS rmc, {buffer} AS buffer
                                /*Scope to a single record.*/
                                WHERE store.id = rmcb.model_uuid
                                AND store.id = rmc.store_model_id
                                AND store.id = buffer.model_uuid
                                /*Where buffer rmc is greater than store rmc*/
                                AND rmcb.instance_id = rmc.instance_id
                                AND rmcb.counter > rmc.counter
                                AND rmcb.transfer_session_id = '{transfer_session_id}'
                                /*Exclude fast-forwards*/
                                AND NOT EXISTS (SELECT 1 FROM {rmcb} AS rmcb2 WHERE store.id = rmcb2.model_uuid
                                                                              AND store.last_saved_instance = rmcb2.instance_id
                                                                              AND store.last_saved_counter <= rmcb2.counter
                                                                              AND rmcb2.transfer_session_id = '{transfer_session_id}')
                           """.format(buffer=Buffer._meta.db_table,
                                      store=Store._meta.db_table,
                                      rmc=RecordMaxCounter._meta.db_table,
                                      rmcb=RecordMaxCounterBuffer._meta.db_table,
                                      transfer_session_id=transfersession_id)
    cursor.execute(merge_conflict_rmc)

def _dequeuing_merge_conflict_buffer(cursor, current_id, transfersession_id):
    # transfer buffer serialized into conflicting store
    merge_conflict_store = """REPLACE INTO {store} (id, serialized, deleted, last_saved_instance, last_saved_counter, model_name,
                                                    profile, partition, source_id, conflicting_serialized_data, dirty_bit, _self_ref_fk)
                                        SELECT store.id, store.serialized, store.deleted OR buffer.deleted, '{current_instance_id}',
                                               {current_instance_counter}, store.model_name, store.profile, store.partition, store.source_id,
                                               buffer.serialized || '\n' || store.conflicting_serialized_data, 1, store._self_ref_fk
                                        FROM {buffer} AS buffer, {store} AS store
                                        /*Scope to a single record.*/
                                        WHERE store.id = buffer.model_uuid
                                        AND buffer.transfer_session_id = '{transfer_session_id}'
                                        /*Exclude fast-forwards*/
                                        AND NOT EXISTS (SELECT 1 FROM {rmcb} AS rmcb2 WHERE store.id = rmcb2.model_uuid
                                                                                      AND store.last_saved_instance = rmcb2.instance_id
                                                                                      AND store.last_saved_counter <= rmcb2.counter
                                                                                      AND rmcb2.transfer_session_id = '{transfer_session_id}')
                                  """.format(buffer=Buffer._meta.db_table,
                                             rmcb=RecordMaxCounterBuffer._meta.db_table,
                                             store=Store._meta.db_table,
                                             rmc=RecordMaxCounter._meta.db_table,
                                             transfer_session_id=transfersession_id,
                                             current_instance_id=current_id.id,
                                             current_instance_counter=current_id.counter)
    cursor.execute(merge_conflict_store)

def _dequeuing_update_rmcs_last_saved_by(cursor, current_id, transfersession_id):
    # update or create rmc for merge conflicts with local instance id
    merge_conflict_store = """REPLACE INTO {rmc} (instance_id, counter, store_model_id)
                            SELECT '{current_instance_id}', {current_instance_counter}, store.id
                            FROM {store} as store, {buffer} as buffer
                            /*Scope to a single record.*/
                            WHERE store.id = buffer.model_uuid
                            AND buffer.transfer_session_id = '{transfer_session_id}'
                            /*Exclude fast-forwards*/
                            AND NOT EXISTS (SELECT 1 FROM {rmcb} AS rmcb2 WHERE store.id = rmcb2.model_uuid
                                                                          AND store.last_saved_instance = rmcb2.instance_id
                                                                          AND store.last_saved_counter <= rmcb2.counter
                                                                          AND rmcb2.transfer_session_id = '{transfer_session_id}')
                                  """.format(buffer=Buffer._meta.db_table,
                                             rmcb=RecordMaxCounterBuffer._meta.db_table,
                                             store=Store._meta.db_table,
                                             rmc=RecordMaxCounter._meta.db_table,
                                             transfer_session_id=transfersession_id,
                                             current_instance_id=current_id.id,
                                             current_instance_counter=current_id.counter)
    cursor.execute(merge_conflict_store)

def _dequeuing_delete_mc_buffer(cursor, transfersession_id):
    # delete records with merge conflicts from buffer
    delete_mc_buffer = """DELETE FROM {buffer}
                                WHERE EXISTS
                                (SELECT 1 FROM {store} AS store, {buffer} AS buffer
                                /*Scope to a single record.*/
                                WHERE store.id = {buffer}.model_uuid
                                AND {buffer}.transfer_session_id = '{transfer_session_id}'
                                /*Exclude fast-forwards*/
                                AND NOT EXISTS (SELECT 1 FROM {rmcb} AS rmcb2 WHERE store.id = rmcb2.model_uuid
                                                                              AND store.last_saved_instance = rmcb2.instance_id
                                                                              AND store.last_saved_counter <= rmcb2.counter
                                                                              AND rmcb2.transfer_session_id = '{transfer_session_id}'))
                           """.format(buffer=Buffer._meta.db_table,
                                      store=Store._meta.db_table,
                                      rmc=RecordMaxCounter._meta.db_table,
                                      rmcb=RecordMaxCounterBuffer._meta.db_table,
                                      transfer_session_id=transfersession_id)
    cursor.execute(delete_mc_buffer)

def _dequeuing_delete_mc_rmcb(cursor, transfersession_id):
    # delete rmcb records with merge conflicts
    delete_mc_rmc = """DELETE FROM {rmcb}
                                WHERE EXISTS
                                (SELECT 1 FROM {store} AS store, {rmc} AS rmc
                                /*Scope to a single record.*/
                                WHERE store.id = {rmcb}.model_uuid
                                AND store.id = rmc.store_model_id
                                /*Where buffer rmc is greater than store rmc*/
                                AND {rmcb}.instance_id = rmc.instance_id
                                AND {rmcb}.transfer_session_id = '{transfer_session_id}'
                                /*Exclude fast fast-forwards*/
                                AND NOT EXISTS (SELECT 1 FROM {rmcb} AS rmcb2 WHERE store.id = rmcb2.model_uuid
                                                                              AND store.last_saved_instance = rmcb2.instance_id
                                                                              AND store.last_saved_counter <= rmcb2.counter
                                                                              AND rmcb2.transfer_session_id = '{transfer_session_id}'))
                           """.format(buffer=Buffer._meta.db_table,
                                      store=Store._meta.db_table,
                                      rmc=RecordMaxCounter._meta.db_table,
                                      rmcb=RecordMaxCounterBuffer._meta.db_table,
                                      transfer_session_id=transfersession_id)
    cursor.execute(delete_mc_rmc)

def _dequeuing_insert_remaining_buffer(cursor, transfersession_id):
    # insert remaining records into store
    insert_remaining_buffer = """REPLACE INTO {store} (id, serialized, deleted, last_saved_instance, last_saved_counter,
                                                           model_name, profile, partition, source_id, conflicting_serialized_data, dirty_bit, _self_ref_fk)
                                SELECT buffer.model_uuid, buffer.serialized, buffer.deleted, buffer.last_saved_instance, buffer.last_saved_counter,
                                       buffer.model_name, buffer.profile, buffer.partition, buffer.source_id, buffer.conflicting_serialized_data, 1,
                                       buffer._self_ref_fk
                                FROM {buffer} AS buffer
                                WHERE buffer.transfer_session_id = '{transfer_session_id}'
                       """.format(buffer=Buffer._meta.db_table,
                                  store=Store._meta.db_table,
                                  transfer_session_id=transfersession_id)

    cursor.execute(insert_remaining_buffer)

def _dequeuing_insert_remaining_rmcb(cursor, transfersession_id):
    # insert remaining records into rmc
    insert_remaining_rmcb = """REPLACE INTO {rmc} (instance_id, counter, store_model_id)
                                SELECT rmcb.instance_id, rmcb.counter, rmcb.model_uuid
                                FROM {rmcb} AS rmcb
                                WHERE rmcb.transfer_session_id = '{transfer_session_id}'
                       """.format(rmc=RecordMaxCounter._meta.db_table,
                                  rmcb=RecordMaxCounterBuffer._meta.db_table,
                                  transfer_session_id=transfersession_id)

    cursor.execute(insert_remaining_rmcb)

def _dequeuing_delete_remaining_rmcb(cursor, transfersession_id):
    # delete the rest for this transfer session
    delete_remaining_rmcb = """DELETE FROM {rmcb}
                              WHERE {rmcb}.transfer_session_id = '{transfer_session_id}'
                           """.format(rmcb=RecordMaxCounterBuffer._meta.db_table,
                                      transfer_session_id=transfersession_id)

    cursor.execute(delete_remaining_rmcb)

def _dequeuing_delete_remaining_buffer(cursor, transfersession_id):
    delete_remaining_buffer = """DELETE FROM {buffer}
                             WHERE {buffer}.transfer_session_id = '{transfer_session_id}'
                          """.format(buffer=Buffer._meta.db_table,
                                     transfer_session_id=transfersession_id)

    cursor.execute(delete_remaining_buffer)

def _dequeue_into_store(transfersession):
    """
    Takes data from the buffers and merges into the store and record max counters.
    """
    with connection.cursor() as cursor:
        _dequeuing_delete_rmcb_records(cursor, transfersession.id)
        _dequeuing_delete_buffered_records(cursor, transfersession.id)
        current_id = InstanceIDModel.get_current_instance_and_increment_counter()
        _dequeuing_merge_conflict_buffer(cursor, current_id, transfersession.id)
        _dequeuing_merge_conflict_rmcb(cursor, transfersession.id)
        _dequeuing_update_rmcs_last_saved_by(cursor, current_id, transfersession.id)
        _dequeuing_delete_mc_rmcb(cursor, transfersession.id)
        _dequeuing_delete_mc_buffer(cursor, transfersession.id)
        _dequeuing_insert_remaining_buffer(cursor, transfersession.id)
        _dequeuing_insert_remaining_rmcb(cursor, transfersession.id)
        _dequeuing_delete_remaining_rmcb(cursor, transfersession.id)
        _dequeuing_delete_remaining_buffer(cursor, transfersession.id)
    if getattr(settings, 'MORANGO_DESERIALIZE_AFTER_DEQUEUING', True):
        _deserialize_from_store(transfersession.sync_session.profile)
