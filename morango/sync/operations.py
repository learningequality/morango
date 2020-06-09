import functools
import json
import logging

from django.conf import settings
from django.core import exceptions
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection
from django.db import connections
from django.db import router
from django.db import transaction
from django.db.models import Q
from django.db.models import signals
from django.utils import six

from .backends.utils import load_backend
from .utils import mute_signals
from morango.models.certificates import Filter
from morango.models.core import Buffer
from morango.models.core import DatabaseMaxCounter
from morango.models.core import DeletedModels
from morango.models.core import HardDeletedModels
from morango.models.core import InstanceIDModel
from morango.models.core import RecordMaxCounter
from morango.models.core import RecordMaxCounterBuffer
from morango.models.core import Store
from morango.registry import syncable_models


logger = logging.getLogger(__name__)

DBBackend = load_backend(connection).SQLWrapper()

# if postgres, get serializable db connection
db_name = router.db_for_write(Store)
USING_DB = db_name
if "postgresql" in transaction.get_connection(USING_DB).vendor:
    USING_DB = db_name + '-serializable'
    assert USING_DB in connections, "Please add a `default-serializable` database connection in your django settings file, \
                                     which copies all the configuration settings of the `default` db connection"

class OperationLogger(object):
    def __init__(self, start_msg, end_msg):
        self.start_msg = start_msg
        self.end_msg = end_msg

    def __enter__(self):
        logger.info(self.start_msg)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            logger.info(self.end_msg)
        else:
            logger.info("Error: {}".format(self.start_msg))


def _join_with_logical_operator(lst, operator):
    op = ") {operator} (".format(operator=operator)
    return "(({items}))".format(items=op.join(lst))


def _self_referential_fk(model):
    """
    Return whether this model has a self ref FK, and the name for the field
    """
    for f in model._meta.concrete_fields:
        if f.related_model:
            if issubclass(model, f.related_model):
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
    return {
        instance: fsic2.get(instance, 0)
        for instance, counter in six.iteritems(fsic1)
        if fsic2.get(instance, 0) < counter
    }


def _serialize_into_store(profile, filter=None):
    """
    Takes data from app layer and serializes the models into the store.
    ALGORITHM: On a per syncable model basis, we iterate through each class model and we go through 2 possible cases:
    1. If there is a store record pertaining to that app model, we update the serialized store record with
    the latest changes from the model's fields. We also update the counter's based on this device's current Instance ID.
    2. If there is no store record for this app model, we proceed to create an in memory store model and append to a list to be
    bulk created on a per class model basis.
    """
    # ensure that we write and retrieve the counter in one go for consistency
    current_id = InstanceIDModel.get_current_instance_and_increment_counter()

    with transaction.atomic(using=USING_DB):
        # create Q objects for filtering by prefixes
        prefix_condition = None
        if filter:
            prefix_condition = functools.reduce(
                lambda x, y: x | y,
                [Q(_morango_partition__startswith=prefix) for prefix in filter],
            )

        # filter through all models with the dirty bit turned on
        for model in syncable_models.get_models(profile):
            new_store_records = []
            new_rmc_records = []
            klass_queryset = model.objects.filter(_morango_dirty_bit=True)
            if prefix_condition:
                klass_queryset = klass_queryset.filter(prefix_condition)
            store_records_dict = Store.objects.in_bulk(
                id_list=klass_queryset.values_list("id", flat=True)
            )
            for app_model in klass_queryset:
                try:
                    store_model = store_records_dict[app_model.id]

                    # if store record dirty and app record dirty, append store serialized to conflicting data
                    if store_model.dirty_bit:
                        store_model.conflicting_serialized_data = (
                            store_model.serialized
                            + "\n"
                            + store_model.conflicting_serialized_data
                        )
                        store_model.dirty_bit = False

                    # set new serialized data on this store model
                    ser_dict = json.loads(store_model.serialized)
                    ser_dict.update(app_model.serialize())
                    store_model.serialized = DjangoJSONEncoder().encode(ser_dict)

                    # create or update instance and counter on the record max counter for this store model
                    RecordMaxCounter.objects.update_or_create(
                        defaults={"counter": current_id.counter},
                        instance_id=current_id.id,
                        store_model_id=store_model.id,
                    )

                    # update last saved bys for this store model
                    store_model.last_saved_instance = current_id.id
                    store_model.last_saved_counter = current_id.counter
                    # update deleted flags in case it was previously deleted
                    store_model.deleted = False
                    store_model.hard_deleted = False

                    # update this model
                    store_model.save()

                except KeyError:
                    kwargs = {
                        "id": app_model.id,
                        "serialized": DjangoJSONEncoder().encode(app_model.serialize()),
                        "last_saved_instance": current_id.id,
                        "last_saved_counter": current_id.counter,
                        "model_name": app_model.morango_model_name,
                        "profile": app_model.morango_profile,
                        "partition": app_model._morango_partition,
                        "source_id": app_model._morango_source_id,
                    }
                    # check if model has FK pointing to it and add the value to a field on the store
                    self_ref_fk = _self_referential_fk(model)
                    if self_ref_fk:
                        self_ref_fk_value = getattr(app_model, self_ref_fk)
                        kwargs.update({"_self_ref_fk": self_ref_fk_value or ""})
                    # create store model and record max counter for the app model
                    new_store_records.append(Store(**kwargs))
                    new_rmc_records.append(
                        RecordMaxCounter(
                            store_model_id=app_model.id,
                            instance_id=current_id.id,
                            counter=current_id.counter,
                        )
                    )

            # bulk create store and rmc records for this class
            Store.objects.bulk_create(new_store_records)
            RecordMaxCounter.objects.bulk_create(new_rmc_records)

            # set dirty bit to false for all instances of this model
            klass_queryset.update(update_dirty_bit_to=False)

        # get list of ids of deleted models
        deleted_ids = DeletedModels.objects.filter(profile=profile).values_list(
            "id", flat=True
        )
        # update last_saved_bys and deleted flag of all deleted store model instances
        deleted_store_records = Store.objects.filter(id__in=deleted_ids)
        deleted_store_records.update(
            dirty_bit=False,
            deleted=True,
            last_saved_instance=current_id.id,
            last_saved_counter=current_id.counter,
        )
        # update rmcs counters for deleted models that have our instance id
        RecordMaxCounter.objects.filter(
            instance_id=current_id.id, store_model_id__in=deleted_ids
        ).update(counter=current_id.counter)
        # get a list of deleted model ids that don't have an rmc for our instance id
        new_rmc_ids = deleted_store_records.exclude(
            recordmaxcounter__instance_id=current_id.id
        ).values_list("id", flat=True)
        # bulk create these new rmcs
        RecordMaxCounter.objects.bulk_create(
            [
                RecordMaxCounter(
                    store_model_id=r_id,
                    instance_id=current_id.id,
                    counter=current_id.counter,
                )
                for r_id in new_rmc_ids
            ]
        )
        # clear deleted models table for this profile
        DeletedModels.objects.filter(profile=profile).delete()

        # handle logic for hard deletion models
        hard_deleted_ids = HardDeletedModels.objects.filter(
            profile=profile
        ).values_list("id", flat=True)
        hard_deleted_store_records = Store.objects.filter(id__in=hard_deleted_ids)
        hard_deleted_store_records.update(
            hard_deleted=True, serialized="{}", conflicting_serialized_data=""
        )
        HardDeletedModels.objects.filter(profile=profile).delete()

        # update our own database max counters after serialization
        if not filter:
            DatabaseMaxCounter.objects.update_or_create(
                instance_id=current_id.id,
                partition="",
                defaults={"counter": current_id.counter},
            )
        else:
            for f in filter:
                DatabaseMaxCounter.objects.update_or_create(
                    instance_id=current_id.id,
                    partition=f,
                    defaults={"counter": current_id.counter},
                )


def _deserialize_from_store(profile):
    """
    Takes data from the store and integrates into the application.
    ALGORITHM: On a per syncable model basis, we iterate through each class model and we go through 2 possible cases:
    1. For class models that have a self referential foreign key, we iterate down the dependency tree deserializing model by model.
    2. On a per app model basis, we append the field values to a single list, and do a single bulk insert/replace query.
    If a model fails to deserialize/validate, we exclude it from being marked as clean in the store.
    """
    # we first serialize to avoid deserialization merge conflicts
    _serialize_into_store(profile)

    fk_cache = {}
    with transaction.atomic(using=USING_DB):
        excluded_list = []
        # iterate through classes which are in foreign key dependency order
        for model in syncable_models.get_models(profile):
            # handle cases where a class has a single FK reference to itself
            self_ref_fk = _self_referential_fk(model)
            query = Q(model_name=model.morango_model_name)
            # this will handle the case of getting the clean parents, since they will be deserialized first
            for klass in model.morango_model_dependencies:
                query |= Q(model_name=klass.morango_model_name)
            if self_ref_fk:
                clean_parents = (
                    Store.objects.filter(dirty_bit=False, profile=profile)
                    .filter(query)
                    .char_ids_list()
                )
                dirty_children = (
                    Store.objects.filter(dirty_bit=True, profile=profile)
                    # handle parents or if the model has no parent
                    .filter(
                        Q(_self_ref_fk__in=clean_parents) | Q(_self_ref_fk="")
                    ).filter(query)
                )

                # keep iterating until size of dirty_children is 0
                while len(dirty_children) > 0:
                    for store_model in dirty_children:
                        try:
                            app_model = store_model._deserialize_store_model(fk_cache)
                            if app_model:
                                with mute_signals(signals.pre_save, signals.post_save):
                                    app_model.save(update_dirty_bit_to=False)
                            # we update a store model after we have deserialized it to be able to mark it as a clean parent
                            store_model.dirty_bit = False
                            store_model.save(update_fields=["dirty_bit"])
                        except exceptions.ValidationError:
                            # if the app model did not validate, we leave the store dirty bit set
                            excluded_list.append(store_model.id)

                    # update lists with new clean parents and dirty children
                    clean_parents = (
                        Store.objects.filter(dirty_bit=False, profile=profile)
                        .filter(query)
                        .char_ids_list()
                    )
                    dirty_children = Store.objects.filter(
                        dirty_bit=True, profile=profile, _self_ref_fk__in=clean_parents
                    ).filter(query)
            else:
                # array for holding db values from the fields of each model for this class
                db_values = []
                fields = model._meta.fields
                for store_model in Store.objects.filter(
                    model_name=model.morango_model_name, profile=profile, dirty_bit=True
                ):
                    try:
                        app_model = store_model._deserialize_store_model(fk_cache)
                        # if the model was not deleted add its field values to the list
                        if app_model:
                            for f in fields:
                                value = getattr(app_model, f.attname)
                                db_value = f.get_db_prep_value(value, connection)
                                db_values.append(db_value)
                    except exceptions.ValidationError:
                        # if the app model did not validate, we leave the store dirty bit set
                        excluded_list.append(store_model.id)

                if db_values:
                    # number of rows to update
                    num_of_rows = len(db_values) // len(fields)
                    # create '%s' placeholders for a single row
                    placeholder_tuple = tuple(["%s" for _ in range(len(fields))])
                    # create list of the '%s' tuple placeholders based on number of rows to update
                    placeholder_list = [
                        str(placeholder_tuple) for _ in range(num_of_rows)
                    ]
                    with connection.cursor() as cursor:
                        DBBackend._bulk_insert_into_app_models(
                            cursor,
                            model._meta.db_table,
                            fields,
                            db_values,
                            placeholder_list,
                        )

        # clear dirty bit for all store models for this profile except for models that did not validate
        Store.objects.exclude(id__in=excluded_list).filter(
            profile=profile, dirty_bit=True
        ).update(dirty_bit=False)


@transaction.atomic(using=USING_DB)
def _queue_into_buffer(transfersession):
    """
    Takes a chunk of data from the store to be put into the buffer to be sent to another morango instance.
    ALGORITHM: We do Filter Specific Instance Counter arithmetic to get our newest data compared to the server's older data.
    We use raw sql queries to place data in the buffer and the record max counter buffer, which matches the conditions of the FSIC,
    as well as the partition for the data we are syncing.
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
    for instance, counter in six.iteritems(fsics):
        last_saved_by_conditions += [
            "(last_saved_instance = '{0}' AND last_saved_counter > {1})".format(
                instance, counter
            )
        ]
    if fsics:
        last_saved_by_conditions = [
            _join_with_logical_operator(last_saved_by_conditions, "OR")
        ]

    partition_conditions = []
    # create condition for filtering by partitions
    for prefix in filter_prefixes:
        partition_conditions += ["partition LIKE '{}%'".format(prefix)]
    if filter_prefixes:
        partition_conditions = [_join_with_logical_operator(partition_conditions, "OR")]

    # combine conditions
    fsic_and_partition_conditions = _join_with_logical_operator(
        last_saved_by_conditions + partition_conditions, "AND"
    )

    # filter by profile
    where_condition = _join_with_logical_operator(
        [
            fsic_and_partition_conditions,
            "profile = '{}'".format(transfersession.sync_session.profile),
        ],
        "AND",
    )

    # execute raw sql to take all records that match condition, to be put into buffer for transfer
    with connection.cursor() as cursor:
        queue_buffer = """INSERT INTO {outgoing_buffer}
                        (model_uuid, serialized, deleted, last_saved_instance, last_saved_counter, hard_deleted,
                         model_name, profile, partition, source_id, conflicting_serialized_data, transfer_session_id, _self_ref_fk)
                        SELECT id, serialized, deleted, last_saved_instance, last_saved_counter, hard_deleted, model_name, profile, partition, source_id, conflicting_serialized_data, '{transfer_session_id}', _self_ref_fk
                        FROM {store} WHERE {condition}""".format(
            outgoing_buffer=Buffer._meta.db_table,
            transfer_session_id=transfersession.id,
            condition=where_condition,
            store=Store._meta.db_table,
        )
        cursor.execute(queue_buffer)
        # take all record max counters that are foreign keyed onto store models, which were queued into the buffer
        queue_rmc_buffer = """INSERT INTO {outgoing_rmcb}
                            (instance_id, counter, transfer_session_id, model_uuid)
                            SELECT instance_id, counter, '{transfer_session_id}', store_model_id
                            FROM {record_max_counter} AS rmc
                            INNER JOIN {outgoing_buffer} AS buffer ON rmc.store_model_id = buffer.model_uuid
                            WHERE buffer.transfer_session_id = '{transfer_session_id}'
                            """.format(
            outgoing_rmcb=RecordMaxCounterBuffer._meta.db_table,
            transfer_session_id=transfersession.id,
            record_max_counter=RecordMaxCounter._meta.db_table,
            outgoing_buffer=Buffer._meta.db_table,
        )
        cursor.execute(queue_rmc_buffer)


@transaction.atomic(using=USING_DB)
def _dequeue_into_store(transfersession):
    """
    Takes data from the buffers and merges into the store and record max counters.
    ALGORITHM: Incrementally insert and delete on a case by case basis to ensure subsequent cases
    are not effected by previous cases.
    """
    with connection.cursor() as cursor:
        DBBackend._dequeuing_delete_rmcb_records(cursor, transfersession.id)
        DBBackend._dequeuing_delete_buffered_records(cursor, transfersession.id)
        current_id = InstanceIDModel.get_current_instance_and_increment_counter()
        DBBackend._dequeuing_merge_conflict_buffer(
            cursor, current_id, transfersession.id
        )
        DBBackend._dequeuing_merge_conflict_rmcb(cursor, transfersession.id)
        DBBackend._dequeuing_update_rmcs_last_saved_by(
            cursor, current_id, transfersession.id
        )
        DBBackend._dequeuing_delete_mc_rmcb(cursor, transfersession.id)
        DBBackend._dequeuing_delete_mc_buffer(cursor, transfersession.id)
        DBBackend._dequeuing_insert_remaining_buffer(cursor, transfersession.id)
        DBBackend._dequeuing_insert_remaining_rmcb(cursor, transfersession.id)
        DBBackend._dequeuing_delete_remaining_rmcb(cursor, transfersession.id)
        DBBackend._dequeuing_delete_remaining_buffer(cursor, transfersession.id)
    if getattr(settings, "MORANGO_DESERIALIZE_AFTER_DEQUEUING", True):
        _deserialize_from_store(transfersession.sync_session.profile)
