import functools
import json
import logging
import uuid

from django.core import exceptions
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection
from django.db import connections
from django.db import router
from django.db import transaction
from django.db.models import Q
from django.db.models import signals
from django.utils import six
from django.utils import timezone
from rest_framework.exceptions import ValidationError

from morango.api.serializers import BufferSerializer
from morango.constants.capabilities import ASYNC_OPERATIONS
from morango.constants import transfer_stages
from morango.constants import transfer_statuses
from morango.errors import MorangoResumeSyncError
from morango.errors import MorangoLimitExceeded
from morango.models.certificates import Filter
from morango.models.core import Buffer
from morango.models.core import DatabaseMaxCounter
from morango.models.core import DeletedModels
from morango.models.core import HardDeletedModels
from morango.models.core import InstanceIDModel
from morango.models.core import RecordMaxCounter
from morango.models.core import RecordMaxCounterBuffer
from morango.models.core import Store
from morango.models.core import TransferSession
from morango.registry import syncable_models
from morango.sync.backends.utils import load_backend
from morango.sync.context import LocalSessionContext
from morango.sync.context import NetworkSessionContext
from morango.sync.utils import mute_signals
from morango.sync.utils import validate_and_create_buffer_data
from morango.utils import _assert
from morango.utils import SETTINGS


logger = logging.getLogger(__name__)

DBBackend = load_backend(connection).SQLWrapper()

# if postgres, get serializable db connection
db_name = router.db_for_write(Store)
USING_DB = db_name
if "postgresql" in transaction.get_connection(USING_DB).vendor:
    USING_DB = db_name + "-serializable"
    _assert(
        USING_DB in connections,
        "Please add a `default-serializable` database connection in your django settings file, \
            which copies all the configuration settings of the `default` db connection",
    )

SQL_UNION_MAX = 500


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
                    # clear last_transfer_session_id
                    store_model.last_transfer_session_id = None

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


def _deserialize_from_store(profile, skip_erroring=False, filter=None):
    """
    Takes data from the store and integrates into the application.

    ALGORITHM: On a per syncable model basis, we iterate through each class model and we go through 2 possible cases:

    1. For class models that have a self referential foreign key, we iterate down the dependency tree deserializing model by model.
    2. On a per app model basis, we append the field values to a single list, and do a single bulk insert/replace query.

    If a model fails to deserialize/validate, we exclude it from being marked as clean in the store.
    """

    fk_cache = {}
    with transaction.atomic(using=USING_DB):
        excluded_list = []
        # iterate through classes which are in foreign key dependency order
        for model in syncable_models.get_models(profile):

            store_models = Store.objects.filter(profile=profile)

            model_condition = Q(model_name=model.morango_model_name)
            for klass in model.morango_model_dependencies:
                model_condition |= Q(model_name=klass.morango_model_name)

            store_models = store_models.filter(model_condition)

            if filter:
                # create Q objects for filtering by prefixes
                prefix_condition = functools.reduce(
                    lambda x, y: x | y,
                    [Q(partition__startswith=prefix) for prefix in filter],
                )
                store_models = store_models.filter(prefix_condition)

            # if requested, skip any records that previously errored, to be faster
            if skip_erroring:
                store_models = store_models.filter(deserialization_error="")

            # handle cases where a class has a single FK reference to itself
            if _self_referential_fk(model):
                clean_parents = store_models.filter(dirty_bit=False).char_ids_list()
                dirty_children = (
                    store_models.filter(dirty_bit=True)
                    # handle parents or if the model has no parent
                    .filter(Q(_self_ref_fk__in=clean_parents) | Q(_self_ref_fk=""))
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
                            store_model.deserialization_error = ""
                            store_model.save(
                                update_fields=["dirty_bit", "deserialization_error"]
                            )
                        except (
                            exceptions.ValidationError,
                            exceptions.ObjectDoesNotExist,
                        ) as e:
                            excluded_list.append(store_model.id)
                            # if the app model did not validate, we leave the store dirty bit set, but mark the error
                            store_model.deserialization_error = str(e)
                            store_model.save(update_fields=["deserialization_error"])

                    # update lists with new clean parents and dirty children
                    clean_parents = store_models.filter(dirty_bit=False).char_ids_list()
                    dirty_children = store_models.filter(
                        dirty_bit=True, _self_ref_fk__in=clean_parents
                    ).exclude(id__in=excluded_list)

                # A. Mark records that were skipped due to missing parents with error info
                # A(i). The ones that have a parent Store entry but it's dirty
                dirty_parents = store_models.filter(dirty_bit=True).char_ids_list()
                store_models.filter(
                    dirty_bit=True, _self_ref_fk__in=dirty_parents
                ).exclude(id__in=excluded_list).update(
                    deserialization_error="Parent is dirty; could not deserialize."
                )
                # A(ii). The ones that don't even have Store entries for parent at all
                all_parents = store_models.char_ids_list()
                store_models.filter(dirty_bit=True).exclude(
                    _self_ref_fk__in=all_parents
                ).exclude(id__in=excluded_list).update(
                    deserialization_error="Parent does not exist in Store; could not deserialize."
                )

            else:
                # array for holding db values from the fields of each model for this class
                db_values = []
                fields = model._meta.fields
                for store_model in store_models.filter(dirty_bit=True):
                    try:
                        app_model = store_model._deserialize_store_model(fk_cache)
                        # if the model was not deleted add its field values to the list
                        if app_model:
                            for f in fields:
                                value = getattr(app_model, f.attname)
                                db_value = f.get_db_prep_value(value, connection)
                                db_values.append(db_value)
                    except (
                        exceptions.ValidationError,
                        exceptions.ObjectDoesNotExist,
                    ) as e:
                        # if the app model did not validate, we leave the store dirty bit set
                        excluded_list.append(store_model.id)
                        store_model.deserialization_error = str(e)
                        store_model.save(update_fields=["deserialization_error"])

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

                # clear dirty bit for all store records for this model/profile except for rows that did not validate
                store_models.exclude(id__in=excluded_list).filter(
                    dirty_bit=True
                ).update(dirty_bit=False)


@transaction.atomic(using=USING_DB)
def _queue_into_buffer(transfersession):
    """
    Takes a chunk of data from the store to be put into the buffer to be sent to another morango instance.

    ALGORITHM: We do Filter Specific Instance Counter arithmetic to get our newest data compared to the server's older data.
    We use raw sql queries to place data in the buffer and the record max counter buffer, which matches the conditions of the FSIC,
    as well as the partition for the data we are syncing.
    """
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

    profile_condition = ["profile = '{}'".format(transfersession.sync_session.profile)]
    partition_conditions = []
    # create condition for filtering by partitions
    for prefix in filter_prefixes:
        partition_conditions += ["partition LIKE '{}%'".format(prefix)]
    if filter_prefixes:
        partition_conditions = [_join_with_logical_operator(partition_conditions, "OR")]

    chunk_size = 200
    fsics = list(fsics.items())
    fsics_len = len(fsics)
    fsics_limit = chunk_size * SQL_UNION_MAX

    if fsics_len >= fsics_limit:
        raise MorangoLimitExceeded(
            "Limit of {limit} instance counters exceeded with {actual}".format(
                limit=fsics_limit, actual=fsics_len
            )
        )

    # chunk fsics creating multiple SQL selects which will be unioned before insert
    i = 0
    chunk = fsics[:chunk_size]
    select_buffers = []
    select_rmc_buffers = []

    while chunk:
        # create condition for all push FSICs where instance_ids are equal, but internal counters are higher than
        # FSICs counters
        last_saved_by_conditions = [
            "(last_saved_instance = '{0}' AND last_saved_counter > {1})".format(
                instance, counter
            )
            for instance, counter in chunk
        ]
        if last_saved_by_conditions:
            last_saved_by_conditions = [
                _join_with_logical_operator(last_saved_by_conditions, "OR")
            ]

        # combine conditions and filter by profile
        where_condition = _join_with_logical_operator(
            profile_condition + last_saved_by_conditions + partition_conditions, "AND"
        )

        # execute raw sql to take all records that match condition, to be put into buffer for transfer
        select_buffers.append(
            """SELECT
                   id, serialized, deleted, last_saved_instance, last_saved_counter, hard_deleted, model_name, profile,
                   partition, source_id, conflicting_serialized_data,
                   CAST ('{transfer_session_id}' AS {transfer_session_id_type}), _self_ref_fk
               FROM {store} WHERE {condition}
            """.format(
                transfer_session_id=transfersession.id,
                transfer_session_id_type=TransferSession._meta.pk.rel_db_type(
                    connection
                ),
                condition=where_condition,
                store=Store._meta.db_table,
            )
        )
        # take all record max counters that are foreign keyed onto store models, which were queued into the buffer
        select_rmc_buffers.append(
            """SELECT instance_id, counter, CAST ('{transfer_session_id}' AS {transfer_session_id_type}), store_model_id
               FROM {record_max_counter} AS rmc
               INNER JOIN {outgoing_buffer} AS buffer ON rmc.store_model_id = buffer.model_uuid
               WHERE buffer.transfer_session_id = '{transfer_session_id}'
            """.format(
                transfer_session_id=transfersession.id,
                transfer_session_id_type=TransferSession._meta.pk.rel_db_type(
                    connection
                ),
                record_max_counter=RecordMaxCounter._meta.db_table,
                outgoing_buffer=Buffer._meta.db_table,
            )
        )
        i += chunk_size
        chunk = fsics[i : i + chunk_size]

    with connection.cursor() as cursor:
        cursor.execute(
            """INSERT INTO {outgoing_buffer}
               (model_uuid, serialized, deleted, last_saved_instance, last_saved_counter,
               hard_deleted, model_name, profile, partition, source_id, conflicting_serialized_data,
               transfer_session_id, _self_ref_fk)
               {select}
            """.format(
                outgoing_buffer=Buffer._meta.db_table,
                select=" UNION ".join(select_buffers),
            )
        )
        cursor.execute(
            """INSERT INTO {outgoing_rmcb}
               (instance_id, counter, transfer_session_id, model_uuid)
               {select}
            """.format(
                outgoing_rmcb=RecordMaxCounterBuffer._meta.db_table,
                select=" UNION ".join(select_rmc_buffers),
            )
        )


@transaction.atomic(using=USING_DB)
def _dequeue_into_store(transfersession):
    """
    Takes data from the buffers and merges into the store and record max counters.

    ALGORITHM: Incrementally insert and delete on a case by case basis to ensure subsequent cases
    are not affected by previous cases.
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


class BaseOperation(object):
    """
    Base Operation class which defines operation specific behavior that occurs during a sync
    """

    __slots__ = ()
    expects_context = None
    """Operation will automatically filter contexts based on this type, before calling `handle`"""

    def __call__(self, context):
        """
        :type context: morango.sync.controller.SessionContext
        """
        debug_msg = "[morango:{}] {} -> {}".format(
            "pull" if context.is_pull else "push",
            context.__class__.__name__,
            self.__class__.__name__,
        )
        result = False
        try:
            # verify context object matches what the operation expects
            if self.expects_context is None or isinstance(
                context, self.expects_context
            ):
                logger.debug("{} = ?".format(debug_msg))
                result = self.handle(context)

            if result is not False and result not in transfer_statuses.ALL:
                raise NotImplementedError(
                    "Transfer operation must return False, or a transfer status"
                )
        except AssertionError:
            # if the operation raises an AssertionError, we equate that to returning False, which
            # means that this operation did not handle it and so other operation instances should
            # be tried to handle it
            result = False
        logger.debug("{} = {}".format(debug_msg, result))
        return result

    def handle(self, context):
        """
        :type context: morango.sync.context.SessionContext
        :return: transfer_status.* - See `SessionController` for how the return status is handled
        """
        raise NotImplementedError("Transfer operation handler not implemented")

    def _assert(self, condition, message="Operation does not handle this condition"):
        """
        :param condition: a bool condition, if false will raise assertion error
        """
        _assert(condition, message)


class LocalOperation(BaseOperation):
    """
    Base class for local operations that expect a local context object
    """

    expects_context = LocalSessionContext


class InitializeOperation(LocalOperation):
    """
    Operation to initialize the transfer session in the local instance
    """

    def handle(self, context):
        """
        :type context: LocalSessionContext
        """
        self._assert(context.transfer_session is None)

        # attributes that we'll use to identify existing sessions. we really only want there to
        # be one of these at a time
        data = dict(
            push=context.is_push, sync_session_id=context.sync_session.id, active=True
        )

        # get the most recent transfer session
        transfer_sessions = TransferSession.objects.order_by("-last_activity_timestamp")

        try:
            transfer_session = transfer_sessions.get(**data)
        except TransferSession.DoesNotExist:
            # build data for creating transfer session
            data.update(
                id=uuid.uuid4().hex,
                filter=str(context.filter),
                start_timestamp=timezone.now(),
                last_activity_timestamp=timezone.now(),
                active=True,
                transfer_stage=transfer_stages.INITIALIZING,
            )
            # if in server context, we'll have request
            if context.request:
                data.update(
                    id=context.request.data.get("id"),
                    records_total=context.request.data.get("records_total")
                    if context.is_push
                    else None,
                    client_fsic=context.request.data.get("client_fsic") or "{}",
                )
            elif context.is_server:
                raise MorangoResumeSyncError(
                    "Cannot create transfer session without request as server"
                )

            # create, validate, and save!
            transfer_session = TransferSession(**data)
            transfer_session.full_clean()
            transfer_session.save()

        # if resuming, this should also update the context such that the next attempted stage
        # is the next stage to invoke
        context.update(transfer_session=transfer_session)
        return transfer_statuses.COMPLETED


class SerializeOperation(LocalOperation):
    """
    Performs serialization related steps which affect the counters involved in the sync, including
    serialization of data if enabled through configuration
    """

    def handle(self, context):
        """
        :type context: LocalSessionContext
        """
        self._assert(context.sync_session is not None)
        self._assert(context.filter is not None)

        if context.is_producer and SETTINGS.MORANGO_SERIALIZE_BEFORE_QUEUING:
            _serialize_into_store(context.sync_session.profile, filter=context.filter)

        fsic = json.dumps(
            DatabaseMaxCounter.calculate_filter_specific_instance_counters(
                context.filter, is_producer=context.is_producer
            )
        )
        if context.is_server:
            context.transfer_session.server_fsic = fsic
            context.transfer_session.client_fsic = context.request.data.get(
                "client_fsic", "{}"
            )
        else:
            context.transfer_session.client_fsic = fsic
        context.transfer_session.save()
        return transfer_statuses.COMPLETED


class ProducerQueueOperation(LocalOperation):
    """
    Performs queuing of data for as local instance
    """

    def handle(self, context):
        """
        :type context: LocalSessionContext
        """
        self._assert(context.is_producer)
        self._assert(context.sync_session is not None)
        self._assert(context.transfer_session is not None)

        _queue_into_buffer(context.transfer_session)

        # update the records_total for client and server transfer session
        records_total = Buffer.objects.filter(
            transfer_session=context.transfer_session
        ).count()

        logger.debug("[morango] Queued {} records".format(records_total))
        context.transfer_session.records_total = records_total
        context.transfer_session.save()
        return transfer_statuses.COMPLETED


class ReceiverQueueOperation(LocalOperation):
    """
    Receiver of transfer data does not need to queue anything
    """

    def handle(self, context):
        """
        :type context: LocalSessionContext
        """
        self._assert(context.is_receiver)
        # TODO: move updating record counts from request to here instead of viewset serializer
        return transfer_statuses.COMPLETED


class PullProducerOperation(LocalOperation):
    """
    Operation that handles the transfer session updates for the server during a pull
    """

    def handle(self, context):
        """
        :type context: LocalSessionContext
        """
        self._assert(context.is_pull)
        self._assert(context.is_producer)
        self._assert(context.request is not None)

        records_transferred = context.request.data.get(
            "records_transferred", context.transfer_session.records_transferred
        )

        if records_transferred == context.transfer_session.records_total:
            return transfer_statuses.COMPLETED
        return transfer_statuses.PENDING


class PushReceiverOperation(LocalOperation):
    """
    Operation that handles the result of a push, as the server, using a local context / session
    """

    def handle(self, context):
        """
        :type context: LocalSessionContext
        """
        self._assert(context.is_push)
        self._assert(context.is_receiver)
        self._assert(context.request is not None)

        # operation can be invoked even though there's nothing to transfer
        if context.transfer_session.records_total > 0:
            data = context.request.data
            if not isinstance(context.request.data, list):
                data = [context.request.data]

            validate_and_create_buffer_data(data, context.transfer_session)

        if (
            context.transfer_session.records_transferred
            == context.transfer_session.records_total
        ):
            return transfer_statuses.COMPLETED
        return transfer_statuses.PENDING


class ProducerDequeueOperation(LocalOperation):
    """
    Producers of transfer data do not need to dequeue
    """

    def handle(self, context):
        """
        :type context: LocalSessionContext
        """
        self._assert(context.is_producer)
        return transfer_statuses.COMPLETED


class ReceiverDequeueOperation(LocalOperation):
    """
    Performs dequeuing of transferred data for receiver contexts
    """

    def handle(self, context):
        """
        :type context: LocalSessionContext
        """
        self._assert(context.is_receiver)
        self._assert(context.transfer_session is not None)
        self._assert(context.filter is not None)

        # if no records were transferred, we can safely skip
        records_transferred = context.transfer_session.records_transferred or 0
        if records_transferred > 0:
            _dequeue_into_store(context.transfer_session)

        return transfer_statuses.COMPLETED


class ProducerDeserializeOperation(LocalOperation):
    """
    Producers of transfer data do not need to deserialize
    """

    def handle(self, context):
        """
        :type context: LocalSessionContext
        """
        self._assert(context.is_producer)
        return transfer_statuses.COMPLETED


class ReceiverDeserializeOperation(LocalOperation):
    """
    Performs deserialization if enabled through configuration and if applicable for local transfer
    session
    """

    def handle(self, context):
        """
        :type context: LocalSessionContext
        """
        self._assert(context.sync_session is not None)
        self._assert(context.transfer_session is not None)
        self._assert(context.filter is not None)

        records_transferred = context.transfer_session.records_transferred or 0
        if SETTINGS.MORANGO_DESERIALIZE_AFTER_DEQUEUING and records_transferred > 0:
            # we first serialize to avoid deserialization merge conflicts
            _serialize_into_store(context.sync_session.profile, filter=context.filter)
            _deserialize_from_store(context.sync_session.profile, filter=context.filter)

        # update database max counters but use latest fsics from client/server
        if context.is_receiver:
            fsic = (
                context.transfer_session.client_fsic
                if context.is_server
                else context.transfer_session.server_fsic
            )
            DatabaseMaxCounter.update_fsics(json.loads(fsic), context.filter)

        return transfer_statuses.COMPLETED


class CleanupOperation(LocalOperation):
    """
    Marks the local transfer session as inactive, and deletes queued buffer data if applicable
    """

    def handle(self, context):
        """
        :type context: LocalSessionContext
        """
        self._assert(context.transfer_session is not None)

        if context.is_producer:
            context.transfer_session.delete_buffers()

        context.transfer_session.active = False
        context.transfer_session.save()
        return transfer_statuses.COMPLETED


class NetworkOperation(BaseOperation):
    expects_context = NetworkSessionContext

    def create_transfer_session(self, context):
        """
        :type context: NetworkSessionContext
        :return: A response dict
        """
        return context.connection._create_transfer_session(
            dict(
                id=context.transfer_session.id,
                filter=context.transfer_session.filter,
                push=context.transfer_session.push,
                sync_session_id=context.sync_session.id,
                client_fsic=context.transfer_session.client_fsic,
            )
        ).json()

    def get_transfer_session(self, context):
        """
        Retrieves remote transfer session

        :type context: NetworkSessionContext
        :return: A response dict
        """
        return context.connection._get_transfer_session(context.transfer_session).json()

    def update_transfer_session(self, context, **data):
        """
        Updates remote transfer session

        :type context: NetworkSessionContext
        :param data: Data to update remote transfer session wiht
        :return: A response dict
        """
        return context.connection._update_transfer_session(
            data, context.transfer_session
        ).json()

    def close_transfer_session(self, context):
        """
        Closes remote transfer session

        :type context: NetworkSessionContext
        :return: The Response
        """
        return context.connection._close_transfer_session(context.transfer_session)

    def put_buffers(self, context, buffers):
        """
        :type context: NetworkSessionContext
        :param buffers: List of serialized Buffer dicts
        :return: The response
        """
        return context.connection._push_record_chunk(buffers)

    def get_buffers(self, context):
        """
        Pulls a single chunk of buffers from the remote server and does some validation

        :type context: NetworkSessionContext
        :return: A list of dicts, serialized Buffers
        """
        response = context.connection._pull_record_chunk(context.transfer_session)

        data = response.json()

        # parse out the results from a paginated set, if needed
        if isinstance(data, dict) and "results" in data:
            data = data["results"]

        # no buffers?
        if len(data) == 0:
            return data

        # ensure the transfer session allows pulls, and is same across records
        transfer_session = TransferSession.objects.get(id=data[0]["transfer_session"])
        if transfer_session.push:
            raise ValidationError("Specified TransferSession does not allow pulling.")

        if len(set(rec["transfer_session"] for rec in data)) > 1:
            raise ValidationError(
                "All pulled records must be associated with the same TransferSession."
            )

        if context.transfer_session.id != transfer_session.id:
            raise ValidationError(
                "Specified TransferSession does not match this SyncClient's current TransferSession."
            )
        return data

    def remote_proceed_to(self, context, stage, **kwargs):
        """
        Uses server API's to push updates to a remote `TransferSession`, which triggers the
        controller's `.proceed_to()` for the stage

        :type context: NetworkSessionContext
        :param stage: A transfer_stage.*
        :param kwargs: Other kwargs to send
        :return: A tuple of the remote's status, and the server response JSON
        """
        stage = transfer_stages.stage(stage)
        data = self.get_transfer_session(context)
        remote_stage = transfer_stages.stage(data.get("transfer_stage"))
        remote_status = data.get("transfer_stage_status")

        if remote_stage < stage:
            # if current stage is not yet at `stage`, push it to that stage through update
            kwargs.update(transfer_stage=stage)
            data = self.update_transfer_session(context, **kwargs)
            remote_status = data.get("transfer_stage_status")
        elif remote_stage > stage:
            # if past this stage, then we just make sure returned status is completed
            remote_status = transfer_statuses.COMPLETED

        if not remote_status:
            raise MorangoResumeSyncError("Remote failed to proceed to {}".format(stage))

        # if still in progress, then we return PENDING which will cause controller
        # to again call the middleware that contains this operation, and check the server status
        if remote_status in transfer_statuses.IN_PROGRESS_STATES:
            remote_status = transfer_statuses.PENDING

        return remote_status, data


class LegacyNetworkInitializeOperation(NetworkOperation):
    """
    Initializes remote transfer session in backwards compatible way, by expecting that the server
    will perform serialization and queuing during the create API call
    """

    def handle(self, context):
        """
        :type context: NetworkSessionContext
        """
        self._assert(context.transfer_session is not None)
        self._assert(ASYNC_OPERATIONS not in context.capabilities)

        # if local stage is transferring or beyond, we definitely don't need to initialize
        local_stage = context.stage
        if transfer_stages.stage(local_stage) >= transfer_stages.stage(
            transfer_stages.TRANSFERRING
        ):
            return transfer_statuses.COMPLETED

        data = self.create_transfer_session(context)
        context.transfer_session.server_fsic = data.get("server_fsic") or "{}"

        # A legacy instance performs queuing during the creation of the transfer session, so since we use a new
        # workflow we need to update the network server when pushing to say how many records we've queued. For pull,
        # we handle that here in the initialization/creation of the transfer session,
        # since that's when it's first available.
        if context.transfer_session.pull:
            context.transfer_session.records_total = data.get("records_total", 0)

        context.transfer_session.save()
        return transfer_statuses.COMPLETED


class NetworkInitializeOperation(NetworkOperation):
    """
    Performs initialization (create) of transfer session on the remote, and does not expect the
    server to advance the transfer session beyond initialization
    """

    def handle(self, context):
        """
        :type context: NetworkSessionContext
        """
        self._assert(context.transfer_session is not None)
        self._assert(ASYNC_OPERATIONS in context.capabilities)

        # if local stage is transferring or beyond, we definitely don't need to initialize
        if context.stage is not None and transfer_stages.stage(
            context.stage
        ) < transfer_stages.stage(transfer_stages.TRANSFERRING):
            self.create_transfer_session(context)

        return transfer_statuses.COMPLETED


class NetworkLegacyNoOpMixin(object):
    """
    Mixin that handles contexts without ASYNC_OPERATIONS capability, either because the remote
    is on older version of Morango, or either the client or server has it disabled.
    """

    def handle(self, context):
        """
        :type context: NetworkSessionContext
        """
        self._assert(ASYNC_OPERATIONS not in context.capabilities)
        return transfer_statuses.COMPLETED


class LegacyNetworkSerializeOperation(NetworkLegacyNoOpMixin, NetworkOperation):
    """
    Without ASYNC_OPERATIONS capability, the server will perform serialization during initialization
    """

    pass


class NetworkSerializeOperation(NetworkOperation):
    """
    Performs serialization on the remote by updating the remote's transfer stage
    """

    def handle(self, context):
        """
        :type context: NetworkSessionContext
        """
        self._assert(context.transfer_session is not None)
        self._assert(ASYNC_OPERATIONS in context.capabilities)

        remote_status, data = self.remote_proceed_to(
            context,
            transfer_stages.SERIALIZING,
            client_fsic=context.transfer_session.client_fsic,
        )

        if remote_status == transfer_statuses.COMPLETED:
            context.transfer_session.server_fsic = data.get("server_fsic")
            context.transfer_session.save()

        return remote_status


class LegacyNetworkQueueOperation(NetworkLegacyNoOpMixin, NetworkOperation):
    """
    Without ASYNC_OPERATIONS capability, the server will perform queuing during initialization
    """

    def handle(self, context):
        """
        :type context: NetworkSessionContext
        """
        self._assert(ASYNC_OPERATIONS not in context.capabilities)

        # A legacy instance performs queuing during the creation of the transfer session, so since we use a new
        # workflow we need to update the network server when pushing to say how many records we've queued. For pull,
        # we handle that in the initialization/creation of the transfer session, since that's when it's first available.
        if context.is_push:
            self.update_transfer_session(context, records_total=context.transfer_session.records_total)

        return transfer_statuses.COMPLETED


class NetworkQueueOperation(NetworkOperation):
    """
    Performs queuing on the remote by updating the remote's transfer stage
    """

    def handle(self, context):
        """
        :type context: NetworkSessionContext
        """
        self._assert(context.transfer_session is not None)
        self._assert(ASYNC_OPERATIONS in context.capabilities)

        update_kwargs = {}
        if context.is_push:
            update_kwargs.update(records_total=context.transfer_session.records_total)

        remote_status, data = self.remote_proceed_to(
            context, transfer_stages.QUEUING, **update_kwargs
        )

        if context.is_pull and remote_status == transfer_statuses.COMPLETED:
            context.transfer_session.records_total = data.get("records_total", 0)
            context.transfer_session.save()

        return remote_status


class NetworkPushTransferOperation(NetworkOperation):
    def handle(self, context):
        """
        :type context: NetworkSessionContext
        """
        self._assert(context.transfer_session is not None)
        self._assert(context.is_push)

        if context.transfer_session.records_total == 0:
            # since we won't be transferring anything, we can say we're done
            return transfer_statuses.COMPLETED

        offset = context.transfer_session.records_transferred
        chunk_size = context.connection.chunk_size

        buffered_records = Buffer.objects.filter(
            transfer_session=context.transfer_session
        ).order_by("pk")

        data = BufferSerializer(
            buffered_records[offset : offset + chunk_size], many=True
        ).data

        # push buffers chunk to server
        self.put_buffers(context, data)

        context.transfer_session.records_transferred = min(
            offset + chunk_size, context.transfer_session.records_total
        )
        context.transfer_session.bytes_sent = context.connection.bytes_sent
        context.transfer_session.bytes_received = context.connection.bytes_received
        context.transfer_session.save()

        # if we've transferred all records, return a completed status
        op_status = transfer_statuses.PENDING
        if (
            context.transfer_session.records_transferred
            >= context.transfer_session.records_total
        ):
            op_status = transfer_statuses.COMPLETED

        return op_status


class NetworkPullTransferOperation(NetworkOperation):
    def handle(self, context):
        """
        :type context: NetworkSessionContext
        """
        self._assert(context.transfer_session is not None)
        self._assert(context.is_pull)

        transfer_session = context.transfer_session

        if transfer_session.records_total > 0:
            # grab buffers, just one chunk
            data = self.get_buffers(context)

            validate_and_create_buffer_data(
                data, transfer_session, connection=context.connection
            )

        # if we've transferred all records, return a completed status
        op_status = transfer_statuses.PENDING
        if transfer_session.records_transferred >= transfer_session.records_total:
            op_status = transfer_statuses.COMPLETED

        # update the records transferred so client and server are in agreement
        self.update_transfer_session(
            context,
            transfer_stage=transfer_stages.TRANSFERRING,
            records_transferred=transfer_session.records_transferred,
            # flip each of these since we're talking about the remote instance
            bytes_received=transfer_session.bytes_sent,
            bytes_sent=transfer_session.bytes_received,
        )

        return op_status


class LegacyNetworkDequeueOperation(NetworkLegacyNoOpMixin, NetworkOperation):
    """
    Without ASYNC_OPERATIONS capability, the server will perform dequeuing during cleanup
    """

    pass


class NetworkDequeueOperation(NetworkOperation):
    """
    Performs dequeuing on the remote by updating the remote's transfer stage
    """

    def handle(self, context):
        """
        :type context: NetworkSessionContext
        """
        self._assert(ASYNC_OPERATIONS in context.capabilities)

        remote_status, _ = self.remote_proceed_to(context, transfer_stages.DEQUEUING)
        return remote_status


class LegacyNetworkDeserializeOperation(NetworkLegacyNoOpMixin, NetworkOperation):
    """
    Without ASYNC_OPERATIONS capability, the server will perform deserialization during cleanup
    """

    pass


class NetworkDeserializeOperation(NetworkOperation):
    """
    Performs deserialization on the remote by updating the remote's transfer stage
    """

    def handle(self, context):
        """
        :type context: NetworkSessionContext
        """
        self._assert(context.transfer_session is not None)
        self._assert(ASYNC_OPERATIONS in context.capabilities)

        remote_status, _ = self.remote_proceed_to(
            context, transfer_stages.DESERIALIZING
        )
        return remote_status


class NetworkCleanupOperation(NetworkOperation):
    """
    "Cleans up" the remote transfer session, which will trigger the LocalCleanupOperation on the
    server
    """

    def handle(self, context):
        """
        :type context: NetworkSessionContext
        """
        response = self.close_transfer_session(context)
        remote_status = transfer_statuses.COMPLETED
        if response.status_code < 200 or response.status_code >= 300:
            remote_status = transfer_statuses.ERRORED
        return remote_status
