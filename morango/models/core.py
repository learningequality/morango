from __future__ import unicode_literals

import functools
import json
import logging
import uuid
from collections import defaultdict
from collections import namedtuple
from functools import reduce

from django.core import exceptions
from django.db import connection
from django.db import models
from django.db import router
from django.db import transaction
from django.db.models import F
from django.db.models import Func
from django.db.models import Max
from django.db.models import Q
from django.db.models import TextField
from django.db.models import Value
from django.db.models.deletion import Collector
from django.db.models.expressions import CombinedExpression
from django.db.models.fields.related import ForeignKey
from django.db.models.functions import Cast
from django.utils import six
from django.utils import timezone
from django.utils.functional import cached_property

from morango import proquint
from morango.constants import transfer_stages
from morango.constants import transfer_statuses
from morango.errors import InvalidMorangoSourceId
from morango.models.certificates import Certificate
from morango.models.certificates import Filter
from morango.models.fields.uuids import sha2_uuid
from morango.models.fields.uuids import UUIDField
from morango.models.fields.uuids import UUIDModelMixin
from morango.models.fsic_utils import remove_redundant_instance_counters
from morango.models.manager import SyncableModelManager
from morango.models.morango_mptt import MorangoMPTTModel
from morango.models.utils import get_0_4_system_parameters
from morango.models.utils import get_0_5_mac_address
from morango.models.utils import get_0_5_system_id
from morango.registry import syncable_models
from morango.utils import _assert
from morango.utils import SETTINGS

logger = logging.getLogger(__name__)


class DatabaseIDManager(models.Manager):
    """
    We override ``model.Manager`` in order to wrap creating a new database ID model within a transaction. With the
    creation of a new database ID model, we set all previously created models current flag to False.
    """

    def create(self, **kwargs):

        # do within transaction so we always have a current database ID
        with transaction.atomic():
            # set current flag to false for all database_id models
            DatabaseIDModel.objects.update(current=False)

            return super(DatabaseIDManager, self).create(**kwargs)


class DatabaseIDModel(UUIDModelMixin):
    """
    Model to be used for tracking database ids.
    """

    uuid_input_fields = "RANDOM"

    objects = DatabaseIDManager()

    current = models.BooleanField(default=True)
    date_generated = models.DateTimeField(default=timezone.now)
    initial_instance_id = models.CharField(max_length=32, blank=True)

    def save(self, *args, **kwargs):

        # do within transaction so we always have a current database ID
        with transaction.atomic():
            # set current flag to false for all database_id models
            if not self.id and self.current:
                DatabaseIDModel.objects.update(current=False)

            super(DatabaseIDModel, self).save(*args, **kwargs)

    @classmethod
    def get_or_create_current_database_id(cls):

        with transaction.atomic():
            try:
                return cls.objects.get(current=True)
            except cls.DoesNotExist:
                return cls.objects.create()


class InstanceIDModel(models.Model):
    """
    ``InstanceIDModel`` is used to track what the current ID of this Morango instance is based on system properties. If system properties
    change, the ID used to track the morango instance also changes. During serialization phase, we associate the current instance ID,
    as well as its counter with all the records that were serialized at the time.
    """

    _cached_instance = None

    uuid_input_fields = (
        "platform",
        "hostname",
        "sysversion",
        "node_id",
        "database_id",
        "db_path",
    )

    id = UUIDField(max_length=32, primary_key=True, editable=False)

    platform = models.TextField()
    hostname = models.TextField()
    sysversion = models.TextField()
    node_id = models.CharField(max_length=20, blank=True)
    database = models.ForeignKey(DatabaseIDModel)
    counter = models.IntegerField(default=0)
    current = models.BooleanField(default=True)
    db_path = models.CharField(max_length=1000)
    system_id = models.CharField(max_length=100, blank=True)

    @property
    def instance_info(self):
        """
        Getter to access custom instance info defined in settings
        :return: dict
        """
        return SETTINGS.MORANGO_INSTANCE_INFO

    @classmethod
    @transaction.atomic
    def get_or_create_current_instance(cls, clear_cache=False):
        """Get the instance model corresponding to the current system, or create a new
        one if the system is new or its properties have changed (e.g. new MAC address)."""
        if clear_cache:
            cls._cached_instance = None

        if cls._cached_instance:
            instance = cls._cached_instance
            # make sure we have the latest counter value and "current" flag
            try:
                instance.refresh_from_db(fields=["counter", "current"])
                # only use cached instance if it's still marked as current, otherwise skip
                if instance.current:
                    return cls._cached_instance, False
            except InstanceIDModel.DoesNotExist:
                # instance does not exist, so skip here so we create a new one
                pass

        with transaction.atomic():

            # check if a matching legacy instance ID is already current, and don't mess with it
            kwargs = get_0_4_system_parameters(
                database_id=DatabaseIDModel.get_or_create_current_database_id().id
            )
            try:
                instance = InstanceIDModel.objects.get(current=True, **kwargs)
                cls._cached_instance = instance
                return instance, False
            except InstanceIDModel.DoesNotExist:
                pass

            # calculate the new ID based on system ID and mac address
            kwargs["system_id"] = get_0_5_system_id()
            kwargs["node_id"] = get_0_5_mac_address()
            kwargs["id"] = sha2_uuid(
                kwargs["database_id"], kwargs["system_id"], kwargs["node_id"]
            )
            kwargs["current"] = True

            # ensure we only ever have 1 current instance ID
            InstanceIDModel.objects.filter(current=True).exclude(
                id=kwargs["id"]
            ).update(current=False)
            # create the model, or get existing if one already exists with this ID
            instance, created = InstanceIDModel.objects.update_or_create(
                id=kwargs["id"], defaults=kwargs
            )

            cls._cached_instance = instance
            return instance, created

    @classmethod
    @transaction.atomic
    def get_current_instance_and_increment_counter(cls):
        instance, _ = cls.get_or_create_current_instance()
        cls.objects.filter(id=instance.id).update(counter=F("counter") + 1)
        instance.refresh_from_db(fields=["counter"])
        return instance

    def get_proquint(self):
        return proquint.from_int(int(self.id[:8], 16))


class SyncSession(models.Model):
    """
    ``SyncSession`` holds metadata for a sync session which keeps track of initial settings and
    the current transfer happening for this sync session.
    """

    id = UUIDField(primary_key=True)

    # track when the session started and the last time there was activity for this session
    start_timestamp = models.DateTimeField(default=timezone.now)
    last_activity_timestamp = models.DateTimeField(blank=True)
    active = models.BooleanField(default=True)

    # track whether this device is acting as the server for the sync session
    is_server = models.BooleanField(default=False)

    # track the certificates being used by each side for this session
    client_certificate = models.ForeignKey(
        Certificate, blank=True, null=True, related_name="syncsessions_client"
    )
    server_certificate = models.ForeignKey(
        Certificate, blank=True, null=True, related_name="syncsessions_server"
    )

    # track the morango profile this sync session is happening for
    profile = models.CharField(max_length=40)

    # information about the connection over which this sync session is happening
    connection_kind = models.CharField(
        max_length=10, choices=[("network", "Network"), ("disk", "Disk")]
    )
    connection_path = models.CharField(
        max_length=1000
    )  # file path if kind=disk, and base URL of server if kind=network

    # for network connections, keep track of the IPs on either end
    client_ip = models.CharField(max_length=100, blank=True)
    server_ip = models.CharField(max_length=100, blank=True)

    # serialized copies of the client and server instance model fields, for debugging/tracking purposes
    client_instance = models.TextField(default="{}")
    server_instance = models.TextField(default="{}")

    # used to store other data we may need to know about this sync session
    extra_fields = models.TextField(default="{}")

    # system process ID for ensuring same sync session does not run in parallel
    process_id = models.IntegerField(blank=True, null=True)

    @cached_property
    def client_instance_data(self):
        return json.loads(self.client_instance)

    @cached_property
    def server_instance_data(self):
        return json.loads(self.server_instance)


class TransferSession(models.Model):
    """
    ``TransferSession`` holds metadata that is related to a specific transfer (push/pull) session
    between 2 morango instances.
    """

    id = UUIDField(primary_key=True)
    filter = (
        models.TextField()
    )  # partition/filter to know what subset of data is to be synced
    push = models.BooleanField()  # is session pushing or pulling data?
    active = models.BooleanField(default=True)  # is this transfer session still active?
    records_transferred = models.IntegerField(
        default=0
    )  # track how many records have already been transferred
    records_total = models.IntegerField(
        blank=True, null=True
    )  # total number of records to be synced across in this transfer
    bytes_sent = models.BigIntegerField(default=0, null=True, blank=True)
    bytes_received = models.BigIntegerField(default=0, null=True, blank=True)

    sync_session = models.ForeignKey(SyncSession)

    # track when the transfer session started and the last time there was activity on it
    start_timestamp = models.DateTimeField(default=timezone.now)
    last_activity_timestamp = models.DateTimeField(blank=True)

    # we keep track of FSICs for both client and server
    client_fsic = models.TextField(blank=True, default="{}")
    server_fsic = models.TextField(blank=True, default="{}")

    # stages and stage status of transfer session
    transfer_stage = models.CharField(
        max_length=20,
        choices=transfer_stages.CHOICES,
        blank=True,
        null=True,
    )
    transfer_stage_status = models.CharField(
        max_length=20,
        choices=transfer_statuses.CHOICES,
        blank=True,
        null=True,
    )

    @property
    def pull(self):
        """Getter for `not push` condition, which adds complexity in conditional statements"""
        return not self.push

    def get_filter(self):
        return Filter(self.filter)

    def update_state(self, stage=None, stage_status=None):
        """
        :type stage: morango.constants.transfer_stages.*|None
        :type stage_status: morango.constants.transfer_statuses.*|None
        """
        if stage is not None:
            if self.transfer_stage and transfer_stages.stage(
                self.transfer_stage
            ) > transfer_stages.stage(stage):
                raise ValueError(
                    "Update stage is behind current stage | current={}, new={}".format(
                        self.transfer_stage, stage
                    )
                )
            self.transfer_stage = stage
        if stage_status is not None:
            self.transfer_stage_status = stage_status
        if stage is not None or stage_status is not None:
            self.last_activity_timestamp = timezone.now()
            self.save()
            self.sync_session.last_activity_timestamp = timezone.now()
            self.sync_session.save()

    def delete_buffers(self):
        """
        Deletes `Buffer` and `RecordMaxCounterBuffer` model records by executing SQL directly
        against the database for better performance
        """
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM morango_buffer WHERE transfer_session_id = %s", (self.id,)
            )
            cursor.execute(
                "DELETE FROM morango_recordmaxcounterbuffer WHERE transfer_session_id = %s",
                (self.id,),
            )

    def get_touched_record_ids_for_model(self, model):
        if isinstance(model, SyncableModel) or (
            isinstance(model, six.class_types) and issubclass(model, SyncableModel)
        ):
            model = model.morango_model_name
        _assert(isinstance(model, six.string_types), "Model must resolve to string")
        return Store.objects.filter(
            model_name=model, last_transfer_session_id=self.id
        ).values_list("id", flat=True)


class DeletedModels(models.Model):
    """
    ``DeletedModels`` helps us keep track of models that are deleted prior
    to serialization.
    """

    id = UUIDField(primary_key=True)
    profile = models.CharField(max_length=40)


class HardDeletedModels(models.Model):
    """
    ``HardDeletedModels`` helps us keep track of models where all their data
    must be purged (`serialized` is nullified).
    """

    id = UUIDField(primary_key=True)
    profile = models.CharField(max_length=40)


class AbstractStore(models.Model):
    """
    Base abstract model for storing serialized data.

    Inherited by both ``Store`` and ``Buffer``.
    """

    profile = models.CharField(max_length=40)

    serialized = models.TextField(blank=True)
    deleted = models.BooleanField(default=False)
    # flag to let other devices know to purge this data
    hard_deleted = models.BooleanField(default=False)

    # ID of last InstanceIDModel and its corresponding counter at time of serialization
    last_saved_instance = UUIDField()
    last_saved_counter = models.IntegerField()

    # fields used to compute UUIDs, filter data, and load data into the correct app models
    partition = models.TextField()
    source_id = models.CharField(max_length=96)
    model_name = models.CharField(max_length=40)

    # conflicting data that needs merge conflict resolution
    conflicting_serialized_data = models.TextField(blank=True)

    _self_ref_fk = models.CharField(max_length=32, blank=True)

    class Meta:
        abstract = True


class StoreQueryset(models.QuerySet):
    def char_ids_list(self):
        return (
            self.annotate(id_cast=Cast("id", TextField()))
            # remove dashes from char uuid
            .annotate(
                fixed_id=Func(F("id_cast"), Value("-"), Value(""), function="replace")
            )
            # return as list
            .values_list("fixed_id", flat=True)
        )


class StoreManager(models.Manager):
    def get_queryset(self):
        return StoreQueryset(self.model, using=self._db)


class Store(AbstractStore):
    """
    ``Store`` is the concrete model where serialized data is persisted, along with
    metadata about counters and history.
    """

    id = UUIDField(primary_key=True)
    # used to know which store records need to be deserialized into the app layer models
    dirty_bit = models.BooleanField(default=False)
    deserialization_error = models.TextField(blank=True)

    last_transfer_session_id = UUIDField(
        blank=True, null=True, default=None, db_index=True
    )

    objects = StoreManager()

    class Meta:
        indexes = [
            models.Index(fields=['partition'], name='idx_morango_store_partition'),
        ]

    def _deserialize_store_model(self, fk_cache, defer_fks=False):  # noqa: C901
        """
        When deserializing a store model, we look at the deleted flags to know if we should delete the app model.
        Upon loading the app model in memory we validate the app models fields, if any errors occurs we follow
        foreign key relationships to see if the related model has been deleted to propagate that deletion to the target app model.
        We return:
            None => if the model was deleted successfully
            model => if the model validates successfully
        """
        deferred_fks = {}
        klass_model = syncable_models.get_model(self.profile, self.model_name)
        # if store model marked as deleted, attempt to delete in app layer
        if self.deleted:
            # if hard deleted, propagate to related models
            if self.hard_deleted:
                try:
                    klass_model.objects.get(id=self.id).delete(hard_delete=True)
                except klass_model.DoesNotExist:
                    pass
            else:
                klass_model.objects.filter(id=self.id).delete()
            return None, deferred_fks
        else:
            # load model into memory
            app_model = klass_model.deserialize(json.loads(self.serialized))
            app_model._morango_source_id = self.source_id
            app_model._morango_partition = self.partition
            app_model._morango_dirty_bit = False

            try:
                # validate and return the model
                if defer_fks:
                    deferred_fks = app_model.deferred_clean_fields()
                else:
                    app_model.cached_clean_fields(fk_cache)
                return app_model, deferred_fks

            except (exceptions.ValidationError, exceptions.ObjectDoesNotExist) as e:

                logger.warning(
                    "Error deserializing instance of {model} with id {id}: {error}".format(
                        model=klass_model.__name__, id=app_model.id, error=e
                    )
                )

                if not defer_fks and isinstance(e, exceptions.ObjectDoesNotExist):
                    # check FKs in store to see if any of those models were deleted or hard_deleted to propagate to this model
                    fk_ids = [
                        getattr(app_model, field.attname)
                        for field in app_model._meta.fields
                        if isinstance(field, ForeignKey)
                    ]
                    for fk_id in fk_ids:
                        try:
                            st_model = Store.objects.get(id=fk_id)
                            if st_model.deleted:
                                # if hard deleted, propagate to store model
                                if st_model.hard_deleted:
                                    app_model._update_hard_deleted_models()
                                app_model._update_deleted_models()
                                return None, {}
                        except Store.DoesNotExist:
                            pass

                # if we got here, it means the validation error wasn't handled by propagating deletion, so re-raise it
                raise e


class Buffer(AbstractStore):
    """
    ``Buffer`` is where records from the internal store are queued up temporarily, before being
    sent to another morango instance, or stored while being received from another instance, before
    dequeuing into the local store.
    """

    transfer_session = models.ForeignKey(TransferSession)
    model_uuid = UUIDField()

    class Meta:
        unique_together = ("transfer_session", "model_uuid")

    def rmcb_list(self):
        return RecordMaxCounterBuffer.objects.filter(
            model_uuid=self.model_uuid, transfer_session_id=self.transfer_session_id
        )


class AbstractCounter(models.Model):
    """
    Abstract class which shares fields across multiple counter models.
    """

    # the UUID of the morango instance for which we're tracking the counter
    instance_id = UUIDField()
    # the counter of the morango instance at the time of serialization or merge conflict resolution
    counter = models.IntegerField()

    class Meta:
        abstract = True


class ValueStartsWithField(CombinedExpression):
    """
    Django expression that's essentially a `startswith` comparison but comparing that a parameter value starts with a
    table field. This also prevents Django from adding unnecessary SQL for the expression
    """

    def __init__(self, value, field):
        """
        {value} LIKE {field} || '%'

        :param value: A str of the value LIKE field
        :param field: A str of the field name comparing with
        """
        # we don't use `Concat` for appending the `%` because it also adds unnecessary SQL
        super(ValueStartsWithField, self).__init__(
            Value(value, output_field=models.CharField()),
            "LIKE",
            CombinedExpression(
                F(field), "||", Value("%", output_field=models.CharField())
            ),
            output_field=models.BooleanField(),
        )


class DatabaseMaxCounter(AbstractCounter):
    """
    ``DatabaseMaxCounter`` is used to keep track of what data this database already has across all
    instances for a particular partition prefix. Whenever 2 morango instances sync with each other we keep track
    of those partition prefixes from the filters, as well as the maximum counter we received for each instance during the sync session.
    """

    partition = models.CharField(max_length=128, default="")

    class Meta:
        unique_together = ("instance_id", "partition")

    @classmethod
    @transaction.atomic
    def update_fsics(cls, fsics, sync_filter, v2_format=False):
        internal_fsic = DatabaseMaxCounter.calculate_filter_specific_instance_counters(
            sync_filter, v2_format=v2_format
        )
        if v2_format:
            internal_fsic = internal_fsic["sub"]
            fsics = fsics["sub"]
            for part, insts in fsics.items():
                for inst, counter in insts.items():
                    if internal_fsic.get("part", {}).get(inst, 0) < counter:
                        DatabaseMaxCounter.objects.update_or_create(
                            instance_id=inst,
                            partition=part,
                            defaults={"counter": counter},
                        )
        else:
            updated_fsic = {}
            for key, value in six.iteritems(fsics):
                if key in internal_fsic:
                    # if same instance id, update fsic with larger value
                    if fsics[key] > internal_fsic[key]:
                        updated_fsic[key] = fsics[key]
                else:
                    # if instance id is not present, add it to updated fsics
                    updated_fsic[key] = fsics[key]

            # load database max counters
            for (key, value) in six.iteritems(updated_fsic):
                for f in sync_filter:
                    DatabaseMaxCounter.objects.update_or_create(
                        instance_id=key, partition=f, defaults={"counter": value}
                    )

    @classmethod
    def get_instance_counters_for_partitions(cls, partitions, is_producer=False):
        queryset = cls.objects.filter(partition__in=partitions)

        # filter out instance_ids that are no longer relevant if settings don't prevent it
        # because these queries can be somewhat intensive
        if not SETTINGS.MORANGO_DISABLE_FSIC_REDUCTION:
            if is_producer:
                # for the producing side, we only need to use instance_ids
                # that are still on Store records
                matching_instances = Store.objects.filter(
                    partition__startswith=models.OuterRef("partition"),
                    last_saved_instance=models.OuterRef("instance_id"),
                ).values("last_saved_instance")
            else:
                # for the receiving side, we include all instances
                # that still exist in the RecordMaxCounters
                matching_instances = RecordMaxCounter.objects.filter(
                    store_model__partition__startswith=models.OuterRef("partition"),
                    instance_id=models.OuterRef("instance_id"),
                ).values("instance_id")

            # manually add EXISTS clause to avoid Django bug which puts `= TRUE` comparison
            # that kills postgres performance
            exists = models.Exists(matching_instances).resolve_expression(
                query=queryset.query, allow_joins=True, reuse=None
            )
            queryset.query.where.add(exists, "AND")

        queryset = queryset.values_list("partition", "instance_id", "counter")

        counters = {}
        for partition, instance_id, counter in queryset:
            if partition not in counters:
                counters[partition] = {}
            counters[partition].update({instance_id: counter})
        return counters

    @classmethod
    def calculate_filter_specific_instance_counters(
        cls,
        filters,
        is_producer=False,
        v2_format=False,
    ):

        if v2_format:

            queryset = cls.objects.all()

            # get the DMC records with partitions that fall under the filter prefixes
            sub_condition = functools.reduce(
                lambda x, y: x | y,
                [Q(partition__startswith=prefix) for prefix in filters],
            )
            sub_partitions = set(
                queryset.filter(sub_condition)
                .values_list("partition", flat=True)
                .distinct()
            )

            # get the DMC records with partitions that are prefixes of the filters
            super_partitions = set()
            for filt in filters:
                qs = (
                    queryset.annotate(
                        filter_matches=ValueStartsWithField(filt, "partition")
                    )
                    .filter(filter_matches=True)
                    .exclude(partition=filt)
                )
                super_partitions.update(
                    qs.values_list("partition", flat=True).distinct()
                )

            # get the instance counters for the partitions, also filtering out old unnecessary instance_ids
            super_fsics = cls.get_instance_counters_for_partitions(
                super_partitions, is_producer=is_producer
            )
            sub_fsics = cls.get_instance_counters_for_partitions(
                sub_partitions, is_producer=is_producer
            )

            raw_fsic = {
                "super": super_fsics,
                "sub": sub_fsics,
            }

            # remove instance counters on partitions that are redundant to counters on super partitions
            remove_redundant_instance_counters(raw_fsic)

            return raw_fsic

        else:

            queryset = cls.objects.all()

            per_filter_max = []

            for filt in filters:
                # {filt} LIKE partition || '%'
                qs = queryset.annotate(
                    filter_matches=ValueStartsWithField(filt, "partition")
                )
                qs = qs.filter(filter_matches=True)
                filt_maxes = qs.values("instance_id").annotate(maxval=Max("counter"))
                per_filter_max.append(
                    {dmc["instance_id"]: dmc["maxval"] for dmc in filt_maxes}
                )

            instance_id_lists = [maxes.keys() for maxes in per_filter_max]
            all_instance_ids = reduce(set.union, instance_id_lists, set())
            if is_producer:
                # when we're sending, we want to make sure we include everything
                result = {
                    instance_id: max([d.get(instance_id, 0) for d in per_filter_max])
                    for instance_id in all_instance_ids
                }
            else:
                # when we're receiving, we don't want to overpromise on what we have
                result = {
                    instance_id: min([d.get(instance_id, 0) for d in per_filter_max])
                    for instance_id in reduce(
                        set.intersection, instance_id_lists, all_instance_ids
                    )
                }

        return result


class RecordMaxCounter(AbstractCounter):
    """
    ``RecordMaxCounter`` keeps track of the maximum counter each serialized record has been saved at,
    for each instance that has modified it. This is used to determine fast-forwards and merge conflicts
    during the sync process.
    """

    store_model = models.ForeignKey(Store)

    class Meta:
        unique_together = ("store_model", "instance_id")


class RecordMaxCounterBuffer(AbstractCounter):
    """
    ``RecordMaxCounterBuffer`` is where combinations of instance ID and counters (from ``RecordMaxCounter``) are stored temporarily,
    until they are sent or received by another morango instance.
    """

    transfer_session = models.ForeignKey(TransferSession)
    model_uuid = UUIDField(db_index=True)


ForeignKeyReference = namedtuple("ForeignKeyReference", ["from_field", "from_pk", "to_pk"])


class SyncableModel(UUIDModelMixin):
    """
    ``SyncableModel`` is the base model class for syncing. Other models inherit from this class if they want to make
    their data syncable across devices.
    """

    # constant value to insert into partition strings in place of current model's ID, as needed (to avoid circularity)
    ID_PLACEHOLDER = "${id}"

    _morango_internal_fields_not_to_serialize = ("_morango_dirty_bit",)
    morango_model_dependencies = ()
    morango_fields_not_to_serialize = ()
    morango_profile = None

    # morango specific field used for tracking model changes
    _morango_dirty_bit = models.BooleanField(default=True, editable=False)
    # morango specific field used to store random uuid or unique together fields
    _morango_source_id = models.CharField(max_length=96, editable=False)
    # morango specific field used to store the partition on the model
    _morango_partition = models.CharField(max_length=128, editable=False)

    objects = SyncableModelManager()

    class Meta:
        abstract = True

    def _update_deleted_models(self):
        DeletedModels.objects.update_or_create(
            defaults={"id": self.id, "profile": self.morango_profile}, id=self.id
        )

    def _update_hard_deleted_models(self):
        HardDeletedModels.objects.update_or_create(
            defaults={"id": self.id, "profile": self.morango_profile}, id=self.id
        )

    def save(self, update_dirty_bit_to=True, *args, **kwargs):
        if update_dirty_bit_to is None:
            pass  # don't do anything with the dirty bit
        elif update_dirty_bit_to:
            self._morango_dirty_bit = True
        elif not update_dirty_bit_to:
            self._morango_dirty_bit = False
        super(SyncableModel, self).save(*args, **kwargs)

    def delete(
        self, using=None, keep_parents=False, hard_delete=False, *args, **kwargs
    ):
        using = using or router.db_for_write(self.__class__, instance=self)
        _assert(
            self._get_pk_val() is not None,
            "%s object can't be deleted because its %s attribute is set to None."
            % (self._meta.object_name, self._meta.pk.attname),
        )
        collector = Collector(using=using)
        collector.collect([self], keep_parents=keep_parents)
        with transaction.atomic():
            if hard_delete:
                # set hard deletion for all related models
                for model, instances in six.iteritems(collector.data):
                    if issubclass(model, SyncableModel) or issubclass(
                        model, MorangoMPTTModel
                    ):
                        for obj in instances:
                            obj._update_hard_deleted_models()
            return collector.delete()

    def cached_clean_fields(self, fk_lookup_cache):
        """
        Immediately validates all fields, but uses a cache for foreign key (FK) lookups to reduce
        repeated queries for many records with the same FK

        :param fk_lookup_cache: A dictionary to use as a cache to prevent querying the database if a
            FK exists in the cache, having already been validated
        """
        excluded_fields = []
        fk_fields = [
            field for field in self._meta.fields if isinstance(field, models.ForeignKey)
        ]

        for f in fk_fields:
            raw_value = getattr(self, f.attname)
            key = "{id}_{db_table}".format(
                db_table=f.related_model._meta.db_table, id=raw_value
            )
            try:
                fk_lookup_cache[key]
                excluded_fields.append(f.name)
            except KeyError:
                try:
                    f.validate(raw_value, self)
                except exceptions.ValidationError:
                    pass
                else:
                    fk_lookup_cache[key] = 1
                    excluded_fields.append(f.name)

        self.clean_fields(exclude=excluded_fields)

        # after cleaning, we can confidently set ourselves in the fk_lookup_cache
        self_key = "{id}_{db_table}".format(
            db_table=self._meta.db_table,
            id=self.id,
        )
        fk_lookup_cache[self_key] = 1

    def deferred_clean_fields(self):
        """
        Calls `.clean_fields()` but excludes all foreign key fields and instead returns them as a
        dictionary for deferred batch processing

        :return: A dictionary containing lists of `ForeignKeyReference`s keyed by the name of the
            model being referenced by the FK
        """
        excluded_fields = []
        deferred_fks = defaultdict(list)
        for field in self._meta.fields:
            if not isinstance(field, models.ForeignKey):
                continue
            # by not excluding the field if it's null, the default validation logic will apply
            # and should raise a ValidationError if the FK field is not nullable
            if getattr(self, field.attname) is None:
                continue
            excluded_fields.append(field.name)
            deferred_fks[field.related_model._meta.verbose_name].append(
                ForeignKeyReference(
                    from_field=field.attname,
                    from_pk=self.pk,
                    to_pk=getattr(self, field.attname)
                )
            )

        self.clean_fields(exclude=excluded_fields)
        return deferred_fks

    def serialize(self):
        """All concrete fields of the ``SyncableModel`` subclass, except for those specifically blacklisted, are returned in a dict."""
        # NOTE: code adapted from https://github.com/django/django/blob/master/django/forms/models.py#L75
        opts = self._meta

        data = {}
        for f in opts.concrete_fields:
            if f.attname in self.morango_fields_not_to_serialize:
                continue
            if f.attname in self._morango_internal_fields_not_to_serialize:
                continue
            # case if model is morango mptt
            if f.attname in getattr(
                self,
                "_internal_mptt_fields_not_to_serialize",
                "_internal_fields_not_to_serialize",
            ):
                continue
            if hasattr(f, "value_from_object_json_compatible"):
                data[f.attname] = f.value_from_object_json_compatible(self)
            else:
                data[f.attname] = f.value_from_object(self)
        return data

    @classmethod
    def deserialize(cls, dict_model):
        """Returns an unsaved class object based on the valid properties passed in."""
        kwargs = {}
        for f in cls._meta.concrete_fields:
            if f.attname in dict_model:
                kwargs[f.attname] = dict_model[f.attname]
        return cls(**kwargs)

    @classmethod
    def merge_conflict(cls, current, push):
        return push

    def calculate_source_id(self):
        """Should return a string that uniquely defines the model instance or `None` for a random uuid."""
        raise NotImplementedError(
            "You must define a 'calculate_source_id' method on models that inherit from SyncableModel."
        )

    def calculate_partition(self):
        """Should return a string specifying this model instance's partition, using `self.ID_PLACEHOLDER` in place of its own ID, if needed."""
        raise NotImplementedError(
            "You must define a 'calculate_partition' method on models that inherit from SyncableModel."
        )

    @staticmethod
    def compute_namespaced_id(partition_value, source_id_value, model_name):
        return sha2_uuid(partition_value, source_id_value, model_name)

    def calculate_uuid(self):
        if not self._morango_source_id:
            self._morango_source_id = self.calculate_source_id()
            if self._morango_source_id is None:
                self._morango_source_id = uuid.uuid4().hex
            elif self._morango_source_id == "":
                # undefined behavior, which due to dynamic nature of calculating it, this could be an unintentional bug
                # so we raise an error to strictly enforce this
                raise InvalidMorangoSourceId(
                    "{}.calculate_source_id() returned empty string".format(
                        self.__class__.__name__
                    )
                )

        namespaced_id = self.compute_namespaced_id(
            self.calculate_partition(), self._morango_source_id, self.morango_model_name
        )
        self._morango_partition = self.calculate_partition().replace(
            self.ID_PLACEHOLDER, namespaced_id
        )
        return namespaced_id
