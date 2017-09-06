import os
import platform
import sys
import uuid

from django.conf import settings
from django.db import connection, models, transaction
from django.db.models import F
from django.utils import timezone

from .certificates import Certificate, ScopeDefinition, Nonce, Filter
from .manager import SyncableModelManager
from .utils.uuids import UUIDField, UUIDModelMixin, sha2_uuid


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


class InstanceIDModel(UUIDModelMixin):
    """
    ``InstanceIDModel`` is used to track what the current ID of this Morango instance is based on system properties. If system properties
    change, the ID used to track the morango instance also changes. During serialization phase, we associate the current instance ID,
    as well as its counter with all the records that were serialized at the time.
    """

    uuid_input_fields = ("platform", "hostname", "sysversion", "macaddress", "database_id", "db_path", "system_id")

    platform = models.TextField()
    hostname = models.TextField()
    sysversion = models.TextField()
    macaddress = models.CharField(max_length=20, blank=True)
    database = models.ForeignKey(DatabaseIDModel)
    counter = models.IntegerField(default=0)
    current = models.BooleanField(default=True)
    db_path = models.CharField(max_length=1000)
    system_id = models.CharField(max_length=100, blank=True)

    @classmethod
    def get_or_create_current_instance(cls):
        """Get the instance model corresponding to the current system, or create a new
        one if the system is new or its properties have changed (e.g. OS from upgrade)."""

        # on Android, platform.platform() barfs, so we handle that safely here
        try:
            plat = platform.platform()
        except:
            plat = "Unknown (Android?)"

        kwargs = {
            "platform": plat,
            "hostname": platform.node(),
            "sysversion": sys.version,
            "database": DatabaseIDModel.get_or_create_current_database_id(),
            "db_path": os.path.abspath(settings.DATABASES['default']['NAME']),
            "system_id": os.environ.get("MORANGO_SYSTEM_ID", ""),
        }

        # try to get the MAC address, but exclude it if it was a fake (random) address
        mac = uuid.getnode()
        if (mac >> 40) % 2 == 0:  # 8th bit (of 48 bits, from left) is 1 if MAC is fake
            kwargs["macaddress"] = mac
        else:
            kwargs["macaddress"] = ""

        # do within transaction so we only ever have 1 current instance ID
        with transaction.atomic():
            obj, created = cls.objects.get_or_create(**kwargs)
            if created:
                cls.objects.exclude(id=obj.id).update(current=False)

        return obj, created

    @staticmethod
    @transaction.atomic
    def get_current_instance_and_increment_counter():
        InstanceIDModel.objects.filter(current=True).update(counter=F('counter') + 1)
        return InstanceIDModel.objects.get(current=True)


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
    local_certificate = models.ForeignKey(Certificate, blank=True, null=True, related_name="syncsessions_local")
    remote_certificate = models.ForeignKey(Certificate, blank=True, null=True, related_name="syncsessions_remote")

    # track the morango profile this sync session is happening for
    profile = models.CharField(max_length=40)

    # information about the connection over which this sync session is happening
    connection_kind = models.CharField(max_length=10, choices=[(u"network", u"Network"), (u"disk", u"Disk")])
    connection_path = models.CharField(max_length=1000)  # file path if kind=disk, and base URL of server if kind=network

    # for network connections, keep track of the IPs on either end
    local_ip = models.CharField(max_length=100, blank=True)
    remote_ip = models.CharField(max_length=100, blank=True)

    # serialized copies of the client and server instance model fields, for debugging/tracking purposes
    local_instance = models.TextField(default=u"{}")
    remote_instance = models.TextField(default=u"{}")


class TransferSession(models.Model):
    """
    ``TransferSession`` holds metatada that is related to a specific transfer (push/pull) session
    between 2 morango instances.
    """

    id = UUIDField(primary_key=True)
    filter = models.TextField()  # partition/filter to know what subset of data is to be synced
    incoming = models.BooleanField()  # is session pushing or pulling data?
    active = models.BooleanField(default=True)  # is this transfer session still active?
    records_transferred = models.IntegerField(default=0)  # track how many records have already been transferred
    records_total = models.IntegerField(blank=True, null=True)  # total number of records to be synced across in this transfer
    sync_session = models.ForeignKey(SyncSession)

    # track when the transfer session started and the last time there was activity on it
    start_timestamp = models.DateTimeField(default=timezone.now)
    last_activity_timestamp = models.DateTimeField(blank=True)

    def get_filter(self):
        return Filter(self.filter)


class DeletedModels(models.Model):
    """
    ``DeletedModels`` helps us keep track of models that are deleted prior
    to serialization.
    """

    id = UUIDField(primary_key=True)
    profile = models.CharField(max_length=40)


class AbstractStore(models.Model):
    """
    ``AbstractStore`` is a base model for storing serialized data.

    This model is an abstract model, and is inherited by both ``Store`` and
    ``Buffer``.
    """

    profile = models.CharField(max_length=40)

    serialized = models.TextField(blank=True)
    deleted = models.BooleanField(default=False)

    # ID of last InstanceIDModel and its corresponding counter at time of serialization
    last_saved_instance = UUIDField()
    last_saved_counter = models.IntegerField()

    # fields used to compute UUIDs, filter data, and load data into the correct app models
    partition = models.TextField()
    source_id = models.CharField(max_length=96)
    model_name = models.CharField(max_length=40)

    # conflicting data that needs merge conflict resolution
    conflicting_serialized_data = models.TextField(blank=True)

    class Meta:
        abstract = True


class Store(AbstractStore):
    """
    ``Store`` is the concrete model where serialized data is persisted, along with
    metadata about counters and history.
    """

    id = UUIDField(primary_key=True)
    # used to know which store records need to be deserialized into the app layer models
    dirty_bit = models.BooleanField(default=False)


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


class DatabaseMaxCounter(AbstractCounter):
    """
    ``DatabaseMaxCounter`` is used to keep track of what data this database already has across all
    instances for a particular partition prefix. Whenever 2 morango instances sync with each other we keep track
    of those partition prefixes from the filters, as well as the maximum counter we received for each instance during the sync session.
    """

    partition = models.CharField(max_length=128, default="")

    @classmethod
    def calculate_filter_max_counters(cls, filters):

        # create string of prefixes to place into sql statement
        condition = " UNION ".join(["SELECT '{}' AS a".format(prefix) for prefix in filters])

        filter_max_calculation = """
        SELECT PMC.instance, MIN(PMC.counter)
        FROM
            (
            SELECT dmc.instance_id as instance, MAX(dmc.counter) as counter, filter as filter_partition
            FROM {dmc_table} as dmc, (SELECT T.a as filter FROM ({filter_list}) as T)
            WHERE filter LIKE dmc.partition || '%'
            GROUP BY instance, filter_partition
            ) as PMC
        GROUP BY PMC.instance
        HAVING {count} = COUNT(PMC.filter_partition)
        """.format(dmc_table=cls._meta.db_table,
                   filter_list=condition,
                   count=len(filters))

        with connection.cursor() as cursor:
            cursor.execute(filter_max_calculation)
            return dict(cursor.fetchall())


class RecordMaxCounter(AbstractCounter):
    """
    ``RecordMaxCounter`` keeps track of the maximum counter each serialized record has been saved at,
     for each instance that has modified it. This is used to determine fast-forwards and merge conflicts
     during the sync process.
    """

    store_model = models.ForeignKey(Store)

    class Meta:
        unique_together = ('store_model', 'instance_id')


class RecordMaxCounterBuffer(AbstractCounter):
    """
    ``RecordMaxCounterBuffer`` is where combinations of instance ID and counters (from ``RecordMaxCounter``) are stored temporarily,
    until they are sent or recieved by another morango instance.
    """

    transfer_session = models.ForeignKey(TransferSession)
    model_uuid = UUIDField()


class SyncableModel(UUIDModelMixin):
    """
    ``SyncableModel`` is the base model class for syncing. Other models inherit from this class if they want to make
    their data syncable across devices.
    """

    # constant value to insert into partition strings in place of current model's ID, as needed (to avoid circularity)
    ID_PLACEHOLDER = "${id}"

    _morango_internal_fields_not_to_serialize = ('_morango_dirty_bit',)
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
        DeletedModels.objects.update_or_create(defaults={'id': self.id, 'profile': self.morango_profile},
                                               id=self.id)

    def save(self, update_dirty_bit_to=True, *args, **kwargs):
        if update_dirty_bit_to is None:
            pass  # don't do anything with the dirty bit
        elif update_dirty_bit_to:
            self._morango_dirty_bit = True
        elif not update_dirty_bit_to:
            self._morango_dirty_bit = False
        super(SyncableModel, self).save(*args, **kwargs)

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
            if f.attname in getattr(self, '_internal_mptt_fields_not_to_serialize', '_internal_fields_not_to_serialize'):
                continue
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
    def merge_conflict(cls, current, incoming):
        return incoming

    def calculate_source_id(self):
        """Should return a string that uniquely defines the model instance or `None` for a random uuid."""
        raise NotImplementedError("You must define a 'calculate_source_id' method on models that inherit from SyncableModel.")

    def calculate_partition(self):
        """Should return a string specifying this model instance's partition, using `self.ID_PLACEHOLDER` in place of its own ID, if needed."""
        raise NotImplementedError("You must define a 'calculate_partition' method on models that inherit from SyncableModel.")

    @staticmethod
    def compute_namespaced_id(partition_value, source_id_value, model_name):
        return sha2_uuid(partition_value, source_id_value, model_name)

    def calculate_uuid(self):
        self._morango_source_id = self.calculate_source_id()
        if self._morango_source_id is None:
            self._morango_source_id = uuid.uuid4().hex

        namespaced_id = self.compute_namespaced_id(self.calculate_partition(), self._morango_source_id, self.morango_model_name)
        self._morango_partition = self.calculate_partition().replace(self.ID_PLACEHOLDER, namespaced_id)
        return namespaced_id
