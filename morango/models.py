import os
import platform
import sys
import uuid

from django.conf import settings
from django.db import models, transaction
from django.utils import timezone

from .certificates import *
from .manager import SyncableModelManager
from .utils.uuids import UUIDField, UUIDModelMixin, sha2_uuid


class DatabaseManager(models.Manager):
    """
    We override ``model.Manager`` in order to wrap creating a new database ID model within a transaction. With the
    creation of a new database ID model, we set all previously created models current flag to False.
    """

    def create(self, **kwargs):

        # do within transaction so we always have a current database ID
        with transaction.atomic():
            # set current flag to false for all database_id models
            DatabaseIDModel.objects.update(current=False)

            return super(DatabaseManager, self).create(**kwargs)


class DatabaseIDModel(UUIDModelMixin):
    """
    Model to be used for tracking database ids.
    """

    uuid_input_fields = "RANDOM"

    objects = DatabaseManager()

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

    @staticmethod
    def get_or_create_current_instance():
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
            "database": DatabaseIDModel.objects.get(current=True),
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
            obj, created = InstanceIDModel.objects.get_or_create(**kwargs)
            if created:
                InstanceIDModel.objects.exclude(id=obj.id).update(current=False)

        return obj, created


class SyncSession(models.Model):
    """
    ``SyncSession`` holds metadata for a sync session which keeps track of initial settings and
    the current transfer happening for this sync session.
    """

    id = models.UUIDField(primary_key=True)
    # we track when the session started and the last time there was activity for this session
    start_timestamp = models.DateTimeField(default=timezone.now)
    last_activity_timestamp = models.DateTimeField(blank=True)
    # JSON of broad scope/(R and/or W) permissions
    local_scope = models.TextField()
    remote_scope = models.TextField()
    host = models.CharField(max_length=255)


class TransferSession(models.Model):
    """
    ``TransferSession`` holds metatada that is related to a specific transfer (push/pull) session
    between 2 morango instances.
    """

    id = models.UUIDField(primary_key=True)
    # partition/filter to know what subset of data is to be synced
    filter = models.TextField()
    # is session pushing or pulling data
    incoming = models.BooleanField()
    # is this session actively pushing or pulling data?
    active = models.BooleanField(default=True)
    chunksize = models.IntegerField(default=500)
    # we track how many records are left to be synced in this session
    records_remaining = models.IntegerField()
    records_total = models.IntegerField()
    sync_session = models.ForeignKey(SyncSession)


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

    serialized = models.TextField(blank=True)
    deleted = models.BooleanField(default=False)
    # morango UUID instance and counter at time of serialization
    last_saved_instance = UUIDField()
    last_saved_counter = models.IntegerField()
    # morango_model_name of model
    model_name = models.CharField(max_length=40)
    profile = models.CharField(max_length=40)
    # colon-separated partition values that specify which segment of data this record belongs to
    partition = models.TextField()
    # conflicting data that needs a merge conflict resolution
    conflicting_serialized_data = models.TextField(blank=True)

    class Meta:
        abstract = True


class Store(AbstractStore):
    """
    ``Store`` is the concrete model where serialized data is persisted, along with
    metadata about counters and history.
    """

    id = UUIDField(primary_key=True)


class Buffer(AbstractStore):
    """
    ``Buffer`` is where records from the internal store are kept temporarily,
    until they are sent or received by another morango instance.
    """

    transfer_session = models.ForeignKey(TransferSession)
    model_uuid = UUIDField()


class AbstractCounter(models.Model):
    """
    Abstract class which shares fields across multiple counter models.
    """

    # the UUID of the morango instance that was last synced for this model or filter
    instance_id = UUIDField()
    # the counter of the morango instance at the time of serialization
    counter = models.IntegerField()

    class Meta:
        abstract = True


class DatabaseMaxCounter(AbstractCounter):
    """
    ``DatabaseMaxCounter`` is used to keep track of what data this database already has across all
    instances for a particular filter. Whenever 2 morango instances sync with each other we keep track
    of those filters, as well as the maximum counter we received for each instance during the sync session.
    """

    filter = models.TextField()


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
