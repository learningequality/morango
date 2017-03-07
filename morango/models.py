import os
import platform
import sys
import uuid

from django.conf import settings
from django.db import models, transaction
from django.db.models import Max, Q
from django.utils import six, timezone
from django.utils.encoding import python_2_unicode_compatible

from .manager import SyncableModelManager
from .utils.uuids import UUIDModelMixin, UUIDField


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

    uuid_input_fields = ("platform", "hostname", "sysversion", "macaddress", "database_id", "db_path")

    platform = models.TextField()
    hostname = models.TextField()
    sysversion = models.TextField()
    macaddress = models.CharField(max_length=20, blank=True)
    database = models.ForeignKey(DatabaseIDModel)
    counter = models.IntegerField(default=0)
    current = models.BooleanField(default=True)
    db_path = models.CharField(max_length=1000)

    @staticmethod
    def get_or_create_current_instance():
        """Get the instance model corresponding to the current system, or create a new
        one if the system is new or its properties have changed (e.g. OS from upgrade)."""

        kwargs = {
            "platform": platform.platform(),
            "hostname": platform.node(),
            "sysversion": sys.version,
            "database": DatabaseIDModel.objects.get(current=True),
            "db_path": os.path.abspath(settings.DATABASES['default']['NAME']),
        }

        # try to get the MAC address, but exclude it if it was a fake (random) address
        mac = uuid.getnode()
        if (mac >> 40) % 2 == 0:  # 8th bit (of 48 bits, from left) is 1 if MAC is fake
            kwargs["macaddress"] = mac

        # do within transaction so we only ever have 1 current instance ID
        with transaction.atomic():
            obj, created = InstanceIDModel.objects.get_or_create(**kwargs)
            if created:
                InstanceIDModel.objects.exclude(id=obj.id.hex).update(current=False)
                return obj, created

        return obj, created


class SyncSession(models.Model):
    """
    ``SyncSession`` holds metadata for a sync session which keeps track of initial settings and
    the current transfer happening for this sync session.
    """

    id = models.UUIDField(primary_key=True)
    start_timestamp = models.DateTimeField(default=timezone.now)
    last_activity_timestamp = models.DateTimeField(blank=True)
    scope_setting = models.TextField(default='{}')
    current_transfer = models.UUIDField(max_length=32, blank=True)


class TransferSession(models.Model):
    """
    ``TransferSession`` holds metatada that is related to a specific transfer (push/pull) session
    between 2 morango instances.
    """

    id = models.UUIDField(primary_key=True)
    filters = models.TextField(default='{}')
    records_left = models.IntegerField()
    push_request = models.BooleanField()


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

    This model is an abstract model, and is inherited by both ``StoreModel`` and
    ``DataTransferBuffer``.
    """

    id = UUIDField(primary_key=True)
    serialized = models.TextField(blank=True)
    deleted = models.BooleanField(default=False)
    version = models.CharField(max_length=40)
    history = models.TextField(blank=True)
    last_saved_instance = models.UUIDField()
    last_saved_counter = models.IntegerField()
    record_max_counters = models.TextField(default="{}")
    model_name = models.CharField(max_length=40)

    class Meta:
        abstract = True


@python_2_unicode_compatible
class AbstractDatabaseMaxCounter(models.Model):
    """
    ``DatabaseMaxCounter`` is used to keep track of what data this database already has across all
    instances for a particular filter. Whenever 2 morango instances sync with each other we keep track
    of those filters, as well as the instance, counter pairs used at the time of sync.
    """

    instance_id = UUIDField()
    max_counter = models.IntegerField()

    class Meta:
        abstract = True

    @classmethod
    def get_max_counters_for_filter(cls, filter):
        """
        Gets the highest instance, counter pairs for a filter and all supersets of that filter.

        :param filter: A dictionary specifying the key-value pairs to be filtered against.
        :return: A list of dictionaries specifying instance, counter pairs.
        :rtype: list
        """
        queries = []
        for key, value in six.iteritems(filter):
            queries.append(Q(**{key: value}) | Q(**{key: "*"}))

        filter = reduce(lambda x, y: x & y, queries)
        rows = cls.objects.filter(filter)
        return rows.values('instance_id').annotate(max_counter=Max('max_counter'))

    def __str__(self):
        return '"{}"@"{}"'.format(self.instance_id, self.max_counter)


class AbstractRecordMaxCounter(models.Model):
    """
    ``RecordMaxCounter`` saves a combination of the instance ID and a counter position that is assigned to a
    serialized record. This is used to determine fast-forwards and merge conflicts during the sync process.
    """

    instance_id = UUIDField()
    counter = models.IntegerField()

    class Meta:
        abstract = True


class SyncableModel(UUIDModelMixin):
    """
    ``SyncableModel`` is the base model class for syncing. Other models inherit from this class if they want to make
    their data syncable across devices.
    """

    _internal_fields_not_to_serialize = ('_dirty_bit',)

    # morango specific field used for tracking model changes
    _dirty_bit = models.BooleanField(default=True)

    objects = SyncableModelManager()

    class Meta:
        abstract = True

    def save(self, update_dirty_bit_to=True, *args, **kwargs):
        if update_dirty_bit_to is None:
            pass  # don't do anything with the dirty bit
        elif update_dirty_bit_to:
            self._dirty_bit = True
        elif not update_dirty_bit_to:
            self._dirty_bit = False
        super(SyncableModel, self).save(*args, **kwargs)

    def serialize(self):
        """All concrete fields of the ``SyncableModel`` subclass, except for those specifically blacklisted, are returned in a dict."""
        # NOTE: code adapted from https://github.com/django/django/blob/master/django/forms/models.py#L75
        opts = self._meta
        data = {}

        for f in opts.concrete_fields:
            if f.attname in self._fields_not_to_serialize:
                continue
            if f.attname in self._internal_fields_not_to_serialize:
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

    def get_partitions(self, *args, **kwargs):
        """Should return a dictionary with any relevant partition keys included, along with their values."""
        raise NotImplemented("You must define a 'get_partition_names' method on models that inherit from SyncableModel.")


###################################################################################################
# CERTIFICATES: Data to manage authorization and the chain-of-trust certificate system
###################################################################################################


class CertificateModel(models.Model):
    signature = models.CharField(max_length=64, primary_key=True)  # long enough to hold SHA256 sigs
    issuer = models.ForeignKey("CertificateModel")

    certificate = models.TextField()
