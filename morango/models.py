import json

from django.db import models
from django.db.models import Max, Q
from django.utils import six

from .utils.uuids import UUIDModelMixin, UUIDField


###################################################################################################
# APP MODELS: Abstract models from which app models should inherit in order to make them syncable
###################################################################################################
class SyncableModelQuerySet(models.query.QuerySet):

    def update(self, dirty_bit_signal=True, **kwargs):
        if dirty_bit_signal is None:
            pass  # don't do anything with the dirty bit
        elif dirty_bit_signal:
            kwargs.update({'_dirty_bit': True})
        elif not dirty_bit_signal:
            kwargs.update({'_dirty_bit': False})
        super(SyncableModelQuerySet, self).update(**kwargs)


class SyncableModelManager(models.Manager):

    def get_queryset(self):
        return SyncableModelQuerySet(self.model, using=self._db)


class SyncableModel(UUIDModelMixin):
    """
    Base model class for syncing. Other models inherit from this class if they want to make
    their data syncable across devices.
    """

    # morango specific field used for tracking model changes
    _dirty_bit = models.BooleanField(default=True)

    objects = SyncableModelManager()

    class Meta:
        abstract = True

    def save(self, dirty_bit_signal=True, *args, **kwargs):
        if dirty_bit_signal is None:
            pass  # don't do anything with the dirty bit
        elif dirty_bit_signal:
            self._dirty_bit = True
        elif not dirty_bit_signal:
            self._dirty_bit = False
        super(SyncableModel, self).save(*args, **kwargs)

    def serialize(self):
        """Should return a Python dict """
        # NOTE: code adapted from https://github.com/django/django/blob/master/django/forms/models.py#L75
        opts = self._meta
        data = {}

        for f in opts.concrete_fields:
            if f.attname in self._fields_not_to_serialize:
                continue
            data[f.attname] = f.value_from_object(self)
        return data

    @classmethod
    def deserialize(cls, dict_model):
        kwargs = {}
        for f in cls._meta.concrete_fields:
            if f.attname in dict_model:
                kwargs[f.attname] = dict_model[f.attname]
        return cls(**kwargs)

    @classmethod
    def merge_conflict(cls, current, incoming):
        return incoming

    def get_partition_names(self, *args, **kwargs):
        """Should return a dictionary with any relevant shard index keys included, along with their values."""
        raise NotImplemented("You must define a 'get_partition_names' method on models that inherit from SyncableModel.")


class AbstractStoreModel(models.Model):
    """
    Base model for storing serialized data.

    This model is an abstract model, and is inherited by ``StoreModel`` and
    ``DataTransferBuffer``.
    """

    id = UUIDField(max_length=32, primary_key=True)
    serialized = models.TextField(blank=True)
    deleted = models.BooleanField(default=False)
    version = models.CharField(max_length=40)
    history = models.TextField(blank=True)
    last_saved_instance = models.UUIDField()
    last_saved_counter = models.IntegerField()
    last_saved_counter_per_instance = models.TextField(default="{}")  # RMC
    model_name = models.CharField(max_length=40)

    class Meta:
        abstract = True


class AbstractDatabaseMaxCounter(models.Model):

    instance_id = models.UUIDField()
    max_counter = models.IntegerField()
    transfer_session_id = UUIDField()
    kind = models.BooleanField()

    class Meta:
        abstract = True

    @classmethod
    def filter_max_counters(cls, query):
        query = json.loads(query)
        filters = []
        for key, value in six.iteritems(query):
            filters.append(Q(**{key: value}) | Q(**{key: "*"}))

            query = reduce(lambda x, y: x & y, filters)
            rows = cls.objects.filter(query)
            return rows.values('instance_id').annotate(max_counter=Max('max_counter'))


###################################################################################################
# CERTIFICATES: Data to manage authorization and the chain-of-trust certificate system
###################################################################################################


class CertificateModel(models.Model):
    signature = models.CharField(max_length=64, primary_key=True)  # long enough to hold SHA256 sigs
    issuer = models.ForeignKey("CertificateModel")

    certificate = models.TextField()
