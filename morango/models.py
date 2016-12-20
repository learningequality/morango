import json

from django.db import models

from .utils.uuids import UUIDModelMixin, UUIDField


###################################################################################################
# APP MODELS: Abstract models from which app models should inherit in order to make them syncable
###################################################################################################
class SyncableModelQuerySet(models.query.QuerySet):

    def update(self, **kwargs):
        kwargs.update({'_dirty_bit': True})
        super(SyncableModelQuerySet, self).update(**kwargs)
    update.queryset_only = True  # Unsure whether django will not place this on manager class by default


class SyncableModel(UUIDModelMixin):
    """
    Base model class for syncing. Other models inherit from this class if they want to make
    their data syncable across devices.
    """
    _morango_partitions = {}

    # morango specific field used for tracking model changes
    _dirty_bit = models.BooleanField(default=True)

    objects = SyncableModelQuerySet.as_manager()
    # special reference to syncable manager in case 'objects' is overridden in subclasses
    syncable_objects = SyncableModelQuerySet.as_manager()

    class Meta:
        abstract = True
        """Should return a Python dict """
        raise NotImplemented("You must define a 'serialize' method on models that inherit from SyncableModel.")
    
    def get_shard_indices(self, *args, **kwargs):
        """Should return a dictionary with any relevant shard index keys included, along with their values."""
        raise NotImplemented("You must define a 'get_shard_indices' method on models that inherit from SyncableModel.")


class DatabaseMaxCounter(models.Model):
    """
    `DatabaseMaxCounter` is used to keep track of what data an instance already has
    from other instances for a particular filter.
    """

    instance_id = models.UUIDField()
    max_counter = models.IntegerField()
    filter = models.TextField()


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

    class Meta:
        abstract = True


###################################################################################################
# CERTIFICATES: Data to manage authorization and the chain-of-trust certificate system
###################################################################################################


class CertificateModel(models.Model):
    signature = models.CharField(max_length=64, primary_key=True)  # long enough to hold SHA256 sigs
    issuer = models.ForeignKey("CertificateModel")

    certificate = models.TextField()
