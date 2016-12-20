"""
`register_morango_profile` should be called when an app wants to create a class that they would
like to inherit from to make their data syncable. This method takes care of registering morango
data structures on a per-profile basis.
"""
import six

from django.apps import apps
from django.db import models
from morango.models import AbstractStoreModel, SyncableModel
from morango.constants import buffer_kinds, morango_structures
from .controller import _profiles


def register_morango_profile(profile="", partitions={}, module=""):

    label = apps.get_containing_app_config(module).label

    class MorangoIndexedModelMeta(models.base.ModelBase):
        """Metaclass for adding Morango "shard" index fields and associated indices."""

        def __new__(mcls, name, bases, namespace):

            # For each of the index fields, add a char (UUID) field to the serialized model
            # this should be loaded from configuration settings
            for field in partitions:
                namespace[field] = models.CharField(max_length=32, blank=True)

            # Create the model class itself
            cls = super(MorangoIndexedModelMeta, mcls).__new__(mcls, name, bases, namespace)
            # Add a joint index on the index fields to facilitate querying
            # TODO(jamalex): performance checks to see whether this is the best indexing approach
            cls._meta.index_together = [partitions]

            return cls

# ###################################################################################################
# # BUFFERS: Where records are copied in preparation for transfer, and stored when first received
# ###################################################################################################

    class DataTransferBuffer(six.with_metaclass(MorangoIndexedModelMeta, AbstractStoreModel)):
        """
        ``DataTransferBuffer`` is where records from the internal store are kept temporarily,
        until they are sent to another morango instance.
        """

        transfer_session_id = models.IntegerField()
        kind = models.CharField(max_length=20, choices=buffer_kinds.choices)

        class Meta:
            app_label = label

###################################################################################################
# STORE: Where serialized data is persisted, along with metadata about counters and history
###################################################################################################

    class StoreModel(six.with_metaclass(MorangoIndexedModelMeta, AbstractStoreModel)):
        """
        ``StoreModel`` is where serialized data is persisted, along with metadata about counters and history.
        """

        class Meta:
            app_label = label

    class BaseSyncableModel(SyncableModel):
        """
        ``BaseSyncableModel`` is where classes should inherit from if they want to make their data syncable.
        """

        _morango_profile = profile
        _morango_partitions = partitions

        class Meta:
            abstract = True

    _profiles[profile] = {morango_structures.BUFFER: DataTransferBuffer, morango_structures.STORE: StoreModel}
    return BaseSyncableModel
