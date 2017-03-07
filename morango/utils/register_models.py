"""
`register_morango_profile` should be called when an app wants to create a class that they would
like to inherit from to make their data syncable. This method takes care of registering morango
data structures on a per-profile basis.
"""

from collections import namedtuple
from django.apps import apps
from django.db import models
from django.utils import six

from .controller import _profiles

Profile = namedtuple('Profile', 'partitions buffer store database_max_counter record_max_counters rmc_buffer')


def register_morango_profile(profile, partitions, module):
    """
    Creates the morango data structures: ``DatabaseMaxCounter``, ``DataTransferBuffer``, ``StoreModel``, and
    ``BaseSyncableModel`` on a per-profile/app basis. The data structures become associated with the app
    where this function was called.

    :param: profile: string that names this profile
    :param: partitions: tuple of partition names
    :param: module: string of the module where the function was called, can be retrieved through ``__package__``
    :return: ``SyncableModel`` class where other models inherit from
    :rtype: class inheriting from ``morango.models.SyncableModel``
    """

    # import here to prevent circular imports
    from morango.models import AbstractStore, AbstractDatabaseMaxCounter, AbstractRecordMaxCounter, TransferSession, SyncableModel

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
            cls._meta.index_together = [list(partitions)]

            return cls

    class DatabaseMaxCounter(six.with_metaclass(MorangoIndexedModelMeta, AbstractDatabaseMaxCounter)):

        class Meta:
            app_label = label

    class DataTransferBuffer(six.with_metaclass(MorangoIndexedModelMeta, AbstractStore)):

        incoming_buffer = models.BooleanField()
        transfer_session = models.ForeignKey(TransferSession, related_name='{}_buffer'.format(profile))

        class Meta:
            app_label = label

    class StoreModel(six.with_metaclass(MorangoIndexedModelMeta, AbstractStore)):

        class Meta:
            app_label = label

    class RecordMaxCounter(AbstractRecordMaxCounter):

        store_model = models.ForeignKey(StoreModel)

        class Meta:
            app_label = label

    class RecordMaxCounterBuffer(RecordMaxCounter):

        incoming_buffer = models.BooleanField()
        transfer_session = models.ForeignKey(TransferSession, related_name='{}_rmc_buffer'.format(profile))

        class Meta:
            app_label = label

    class BaseSyncableModel(SyncableModel):
        """
        ``BaseSyncableModel`` is where classes should inherit from if they want to make their data syncable.
        """

        _morango_profile = profile
        _fields_not_to_serialize = ()

        class Meta:
            abstract = True

    _profiles[profile] = Profile(partitions=partitions,
                                 buffer=DataTransferBuffer,
                                 store=StoreModel,
                                 database_max_counter=DatabaseMaxCounter,
                                 record_max_counters=RecordMaxCounter,
                                 rmc_buffer=RecordMaxCounterBuffer)
    return BaseSyncableModel
