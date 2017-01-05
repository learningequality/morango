from mptt import managers, models, querysets
from morango.models import SyncableModelQuerySet


class MorangoTreeQuerySet(querysets.TreeQuerySet, SyncableModelQuerySet):
    pass


class MorangoMPTTTreeManager(managers.TreeManager):

    def get_queryset(self):
        return MorangoTreeQuerySet(self.model, using=self._db)

    @managers.delegate_manager
    def _mptt_update(self, qs=None, **items):
        if qs is None:
            qs = self
        return qs.update(dirty_bit_signal=None, **self._translate_lookups(**items))


class MorangoMPTTModel(models.MPTTModel):
    """
    Any model that inherits from SyncableModel that wants to inherit from MPTTModel should instead inherit
    from MorangoMPTTModel, which modifies some behavior to make it safe for the syncing system.
    """
    objects = MorangoMPTTTreeManager()

    class Meta:
        abstract = True
