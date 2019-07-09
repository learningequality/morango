from mptt import managers
from mptt import models
from mptt import querysets

from .manager import SyncableModelManager
from .query import SyncableModelQuerySet


class MorangoTreeQuerySet(querysets.TreeQuerySet, SyncableModelQuerySet):
    pass


class MorangoMPTTTreeManager(managers.TreeManager, SyncableModelManager):
    def get_queryset(self):
        return MorangoTreeQuerySet(self.model, using=self._db)

    def _mptt_update(self, qs=None, **items):
        items["update_dirty_bit_to"] = None
        return super(MorangoMPTTTreeManager, self)._mptt_update(qs, **items)


class MorangoMPTTModel(models.MPTTModel):
    """
    Any model that inherits from ``SyncableModel`` that also wants to inherit from ``MPTTModel`` should instead inherit
    from ``MorangoMPTTModel``, which modifies some behavior to make it safe for the syncing system.
    """

    _internal_mptt_fields_not_to_serialize = ("lft", "rght", "tree_id", "level")

    objects = MorangoMPTTTreeManager()

    class Meta:
        abstract = True
