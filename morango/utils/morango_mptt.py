import uuid

from mptt import managers, models
from morango.models import SyncableModel

from .uuids import UUIDField


class MorangoTreeManager(managers.TreeManager):
    # Override the logic in Django MPTT that assigns tree ids, as we need to ensure tree ids do not
    # conflict for facilities created on differentdevices and then later synced to the same device
    # By default, tree ids are auto-increasing integers, but we use UUIDs to avoid collisions.
    def _get_next_tree_id(self):
        return uuid.uuid4().hex


class MorangoMPTTModel(models.MPTTModel):
    """
    Any model that inherits from SyncableModel that wants to inherit from MPTTModel should instead inherit
    from MorangoMPTTModel, which modifies some behavior to make it safe for the syncing system.
    """
    _default_manager = MorangoTreeManager()

    # change tree_id to a uuid to avoid collisions; see explanation above in the MorangoTreeManager class
    tree_id = UUIDField()

    class Meta:
        abstract = True

    def save(self, set_dirty_bit=True, *args, **kwargs):

        assert isinstance(self, SyncableModel), "Model `{}` should also inherit from `SyncableModel` in morango.".format(self._meta.model_name)
        # we get calculate_uuid and dirty bit from syncable model
        if not self.id:
            self.id = self.calculate_uuid()

        if set_dirty_bit:
            self._dirty_bit = True

        super(MorangoMPTTModel, self).save(*args, **kwargs)
