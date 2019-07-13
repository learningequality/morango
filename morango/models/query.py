from django.db import models


class SyncableModelQuerySet(models.query.QuerySet):
    def as_manager(cls):
        # Address the circular dependency between `SyncableModelQueryset` and `SyncableModelManager`.
        from .manager import SyncableModelManager

        manager = SyncableModelManager.from_queryset(cls)()
        manager._built_with_as_manager = True
        return manager

    as_manager.queryset_only = True
    as_manager = classmethod(as_manager)

    def update(self, update_dirty_bit_to=True, **kwargs):
        if update_dirty_bit_to is None:
            pass  # don't do anything with the dirty bit
        elif update_dirty_bit_to:
            kwargs.update({"_morango_dirty_bit": True})
        elif not update_dirty_bit_to:
            kwargs.update({"_morango_dirty_bit": False})
        super(SyncableModelQuerySet, self).update(**kwargs)
