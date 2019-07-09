from django.db.models.signals import post_delete
from django.dispatch import receiver

from .core import SyncableModel


@receiver(post_delete)
def add_to_deleted_models(sender, instance=None, *args, **kwargs):
    """
    Whenever a model is deleted, we record its ID in a separate model for tracking purposes. During serialization, we will mark
    the model as deleted in the store.
    """
    if issubclass(sender, SyncableModel):
        instance._update_deleted_models()
