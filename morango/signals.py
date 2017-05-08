from django.db.models.signals import post_delete
from django.dispatch import receiver

from .models import DeletedModels, SyncableModel


@receiver(post_delete)
def add_to_deleted_models(sender, instance=None, *args, **kwargs):
    """
    Whenever a model is deleted, we record its ID in a separate model.
    """
    if issubclass(sender, SyncableModel):
        DeletedModels.objects.update_or_create(defaults={'id': instance.id, 'profile': instance.morango_profile},
                                               id=instance.id)
