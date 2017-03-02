from django.db import models

from .query import SyncableModelQuerySet


class SyncableModelManager(models.Manager):

    def get_queryset(self):
        return SyncableModelQuerySet(self.model, using=self._db)
