from django.db import models

from .query import SyncableModelQuerySet


class SyncableModelManager(models.Manager.from_queryset(SyncableModelQuerySet)):
    pass
