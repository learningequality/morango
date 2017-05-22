from __future__ import unicode_literals

import uuid

from django.contrib.auth.models import AbstractBaseUser
from django.db import models
from django.utils import timezone
from morango.models import SyncableModel
from morango.query import SyncableModelQuerySet
from morango.utils.morango_mptt import MorangoMPTTModel
from morango.utils.uuids import UUIDField
from mptt.models import TreeForeignKey


class FacilityDataSyncableModel(SyncableModel):
    morango_profile = 'facilitydata'

    class Meta:
        abstract = True

class BaseQuerySet(SyncableModelQuerySet):
    pass


class Facility(MorangoMPTTModel, FacilityDataSyncableModel):

    # Morango syncing settings
    morango_model_name = "facility"

    name = models.CharField(max_length=100)
    now_date = models.DateTimeField(default=timezone.now)
    parent = TreeForeignKey('self', null=True, blank=True, related_name='children', db_index=True)

    def calculate_source_id(self, *args, **kwargs):
        return self.name

    def calculate_partition(self, *args, **kwargs):
        return ''

class MyUser(AbstractBaseUser, FacilityDataSyncableModel):
    # Morango syncing settings
    morango_model_name = "user"
    uuid_input_fields = ("username",)

    USERNAME_FIELD = "username"

    username = models.CharField(max_length=20)

    objects = BaseQuerySet.as_manager()

    def calculate_source_id(self, *args, **kwargs):
        return self.username

    def calculate_partition(self, *args, **kwargs):
        return ''


class SummaryLog(FacilityDataSyncableModel):
    # Morango syncing settings
    morango_model_name = "contentsummarylog"
    uuid_input_fields = ("user_id", "content_id")

    user = models.ForeignKey(MyUser)
    content_id = UUIDField(db_index=True, default=uuid.uuid4)

    def calculate_source_id(self, *args, **kwargs):
        return '{}:{}'.format(self.user.id, self.content_id)

    def calculate_partition(self, *args, **kwargs):
        return ''


class InteractionLog(FacilityDataSyncableModel):
    # Morango syncing settings
    morango_model_name = "contentinteractionlog"

    user = models.ForeignKey(MyUser)
    content_id = UUIDField(db_index=True, default=uuid.uuid4)

    def calculate_source_id(self, *args, **kwargs):
        return None

    def calculate_partition(self, *args, **kwargs):
        return ''


class ProxyParent(MorangoMPTTModel):

    kind = models.CharField(max_length=20)

    def save(self, *args, **kwargs):
        self._ensure_kind()
        super(ProxyParent, self).save(*args, **kwargs)

    def _ensure_kind(self):
        if self._KIND:
            self.kind = self._KIND

    def calculate_source_id(self, *args, **kwargs):
        return ''

    def calculate_partition(self, *args, **kwargs):
        return ''

class ProxyManager(models.Manager):
    pass


class ProxyModel(ProxyParent):

    morango_model_name = 'proxy'
    _KIND = 'proxy'

    objects = ProxyManager()

    class Meta:
        proxy = True

    def calculate_source_id(self, *args, **kwargs):
        return ''

    def calculate_partition(self, *args, **kwargs):
        return ''
