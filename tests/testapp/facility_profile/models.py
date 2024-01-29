import uuid

from django.contrib.auth.models import AbstractBaseUser
from django.contrib.auth.models import UserManager
from django.db import models
from django.utils import timezone

from morango.models.core import SyncableModel
from morango.models.fields.uuids import UUIDField
from morango.models.manager import SyncableModelManager


class FacilityDataSyncableModel(SyncableModel):
    morango_profile = 'facilitydata'

    class Meta:
        abstract = True


class SyncableUserModelManager(SyncableModelManager, UserManager):
    pass


class Facility(FacilityDataSyncableModel):

    # Morango syncing settings
    morango_model_name = "facility"

    name = models.CharField(max_length=100)
    now_date = models.DateTimeField(default=timezone.now)
    parent = models.ForeignKey('self', null=True, blank=True, related_name='children', db_index=True, on_delete=models.CASCADE)

    def calculate_source_id(self, *args, **kwargs):
        return self.name

    def calculate_partition(self, *args, **kwargs):
        return ''

    def clean_fields(self, *args, **kwargs):
        # reference parent here just to trigger a non-validation error to make sure we handle it
        _ = self.parent
        super(Facility, self).clean_fields(*args, **kwargs)


class MyUser(AbstractBaseUser, FacilityDataSyncableModel):
    # Morango syncing settings
    morango_model_name = "user"

    USERNAME_FIELD = "username"

    is_staff = models.BooleanField(default=False)
    is_superuser = models.BooleanField(default=False)

    username = models.CharField(max_length=20, unique=True)

    objects = SyncableUserModelManager()

    def calculate_source_id(self, *args, **kwargs):
        if self._morango_source_id:
            return self._morango_source_id
        else:
            return uuid.uuid5(uuid.UUID("a" * 32), self.username).hex

    def calculate_partition(self, *args, **kwargs):
        return '{id}:user'.format(id=self.ID_PLACEHOLDER)

    def has_morango_certificate_scope_permission(self, scope_definition_id, scope_params):
        return self.is_superuser

    @staticmethod
    def compute_namespaced_id(partition_value, source_id_value, model_name):
        return source_id_value


class SummaryLog(FacilityDataSyncableModel):
    # Morango syncing settings
    morango_model_name = "contentsummarylog"

    user = models.ForeignKey(MyUser, on_delete=models.CASCADE)
    content_id = UUIDField(db_index=True, default=uuid.uuid4)

    def calculate_source_id(self, *args, **kwargs):
        return '{}:{}'.format(self.user.id, self.content_id)

    def calculate_partition(self, *args, **kwargs):
        return '{user_id}:user:summary'.format(user_id=self.user.id)


class InteractionLog(FacilityDataSyncableModel):
    # Morango syncing settings
    morango_model_name = "contentinteractionlog"

    user = models.ForeignKey(MyUser, blank=True, null=True, on_delete=models.CASCADE)
    content_id = UUIDField(db_index=True, default=uuid.uuid4)

    def calculate_source_id(self, *args, **kwargs):
        return None

    def calculate_partition(self, *args, **kwargs):
        return '{user_id}:user:interaction'.format(user_id=self.user.id)
