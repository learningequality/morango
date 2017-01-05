import os
import platform
import sys
import uuid

from django.db import models
from django.conf import settings
from django.utils import timezone

from . import NAMESPACE_MORANGO


class UUIDField(models.CharField):
    """
    Adaptation of Django's UUIDField, but with 32-char hex representation as Python representation rather than a UUID instance.
    """

    def __init__(self, *args, **kwargs):
        kwargs['max_length'] = 32
        super(UUIDField, self).__init__(*args, **kwargs)

    def prepare_value(self, value):
        if isinstance(value, uuid.UUID):
            return value.hex
        return value

    def deconstruct(self):
        name, path, args, kwargs = super(UUIDField, self).deconstruct()
        del kwargs['max_length']
        return name, path, args, kwargs

    def get_internal_type(self):
        return "UUIDField"

    def get_db_prep_value(self, value, connection, prepared=False):
        if value is None:
            return None
        if not isinstance(value, uuid.UUID):
            try:
                value = uuid.UUID(value)
            except AttributeError:
                raise TypeError(self.error_messages['invalid'] % {'value': value})
        return value.hex

    def from_db_value(self, value, expression, connection, context):
        return self.to_python(value)

    def to_python(self, value):
        if isinstance(value, uuid.UUID):
            return value.hex
        return value


class UUIDModelMixin(models.Model):
    """Mixin for Django models that makes the primary key "id" into a UUID, which is calculated
    as a function of jointly unique parameters on the model, to ensure consistency across instances.
    """

    # a tuple of the names of model fields from which to calculate the UUID, or the string "RANDOM" for a random UUID
    uuid_input_fields = None

    # field to hold the model's UUID primary key
    id = UUIDField(max_length=32, primary_key=True)

    class Meta:
        abstract = True

    def calculate_uuid(self):
        """Should return a 32-digit hex UUID that is calculated as a function of the jointly
        unique fields on the model."""

        # raise an error if no inputs to the UUID calculation were specified
        if self.uuid_input_fields is None:
            raise NotImplementedError("""You must define either a 'uuid_input_fields' attribute
                (with a tuple of field names) or override the 'calculate_uuid' method, on models
                that inherit from UUIDModelMixin. If you want a fully random UUID, you can set
                'uuid_input_fields' to the string 'RANDOM'.""")

        # if the UUID has been set to be random, return a random UUID
        if self.uuid_input_fields == "RANDOM":
            return uuid.uuid4()

        # if we got this far, uuid_input_fields should be a tuple
        assert isinstance(self.uuid_input_fields, tuple), "'uuid_input_fields' must either be a tuple or the string 'RANDOM'"

        # calculate the input to the UUID function
        hashable_input_vals = []
        for field in self.uuid_input_fields:
            new_value = getattr(self, field)
            if new_value:
                hashable_input_vals.append(str(new_value))
        hashable_input = ":".join(hashable_input_vals)

        # if all the values were falsey, just return a random UUID, to avoid collisions
        if not hashable_input:
            return uuid.uuid4()

        # compute the UUID as a function of the input values
        return uuid.uuid5(NAMESPACE_MORANGO, hashable_input)

    def save(self, *args, **kwargs):

        if not self.id:
            self.id = self.calculate_uuid()

        super(UUIDModelMixin, self).save(*args, **kwargs)


class DatabaseManager(models.Manager):

    def create(self, **kwargs):

        # set current flag to false for all database_id models
        DatabaseIDModel.objects.update(current=False)
        super(DatabaseManager, self).create(**kwargs)


class DatabaseIDModel(UUIDModelMixin):
    """
    Model to be used for tracking database ids.
    """

    uuid_input_fields = "RANDOM"

    objects = DatabaseManager()

    current = models.BooleanField(default=True)
    date_generated = models.DateTimeField(default=timezone.now)
    initial_instance_id = models.CharField(max_length=32, blank=True)

    def save(self, *args, **kwargs):

        if not self.id:
            DatabaseIDModel.objects.update(current=False)

        super(DatabaseIDModel, self).save(*args, **kwargs)


class InstanceIDModel(UUIDModelMixin):

    uuid_input_fields = ("platform", "hostname", "sysversion", "macaddress", "database_id", "db_path")

    platform = models.TextField()
    hostname = models.TextField()
    sysversion = models.TextField()
    macaddress = models.CharField(max_length=20, blank=True)
    database = models.ForeignKey(DatabaseIDModel)
    counter = models.IntegerField(default=0)
    current = models.BooleanField(default=True)
    db_path = models.CharField(max_length=100)

    @staticmethod
    def get_or_create_current_instance():
        """Get the instance model corresponding to the current system, or create a new
        one if the system is new or its properties have changed (e.g. OS from upgrade)."""

        kwargs = {
            "platform": platform.platform(),
            "hostname": platform.node(),
            "sysversion": sys.version,
            "database": DatabaseIDModel.objects.get(current=True),
            "db_path": os.path.abspath(settings.DATABASES['default']['NAME']),
        }

        # try to get the MAC address, but exclude it if it was a fake (random) address
        mac = uuid.getnode()
        if (mac >> 40) % 2 == 0:  # 8th bit (of 48 bits, from left) is 1 if MAC is fake
            kwargs["macaddress"] = mac

        return InstanceIDModel.objects.get_or_create(**kwargs)
