import hashlib
import uuid

from django.db import models

from morango.utils import _assert


def sha2_uuid(*args):
    return hashlib.sha256("::".join(args).encode("utf-8")).hexdigest()[:32]


class UUIDField(models.UUIDField):
    """
    Adaptation of Django's UUIDField, but with 32-char hex representation as Python representation rather than a UUID instance.
    """

    def get_db_prep_value(self, value, connection, prepared=False):
        if value is None:
            return None
        if not isinstance(value, uuid.UUID):
            value = super(UUIDField, self).to_python(value)

        if connection.features.has_native_uuid_field:
            return value
        return value.hex

    def from_db_value(self, value, expression, connection, context):
        return self.to_python(value)

    def to_python(self, value):
        value = super(UUIDField, self).to_python(value)
        return value.hex if isinstance(value, uuid.UUID) else value


class UUIDModelMixin(models.Model):
    """
    Mixin for Django models that makes the primary key "id" into a UUID, which is calculated
    as a function of jointly unique parameters on the model, to ensure consistency across instances.
    """

    # a tuple of the names of model fields from which to calculate the UUID, or the string "RANDOM" for a random UUID
    uuid_input_fields = None

    # field to hold the model's UUID primary key
    id = UUIDField(max_length=32, primary_key=True, editable=False)

    class Meta:
        abstract = True

    def calculate_uuid(self):
        """Should return a 32-digit hex string for a UUID that is calculated as a function of a set of fields from the model."""

        # raise an error if no inputs to the UUID calculation were specified
        if self.uuid_input_fields is None:
            raise NotImplementedError(
                """You must define either a 'uuid_input_fields' attribute
                (with a tuple of field names) or override the 'calculate_uuid' method, on models
                that inherit from UUIDModelMixin. If you want a fully random UUID, you can set
                'uuid_input_fields' to the string 'RANDOM'."""
            )

        # if the UUID has been set to be random, return a random UUID
        if self.uuid_input_fields == "RANDOM":
            return uuid.uuid4().hex

        # if we got this far, uuid_input_fields should be a tuple
        _assert(
            isinstance(self.uuid_input_fields, tuple),
            "'uuid_input_fields' must either be a tuple or the string 'RANDOM'",
        )

        # calculate the input to the UUID function
        hashable_input_vals = []
        for field in self.uuid_input_fields:
            new_value = getattr(self, field)
            if new_value:
                hashable_input_vals.append(str(new_value))
        hashable_input = ":".join(hashable_input_vals)

        # if all the values were falsey, just return a random UUID, to avoid collisions
        if not hashable_input:
            return uuid.uuid4().hex

        # compute the UUID as a function of the input values
        return sha2_uuid(hashable_input)

    def save(self, *args, **kwargs):

        if not self.id:
            self.id = self.calculate_uuid()

        super(UUIDModelMixin, self).save(*args, **kwargs)
