from rest_framework import serializers

from ..crypto import Key


class PublicKeyField(serializers.Field):

    def to_representation(self, obj):
        return str(obj)

    def to_internal_value(self, data):
        return Key(public_key_string=data)