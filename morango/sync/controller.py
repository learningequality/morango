from .operations import _deserialize_from_store
from .operations import _serialize_into_store
from .operations import OperationLogger
from .syncsession import NetworkSyncConnection


def _self_referential_fk(klass_model):
    """
    Return whether this model has a self ref FK, and the name for the field
    """
    for f in klass_model._meta.concrete_fields:
        if f.related_model:
            if issubclass(klass_model, f.related_model):
                return f.attname
    return None


class MorangoProfileController(object):
    def __init__(self, profile):
        assert profile, "profile needs to be defined."
        self.profile = profile

    def serialize_into_store(self, filter=None):
        """
        Takes data from app layer and serializes the models into the store.
        """
        with OperationLogger("Serializing records", "Serialization complete"):
            _serialize_into_store(self.profile, filter=filter)

    def deserialize_from_store(self):
        """
        Takes data from the store and integrates into the application.
        """
        with OperationLogger("Deserializing records", "Deserialization complete"):
            _deserialize_from_store(self.profile)

    def create_network_connection(self, base_url):
        return NetworkSyncConnection(base_url=base_url)

    def create_disk_connection(path):
        raise NotImplementedError("Coming soon...")
