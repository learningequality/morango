from django.db import models

from .utils.uuids import UUIDModelMixin
from .settings import INDEX_FIELDS


###################################################################################################
# METADATA: Additional data to keep track of the state of the system (sessions, counters, etc)
###################################################################################################



###################################################################################################
# APP MODELS: Abstract models from which app models should inherit in order to make them syncable
###################################################################################################


class SyncableModel(models.Model):
    
    def serialize(self, *args, **kwargs):
        """Should return a Python dict """
        raise NotImplemented("You must define a 'serialize' method on models that inherit from SyncableModel.")
    
    def get_shard_indices(self, *args, **kwargs):
        """Should return a dictionary with any relevant shard index keys included, along with their values."""
        raise NotImplemented("You must define a 'get_shard_indices' method on models that inherit from SyncableModel.")
    
    class Meta:
        abstract = True
        
        

###################################################################################################
# STORE: Where serialized data is persisted, along with metadata about counters and history
###################################################################################################

class MorangoIndexedModelMeta(models.base.ModelBase):
    """Metaclass for adding Morango "shard" index fields and associated indices."""
    
    def __new__(mcls, name, bases, namespace):
        
        # For each of the index fields, add a char (UUID) field to the serialized model
        for field in INDEX_FIELDS:
            namespace[field] = models.CharField(max_length=32, blank=True)

        # Create the model class itself
        cls = super(MorangoIndexedModelMeta, mcls).__new__(mcls, name, bases, namespace)
        
        # Add a joint index on the index fields to facilitate querying
        # TODO(jamalex): performance checks to see whether this is the best indexing approach
        cls._meta.index_together.append(INDEX_FIELDS)
        
        return cls

class SerializedModel(models.Model, UUIDModelMixin):

    __metaclass__ = MorangoIndexedModelMeta

    serialized = models.TextField(blank=True)
    deleted = models.BooleanField(default=False)
    version = models.CharField(max_length=40)
    history = models.TextField(blank=True)


###################################################################################################
# BUFFERS: Where records are copied in preparation for transfer, and stored when first received
###################################################################################################

# *all fields from store model, plus:
# transfer_session_id
# seq_num
# (and an auto-pk)


###################################################################################################
# CERTIFICATES: Data to manage authorization and the chain-of-trust certificate system
###################################################################################################


class CertificateModel(models.Model):
    signature = models.CharField(max_length=64, primary_key=True) # long enough to hold SHA256 sigs
    issuer = models.ForeignKey("CertificateModel")
    
    certificate = models.TextField()
    
