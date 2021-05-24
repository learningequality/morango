ALLOW_CERTIFICATE_PUSHING = False
MORANGO_SERIALIZE_BEFORE_QUEUING = True
MORANGO_DESERIALIZE_AFTER_DEQUEUING = True
MORANGO_DISALLOW_ASYNC_OPERATIONS = False
MORANGO_INITIALIZE_OPERATIONS = (
    "morango.sync.operations:LocalInitializeOperation",
    "morango.sync.operations:RemoteSynchronousInitializeOperation",
    "morango.sync.operations:RemoteInitializeOperation",
)
MORANGO_SERIALIZE_OPERATIONS = (
    "morango.sync.operations:LocalSerializeOperation",
    "morango.sync.operations:RemoteSynchronousSerializeOperation",
    "morango.sync.operations:RemoteSerializeOperation",
)
MORANGO_QUEUE_OPERATIONS = (
    "morango.sync.operations:LocalQueueOperation",
    "morango.sync.operations:RemoteSynchronousQueueOperation",
    "morango.sync.operations:RemoteQueueOperation",
)
MORANGO_DEQUEUE_OPERATIONS = (
    "morango.sync.operations:LocalDequeueOperation",
    "morango.sync.operations:RemoteSynchronousDequeueOperation",
    "morango.sync.operations:RemoteDequeueOperation",
)
MORANGO_DESERIALIZE_OPERATIONS = (
    "morango.sync.operations:LocalDeserializeOperation",
    "morango.sync.operations:RemoteSynchronousDeserializeOperation",
    "morango.sync.operations:RemoteDeserializeOperation",
)
MORANGO_CLEANUP_OPERATIONS = (
    "morango.sync.operations:LocalCleanupOperation",
    "morango.sync.operations:RemoteSynchronousCleanupOperation",
    "morango.sync.operations:RemoteCleanupOperation",
)
