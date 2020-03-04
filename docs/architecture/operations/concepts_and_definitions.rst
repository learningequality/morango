Concepts and Definitions
========================

High Level Sync Process
~~~~~~~~~~~~~~~~~~~~~~~

The Application layer is where app logic and app data (in the form of App Models) resides.
There is the Morango layer where the ``Store``, ``RecordMaxCounter``, ``Buffer``, and ``RecordMaxCounterBuffer`` reside.
The application models are serialized in JSON format and saved to the ``Store`` layer. Sync between 2 Morango instances
proceeds through an exchange of certificates. Morango instance A, which is responsible for sending the data, queues this
data to the ``Buffer`` and ``RecordMaxCounterBuffer`` from the ``Store`` and ``RecordMaxCounter`` layer. The transfer happens from the
sender's (Instance A) *buffers* to receiver’s (Instance B) *buffers*. The data which is received is stored in the buffer
for processing and is later integrated into the ``Store`` as well as App Models, through deserialization.

.. image:: ../../overview/img/sync_process.png

1. **Serialization**: This is the process of serializing data that is associated with Django models
   in the Application layer, and storing it in JSON format in a record in the ``Store`` layer, along
   with additional metadata fields needed to facilitate syncing and integration.
2. **Queueing**: Queuing is similar to snapshotting where a chunk of serialized records that need
   to be sent during the sync process are stored in the *buffers*. This prevents inconsistencies
   caused by a record being sent on network and its copy being changed by an in-process serialization.
3. **Sending data through Network**: The morango models will be serialized into JSON to send them over
   the network to another morango instance. We will be sending them by x number of records at a time, or another chunk number specified
   by the user. This is all faciliated by the ``SyncClient`` created upon generating a sync session between two morango instances.
4. **Dequeuing**: Process of merging the data received in the *buffers*(by another morango instance)
   to ``Store`` layer as well as Application layer.
5. **Deserialization**: After integrating the received data into the ``Store``, we can then deserialize the data into models
   to be used in the application.

*Algorithms for each operation detailed below*:

.. automodule:: morango.sync.operations
    :members: _serialize_into_store, _deserialize_from_store, _queue_into_buffer, _dequeue_into_store
