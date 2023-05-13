Syncing
=======


Concepts
--------

The **store** holds serialized versions of syncable models. This includes both data that is on the current device and data synced from other devices. The store is represented as a standard Django model, containing syncable models as JSON.

The **outgoing buffer** and **incoming buffer** mirror the schema of the store. They also include a transfer session ID which used to identify sets of data that are being synced as a coherent group to other Morango instances.


Process
-------

Syncing is the actual exchange of data in a sync session. The general steps for syncing data are:

1. **Serialization** - serializing data that is associated with Django models in the Application layer, and storing it in JSON format in a record in the Store. The serialized data in the store is versioned via a counter (described in `Counters <../counters#counters>`__).
2. **Queuing/Buffering** - storing serialized records and their modification history to a separate Buffers data structure. This Django model only contains the changes to be synced with the other Morango instance. This is in contrast to the Store, which contains all data, regardless of what is getting transferred in this sync session.
3. **Transfer/chunking of data** - the actual transfer of data over a request/response cycle in a set of chunked records. If both sides support it, the chunked records are compressed before being sent of the network. The actual transfer is done over HTTP.
4. **Dequeuing** - merging the data received in the receiving buffers to the receiving store and record-max counter. During this step, the data from the incoming buffer is merged into the store on the receiving side. Merge conflicts in case of version splits can be solved automatically. As new data is written into the store, the dirty bit on that object is set to indicate that the data needs to be deserialized and pushed to the Application Layer.
5. **Deserialization** - merging data from the receiving Store into the Django models in the Application layer. Data marked as stale in the Application Layer (where a newer version is available in the Store, on a record with the dirty bit set), the data in the store is deserialized from JSON into a Django model and integrated into the Application Layer.

The individual steps of the syncing process are implemented in `morango/sync/operations.py <https://github.com/learningequality/morango/blob/HEAD/morango/sync/operations.py>`_. They are implemented as operations that are registered for every process step described above. A project using Morango can define their own operations and register them to be executed as part of an arbitrary step in the process via configuration options such as ``MORANGO_INITIALIZE_OPERATIONS``.


In the illustration below, the application layer (on the right) is where app data resides as Django models, and the Morango layer (on the left) is where the Morango stores, counters, and buffers reside. *Instance A* (on the top) is sending data to *Instance B* (on the bottom). Application Django models in *Instance A* are serialized in JSON format and saved to the store. Data is queued in the buffers on *Instance A*, and then transmitted to the corresponding buffers on *Instance B*. The data is then integrated into the store and Django app models on *Instance B*.

.. image:: ./sync_process.png

**Store, Buffer \& Dirty Bit**

Both store and buffer are tables in the backend database (generally either SQLite or Postgres). Check `Counters <../counters#counters>`__ for the update logic.

* **Store**: Holds every Serializable Models in the instance and synced instances including counters / maxcounters.
* **Buffer**: Holds Serializable Models marked for transfer (sending or receiving) during a sync session.
* **Dirty Bit**: Flag in store that is set, when a Serializable Model was updated during a dequeue from the Buffer. Gets unset as soon as the Django Model gets updated and is consistent with the store again.

Orchestration
-------------

In order to facilitate synchronization between several Morango instances, it can be convenient to create a Django management command which uses the Morango machinery.

For example, in Kolibri we have created a management command called `kolibri manage sync <https://github.com/learningequality/kolibri/blob/91ddf6fe8e9404fd54278d91dc6d43b9540ea327/kolibri/core/auth/management/commands/sync.py>`_. Note that any time this command is run, we always both pull and push, which guarantees that both Kolibri databases will have the same data afterwards.

Of particular importance is the ``MorangoProfileController`` which can create a ``NetworkSyncConnection`` with another Morango instance.

Once the client establishes a network connection, both instances must exchange certificates so that they can prove that they have the proper permissions in order to push or pull the data. If the client side lacks the proper certificates, they should use the network connection to do a ``certificate_signing_request``, where they enter admin credentials of the other instance to generate a certificate with the valid permissions.

Once both sides have the proper certificates, the client can initiate a sync session with ``create_sync_session``. This creates a ``SyncClient`` that can handle either pushing or pulling data to/from the other Morango instance.



Signals
-------

During the sync process, Morango fires a few different signals from ``signals`` in ``PullClient`` and ``PushClient``. These can be used to track the progress of the sync.

The operations described in the previous section are triggered via such a signal, which has the operations attached to it. The ``SyncSignal`` definition can be found under `morango/sync/utils.py <https://github.com/learningequality/morango/blob/HEAD/morango/sync/utils.py>`_.

There are four signal groups:

- ``session``
- ``queuing``
- ``transferring``
- ``dequeuing``

Each signal group has 3 stages that can be fired:

- ``started``
- ``in_progress``
- ``completed``

The ``SessionController`` is responsible to register the configured operations to the corresponding signal, and triggers the individual steps when its ``proceed_to`` function is called.

For a push or pull sync lifecycle, the order of the fired signals would be as follows:

1) Session started
2) Queuing started
3) Queueing completed
4) Transferring started
5) Transferring in progress
6) Transferring completed
7) Dequeuing started
8) Dequeuing completed
9) Session completed

