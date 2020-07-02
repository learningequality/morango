Developer Guide
===============

Overview
--------

Morango is a database replication engine for Django, written in pure Python, that
supports P2P syncing of data. Morango is structured as a Django application that
can be included in a Django project in order to make specific application models syncable.

Morango includes the following features:

1. a certificate-based authentication system to protect privacy and integrity of data
2. change-tracking system to support calculation of differences between databases
   across low-bandwidth connections, and handle merge conflict resolution

Architecture and Algorithms
---------------------------

.. toctree::
   :maxdepth: 1

   concepts_and_definitions
   data_syncing

Models
------
The Django models used by the application (Kolibri) are stored in their own
tables as usual, and inherit from the abstract SyncableModel class provided by
Morango. Morango then adds an additional table (SerializedModel) to store the
serialized versions of the models.
Models in Kolibri are serialized and stored on a per record basis in Morango
Store along with some metadata. This is the table that is used during the actual
sync process by a Morango instance I.

.. automodule:: morango.models
    :members:
    :noindex:

Signaling
---------
Morango fires a few different signals from the ``signals`` object on both ``PullClient`` and
``PushClient`` during the sync process which can be used to track the progress of the sync. These
signal groups are ``session``, ``queuing``, ``transferring``, and ``dequeuing``. Each signal group
has 3 stages that can be fired: ``started``, ``in_progress``, and ``completed``. For a push or pull
sync, the order of the fired signals would be as follows:

1) Session started
2) Queuing started
3) Queueing completed
4) Transferring started
5) Transferring in progress
6) Transferring completed
7) Dequeuing started
8) Dequeuing completed
9) Session completed

.. autoclass:: morango.sync.syncsession.BaseSyncClient
    :members: signals
    :noindex:

.. autoclass:: morango.sync.syncsession.SyncClientSignals
    :members: session, queuing, pushing, pulling, dequeuing
    :noindex:

.. autoclass:: morango.sync.syncsession.SyncSignalGroup
    :members:
    :inherited-members:
    :noindex:

.. autoclass:: morango.sync.syncsession.SyncSignal
    :members:
    :noindex:
