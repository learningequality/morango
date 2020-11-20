Overview
========

Morango is a database replication engine for Django, written in pure Python, that
supports *Peer to Peer* syncing of data. Morango is structured as a Django application that
can be included in a Django project in order to make specific application models syncable.

Morango includes the following features:

1. a certificate-based authentication system to protect privacy and integrity of data
2. change-tracking system to support calculation of differences between databases
   across low-bandwidth connections, and handle merge conflict resolution

Objectives
----------
There are several high-level goals (often competing with one another) in the design of Morango, including:

- **Privacy**: Data should only be synced to the devices which are authorized to access that data, and made available to users with the appropriate permissions.
- **Ease** of use: The sync process should be as streamlined and hands-off as possible.
- **Efficiency**: Storage, bandwidth, and processing power all come at a premium in these contexts, and requirements should be minimized.
- **Integrity**: We need to avoid corruption of data, protecting it from either accidental or malicious damage.
- **Flexibility**: In some cases, we may, for instance, only want to sync a subset of the data from a database (e.g. the data for a particular user).
- **Peer to Peer network**: Devices should be able to communicate without a central server.
- **Eventual Consistency**:  If no new updates are made to a given data item, eventually all accesses to that item will return the last updated value.

Example User Story
------------------
Lets set up a scenario where we have three devices. ``Device A`` is in a remote area, ``Device B`` is in the city, and ``Device C`` is in the cloud/internet.
``Device B`` seeks to retrieve the data from ``Device A``. In order to achieve this, ``Device B`` must travel to the remote area. Once
``Device B`` arrives to the remote area, ``Device B`` syncs the data from ``Device A``. As ``Device B`` makes its way back to the city, ``Device A``
is able to connect to the internet and syncs its data to ``Device C``. When ``Device B`` returns to the city, it attempts to sync to ``Device C``,
but since ``Device C`` already has the data from ``Device A``, no data is transferred. Ultimately, ``Device A``, ``B``, and ``C`` have the same data, and are said to be in sync.

Problems to be addressed
------------------------
With any syncing system, there are certain problems that need to be solved in order to address the objectives above:

- In order to make the system efficient as possible, we only want to sync *new* data between devices
- We never want to lose data, as that could affect the consistency of the system as a whole
- Part of keeping the integrity of the system requires syncing data as snapshots
- For cases where data may collide when being edited and synced from multiple sources, the system should be able to resolve the conflicts
- Unauthorized users or systems should not be able to access/sync data they do not have permissions for
- Data across different systems must be unique, which prevents data collisions from malicious or normal syncing

High-Level Data Structures and Flow
-----------------------------------
Certificate Exchange
~~~~~~~~~~~~~~~~~~~~
One of the first actions that must occur is an exchange of certificates between two Morango instances.
This ensures that both sides have the proper permissions to sync the data.

.. image:: ./img/cert_exchange.png

``Certificates`` - grants varying levels of permissions for syncing

Sync Process
~~~~~~~~~~~~
The next step is the actual exchange of data in a sync session. A sequence of steps initializes, prepares, and exchanges data between
the Morango instances.
A general push sync scenario follows steps in this order:

1. ``Serialization`` - process of serializing data that is associated with Django models in the Application layer, and storing it in JSON format in a record in the ``Store``

2. ``Queuing/Buffering`` - process of storing, in separate `Buffers` data structure, serialized records and their modification history which need to be sent during the data transfer

3. ``Transfer/chunking of data`` - actual transfer of data over a request/response cycle in 500 (default) record chunks

4. ``Dequeuing`` - process of merging the data received in the ``Buffers`` to ``Store`` and ``RecordMaxCounter`` layer

5. ``Deserialization`` - process of merging data from the ``Store`` layer into the Django models in the Application layer

.. image:: ./img/sync_process.png

``Store`` - serialized (JSON) versions of app models

``RecordMaxCounter`` - instance modification history

``Buffer`` - transfer holding space for serialized versions of app models

``RecordMaxCounterBuffer`` - transfer holding space for instance modification history

``App models`` - underlying application's django models
