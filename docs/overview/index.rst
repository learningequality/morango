Overview
========

Morango is a database replication engine for Django, written in pure Python, that supports peer-to-peer syncing of data. Morango is structured as a Django application that can be included in a Django project in order to make specific application models syncable.

Morango includes the following features:

1. A certificate-based authentication system to protect privacy and integrity of data
2. A change-tracking system to support calculation of differences between databases
   across low-bandwidth connections and handle merge conflict resolution

Objectives
----------
There are several high-level goals (often competing with one another) in the design of Morango, including:

- **Privacy:** Data should only be synced to the devices which are authorized to access that data, and made available to users with the appropriate permissions
- **Ease of use:** The sync process should be as streamlined and hands-off as possible
- **Efficiency:** Storage, bandwidth, and processing power all come at a premium and requirements should be minimized
- **Integrity:** We need to avoid corruption of data, protecting it from either accidental or malicious damage
- **Flexibility:** In some cases, we may, for instance, only want to sync a subset of the data from a database (e.g. the data for a particular user)
- **Peer to Peer network:** Devices should be able to communicate without a central server
- **Eventual Consistency:**  If no new updates are made to a given data item, eventually all accesses to that item will return the last updated value

Example User Story
------------------
Lets set up a scenario where we have three devices:

- ``Remote`` is in a remote area
- ``City`` is in the city
- ``Cloud`` is in the cloud/internet

Assume that of the three devices, only the ``Remote`` device has new data.

A user wants to get data from the ``Remote`` device into the ``City`` device. In order to achieve this, the user may bring ``City`` to the remote area. Once ``City`` arrives in the remote area, ``Remote`` and ``City`` can sync over a local network.

As ``City`` is being brought back to the city, someone transiently connects ``Remote`` to the internet via satellite and syncs its data with ``Cloud``. When ``City`` returns to the city, it is also synced with ``Cloud``. Since ``Cloud`` already has the data from ``Remote``, no data is transferred. Here, the trip to the remote area might have been skipped if the data sync was the trip's only purpose.

Ultimately, ``Remote``, ``City``, and ``Cloud`` have the same data, and are said to be in sync.

Design challenges
-----------------
With any syncing system, there are certain problems that need to be solved in order to address the objectives above:

- In order to make the system efficient as possible, we only want to sync *new* data between devices
- We never want to lose data, as that could affect the consistency of the system as a whole
- Data integrity of the system requires must be preserved
- For cases where data may collide when being edited and synced from multiple sources, the system should be able to resolve the conflicts
- Unauthorized users or systems should not be able to access or sync data for which they do not have permissions
- Data collisions from malicious or normal syncing must be prevented

High-Level Data Structures and Flow
-----------------------------------
Certificate Exchange
~~~~~~~~~~~~~~~~~~~~
One of the first actions that must occur is an exchange of certificates between two Morango instances. This ensures that both sides have the proper permissions to sync the data.

.. image:: ./img/cert_exchange.png

**Certificates** grant varying levels of permissions for syncing

Sync Process
~~~~~~~~~~~~
Syncing is the actual exchange of data in a sync session. A sequence of steps initializes, prepares, and exchanges data between the Morango instances. A general push sync scenario follows steps in this order:

1. **Serialization** - process of serializing data that is associated with Django models in the Application layer, and storing it in JSON format in a record in the ``Store``
2. **Queuing/Buffering** - process of storing, in separate `Buffers` data structure, serialized records and their modification history which need to be sent during the data transfer
3. **Transfer/chunking of data** - actual transfer of data over a request/response cycle in 500 (default) record chunks
4. **Dequeuing** - process of merging the data received in the ``Buffers`` to ``Store`` and ``RecordMaxCounter`` layer
5. **Deserialization** - process of merging data from the ``Store`` layer into the Django models in the Application layer

.. image:: ./img/sync_process.png

- ``Store`` - serialized (JSON) versions of app models
- ``RecordMaxCounter`` - instance modification history
- ``Buffer`` - transfer holding space for serialized versions of app models
- ``RecordMaxCounterBuffer`` - transfer holding space for instance modification history
- ``App models`` - underlying application's django models
