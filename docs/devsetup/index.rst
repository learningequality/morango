Dev Setup
========

Installation
Dependencies

Tests
SQLite
Postgres
Integrationtest->Kolibri

Soft-deletion
-------------

Typically, deletion merely hides records, rather than actually erasing data.

When a record for a subclass of ``SyncableModel`` is deleted, its ID is added to the ``DeletedModels`` table. When a subsequent serialization occurs, this information is used to turn on the ``deleted`` flag in the store for that record. When syncing with other Morango instances, the soft deletion will propagate to the store record of other instances.

This is considered a "soft-delete" in the store because the data is not actually cleared.


Hard-deletion
-------------

There are times, such as GDPR removal requests, when it's necessary to actually to erase data.

This is handled using a ``HardDeletedModels`` table. Subclasses of ``SyncableModel`` should override the ``delete`` method to take a ``hard_delete`` boolean, and add the record to the ``HardDeletedModels`` table when this is passed.

On serialization, Morango clears the ``serialized`` field entry in the store for records in ``HardDeletedModels`` and turns on the ``hard_deleted`` flag. Upon syncing with other Morango instances, the hard deletion will propagate to the store record of other instances.

