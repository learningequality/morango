Implementation Details
======================

In order to maintain the consistency and snapshot of Morango operations, we have come up with the solution below.

Soft-Deletion
-------------
Whenever we delete a model that is a subclass of ``SyncableModel``, we add its ID to the ``DeletedModels`` table
for tracking purposes. When a serialization occurs, we go through the ``DeletedModels``, and turn on the ``deleted`` flag in
the ``Store`` for that record. This is considered a "soft-delete" in the ``Store``, since we are not actually clearing
the data. Upon syncing with other Morango instances, the soft deletion will propagate to the ``Store`` record of other instances.


Hard-Deletion
-------------
There may be times when users would actually like to erase their data. For example, in the case of a GDPR request.
To deal with these cases we have also defined a ``HardDeletedModels`` table. For subclasses of ``SyncableModel``, we override the ``delete``
method to take a ``kwargs`` of ``hard_delete``.  When calling ``Model.delete(hard_delete=True)`` this will add an entry to the ``HardDeletedModels``
table, as well as add an entry for related models. Upon serialization, we go through the ``HardDeletedModels`` and clear the ``serialized``
field entry in the ``Store`` for that record, as well as turning on the ``hard_deleted`` flag. Upon syncing with other Morango instances,
the hard deletion will propagate to the ``Store`` record of other instances.
