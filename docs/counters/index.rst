IDs and Counters
================

Identifiers
-----------

There is generally one Morango instance for every Kolibri instance, and each of these are identified by a unique Morango **instance ID**. The instance ID is calculated as a function of a number of system properties, and will change when those properties change. Changes to the instance ID are not fatal, but stability is generally preferable.

The **database ID** identifies the actual database being used by a Morango instance. If a database has been backed up and restored or copied to a different Morango instance, a new database ID should be generated to help other Morango instances that may have already seen the previous state of the database.

Each syncable model instance within the database is identified by a unique **model source ID**. This is calculated randomly by default and takes the calculated partition and Morango model name into account. Models can also define their own behavior by overriding ``calculate_source_id``.

.. _counters:
Counters
--------

A **counter** is a monotonically increasing version number. Comparing two counter values associated with the same object will show which one is newer.

Whenever a syncable model record is modified, a unique combination of the Morango instance ID and an incrementing counter version are assigned to the record. This combination specifies the record version.

Morango instances use **record-max counters** to keep track of the maximum version each record has been saved at. This is used to determine drive different merge behaviors during the sync process.

The **database-max counter** table tracks a mapping of scope filter strings to lists of (instance ID, counter) pairs. These (instance ID, counter) pairs reflect different Morango instances that have been previously synced at some counter value.

Morango sends **filter-max counters** to determine what data is already shared before syncing to efficiently determine the difference in data. Filter-max counters are the highest counters associated with every instance ID for both a filter and its supersets.

**Example** (in pseudocode)

#. Instance A creates a model, e.g.  exam_x. It registers it in its store:  ``{ "model" : "exam_x", "counter" : 1 }``
#. It then syncs this exam to instance B and registers it in its store: ``{ "model" : "exam_x", "counter" : 1, "max_counters": { "B" : 1 }}``
#. After some time, instance A updates the model because the exam changed. It registers this in the store: ``{ "model" : "exam_x", "counter" : 2, "max_counters": { "B" : 1 }}``
#. The next time instance A syncs with instance B, it registers that the counter of ``exam_x`` is bigger than the ``max_counter`` of instance B.
#. This triggers a transfer_session in which the model ``exam_x`` is transferred to instance B and then updated in the store: ``{ "model" : "exam_x", "counter" : 2, "max_counters": { "B" : 2 }}``

