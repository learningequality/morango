Counters
========

A counter system helps identify what data needs to be synced and how to resolve conflicting data.


Record-max counters
-------------------

Whenever a serialized model record is modified and saved by an instance, a combination of the instance ID and its counter position is assigned to the record. This combination specifies the record version. The record version is unique and is used to determine fast-forwards and merge conflicts during the sync process.


Database-max counters
---------------------

``DatabaseMaxCounter`` is a hashmap data structure with "filters" as keys mapped to lists of ``(instance ID, counter)`` pairs.

These ``(instance ID, counter)`` pairs reflect different Morango instances that have been previously synced at some counter value. To efficiently find out the difference of data between 2 devices, we would like to exchange ``DatabaseMaxCounter`` values. Exchanging all this data is unnecessary, so we instead send the highest counters associated to all the unique instance IDs for a filter and all filter’s supersets. This is what we call "Filter-max counters".


Filter-max counters
-------------------

We calculate highest counters associated to all the unique instance IDs for a filter and all filter’s supersets. We generate a list of instance ID and associated highest counters pertaining to a filter, which the requesting Morango instance should send to another Morango instance that it would like data from. This becomes an efficient way to determine what data a Morango instance already has, so we can only send the data that the Morango instance needs.


