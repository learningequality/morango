Efficient Data Syncing
======================


Morango Instance ID
-------------------
Each Morango instance is identified by its own unique instance ID. This ID is
calculated as a function of a number of system properties. These IDs should be
relatively stable, but they may change if system properties change. Changes to
instance IDs will not break the system; however we want to minimize the frequency of
changes, primarily to avoid data bloat.

Morango instance IDs are used with counters that allow us to efficiently sync data
across devices.

Counters
--------
A counter system helps in identifying what data needs
to be synced, as well as determining how to resolve conflicting data.


Database Max Counters
~~~~~~~~~~~~~~~~~~~~~
Database Max Counters is a hashmap data structure with
filters as keys, mapped to a list of (instance ID, counter) pairs. These (instance
ID, counter) pairs reflect different Morango instances that have been synced
with before at their respective counters. To efficiently find out the difference
of data between 2 devices, we exchange Database Max Counters.
Exchanging all the data would be costly and unnecessary, so we instead send the
highest counters associated with the unique instance IDs for a filter and all
filter’s supersets. This is what we call Filter Max Counters.

Filter Max Counters
~~~~~~~~~~~~~~~~~~~
We calculate highest counters associated to all the unique instance IDs for a
filter and all the filter’s supersets. We generate a list of instance IDs and
associated highest counters pertaining to a filter, which the requesting Morango
instance should send to another Morango instance that it would like data from.
This becomes an efficient way to determine what data a Morango instance already
has, so we can only send the data that the Morango instance needs.

Record Max Counters
~~~~~~~~~~~~~~~~~~~
Whenever a serialized model record is modified and saved by an instance, a
combination of the instance ID and its counter position is assigned to the
record, this combination specifies the record version. The record version is
unique across the universe and is used to determine fast-forwards and merge
conflicts during the sync process. 2 records having same record ID but different
Last Saved By Instance and Last Saved By Counter will defer to 1 of the
following resolutions:

.. toctree::
   :maxdepth: 1

   fast_forward
   merge_conflict
