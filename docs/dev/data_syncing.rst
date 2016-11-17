Efficient Data Syncing
======================


Instance ID
-----------
Each morango instance is identified by its own unique instance ID. This ID is
calculated as a function of a number of system properties. This ID may change if
system properties change. It is not a big deal if the “Instance ID” changes
occasionally within an installation, though we want to minimize the frequency of
this, to avoid data bloat.

These IDs will be used with counters that will allow us to efficiently sync data
across devices.

Counters
--------
We implement a counter system which will help us in identifying what data needs
to be synced, as well as determining how to resolve conflicting data.


Database Max Counters
~~~~~~~~~~~~~~~~~~~~~
Database Max Counters is an external data structure that is a hashmap with
filters as key mapped to a list of instance ID, counter pairs. These instance
ID, counter pairs reflect different morango instances that have been synced
with before at their respective counters. To efficiently find out the difference
of data between 2 devices, we would like to exchange Database Max Counters. As
exchanging all this data can be costly and unnecessary, we instead send the
highest counters associated to all the unique instance IDs for a filter and all
filter’s supersets. This is what we call Filter Max Counters.

Filter Max Counters
~~~~~~~~~~~~~~~~~~~
We calculate highest counters associated to all the unique instance IDs for a
filter and all filter’s supersets. We generate a list of instance ID and
associated highest counters pertaining to a filter, which the requesting morango
instance should send to another morango instance that it would like data from.
This becomes an efficient way to determine what data a morango instance already
has, so we can only send the data that the morango instance needs.

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
