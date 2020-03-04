.. _data-merging:

Data merging
============

There are two possible cases for the merging of data:

Fast-Forward
------------
.. image:: img/fast_forward.png

In this diagram, ``Device A`` (green) produces record with record ID ``r``, ``r@A`` and
assigns it a record version of ``A1``. ``Device A`` further makes changes to ``r@A`` and the
record version changes to ``A2``, the history of ``r@A`` is [``A2``, ``A1``] at this point.
``Device B`` (red) now syncs data with ``Device A`` and both the devices have same
version of the record(``r@A`` and ``r@B``). ``Device B`` makes modifications to ``r@B`` which
changes record version to ``B1``.The history of ``r@B`` grows to [``B1``, ``A2``, ``A1``]. When
``Device A`` syncs data with ``Device B``, the situation is known as a Fast-Forward. We
can determine this by checking if ``r@A``’s current version is in ``r@B``’s history or
vice-versa.

Merge-Conflict
--------------
.. image:: img/merge_conflict.png

``Device A`` (green) produces record with record ID ``r``, ``r@A`` and assigns it a record
version of ``A1`` and history of [``A1``]. ``Device B`` (red) now syncs data with ``Device A``
and both the devices have same copy of the record(``r@A`` and ``r@B``) at this point.
Device B makes  modifications to ``r@B`` and the record version changes to ``B1``, with
history growing up to [``B1``,  ``A1``]. ``Device A`` makes modification to its copy of
record ``r@A`` and saves it as ``A2``. ``r@A``’s history after the second modification by A
is [ ``A2``, ``A1`` ]. Now when ``Device A`` syncs data with ``Device B``, this situation is
known as a Merge-Conflict. We can determine this from the history by noting that
``r@A``’s record version is ``A2`` which does not reside in ``r@B``’s history and ``r@B``’s
record version is ``B1`` which does not reside in ``r@A``’s history.

**Note**: It is up to the implementing application to determine what the merge conflict resolution strategy is.
