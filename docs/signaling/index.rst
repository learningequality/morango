

Signals
=======

Lifecycle and events
--------------------

During the sync process, Morango fires a few different signals from ``signals`` in ``PullClient`` and ``PushClient``. These can be used to track the progress of the sync.

There are four signal groups:

- ``session``
- ``queuing``
- ``transferring``
- ``dequeuing``

Each signal group has 3 stages that can be fired:

- ``started``
- ``in_progress``
- ``completed``

For a push or pull sync lifecycle, the order of the fired signals would be as follows:

1) Session started
2) Queuing started
3) Queueing completed
4) Transferring started
5) Transferring in progress
6) Transferring completed
7) Dequeuing started
8) Dequeuing completed
9) Session completed
