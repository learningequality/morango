

Signaling
---------
Morango fires a few different signals from the ``signals`` object on both ``PullClient`` and
``PushClient`` during the sync process which can be used to track the progress of the sync. These
signal groups are ``session``, ``queuing``, ``transferring``, and ``dequeuing``. Each signal group
has 3 stages that can be fired: ``started``, ``in_progress``, and ``completed``. For a push or pull
sync, the order of the fired signals would be as follows:

1) Session started
2) Queuing started
3) Queueing completed
4) Transferring started
5) Transferring in progress
6) Transferring completed
7) Dequeuing started
8) Dequeuing completed
9) Session completed

.. autoclass:: morango.sync.syncsession.BaseSyncClient
    :members: signals
    :noindex:

.. autoclass:: morango.sync.syncsession.SyncClientSignals
    :members: session, queuing, pushing, pulling, dequeuing
    :noindex:

.. autoclass:: morango.sync.syncsession.SyncSignalGroup
    :members:
    :inherited-members:
    :noindex:

.. autoclass:: morango.sync.syncsession.SyncSignal
    :members:
    :noindex:
