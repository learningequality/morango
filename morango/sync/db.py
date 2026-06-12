import logging
from contextlib import contextmanager

from django.db import connection
from django.db import transaction

from morango.sync.backends.utils import load_backend
from morango.sync.utils import lock_partitions

logger = logging.getLogger(__name__)

DBBackend = load_backend(connection)


@contextmanager
def begin_transaction(sync_filter, isolated=False, shared_lock=False):
    """
    Starts a transaction, sets the transaction isolation level to repeatable read, and locks
    affected partitions

    :param sync_filter: The filter for filtering applicable records of the sync
    :type sync_filter: morango.models.certificates.Filter|None
    :param isolated: Whether to alter the transaction isolation to repeatable-read
    :type isolated: bool
    :param shared_lock: Whether the advisory lock should be exclusive or shared
    :type shared_lock: bool
    """
    if isolated:
        # when isolation is requested, we modify the transaction isolation of the connection for the
        # duration of the transaction
        with DBBackend._set_transaction_repeatable_read():
            with transaction.atomic(savepoint=False):
                lock_partitions(DBBackend, sync_filter=sync_filter, shared=shared_lock)
                yield
    else:
        with transaction.atomic():
            lock_partitions(DBBackend, sync_filter=sync_filter, shared=shared_lock)
            yield
