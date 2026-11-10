from contextlib import contextmanager
from threading import local

from django.db import connection
from django.db import connections
from django.db import DEFAULT_DB_ALIAS
from django.db import transaction
from django.db.models import F

from morango.models.core import InstanceIDModel
from morango.sync.backends.utils import load_backend
from morango.sync.utils import lock_partitions


class SyncDatabase:
    """Database access for sync, with routed ORM access when no alias is supplied."""

    def __init__(self, alias=None):
        self.alias = alias
        self._backends = local()

    @classmethod
    def default(cls):
        return cls()

    @property
    def connection(self):
        alias = DEFAULT_DB_ALIAS if self.alias is None else self.alias
        return connections[alias]

    @property
    def backend(self):
        if self.alias is None:
            return DBBackend

        conn = self.connection
        backend = getattr(self._backends, "backend", None)
        if backend is None or backend.connection is not conn:
            backend = self._backends.backend = load_backend(conn)
        return backend

    def manager(self, model):
        manager = model._default_manager
        if self.alias is None:
            return manager
        return manager.db_manager(self.alias)

    def atomic(self, savepoint=True):
        return transaction.atomic(using=self.connection.alias, savepoint=savepoint)

    @contextmanager
    def begin_transaction(self, sync_filter, isolated=False, shared_lock=False):
        """
        Start a transaction and lock the affected partitions.

        :param sync_filter: The sync filter, or None to lock all partitions
        :type sync_filter: morango.models.certificates.Filter | None
        :param isolated: Whether to set repeatable-read isolation for the transaction
        :param shared_lock: Whether to acquire shared rather than exclusive locks
        """
        if isolated:
            with self.backend._set_transaction_repeatable_read():
                with self.atomic(savepoint=False):
                    lock_partitions(self.backend, sync_filter=sync_filter, shared=shared_lock)
                    yield
        else:
            with self.atomic():
                lock_partitions(self.backend, sync_filter=sync_filter, shared=shared_lock)
                yield

    def cursor(self):
        return self.connection.cursor()

    def current_instance_and_increment_counter(self):
        """Increment the current instance, which must already exist on a pinned alias."""
        if self.alias is None:
            return InstanceIDModel.get_current_instance_and_increment_counter()

        with self.atomic():
            instances = self.manager(InstanceIDModel)
            instance = instances.get(current=True)
            instances.filter(pk=instance.pk).update(counter=F("counter") + 1)
            instance.refresh_from_db(using=self.alias, fields=["counter"])
            return instance


# Django's default connection proxy resolves the connection on the calling thread.
DBBackend = load_backend(connection)
_default = SyncDatabase.default()


def begin_transaction(sync_filter, isolated=False, shared_lock=False):
    """
    Start a sync transaction on the default database and lock affected partitions.

    :param sync_filter: The sync filter, or None to lock all partitions
    :type sync_filter: morango.models.certificates.Filter | None
    :param isolated: Whether to set repeatable-read isolation for the transaction
    :param shared_lock: Whether to acquire shared rather than exclusive locks
    """
    return _default.begin_transaction(sync_filter, isolated=isolated, shared_lock=shared_lock)
