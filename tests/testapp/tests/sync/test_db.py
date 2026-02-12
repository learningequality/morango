import threading
import uuid
from time import sleep

import pytest
from django.conf import settings
from django.db import connection
from django.test import override_settings
from django.test import TransactionTestCase
from django.utils import timezone

from ..helpers import create_buffer_and_store_dummy_data
from morango.models.certificates import Filter
from morango.models.core import Store
from morango.models.core import SyncSession
from morango.models.core import TransferSession
from morango.sync.backends.utils import load_backend
from morango.sync.db import begin_transaction


DBBackend = load_backend(connection)


def _concurrent_store_write(thread_event, store_id):
    while not thread_event.is_set():
        sleep(.1)
    Store.objects.filter(id=store_id).delete()
    connection.close()


class TransactionIsolationTestCase(TransactionTestCase):
    serialized_rollback = True

    def _fixture_setup(self):
        """Don't setup fixtures for this test case"""
        pass

    @override_settings(MORANGO_TEST_POSTGRESQL=False)
    def test_begin_transaction(self):
        """
        Assert that we can start a transaction using our util and make some writes without
        raising errors, specifically
        """
        # the utility we're testing here avoids setting the isolation level when this setting is True
        # because tests usually run within their own transaction. By the time the isolation level
        # is attempted to be set within a test, there have been reads and writes and the isolation
        # cannot be changed
        self.assertFalse(connection.in_atomic_block)
        with begin_transaction(None, isolated=True):
            session = SyncSession.objects.create(
                id=uuid.uuid4().hex,
                profile="facilitydata",
                last_activity_timestamp=timezone.now(),
            )
            transfer_session = TransferSession.objects.create(
                id=uuid.uuid4().hex,
                sync_session=session,
                push=True,
                last_activity_timestamp=timezone.now(),
            )
            create_buffer_and_store_dummy_data(transfer_session.id)

        # manual cleanup
        self.assertNotEqual(0, Store.objects.all().count())
        # will cascade delete
        SyncSession.objects.all().delete()
        Store.objects.all().delete()

    @pytest.mark.skipif(
        not getattr(settings, "MORANGO_TEST_POSTGRESQL", False), reason="Not supported"
    )
    def test_transaction_isolation_handling(self):
        from psycopg2.extensions import ISOLATION_LEVEL_REPEATABLE_READ

        store = Store.objects.create(
            id=uuid.uuid4().hex,
            last_saved_instance=uuid.uuid4().hex,
            last_saved_counter=1,
            partition=uuid.uuid4().hex,
            profile="facilitydata",
            source_id="qqq",
            model_name="qqq",
        )

        concurrent_event = threading.Event()
        concurrent_thread = threading.Thread(
            target=_concurrent_store_write,
            args=(concurrent_event, store.id),
        )
        concurrent_thread.start()

        # this test is only for postgres, but we don't want the code to know it's a test
        with override_settings(MORANGO_TEST_POSTGRESQL=False):
            try:
                self.assertNotEqual(connection.connection.isolation_level, ISOLATION_LEVEL_REPEATABLE_READ)
                with begin_transaction(Filter(store.partition), isolated=True):
                    self.assertEqual(connection.connection.isolation_level, ISOLATION_LEVEL_REPEATABLE_READ)
                    s = Store.objects.get(id=store.id)
                    concurrent_event.set()
                    sleep(.2)
                    s.last_saved_counter += 1
                    s.save()
                raise AssertionError("Didn't raise transactional error")
            except Exception as e:
                self.assertTrue(DBBackend._is_transaction_isolation_error(e))
                self.assertNotEqual(connection.connection.isolation_level, ISOLATION_LEVEL_REPEATABLE_READ)
            finally:
                concurrent_thread.join(5)
