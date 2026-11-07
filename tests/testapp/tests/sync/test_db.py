import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from time import sleep

import pytest
from django.conf import settings
from django.db import connection
from django.db import connections
from django.db import transaction
from django.test import override_settings
from django.test import TestCase
from django.test import TransactionTestCase
from django.utils import timezone

from morango.models.certificates import Filter
from morango.models.core import DatabaseIDModel
from morango.models.core import InstanceIDModel
from morango.models.core import Store
from morango.models.core import SyncSession
from morango.models.core import TransferSession
from morango.sync.backends.sqlite import SQLWrapper as SQLiteWrapper
from morango.sync.backends.utils import load_backend
from morango.sync.db import begin_transaction
from morango.sync.db import SyncDatabase

from ..helpers import assert_no_default_db_queries
from ..helpers import create_buffer_and_store_dummy_data

DBBackend = load_backend(connection)


class SyncDatabaseTestCase(TestCase):
    def test_default_manager(self):
        self.assertIs(SyncDatabase.default().manager(Store), Store._default_manager)

    def test_default_counter(self):
        instance, _ = InstanceIDModel.get_or_create_current_instance(clear_cache=True)
        counter = instance.counter
        updated = SyncDatabase.default().current_instance_and_increment_counter()
        self.assertEqual(updated.pk, instance.pk)
        self.assertEqual(updated.counter, counter + 1)

    def test_guard_rejects_default_query(self):
        with self.assertRaisesRegex(AssertionError, "SELECT COUNT"):
            with assert_no_default_db_queries():
                Store.objects.using("default").count()
        Store.objects.using("default").count()

    def test_guard_rejects_default_savepoint(self):
        with self.assertRaisesRegex(AssertionError, "SAVEPOINT"):
            with assert_no_default_db_queries():
                with transaction.atomic():
                    pass

    def test_guard_reports_a_caught_query_violation(self):
        with self.assertRaisesRegex(AssertionError, "SELECT COUNT"):
            with assert_no_default_db_queries():
                with self.assertRaises(AssertionError):
                    Store.objects.using("default").count()


@pytest.mark.skipif("default2" not in settings.DATABASES, reason="requires a second database")
class PinnedSyncDatabaseTestCase(TransactionTestCase):
    databases = {"default", "default2"}

    def test_pinned_manager_and_backend(self):
        db = SyncDatabase("default2")
        self.assertEqual(db.manager(Store).db, "default2")
        self.assertIsInstance(db.backend, SQLiteWrapper)
        self.assertIs(db.backend, db.backend)
        self.assertIs(db.backend.connection, connections["default2"])

    def test_pinned_backend_uses_calling_threads_connection(self):
        db = SyncDatabase("default2")
        main_backend = db.backend

        def get_worker_backend():
            backend = db.backend
            self.assertIs(backend.connection, connections["default2"])
            return backend

        with ThreadPoolExecutor(max_workers=1) as executor:
            self.assertIsNot(executor.submit(get_worker_backend).result(), main_backend)
        self.assertIs(db.backend, main_backend)

    def test_pinned_transaction_and_cursor(self):
        db = SyncDatabase("default2")
        for isolated in (False, True):
            with self.subTest(isolated=isolated):
                with assert_no_default_db_queries(), db.begin_transaction(None, isolated=isolated):
                    self.assertFalse(connection.in_atomic_block)
                    self.assertTrue(connections["default2"].in_atomic_block)
                    self.assertEqual(db.manager(Store).count(), 0)
                    with db.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        self.assertEqual(cursor.fetchone(), (1,))
                self.assertFalse(connections["default2"].in_atomic_block)

    def test_pinned_counter_uses_alias_with_default_instance_cached(self):
        default_instance, _ = InstanceIDModel.get_or_create_current_instance(clear_cache=True)
        counter = default_instance.counter
        instance = InstanceIDModel.objects.get(pk=default_instance.pk)
        database = instance.database
        db = SyncDatabase("default2")
        # Seed a database identity without invoking the routed model save methods.
        with assert_no_default_db_queries():
            db.manager(DatabaseIDModel).bulk_create([database])
            db.manager(InstanceIDModel).bulk_create([instance])
            updated = db.current_instance_and_increment_counter()
            self.assertEqual(updated.counter, counter + 1)
            self.assertEqual(db.manager(InstanceIDModel).get(pk=updated.pk).counter, counter + 1)
        default_instance.refresh_from_db()
        self.assertEqual(default_instance.counter, counter)


def _concurrent_store_write(thread_event, store_id):
    while not thread_event.is_set():
        sleep(0.1)
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
                self.assertNotEqual(
                    connection.connection.isolation_level, ISOLATION_LEVEL_REPEATABLE_READ
                )
                with begin_transaction(Filter(store.partition), isolated=True):
                    self.assertEqual(
                        connection.connection.isolation_level, ISOLATION_LEVEL_REPEATABLE_READ
                    )
                    s = Store.objects.get(id=store.id)
                    concurrent_event.set()
                    sleep(0.2)
                    s.last_saved_counter += 1
                    s.save()
                raise AssertionError("Didn't raise transactional error")
            except Exception as e:
                self.assertTrue(DBBackend._is_transaction_isolation_error(e))
                self.assertNotEqual(
                    connection.connection.isolation_level, ISOLATION_LEVEL_REPEATABLE_READ
                )
            finally:
                concurrent_thread.join(5)
