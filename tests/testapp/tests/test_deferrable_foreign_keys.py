import pytest
from django.apps import apps as global_apps
from django.conf import settings
from django.db import connection
from django.test import TransactionTestCase
from facility_profile.models import Facility

from morango.deferrable_foreign_keys import _get_table_sql
from morango.deferrable_foreign_keys import MakeForeignKeysDeferrable


BASE_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
]


class MakeDeferrableForeignKeysTestCase(TransactionTestCase):
    def _rewrite_model_fk_immediate(self):
        table = Facility._meta.db_table
        sql = _get_table_sql(connection, table)
        immediate = sql.replace(" DEFERRABLE INITIALLY DEFERRED", "")
        assert "DEFERRABLE" not in immediate
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=%s AND sql IS NOT NULL",
                [table],
            )
            indexes = [r[0] for r in cursor.fetchall()]
            cursor.execute("PRAGMA foreign_keys = OFF")
            cursor.execute("DROP TABLE {}".format(connection.ops.quote_name(table)))
            cursor.execute(immediate)
            for idx in indexes:
                cursor.execute(idx)
            cursor.execute("PRAGMA foreign_keys = ON")

    @pytest.mark.skipif(
        getattr(settings, "MORANGO_TEST_POSTGRESQL", False), reason="SQLite only"
    )
    def test_remake_makes_deferrable_and_preserves_data(self):
        table = Facility._meta.db_table
        # fresh schema should be deferrable
        self.assertIn("DEFERRABLE", _get_table_sql(connection, table))

        # downgrade schema to immediate FK (table is rebuilt empty)
        self._rewrite_model_fk_immediate()
        self.assertNotIn("DEFERRABLE", _get_table_sql(connection, table))

        # create some data on the immediate-FK schema
        f = Facility.objects.create(name="testfac")
        f_id = f.id
        self.assertTrue(Facility.objects.filter(id=f_id).exists())

        # run the helper with the real app registry + a schema editor
        op = MakeForeignKeysDeferrable(include_app_labels=["facility_profile"])
        with connection.schema_editor(atomic=False) as schema_editor:
            op(global_apps, schema_editor)

        # now deferrable again, and data preserved
        self.assertIn("DEFERRABLE", _get_table_sql(connection, table))
        self.assertTrue(Facility.objects.filter(id=f_id).exists())

    def test_iter_apps__all(self):
        op = MakeForeignKeysDeferrable()
        self.assertEqual([app.name for app in op._iter_apps(global_apps)], [
            *BASE_APPS,
            'morango',
            'facility_profile'
        ])

    def test_iter_apps__exclude(self):
        op = MakeForeignKeysDeferrable(exclude_app_labels=['facility_profile'])
        self.assertEqual([app.name for app in op._iter_apps(global_apps)], [
            *BASE_APPS,
            'morango',
        ])

    def test_iter_apps__include(self):
        op = MakeForeignKeysDeferrable(include_app_labels=['morango'])
        self.assertEqual([app.name for app in op._iter_apps(global_apps)], [
            'morango',
        ])
