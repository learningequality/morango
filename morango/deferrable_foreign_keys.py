"""
Helpers for ensuring that existing SQLite foreign key constraints are
``DEFERRABLE INITIALLY DEFERRED``.

Prior to Django 3.1, SQLite tables were created with immediate foreign key constraints, e.g.::

    "store_model_id" char(32) NOT NULL REFERENCES "morango_store" ("id")

Since Django 3.1 the same column is created as::

    "store_model_id" char(32) NOT NULL REFERENCES "morango_store" ("id") DEFERRABLE INITIALLY DEFERRED

Django relies on deferred constraint checking for correct cascade-deletion ordering. Databases
created before Morango 0.8 (with Django 1.11) therefore retain immediate constraints, which can
raise ``IntegrityError: FOREIGN KEY constraint failed`` during operations such as sync
deserialization once foreign key enforcement is enabled at the database level (the default since
Django 3.2).

The migration framework does not regenerate these constraints on its own because the deferrable
clause is not part of the field definition that migrations track. This module rebuilds the affected
tables so that a migrated database ends up with the same schema as a freshly created one.
"""
import logging
from typing import List
from typing import Optional

from django.apps import apps as global_apps
from django.apps.registry import Apps
from django.db import connection
from django.db import DatabaseError
from django.db import router
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.sqlite3.schema import DatabaseSchemaEditor

logger = logging.getLogger(__name__)


def _get_table_sql(conn, table_name: str) -> Optional[str]:
    """
    Return the ``CREATE TABLE`` statement stored by SQLite for ``table_name``, or ``None`` if the
    table does not exist.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = %s",
            [table_name],
        )
        row = cursor.fetchone()
    return row[0] if row else None


def _table_has_immediate_foreign_key(conn, table_name: str) -> bool:
    """
    Determine whether ``table_name`` has at least one foreign key constraint that is not deferrable.
    Tables are created with all constraints in the same style, so the presence of a ``REFERENCES``
    clause without ``DEFERRABLE`` indicates an immediate constraint that needs to be rebuilt.
    """
    sql = _get_table_sql(conn, table_name)
    if not sql:
        return False
    references_count = sql.count("REFERENCES")
    if references_count == 0:
        return False
    # make sure that all FK columns (identified by references) have a deferrable constraint
    return references_count != sql.count("DEFERRABLE")


class MakeForeignKeysDeferrable:
    """
    Invokable utility class that rebuilds of any table belonging to one of ``*app_labels`` whose
    foreign key constraints are still immediate, so that they become
    ``DEFERRABLE INITIALLY DEFERRED``.

    This can be utilized by passing this class to ``RunPython`` in a Django migration, or by
    manually invoking its ``run`` method.

    This is a no-op on non-SQLite backends (PostgreSQL already creates deferrable
    constraints) and on tables that already use deferrable constraints, so it is
    safe to apply to any database.
    """

    def __init__(
        self,
        include_app_labels: Optional[List[str]] = None,
        exclude_app_labels: Optional[List[str]] = None
    ):
        self.include_app_labels = include_app_labels
        self.exclude_app_labels = exclude_app_labels

    def _iter_apps(self, apps: Apps):
        for app_config in apps.get_app_configs():
            if self.exclude_app_labels is not None and app_config.label in self.exclude_app_labels:
                continue
            if self.include_app_labels is None or app_config.label in self.include_app_labels:
                yield app_config

    def __call__(self, apps: Apps, schema_editor: BaseDatabaseSchemaEditor):
        """
        Loops through all relevant django apps and their models, checking if the model's table has
        FKs, and if so, runs the schema editor's utility that synchronizes the schema to the model,
        which will make FKs deferrable
        """
        conn = schema_editor.connection
        # these should be in sync, but since we call `_remake_table`, specific to this schema editor
        # class, we ensure we have the correct instance to begin with
        if conn.vendor != "sqlite" or not isinstance(schema_editor, DatabaseSchemaEditor):
            return

        for app_config in self._iter_apps(apps):
            for model in app_config.get_models(include_auto_created=True):
                if not router.allow_migrate_model(conn.alias, model):
                    continue
                table_name = model._meta.db_table
                if not _table_has_immediate_foreign_key(conn, table_name):
                    continue
                logger.info(
                    "Rebuilding table %s to make foreign key constraints deferrable",
                    table_name,
                )
                try:
                    # Rebuilding the table from the current model state regenerates the column
                    # definitions, which SQLite always emits with the deferrable clause (see
                    # DatabaseSchemaEditor.sql_create_inline_fk). Only tested with Django 3.2
                    schema_editor._remake_table(model)
                except DatabaseError as e:
                    logger.error("Failed to rebuild table %s", table_name)
                    logger.exception(e)

    def run(self):
        """
        Invoke this utility outside of Django migrations, manually passing in Django's app registry
        and schema editor.
        """
        # since this method is meant for manual invocation, we let the editor create a transaction
        with connection.schema_editor(atomic=True) as schema_editor:
            self.__call__(global_apps, schema_editor)
