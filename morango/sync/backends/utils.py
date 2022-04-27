import sqlite3
from importlib import import_module

from django.utils.lru_cache import lru_cache

from morango.errors import MorangoError


def load_backend(conn):
    """
    :rtype: morango.sync.backends.base.BaseSQLWrapper
    """
    if "postgresql" in conn.vendor:
        SQLWrapper = import_module("morango.sync.backends.postgres").SQLWrapper
    elif "sqlite" in conn.vendor:
        SQLWrapper = import_module("morango.sync.backends.sqlite").SQLWrapper
    else:
        raise MorangoError("Incompatible database backend for syncing")
    return SQLWrapper(conn)


@lru_cache(maxsize=1)
def calculate_max_sqlite_variables():
    """
    SQLite has a limit on the max number of variables allowed for parameter substitution. This limit used to be 999, but
    can be compiled to a different number, and is now often much larger. This function reads the value from the compile options.
    We use this value to chunk our SQL bulk insert statements when deserializing from the store to the app layer.
    """
    conn = sqlite3.connect(":memory:")

    # default to 999, in case we can't read the compilation option
    MAX_VARIABLE_NUMBER = 999

    # check that target compilation option is specified, before we start looping through
    is_defined = list(
        conn.execute("SELECT sqlite_compileoption_used('MAX_VARIABLE_NUMBER');")
    )[0][0]

    if is_defined:
        for i in range(500):
            option_str = list(conn.execute("SELECT sqlite_compileoption_get(?);", [i]))[
                0
            ][0]
            if option_str is None:
                # we've hit the end of the compilation options, so we can stop
                break
            if option_str.startswith("MAX_VARIABLE_NUMBER="):
                # we found the target option, so just read it and stop
                MAX_VARIABLE_NUMBER = int(option_str.split("=")[1])
                break

    return MAX_VARIABLE_NUMBER


def get_pk_field(fields):
    try:
        return next(f for f in fields if f.primary_key)
    except StopIteration:
        raise ValueError("No primary key found in fields")


class TemporaryTable(object):
    """
    Utility class for managing a temporary table within the database
    """

    __slots__ = ("connection", "name", "fields", "backend", "_meta")

    def __init__(self, connection, name, **fields):
        """
        :param connection: A database connection object
        :param name: A str name for the table in the database
        :param fields: Keyword arguments are assumed to be fields for defining the schema of the
            temporary table
        """
        self.connection = connection
        self.name = name
        self.fields = []
        self.backend = load_backend(connection)
        self._meta = self.Meta()

        for name, field in fields.items():
            field.set_attributes_from_name(name)
            self.fields.append(field)

    def __enter__(self):
        self.create()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.drop()

    @property
    def sql_name(self):
        """
        :return: The name actual name of the table used in the DB, prefixed to avoid collisions
        """
        return self.connection.ops.quote_name("t_{}".format(self.name))

    def get_field(self, name):
        """
        :param name: A str of the name of which field to find
        :return: The field object
        """
        return next(f for f in self.fields if f.name == name)

    def create(self):
        """
        Creates the temporary table within the database
        """
        fields = []
        params = []
        with self.connection.schema_editor() as schema_editor:
            for field in self.fields:
                # generates the SQL expression for the table column
                field_sql, field_params = schema_editor.column_sql(
                    self, field, include_default=True
                )
                field_sql_name = self.connection.ops.quote_name(field.column)
                fields.append("{name} {sql}".format(name=field_sql_name, sql=field_sql))
                params.extend(field_params)
        with self.connection.cursor() as c:
            self.backend._create_temporary_table(c, self.sql_name, fields, params)

    def drop(self):
        """
        Drops the temporary table within the database
        """
        with self.connection.cursor() as c:
            c.execute("DROP TABLE IF EXISTS {name}".format(name=self.sql_name))

    def bulk_insert(self, values):
        """
        Bulk inserts a list of records into the temporary table

        :param values: A list of dictionaries containing data to insert, keyed by field name
        """
        params = []
        for value_dict in values:
            for field in self.fields:
                params.append(value_dict.get(field.attname))
        with self.connection.cursor() as c:
            self.backend._bulk_insert(c, self.sql_name, self.fields, params)

    class Meta:
        """
        HACK: some Django code bits require a model, only to access meta information, so we use
        this to mimic a model class
        """

        db_tablespace = None
