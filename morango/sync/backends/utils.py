import sqlite3
from importlib import import_module

from django.utils.lru_cache import lru_cache

from morango.errors import MorangoError


def load_backend(conn):

    if "postgresql" in conn.vendor:
        return import_module("morango.sync.backends.postgres")
    if "sqlite" in conn.vendor:
        return import_module("morango.sync.backends.sqlite")
    raise MorangoError("Incompatible database backend for syncing")


@lru_cache(maxsize=1)
def calculate_max_sqlite_variables():
    """
    SQLite has a limit on the max number of variables allowed for parameter substitution. This limit is usually 999, but
    can be compiled to a different number. This function calculates what the max is for the sqlite version running on the device.
    We use the calculated value to chunk our SQL bulk insert statements when deserializing from the store to the app layer.
    Source: https://stackoverflow.com/questions/17872665/determine-maximum-number-of-columns-from-sqlite3
    """
    conn = sqlite3.connect(":memory:")
    MAX_VARIABLE_NUMBER = 999
    statement = """
        WITH opts(n, opt) AS (
        VALUES(0, NULL)
        UNION ALL
        SELECT n + 1,
                sqlite_compileoption_get(n)
        FROM opts
        WHERE sqlite_compileoption_get(n) IS NOT NULL
        )
        SELECT CASE WHEN sqlite_compileoption_used('MAX_VARIABLE_NUMBER')
            THEN (SELECT opt FROM opts WHERE opt LIKE 'MAX_VARIABLE_NUMBER%')
            ELSE 'MAX_VARIABLE_NUMBER=999'
        END;
    """
    cursor = conn.execute(statement)
    output = cursor.fetchone()[0]
    exec(output)
    return MAX_VARIABLE_NUMBER
