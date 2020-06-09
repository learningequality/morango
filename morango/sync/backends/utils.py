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
    low = 1
    high = 1000  # hard limit for SQLITE_MAX_VARIABLE_NUMBER <http://www.sqlite.org/limits.html>
    conn.execute("CREATE TABLE T1 (id C1)")
    while low < high - 1:
        guess = (low + high) // 2
        try:
            statement = "select * from T1 where id in (%s)" % ",".join(
                ["?" for _ in range(guess)]
            )
            values = [i for i in range(guess)]
            conn.execute(statement, values)
        except sqlite3.DatabaseError as ex:
            if "too many SQL variables" in str(ex):
                high = guess
            else:
                raise
        else:
            low = guess
    conn.close()
    return low
