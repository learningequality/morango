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
