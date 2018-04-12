from importlib import import_module
from morango.errors import MorangoError

def load_backend(conn):

    if 'postgresql' in conn.vendor:
        return import_module('morango.utils.backends.postgres')
    if 'sqlite' in conn.vendor:
        return import_module('morango.utils.backends.sqlite')
    raise MorangoError("Incompatible database backend for syncing")
