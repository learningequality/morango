import os
from unittest.mock import patch as _patch


class EnvironmentVarGuard:
    """
    Vendored replacement for the removed test.support EnvironmentVarGuard.
    Uses unittest.mock.patch.dict(os.environ) under the hood.
    Supports the context-manager-with-dict-assignment pattern:
        with EnvironmentVarGuard() as env: env[k] = v
    """

    def __init__(self):
        self._patcher = _patch.dict(os.environ)

    def __enter__(self):
        self._patcher.start()
        return os.environ

    def __exit__(self, *args):
        self._patcher.stop()
