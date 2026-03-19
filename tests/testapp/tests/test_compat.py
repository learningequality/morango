import os
import uuid

from django.test import SimpleTestCase

from .compat import EnvironmentVarGuard


class EnvironmentVarGuardTestCase(SimpleTestCase):
    """Tests for the vendored EnvironmentVarGuard."""

    def test_sets_env_var_inside_context(self):
        key = "MORANGO_TEST_" + uuid.uuid4().hex[:8]
        with EnvironmentVarGuard() as env:
            env[key] = "test_value"
            self.assertEqual(os.environ[key], "test_value")

    def test_reverts_env_var_on_exit(self):
        key = "MORANGO_TEST_" + uuid.uuid4().hex[:8]
        with EnvironmentVarGuard() as env:
            env[key] = "test_value"
        self.assertNotIn(key, os.environ)

    def test_reverts_modified_env_var_on_exit(self):
        key = "MORANGO_TEST_" + uuid.uuid4().hex[:8]
        os.environ[key] = "original"
        try:
            with EnvironmentVarGuard() as env:
                env[key] = "modified"
                self.assertEqual(os.environ[key], "modified")
            self.assertEqual(os.environ[key], "original")
        finally:
            os.environ.pop(key, None)

    def test_returns_os_environ(self):
        with EnvironmentVarGuard() as env:
            self.assertIs(env, os.environ)
