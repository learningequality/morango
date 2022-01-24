import os
from requests import Request
from django.http.request import HttpRequest
from django.test.testcases import SimpleTestCase
import mock
import pytest

from morango.constants.capabilities import ALLOW_CERTIFICATE_PUSHING
from morango.constants.capabilities import ASYNC_OPERATIONS
from morango.constants.capabilities import FSIC_V2_FORMAT
from morango.constants import transfer_stages
from morango.utils import SETTINGS
from morango.utils import CAPABILITIES_CLIENT_HEADER
from morango.utils import CAPABILITIES_SERVER_HEADER
from morango.utils import get_capabilities
from morango.utils import serialize_capabilities_to_client_request
from morango.utils import parse_capabilities_from_server_request
from morango.utils import pid_exists
from morango.utils import _posix_pid_exists
from morango.utils import _windows_pid_exists


class SettingsTestCase(SimpleTestCase):

    def assertLength(self, expected, iterable):
        self.assertEqual(expected, len(iterable))

    def test_defaults(self):
        self.assertEqual(SETTINGS.ALLOW_CERTIFICATE_PUSHING, False)
        self.assertEqual(SETTINGS.MORANGO_SERIALIZE_BEFORE_QUEUING, True)
        self.assertEqual(SETTINGS.MORANGO_DESERIALIZE_AFTER_DEQUEUING, True)
        self.assertEqual(SETTINGS.MORANGO_DISALLOW_ASYNC_OPERATIONS, False)
        self.assertEqual(SETTINGS.MORANGO_DISABLE_FSIC_V2_FORMAT, False)
        self.assertLength(3, SETTINGS.MORANGO_INITIALIZE_OPERATIONS)
        self.assertLength(3, SETTINGS.MORANGO_SERIALIZE_OPERATIONS)
        self.assertLength(4, SETTINGS.MORANGO_QUEUE_OPERATIONS)
        self.assertLength(4, SETTINGS.MORANGO_DEQUEUE_OPERATIONS)
        self.assertLength(4, SETTINGS.MORANGO_DESERIALIZE_OPERATIONS)
        self.assertLength(2, SETTINGS.MORANGO_CLEANUP_OPERATIONS)

    def test_overriding(self):
        with self.settings(ALLOW_CERTIFICATE_PUSHING=True):
            self.assertEqual(SETTINGS.ALLOW_CERTIFICATE_PUSHING, True)

        with self.settings(MORANGO_INITIALIZE_OPERATIONS=("test",)):
            self.assertEqual(SETTINGS.MORANGO_INITIALIZE_OPERATIONS, ("test",))


class CapabilitiesTestCase(SimpleTestCase):
    def test_get_capabilities__certs(self):
        with self.settings(ALLOW_CERTIFICATE_PUSHING=True):
            self.assertIn(ALLOW_CERTIFICATE_PUSHING, get_capabilities())

        with self.settings(ALLOW_CERTIFICATE_PUSHING=False):
            self.assertNotIn(ALLOW_CERTIFICATE_PUSHING, get_capabilities())

    def test_get_capabilities__async_ops(self):
        with self.settings(MORANGO_DISALLOW_ASYNC_OPERATIONS=False):
            self.assertIn(ASYNC_OPERATIONS, get_capabilities())

        with self.settings(MORANGO_DISALLOW_ASYNC_OPERATIONS=True):
            self.assertNotIn(ASYNC_OPERATIONS, get_capabilities())

    def test_get_capabilities__fsic_v2_format(self):
        with self.settings(MORANGO_DISABLE_FSIC_V2_FORMAT=False):
            self.assertIn(FSIC_V2_FORMAT, get_capabilities())

        with self.settings(MORANGO_DISABLE_FSIC_V2_FORMAT=True):
            self.assertNotIn(FSIC_V2_FORMAT, get_capabilities())

    @mock.patch("morango.utils.CAPABILITIES", ("TEST", "SERIALIZE"))
    def test_serialize(self):
        req = Request()
        serialize_capabilities_to_client_request(req)
        self.assertIn(CAPABILITIES_CLIENT_HEADER, req.headers)
        self.assertEqual(req.headers[CAPABILITIES_CLIENT_HEADER], "TEST SERIALIZE")

    def test_parse(self):
        req = HttpRequest()
        req.META.update(HTTP_X_MORANGO_CAPABILITIES="TEST PARSE")
        result = parse_capabilities_from_server_request(req)
        self.assertEqual({"PARSE", "TEST"}, result)


class TransferStageTestCase(SimpleTestCase):
    def test_stage(self):
        stage_a = transfer_stages.stage(transfer_stages.INITIALIZING)
        stage_b = transfer_stages.stage(transfer_stages.CLEANUP)
        stage_c = transfer_stages.stage(transfer_stages.CLEANUP)

        self.assertTrue(stage_b > stage_a)
        self.assertFalse(stage_b < stage_a)
        self.assertFalse(stage_b <= stage_a)
        self.assertTrue(stage_b <= stage_c)
        self.assertTrue(stage_b >= stage_c)


class ProcessIDExistsTestCase(SimpleTestCase):
    @pytest.mark.skipif(os.name != "posix", reason="Not POSIX OS")
    def test_posix(self):
        self.assertEqual(_posix_pid_exists, pid_exists)
        pid = os.getpid()
        self.assertTrue(pid_exists(pid))
        self.assertFalse(pid_exists(123456789))

    @pytest.mark.windows
    @pytest.mark.skipif(os.name == "posix", reason="POSIX platform")
    def test_windows(self):
        self.assertEqual(_windows_pid_exists, pid_exists)
        pid = os.getpid()
        self.assertTrue(pid_exists(pid))
        self.assertFalse(pid_exists(123456789))
