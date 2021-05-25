from requests import Request
from django.http.request import HttpRequest
from django.test.testcases import SimpleTestCase
import mock

from morango.constants import transfer_stage
from morango.utils import Settings
from morango.utils import CAPABILITIES_CLIENT_HEADER
from morango.utils import serialize_capabilities_to_client_request
from morango.utils import parse_capabilities_from_server_request


class SettingsTestCase(SimpleTestCase):

    def assertLength(self, expected, iterable):
        self.assertEqual(expected, len(iterable))

    def test_defaults(self):
        settings = Settings()
        self.assertEqual(settings.ALLOW_CERTIFICATE_PUSHING, False)
        self.assertEqual(settings.MORANGO_SERIALIZE_BEFORE_QUEUING, True)
        self.assertEqual(settings.MORANGO_DESERIALIZE_AFTER_DEQUEUING, True)
        self.assertEqual(settings.MORANGO_DISALLOW_ASYNC_OPERATIONS, False)
        self.assertLength(3, settings.MORANGO_INITIALIZE_OPERATIONS)
        self.assertLength(3, settings.MORANGO_SERIALIZE_OPERATIONS)
        self.assertLength(3, settings.MORANGO_QUEUE_OPERATIONS)
        self.assertLength(3, settings.MORANGO_DEQUEUE_OPERATIONS)
        self.assertLength(3, settings.MORANGO_DESERIALIZE_OPERATIONS)
        self.assertLength(2, settings.MORANGO_CLEANUP_OPERATIONS)

    def test_overriding(self):
        with self.settings(ALLOW_CERTIFICATE_PUSHING=True):
            settings = Settings()
            self.assertEqual(settings.ALLOW_CERTIFICATE_PUSHING, True)

        with self.settings(MORANGO_INITIALIZE_OPERATIONS=("test",)):
            settings = Settings()
            self.assertEqual(settings.MORANGO_INITIALIZE_OPERATIONS, ("test",))


class CapabilitiesTestCase(SimpleTestCase):
    def test_get_capabilities(self):
        # TODO
        pass

    @mock.patch("morango.utils.CAPABILITIES", ("TEST", "SERIALIZE"))
    def test_serialize(self):
        req = Request()
        serialize_capabilities_to_client_request(req)
        self.assertIn(CAPABILITIES_CLIENT_HEADER, req.headers)
        self.assertEqual(req.headers[CAPABILITIES_CLIENT_HEADER], "TEST SERIALIZE")

    def test_parse(self):
        req = HttpRequest()
        req.META.update(X_MORANGO_CAPABILITIES="TEST PARSE")
        result = parse_capabilities_from_server_request(req)
        self.assertEqual({"PARSE", "TEST"}, result)


class TransferStageTestCase(SimpleTestCase):
    def test_stage(self):
        stage_a = transfer_stage.stage(transfer_stage.INITIALIZING)
        stage_b = transfer_stage.stage(transfer_stage.CLEANUP)
        stage_c = transfer_stage.stage(transfer_stage.CLEANUP)

        self.assertTrue(stage_b > stage_a)
        self.assertFalse(stage_b < stage_a)
        self.assertFalse(stage_b <= stage_a)
        self.assertTrue(stage_b <= stage_c)
        self.assertTrue(stage_b >= stage_c)
