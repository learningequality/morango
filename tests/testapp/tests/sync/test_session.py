import mock
from django.test import TestCase
from requests.exceptions import ConnectionError
from requests.exceptions import HTTPError
from requests.exceptions import RequestException
from requests.exceptions import RetryError
from urllib3.exceptions import MaxRetryError
from urllib3.util.retry import Retry

from morango.sync.session import _length_of_headers
from morango.sync.session import SessionWrapper


class SessionWrapperTestCase(TestCase):
    @mock.patch("morango.sync.session.Session.request")
    def test_request(self, mocked_super_request):
        headers = {"Content-Length": 1024}
        mock_response = mock.Mock(
            headers=headers, raise_for_status=mock.Mock(), status_code=200, reason="OK"
        )

        wrapper = SessionWrapper(Retry.DEFAULT)

        def dispatch_hooks(method, url, **kwargs):
            for hook in wrapper.hooks.get("response", []):
                hook(mock_response)
            return mock_response

        mocked_super_request.side_effect = dispatch_hooks

        actual = wrapper.request("GET", "test_url", is_test=True)
        mocked_super_request.assert_called_once_with("GET", "test_url", is_test=True)
        self.assertEqual(mock_response, actual)

        head_length = len("HTTP/1.1 200 OK") + _length_of_headers(headers)
        self.assertEqual(wrapper.bytes_received, 1024 + head_length)

    def test_request_user_agent(self):
        from morango import __version__ as morango_version
        from requests import __version__ as requests_version

        wrapper = SessionWrapper(Retry.DEFAULT)
        expected_user_agent = "morango/{} python-requests/{}".format(morango_version, requests_version)
        self.assertEqual(wrapper.headers["User-Agent"], expected_user_agent)

        with self.settings(CUSTOM_INSTANCE_INFO={"kolibri": "0.16.0"}):
            wrapper = SessionWrapper(Retry.DEFAULT)
            expected_user_agent = "morango/{} kolibri/0.16.0 python-requests/{}".format(morango_version, requests_version)
            self.assertEqual(wrapper.headers["User-Agent"], expected_user_agent)

    @mock.patch("morango.sync.session.logger")
    @mock.patch("morango.sync.session.Session.request")
    def test_request__not_ok(self, mocked_super_request, mocked_logger):
        raise_for_status = mock.Mock()
        expected = mocked_super_request.return_value = mock.Mock(
            headers={"Content-Length": 1024},
            raise_for_status=raise_for_status,
            content="Connection timeout",
        )

        raise_for_status.side_effect = HTTPError(response=expected)

        wrapper = SessionWrapper(Retry.DEFAULT)

        with self.assertRaises(HTTPError):
            wrapper.request("GET", "test_url", is_test=True)

        mocked_super_request.assert_called_once_with("GET", "test_url", is_test=True)
        mocked_logger.error.assert_called_once_with(
            "HTTPError Reason: Connection timeout"
        )

    @mock.patch("morango.sync.session.logger")
    @mock.patch("morango.sync.session.Session.request")
    def test_request__really_not_ok(self, mocked_super_request, mocked_logger):
        mocked_super_request.side_effect = RequestException()

        wrapper = SessionWrapper(Retry.DEFAULT)

        with self.assertRaises(RequestException):
            wrapper.request("GET", "test_url", is_test=True)

        mocked_super_request.assert_called_once_with("GET", "test_url", is_test=True)
        mocked_logger.error.assert_called_once_with(
            "RequestException Reason: (no response)"
        )

    @mock.patch("morango.sync.session.Session.prepare_request")
    def test_prepare_request(self, mocked_super_prepare_request):
        expected = mocked_super_prepare_request.return_value = mock.Mock()

        request = mock.Mock(url="http://test_app/path/to/resource", method="GET", headers={})
        wrapper = SessionWrapper(Retry.DEFAULT)
        actual = wrapper.prepare_request(request)
        mocked_super_prepare_request.assert_called_once_with(request)

        self.assertEqual(expected, actual)
        self.assertEqual(wrapper.bytes_sent, 0)

    @mock.patch("morango.sync.session.Session.send")
    def test_send(self, mocked_super_send):
        headers = {"Content-Length": 256}
        mocked_super_send.return_value = mock.Mock()

        prepared_request = mock.Mock(
            url="http://test_app/path/to/resource",
            method="GET",
            headers=headers,
        )
        wrapper = SessionWrapper(Retry.DEFAULT)
        wrapper.send(prepared_request)

        mocked_super_send.assert_called_once_with(prepared_request)
        head_length = len("GET /path/to/resource HTTP/1.1") + _length_of_headers(headers)
        self.assertEqual(wrapper.bytes_sent, 256 + head_length)

    def test_should_retry__raises_retry_error_on_max_retries(self):
        retries = mock.Mock()
        retries.allowed_methods = None
        retries.increment.side_effect = MaxRetryError(None, "http://test_app/path/to/resource")

        prepared_request = mock.Mock(
            url="http://test_app/path/to/resource",
            method="GET",
        )

        wrapper = SessionWrapper(Retry.DEFAULT)
        with self.assertRaises(RetryError) as cm:
            wrapper._should_retry(retries, prepared_request, ConnectionError())

        self.assertIs(cm.exception.request, prepared_request)
