import mock
from django.test import TestCase
from requests.exceptions import HTTPError
from requests.exceptions import RequestException


from morango.sync.session import _length_of_headers
from morango.sync.session import SessionWrapper


class SessionWrapperTestCase(TestCase):
    @mock.patch("morango.sync.session.Session.request")
    def test_request(self, mocked_super_request):
        headers = {"Content-Length": 1024}
        expected = mocked_super_request.return_value = mock.Mock(
            headers=headers, raise_for_status=mock.Mock(), status_code=200, reason="OK"
        )

        wrapper = SessionWrapper()
        actual = wrapper.request("GET", "test_url", is_test=True)
        mocked_super_request.assert_called_once_with("GET", "test_url", is_test=True)
        self.assertEqual(expected, actual)

        head_length = len("HTTP/1.1 200 OK") + _length_of_headers(headers)
        self.assertEqual(wrapper.bytes_received, 1024 + head_length)

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

        wrapper = SessionWrapper()

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

        wrapper = SessionWrapper()

        with self.assertRaises(RequestException):
            wrapper.request("GET", "test_url", is_test=True)

        mocked_super_request.assert_called_once_with("GET", "test_url", is_test=True)
        mocked_logger.error.assert_called_once_with(
            "RequestException Reason: (no response)"
        )

    @mock.patch("morango.sync.session.Session.prepare_request")
    def test_prepare_request(self, mocked_super_prepare_request):
        headers = {"Content-Length": 256}
        expected = mocked_super_prepare_request.return_value = mock.Mock(
            headers=headers,
        )

        request = mock.Mock(url="http://test_app/path/to/resource", method="GET", headers={})
        wrapper = SessionWrapper()
        actual = wrapper.prepare_request(request)
        mocked_super_prepare_request.assert_called_once_with(request)

        self.assertEqual(expected, actual)
        head_length = len("GET /path/to/resource HTTP/1.1") + _length_of_headers(
            headers
        )
        self.assertEqual(wrapper.bytes_sent, 256 + head_length)
