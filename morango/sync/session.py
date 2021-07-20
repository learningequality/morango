import logging

from requests import exceptions
from requests.sessions import Session
from requests.utils import super_len
from requests.packages.urllib3.util.url import parse_url

from morango.utils import serialize_capabilities_to_client_request


logger = logging.getLogger(__name__)


def _headers_content_length(headers):
    try:
        content_length = int(headers.get("Content-Length", 0))
        if content_length > 0:
            return content_length
    except TypeError:
        pass
    return 0


def _length_of_headers(headers):
    return super_len(
        "\n".join(["{}: {}".format(key, value) for key, value in headers.items()])
    )


class SessionWrapper(Session):
    """
    Wrapper around `requests.sessions.Session` in order to implement logging around all request errors.
    """

    bytes_sent = 0
    bytes_received = 0

    def request(self, method, url, **kwargs):
        response = None
        try:
            response = super(SessionWrapper, self).request(method, url, **kwargs)

            # capture bytes received from the response, the length header could be missing if it's
            # a chunked response though
            content_length = _headers_content_length(response.headers)
            if not content_length:
                content_length = super_len(response.content)

            self.bytes_received += len(
                "HTTP/1.1 {} {}".format(response.status_code, response.reason)
            )
            self.bytes_received += _length_of_headers(response.headers)
            self.bytes_received += content_length

            response.raise_for_status()
            return response
        except exceptions.RequestException as req_err:
            # we want to log all request errors for debugging purposes
            if response is None:
                response = req_err.response

            response_content = response.content if response else "(no response)"
            logger.error(
                "{} Reason: {}".format(req_err.__class__.__name__, response_content)
            )
            raise req_err

    def prepare_request(self, request):
        """
        Override request preparer so we can get the prepared content length, for tracking
        transfer sizes

        :type request: requests.Request
        :rtype: requests.PreparedRequest
        """
        # add header with client's morango capabilities so server has that information
        serialize_capabilities_to_client_request(request)
        prepped = super(SessionWrapper, self).prepare_request(request)
        parsed_url = parse_url(request.url)

        # we don't bother checking if the content length header exists here because we've probably
        # been given the request body as Morango sends bodies that aren't streamed, so the
        # underlying requests code will set it appropriately
        self.bytes_sent += len("{} {} HTTP/1.1".format(request.method, parsed_url.path))
        self.bytes_sent += _length_of_headers(prepped.headers)
        self.bytes_sent += _headers_content_length(prepped.headers)

        return prepped

    def reset_transfer_bytes(self):
        """
        Resets the `bytes_sent` and `bytes_received` values to zero
        """
        self.bytes_sent = 0
        self.bytes_received = 0
