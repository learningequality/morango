import logging

from requests import exceptions
from requests.sessions import Session
from requests.utils import super_len


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
    return super_len("\n".join(["{}: {}".format(key, value) for key, value in headers.items()]))


class SessionWrapper(Session):
    """
    Wrapper around `requests.sessions.Session` in order to implement logging around all request errors.
    """

    bytes_sent = 0
    bytes_received = 0

    def request(self, method, url, **kwargs):
        try:
            response = super(SessionWrapper, self).request(method, url, **kwargs)

            # capture bytes received from the response
            content_length = _headers_content_length(response.headers)
            if not content_length:
                content_length = super_len(response.content)

            self.bytes_received += _length_of_headers(response.headers)
            self.bytes_received += content_length

            response.raise_for_status()
            return response
        except exceptions.HTTPError as httpErr:
            logger.error("{} Reason: {}".format(str(httpErr), httpErr.response.json()))
            raise httpErr
        except exceptions.RequestException as reqErr:
            # we want to log all request errors for debugging purposes
            logger.error(str(reqErr))
            raise reqErr

    def prepare_request(self, request):
        """
        Override request preparer so we can get the prepared content length, for tracking
        transfer sizes

        :type request: requests.Request
        :rtype: requests.PreparedRequest
        """
        prepped = super(SessionWrapper, self).prepare_request(request)

        self.bytes_sent += _length_of_headers(prepped.headers)
        self.bytes_sent += _headers_content_length(prepped.headers)

        return prepped

    def reset_transfer_bytes(self):
        """
        Resets the `bytes_sent` and `bytes_received` values to zero
        """
        self.bytes_sent = 0
        self.bytes_received = 0
