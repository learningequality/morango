import logging

from requests import exceptions
from requests.sessions import Session


logger = logging.getLogger(__name__)


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
            try:
                content_length = response.headers.get("Content-Length", 0)
                if content_length:
                    self.bytes_received += int(content_length)
            except TypeError:
                pass

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

        try:
            content_length = prepped.headers.get("Content-Length", 0)
            if content_length:
                self.bytes_sent += int(content_length)
        except TypeError:
            pass

        return prepped

    def reset_transfer_bytes(self):
        """
        Resets the `bytes_sent` and `bytes_received` values to zero
        """
        self.bytes_sent = 0
        self.bytes_received = 0
