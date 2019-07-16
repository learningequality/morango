import logging

from requests import exceptions
from requests.sessions import Session

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError

logger = logging.getLogger(__name__)


class SessionWrapper(Session):
    """
    Wrapper around `requests.sessions.Session` in order to implement logging around all request errors.
    """

    def request(self, method, url, **kwargs):
        try:
            response = super(SessionWrapper, self).request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except exceptions.HTTPError as httpErr:
            try:
                reason = httpErr.response.json()
            except JSONDecodeError:
                reason = httpErr.response.reason
            logger.error("{} Reason: {}".format(str(httpErr), reason))
            raise httpErr
        except exceptions.RequestException as reqErr:
            # we want to log all request errors for debugging purposes
            logger.error(str(reqErr))
            raise reqErr
