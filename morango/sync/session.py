import logging
from contextlib import contextmanager

from requests import exceptions
from requests.adapters import HTTPAdapter
from requests.sessions import Session
from requests.utils import super_len
from urllib3.exceptions import MaxRetryError
from urllib3.util.url import parse_url

from morango import __version__
from morango.utils import nullcontext
from morango.utils import serialize_capabilities_to_client_request
from morango.utils import SETTINGS

logger = logging.getLogger(__name__)


_RETRY_REQUEST_EXCEPTIONS = (
    exceptions.ConnectionError,
    exceptions.ChunkedEncodingError,
    exceptions.ContentDecodingError,
)


def _headers_content_length(headers):
    """
    Obtains the value of 'Content-Length' from the provided headers.

    :param headers: Dictionary of headers
    :type headers: dict
    :return: The integer value of 'Content-Length' if found and valid, otherwise 0.
    :rtype: int
    """
    try:
        content_length = int(headers.get("Content-Length", 0))
        if content_length > 0:
            return content_length
    except TypeError:
        pass
    return 0


def _length_of_headers(headers):
    """
    Calculates the total length of all headers.

    :param headers: Dictionary of headers
    :type headers: dict
    :return: The total length of the string representation of all headers.
    :rtype: int
    """
    return super_len("\n".join(["{}: {}".format(key, value) for key, value in headers.items()]))


def _is_retryable_method(retries, method):
    """
    Checks if the request method is configured as retryable.

    :type retries: urllib3.util.retry.Retry
    :type method: str|None
    :rtype: bool
    """
    allowed_methods = getattr(retries, "allowed_methods", None)
    if allowed_methods is False or allowed_methods is None:
        return True
    return method.upper() in allowed_methods if method is not None else False


def _log_response_error(err, response):
    """
    Logs an error and its associated response content.

    :param err: The exception instance that represents the error encountered.
    :type err: Exception
    :param response: The HTTP response object associated with the error.
        If None, it is interpreted as no response being available.
    :type response: Optional[Response]
    """
    try:
        response_content = response.content if response else "(no response)"
    except Exception:
        response_content = "(unable to read response)"
    logger.error("{} Reason: {}".format(err.__class__.__name__, response_content))


class ContextualRetryHTTPAdapter(HTTPAdapter):
    @contextmanager
    def use_retries(self, max_retries):
        """
        Context manager for temporarily changing the retry configuration.

        :param max_retries: The temporary Retry object
        :type max_retries: urllib3.util.retry.Retry
        """
        original_retries = self.max_retries
        try:
            self.max_retries = max_retries
            yield
        finally:
            self.max_retries = original_retries


class SessionWrapper(Session):
    """
    Wrapper around `requests.sessions.Session` in order to implement logging around all request errors.
    """

    def __init__(self, max_retries):
        """
        :param max_retries: The urllib3 Retry object
        :type max_retries: urllib3.util.retry.Retry
        """
        super(SessionWrapper, self).__init__()
        self.max_retries = max_retries

        user_agent_header = "morango/{}".format(__version__)
        if SETTINGS.CUSTOM_INSTANCE_INFO is not None:
            instances = list(SETTINGS.CUSTOM_INSTANCE_INFO)
            if instances:
                user_agent_header += " " + "{}/{}".format(
                    instances[0], SETTINGS.CUSTOM_INSTANCE_INFO.get(instances[0])
                )
        self.headers["User-Agent"] = "{} {}".format(user_agent_header, self.headers["User-Agent"])
        self.hooks["response"].append(self._track_bytes_received)
        self.bytes_sent = 0
        self.bytes_received = 0

        # use custom adapter
        adapter = ContextualRetryHTTPAdapter()
        self.mount("http://", adapter)
        self.mount("https://", adapter)

    def _track_bytes_sent(self, request):
        """
        Request hook that tracks the size of the request, by capturing the size of headers and the
        request body. Note: python requests only supports the `response` hook, so this is invoked
        manually

        :type request: requests.Request|requests.PreparedRequest
        """
        try:
            parsed_url = parse_url(request.url)
            # we don't bother checking if the content length header exists here because we've probably
            # been given the request body as Morango sends bodies that aren't streamed, so the
            # underlying requests code will set it appropriately
            self.bytes_sent += len("{} {} HTTP/1.1".format(request.method, parsed_url.path))
            self.bytes_sent += _length_of_headers(request.headers)
            self.bytes_sent += _headers_content_length(request.headers)
        except Exception as e:
            # tracking bandwidth usage is useful but not critical
            logger.exception(e)

    def _track_bytes_received(self, response, *args, **kwargs):
        """
        Response hook that tracks the size of the response, by capturing the size of headers and
        the response body

        :type response: requests.Response
        """
        try:
            # headers:
            self.bytes_received += len(
                "HTTP/1.1 {} {}".format(response.status_code, response.reason)
            )
            self.bytes_received += _length_of_headers(response.headers)

            # body:
            # capture bytes received from the response, the length header could be missing if it's
            # a chunked response though
            content_length = _headers_content_length(response.headers)
            if not content_length:
                content_length = super_len(response.content)
            self.bytes_received += content_length
        except Exception as e:
            # tracking bandwidth usage is useful but not critical
            logger.exception(e)

    def _get_adapter(self, url):
        """
        :param url: the request URL
        :type url: bytes|str|None
        :rtype: Optional[HTTPAdapter]
        """
        if url is None:
            return None
        # requests allows bytes
        if isinstance(url, bytes):
            url = url.decode("utf-8")
        try:
            return self.get_adapter(url)
        except exceptions.InvalidSchema:
            return None

    def prepare_request(self, request):
        """
        Override request preparer so we can add morango capabilities to the request, and invoke
        the sent bytes hook.

        :type request: requests.Request
        :rtype: requests.PreparedRequest
        """
        # add header with client's morango capabilities so server has that information
        serialize_capabilities_to_client_request(request)
        return super(SessionWrapper, self).prepare_request(request)

    def request(self, method, url, **kwargs):
        """
        Issues an HTTP request, with conditional retry behavior if passed `is_retryable` kwarg, and
        logs any errors from the request flow.

        :param method: The HTTP request method (e.g., 'GET', 'POST', 'PUT', etc.).
        :type method: str
        :param url: The URL to send the request to.
        :type url: str
        :param kwargs: Additional arguments to pass to the underlying request method.
        :return: The HTTP response object obtained from the request.
        :rtype: requests.Response
        :raises Exception: Logs then re-raises any exception that occurs during the request.
        """
        adapter = self._get_adapter(url)

        # super's request has strict kwarg list, so we have to pop `is_retryable` and modify
        # the adapter state based on the value
        is_retryable = kwargs.pop("is_retryable", False)
        if is_retryable and isinstance(adapter, ContextualRetryHTTPAdapter):
            ctx = adapter.use_retries(self.max_retries)
        else:
            ctx = nullcontext()

        with ctx:
            response = None
            try:
                response = super(SessionWrapper, self).request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except Exception as e:
                if response is None:
                    response = getattr(e, "response", None)

                _log_response_error(e, response)
                raise e

    def send(self, request, **kwargs):
        """
        Issues an HTTP request with automatic retry handling for transport-level failures and
        logging of request-related errors.

        :param request: The prepared request
        :type request: requests.PreparedRequest
        :param kwargs: Additional arguments to pass to the underlying `send` method.
        :return: The HTTP response object obtained from the request.
        :rtype: requests.Response
        """
        adapter = self._get_adapter(request.url)
        retries = adapter.max_retries if adapter is not None else None

        while True:
            # sent bytes from low-level retries in urllib3 are not captured, but this is good enough
            self._track_bytes_sent(request)
            response = None
            try:
                response = super(SessionWrapper, self).send(request, **kwargs)
                return response
            except exceptions.RequestException as e:
                retries = self._should_retry(retries, request, e, response=response)
                if retries is None:
                    raise e

    def _should_retry(self, retries, request, e, response=None):
        """
        Determines whether a request should be retried based on the provided criteria, including
        the retry settings, the type of exception occurred, and the HTTP method used. This method
        also handles sleeping between retries and increments retry counters appropriately. If
        retries are exhausted, it raises a `RetryError`.

        :param retries: The retry configuration instance that tracks and manages retry attempts.
        :type retries: Optional[urllib3.util.retry.Retry]
        :param request: The HTTP request object that specifies the details of the request being made.
        :type request: requests.PreparedRequest
        :param e: The exception raised during the request execution that triggered this check.
        :type e: requests.exceptions.RequestException
        :param response: Optional. The HTTP response object corresponding to the request, if available.
        :type response: Optional[requests.Response]
        :return: The new Retry object if the retry is allowed, otherwise None.
        :rtype: Optional[urllib3.util.retry.Retry]
        """
        if response is None:
            response = e.response

        if (
            retries is None
            or retries.total == 0
            or not isinstance(e, _RETRY_REQUEST_EXCEPTIONS)
            or not _is_retryable_method(retries, request.method)
        ):
            return None

        # requests might wrap `MaxRetryError` in a `ConnectionError`
        if any(isinstance(arg, MaxRetryError) for arg in e.args):
            return None

        try:
            # may raise if retries have been exhausted
            retries = retries.increment(
                method=request.method, url=request.url, response=response, error=e
            )
            logger.debug(f"Sleeping before retrying {request.method} request to {request.url}")
            retries.sleep(response)
            return retries
        except MaxRetryError as _e:
            # re-raise wrapped exception, like requests would. see `HTTPAdapter.send`
            raise exceptions.RetryError(_e.reason, request=request, response=response)

    def reset_transfer_bytes(self):
        """
        Resets the `bytes_sent` and `bytes_received` values to zero
        """
        self.bytes_sent = 0
        self.bytes_received = 0
