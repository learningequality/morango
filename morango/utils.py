from django.conf import settings

from morango.constants import settings as default_settings
from morango.constants.capabilities import ALLOW_CERTIFICATE_PUSHING
from morango.constants.capabilities import GZIP_BUFFER_POST
from morango.constants.capabilities import ASYNC_OPERATIONS


class Settings(object):
    """
    Maps the settings from constants file with their defaults to this class which allows access
    via attributes similar to Django settings
    """
    __slots__ = [key for key in dir(default_settings) if not key.startswith('__')]

    def __getattribute__(self, key):
        """Coalesces settings with the defaults"""
        return getattr(settings, key, getattr(default_settings, key, None))


SETTINGS = Settings()


def get_capabilities():
    capabilities = set()

    try:
        import gzip  # noqa

        capabilities.add(GZIP_BUFFER_POST)
    except ImportError:
        pass

    if SETTINGS.ALLOW_CERTIFICATE_PUSHING:
        capabilities.add(ALLOW_CERTIFICATE_PUSHING)

    # Middleware async operation capabilities are standard in 0.6.0 and above
    if SETTINGS.MORANGO_DISALLOW_ASYNC_OPERATIONS:
        capabilities.add(ASYNC_OPERATIONS)

    return capabilities


CAPABILITIES = get_capabilities()
CAPABILITIES_CLIENT_HEADER = "X-Morango-Capabilities"
CAPABILITIES_SERVER_HEADER = CAPABILITIES_CLIENT_HEADER.upper().replace('-', '_')


def serialize_capabilities_to_client_request(request):
    """
    :param request: The client request sending to another Morango server
    :type request: requests.Request
    """
    request.headers[CAPABILITIES_CLIENT_HEADER] = ' '.join(CAPABILITIES)


def parse_capabilities_from_server_request(request):
    """
    :param request: The request object received from a Morango client
    :type request: django.http.request.HttpRequest
    :return: A set of capabilities
    """
    return set(request.META.get(CAPABILITIES_SERVER_HEADER, '').split(' '))
