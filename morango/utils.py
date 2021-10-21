import os
import six
from importlib import import_module

from django.conf import settings

from morango.constants import settings as default_settings
from morango.constants.capabilities import ALLOW_CERTIFICATE_PUSHING
from morango.constants.capabilities import GZIP_BUFFER_POST
from morango.constants.capabilities import ASYNC_OPERATIONS


def do_import(import_string):
    """
    Imports an object from a package
    :param import_string: An import string formatted - package.sub.file:ThingToImport
    :return: The imported object
    """
    callable_module, callable_name = import_string.rsplit(":", 1)
    return getattr(import_module(callable_module), callable_name)


class Settings(object):
    """
    Maps the settings from constants file with their defaults to this class which allows access
    via attributes similar to Django settings
    """

    __slots__ = [key for key in dir(default_settings) if not key.startswith("__")]

    def __getattribute__(self, key):
        """Coalesces settings with the defaults"""
        value = getattr(settings, key, getattr(default_settings, key, None))
        if key == "MORANGO_INSTANCE_INFO" and isinstance(value, six.string_types):
            value = dict(do_import(value))
        return value


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
    if not SETTINGS.MORANGO_DISALLOW_ASYNC_OPERATIONS:
        capabilities.add(ASYNC_OPERATIONS)

    return capabilities


CAPABILITIES = get_capabilities()
CAPABILITIES_CLIENT_HEADER = "X-Morango-Capabilities"
CAPABILITIES_SERVER_HEADER = "HTTP_{}".format(
    CAPABILITIES_CLIENT_HEADER.upper().replace("-", "_")
)


def serialize_capabilities_to_client_request(request):
    """
    :param request: The client request sending to another Morango server
    :type request: requests.Request
    """
    request.headers[CAPABILITIES_CLIENT_HEADER] = " ".join(CAPABILITIES)


def parse_capabilities_from_server_request(request):
    """
    :param request: The request object received from a Morango client
    :type request: django.http.request.HttpRequest
    :return: A set of capabilities
    """
    return set(request.META.get(CAPABILITIES_SERVER_HEADER, "").split(" "))


def _posix_pid_exists(pid):
    """Check whether PID exists in the current process table."""
    import errno

    if pid < 0:
        return False
    try:
        # Send signal 0, this is harmless
        os.kill(pid, 0)
    except OSError as e:
        return e.errno == errno.EPERM
    else:
        return True


def _windows_pid_exists(pid):
    import ctypes

    kernel32 = ctypes.windll.kernel32
    SYNCHRONIZE = 0x100000

    process = kernel32.OpenProcess(SYNCHRONIZE, 0, pid)
    if process != 0:
        kernel32.CloseHandle(process)
        return True
    else:
        return False


if os.name == "posix":
    pid_exists = _posix_pid_exists
else:
    pid_exists = _windows_pid_exists


def _assert(condition, message):
    """
    :param condition: A bool condition that if false will raise an AssertionError
    :param message: assertion error detail message
    """
    if not condition:
        raise AssertionError(message)
