from django.conf import settings

from morango.constants.capabilities import ALLOW_CERTIFICATE_PUSHING
from morango.constants.capabilities import GZIP_BUFFER_POST


def get_capabilities():
    capabilities = set()

    try:
        import gzip  # noqa

        capabilities.add(GZIP_BUFFER_POST)
    except ImportError:
        pass

    if getattr(settings, "ALLOW_CERTIFICATE_PUSHING", False):
        capabilities.add(ALLOW_CERTIFICATE_PUSHING)
    return capabilities


CAPABILITIES = get_capabilities()
