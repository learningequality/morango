import os

from .settings import *  # noqa

DATABASES = {
    "default": DATABASES["default2"],  # noqa
}

# emulates a server on a morango version before 0.6.0, which lacks both of these capabilities
if os.environ.get("MORANGO_TEST_LEGACY_SERVER"):
    MORANGO_DISALLOW_ASYNC_OPERATIONS = True
    MORANGO_DISABLE_FSIC_V2_FORMAT = True
