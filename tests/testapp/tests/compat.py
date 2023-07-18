import sys

try:
    # In the Python 2.7 GH workflows, we have to install backported version
    if sys.version_info.major == 2:
        from backports.test.support import EnvironmentVarGuard # noqa F401
    else:
        from test.support import EnvironmentVarGuard # noqa F401
except ImportError:
    # In Python 3.10, this has been moved to test.support.os_helper
    from test.support.os_helper import EnvironmentVarGuard # noqa F401
