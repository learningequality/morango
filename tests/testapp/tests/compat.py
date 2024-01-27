try:
    # For python >2.7 and <3.10
    from test.support import EnvironmentVarGuard # noqa F401
except ImportError:
    # In Python 3.10, this has been moved to test.support.os_helper
    from test.support.os_helper import EnvironmentVarGuard # noqa F401
