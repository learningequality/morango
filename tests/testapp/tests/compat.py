try:
    # In the Python EOL GH workflows, we have to install backported version
    # as the Docker container images we use does not have the test module installed.
    from backports.test.support import EnvironmentVarGuard # noqa F401
except ImportError:
    try:
        # For python >3.8 and <3.10
        from test.support import EnvironmentVarGuard # noqa F401
    except ImportError:
        # In Python 3.10, this has been moved to test.support.os_helper
        from test.support.os_helper import EnvironmentVarGuard # noqa F401
