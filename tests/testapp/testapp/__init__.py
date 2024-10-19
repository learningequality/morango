import sys


def forward_port_cgi_module():
    """
    Forward ports the required parts of the removed cgi module.
    This can be removed when we upgrade to a version of Django that is Python 3.13 compatible.
    """
    if sys.version_info < (3, 13):
        return
    from importlib import import_module

    module = import_module("testapp.cgi")
    sys.modules["cgi"] = module


forward_port_cgi_module()
