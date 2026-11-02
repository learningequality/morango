import os

import mock
import pytest

HOST_MAC_ADDRESS = "00:11:22:33:44:55"


@pytest.fixture(scope="session", autouse=True)
def fixed_host_identity():
    """
    Run every test as if the host has the same readable MAC address, and without the instance ID
    environment overrides, so the instance ID does not depend on the host running the tests.
    Tests for other hosts, such as one without a readable MAC address, mock `ifcfg.interfaces`
    and `uuid.getnode` themselves.
    """
    env = {k: v for k, v in os.environ.items() if k not in ("MORANGO_SYSTEM_ID", "MORANGO_NODE_ID")}
    interfaces = {"eth0": {"device": "eth0", "ether": HOST_MAC_ADDRESS}}
    node = int(HOST_MAC_ADDRESS.replace(":", ""), 16)
    with mock.patch.dict(os.environ, env, clear=True):
        with mock.patch("uuid.getnode", return_value=node):
            with mock.patch("ifcfg.interfaces", return_value=interfaces):
                yield
