import hashlib
import ifcfg
import os
import platform
import subprocess
import sys
import uuid

from .fields.uuids import sha2_uuid

from django.conf import settings
from django.utils import six


def _get_database_path():
    return os.path.abspath(settings.DATABASES["default"]["NAME"])


def get_0_4_system_parameters(database_id):
    """
    NOTE: Do not modify this function. It is here to ensure that we maintain
    backwards compatibility with the 0.4.x method for calculating the instance ID.
    """

    # on Android, platform.platform() barfs, so we handle that safely here
    try:
        plat = platform.platform()
    except:  # noqa: E722
        plat = "Unknown (Android?)"

    params = {
        "platform": plat,
        "hostname": platform.node(),
        "sysversion": sys.version,
        "database_id": database_id,
        "db_path": _get_database_path(),
    }

    # try to get the MAC address, but exclude it if it was a fake (random) address
    mac = uuid.getnode()
    if (mac >> 40) % 2 == 0:  # 8th bit (of 48 bits, from left) is 1 if MAC is fake
        hashable_identifier = "{}:{}".format(params["database_id"], mac)
        params["node_id"] = hashlib.sha1(
            hashable_identifier.encode("utf-8")
        ).hexdigest()[:20]
    else:
        params["node_id"] = ""

    return params


def _calculate_0_4_uuid(parameters):

    uuid_input_fields = (
        "platform",
        "hostname",
        "sysversion",
        "node_id",
        "database_id",
        "db_path",
    )

    # calculate the input to the UUID function
    hashable_input_vals = []
    for field in uuid_input_fields:
        new_value = parameters.get(field)
        if new_value:
            hashable_input_vals.append(str(new_value))
    hashable_input = ":".join(hashable_input_vals)

    # compute the UUID as a function of the input values
    return sha2_uuid(hashable_input)


def _query_wmic(namespace, key):
    try:
        result = (
            subprocess.check_output("wmic {} get {}".format(namespace, key))
            .decode()
            .split()[-1]
        )

        if "-" in result:
            return result
    except:  # noqa: E722
        pass


def _get_macos_uuid():
    try:
        command = "ioreg -rd1 -c IOPlatformExpertDevice | grep -E '(UUID)'"
        result = subprocess.check_output(command, shell=True)
        return result.decode().split('"')[-2]
    except:  # noqa: E722
        pass


def _get_android_uuid():
    try:
        for propname in ["ro.serialno", "ril.serialnumber", "ro.boot.serialno"]:
            output = subprocess.check_output(["getprop", propname]).decode().strip()
            if output:
                return output
    except:  # noqa: E722
        pass


def _do_salted_hash(value):
    if not value:
        return ""
    if not isinstance(value, six.string_types):
        value = str(value)
    try:
        value = value.encode()
    except:  # noqa: E722
        pass
    value += "::b88281f3-302c-4def-bb87-043f681e4183".encode()
    value = value.lower()
    return hashlib.sha1(value).hexdigest()[:20]


def get_0_5_system_id():
    """
    See https://github.com/learningequality/morango/issues/79#issuecomment-590584559
    """

    # check whether envvar was set, and use that if available
    system_id = os.environ.get("MORANGO_SYSTEM_ID")
    if system_id and len(system_id.strip()) >= 3:
        # skip to the end of the elif's to return
        pass

    # Windows
    elif sys.platform == "win32":

        # try to get the system serial number, if available
        system_id = _query_wmic("csproduct", "UUID")
        # if UUID consists only of "F" digits, it's not usable
        if system_id and not system_id.replace("F", "").replace("-", "").strip():
            system_id = None

        # otherwise, try to get the serial number of the disk drive
        if not system_id:
            system_id = _query_wmic("DISKDRIVE", "SerialNumber")

    # MacOS
    elif sys.platform == "darwin":
        system_id = _get_macos_uuid()

    # Android
    elif "ANDROID_ARGUMENT" in os.environ:
        system_id = _get_android_uuid()

    # Linux
    elif sys.platform.startswith("linux"):
        if os.path.isfile("/etc/machine-id"):
            with open("/etc/machine-id") as f:
                system_id = f.read().strip()

    return _do_salted_hash(system_id)


def _device_sort_key(iface):
    """
    Sort interfaces by device name to give preference to interfaces
    that are more likely to be stable (not change/be removed).
    """
    dev = (iface.get("device") or "").lower()
    if dev.startswith("eth") or dev.startswith("en"):
        return "0" + dev
    if dev.startswith("wl"):
        return "1" + dev
    if dev.startswith("e") or dev.startswith("w"):
        return "2" + dev
    else:
        return dev


def _mac_int_to_ether(mac):
    return ":".join(("%012x" % mac)[i : i + 2] for i in range(0, 12, 2))


def _get_mac_address_flags(mac):
    """
    See: https://en.wikipedia.org/wiki/MAC_address#Universal_vs._local
    """
    if isinstance(mac, six.integer_types):
        mac = _mac_int_to_ether(mac)

    first_octet = int(mac[:2], base=16)

    multicast = first_octet % 2 == 1
    local = (first_octet >> 1) % 2 == 1

    return multicast, local


def _mac_is_multicast(mac):
    return _get_mac_address_flags(mac)[0]


def _mac_is_local(mac):
    return _get_mac_address_flags(mac)[1]


def get_0_5_mac_address():

    # first, try using ifcfg
    interfaces = []
    try:
        interfaces = ifcfg.interfaces().values()
    except:  # noqa: E722
        pass
    for iface in sorted(interfaces, key=_device_sort_key):
        ether = iface.get("ether")
        if ether and not _mac_is_local(ether):
            return _do_salted_hash(ether)

    # fall back to trying uuid.getnode
    mac = uuid.getnode()
    if not _mac_is_multicast(
        mac
    ):  # when uuid.getnode returns a fake MAC, it marks as multicast
        return _do_salted_hash(_mac_int_to_ether(mac))

    return ""
