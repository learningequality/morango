import hashlib
import ifcfg
import os
import platform
import sys
import uuid

from .fields.uuids import sha2_uuid


from django.conf import settings


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
