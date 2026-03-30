#!/usr/bin/env python3
"""
Comparison script to test ifcfg vs getmac behavior
"""
import locale
import os
import platform
import sys


def get_system_language():
    """Get system language settings"""
    try:
        # Try to get system language settings
        lang_info = {
            "locale": locale.getlocale(),
            "default_locale": locale.getdefaultlocale(),
            "environment": {},
        }

        # Check environment variables
        for var in ["LANG", "LC_ALL", "LC_MESSAGES", "LANGUAGE"]:
            if var in os.environ:
                lang_info["environment"][var] = os.environ[var]

        return lang_info
    except Exception as e:
        return {"error": str(e)}


def get_ifcfg_macs():
    """Get MAC addresses using ifcfg"""
    try:
        import ifcfg

        interfaces = ifcfg.interfaces().values()
        results = {}
        for iface in interfaces:
            ether = iface.get("ether")
            if ether:
                results[iface.get("device", "unknown")] = ether
        return results
    except Exception as e:
        return {"error": str(e)}


def get_getmac_macs():
    """Get MAC addresses using getmac"""
    try:
        import getmac

        results = {}

        # Default interface
        try:
            mac = getmac.get_mac_address()
            if mac:
                results["default"] = mac
        except Exception as e:
            results["default_error"] = str(e)

        # Common interfaces
        common_interfaces = ["eth0", "en0", "wlan0", "wi-fi", "Ethernet"]
        for interface in common_interfaces:
            try:
                mac = getmac.get_mac_address(interface=interface)
                if mac:
                    results[interface] = mac
            except Exception as e:
                results[f"{interface}_error"] = str(e)

        return results
    except Exception as e:
        return {"error": str(e)}


def main():
    print(f"Platform: {platform.platform()}")
    print(f"System: {platform.system()}")
    print(f"Python: {sys.version}")

    # Get and display system language information
    print(f"\n=== System Language Information ===")
    lang_info = get_system_language()
    for key, value in lang_info.items():
        if key == "environment":
            print(f"{key}:")
            for env_var, env_value in value.items():
                print(f"  {env_var}: {env_value}")
        else:
            print(f"{key}: {value}")
    print()

    print("=== ifcfg results ===")
    ifcfg_results = get_ifcfg_macs()
    for interface, mac in ifcfg_results.items():
        print(f"{interface}: {mac}")
    print()

    print("=== getmac results ===")
    getmac_results = get_getmac_macs()
    for interface, mac in getmac_results.items():
        print(f"{interface}: {mac}")
    print()

    # Compare default interfaces
    ifcfg_default = ifcfg_results.get("eth0") or next(
        iter(ifcfg_results.values()), None
    )
    getmac_default = getmac_results.get("default")

    print("=== Comparison ===")
    print(f"ifcfg default: {ifcfg_default}")
    print(f"getmac default: {getmac_default}")
    print(f"Match: {ifcfg_default == getmac_default}")

    # Additional analysis
    print(f"\n=== Analysis ===")
    ifcfg_count = len(
        [
            v
            for v in ifcfg_results.values()
            if not isinstance(v, str) or not v.startswith("error")
        ]
    )
    getmac_count = len(
        [
            v
            for v in getmac_results.values()
            if not isinstance(v, str) or not v.startswith("error")
        ]
    )
    print(f"ifcfg found {ifcfg_count} interfaces")
    print(f"getmac found {getmac_count} interfaces")

    # Check for errors
    ifcfg_errors = [
        v
        for v in ifcfg_results.values()
        if isinstance(v, str) and v.startswith("error")
    ]
    getmac_errors = [
        v
        for v in getmac_results.values()
        if isinstance(v, str) and v.startswith("error")
    ]

    print(f"ifcfg errors: {len(ifcfg_errors)}")
    print(f"getmac errors: {len(getmac_errors)}")


if __name__ == "__main__":
    main()
