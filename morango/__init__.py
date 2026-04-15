try:
    from importlib.metadata import version

    __version__ = version("morango")
except ImportError:
    # Fallback for older Python versions or when package is not installed
    __version__ = "unknown"
