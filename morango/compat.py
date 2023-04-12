import sys


def monkey_patch_collections():
    """
    Monkey-patching for the collections module is required for Python 3.10
    and above.
    Prior to 3.10, the collections module still contained all the entities defined in
    collections.abc from Python 3.3 onwards. Here we patch those back into main
    collections module.
    This can be removed when we upgrade to a version of Django that is Python 3.10 compatible.
    Copied from:
    https://github.com/learningequality/kolibri/blob/589dd15aa79e8694aff8754bb34f12384315dbb6/kolibri/utils/compat.py#L90
    """
    if sys.version_info < (3, 10):
        return
    import collections
    from collections import abc

    for name in dir(abc):
        if not hasattr(collections, name):
            setattr(collections, name, getattr(abc, name))
