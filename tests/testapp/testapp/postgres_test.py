"""
A settings module for running tests using a postgres db backend.
"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

try:
    isolation_level = None
    import psycopg2
    isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
except ImportError:
    pass

from .settings import *  # noqa

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'USER': 'postgres',
        'PASSWORD': '',
        'NAME': 'default',  # This module should never be used outside of tests -- so this name is irrelevant
        'TEST': {
            'NAME': 'travis_ci_default'
        }
    },
    'default-serializable': {
        'ENGINE': 'django.db.backends.postgresql',
        'USER': 'postgres',
        'PASSWORD': '',
        'NAME': 'default',  # This module should never be used outside of tests -- so this name is irrelevant
        'TEST': {
            'NAME': 'travis_ci_default'
        },
        'OPTIONS': {
            'isolation_level': isolation_level,
        }
    }
}
