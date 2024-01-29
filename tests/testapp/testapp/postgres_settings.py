"""
A settings module for running tests using a postgres db backend.
"""
from .settings import *  # noqa

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'HOST': 'localhost',
        'USER': 'postgres',
        'PASSWORD': 'postgres',
        'NAME': 'default',  # This module should never be used outside of tests -- so this name is irrelevant
        'TEST': {
            'NAME': 'test'
        }
    },
}

MORANGO_TEST_POSTGRESQL = True
