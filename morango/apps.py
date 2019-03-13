from __future__ import unicode_literals

import logging as logger

from django.apps import AppConfig
from django.db import connection
from morango.util import max_parameter_substitution
from morango.utils.register_models import add_syncable_models

logging = logger.getLogger(__name__)


class MorangoConfig(AppConfig):
    name = 'morango'
    verbose_name = 'Morango'

    def ready(self):
        from .signals import add_to_deleted_models  # noqa: F401

        # add models to be synced by profile
        add_syncable_models()
        if 'sqlite' in connection.vendor:
            max_parameter_substitution()
