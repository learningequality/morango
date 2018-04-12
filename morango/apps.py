from __future__ import unicode_literals

import logging as logger

from django.apps import AppConfig
from morango.utils.register_models import add_syncable_models

logging = logger.getLogger(__name__)


class MorangoConfig(AppConfig):
    name = 'morango'
    verbose_name = 'Morango'

    def ready(self):
        from .signals import add_to_deleted_models  # noqa: F401

        # add models to be synced by profile
        add_syncable_models()
