from __future__ import unicode_literals

from django.apps import AppConfig
from morango.utils.syncing_utils import add_syncing_models


class MorangoConfig(AppConfig):
    name = 'morango'
    verbose_name = 'Morango'

    def ready(self):
        add_syncing_models()
