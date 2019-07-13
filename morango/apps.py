from __future__ import unicode_literals

from django.apps import AppConfig

from morango.registry import syncable_models


class MorangoConfig(AppConfig):
    name = "morango"
    verbose_name = "Morango"

    def ready(self):
        from morango.models.signals import add_to_deleted_models  # noqa: F401

        # populate syncable model registry by profile
        syncable_models.populate()
