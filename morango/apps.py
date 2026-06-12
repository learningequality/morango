from django.apps import AppConfig

from morango.registry import session_middleware
from morango.registry import syncable_models


class MorangoConfig(AppConfig):
    name = "morango"
    verbose_name = "Morango"
    default_auto_field = "django.db.models.AutoField"

    def ready(self):
        from morango.models.signals import add_to_deleted_models  # noqa: F401

        # populate syncable model registry by profile
        syncable_models.populate()

        # populate session controller middleware from settings
        session_middleware.populate()
