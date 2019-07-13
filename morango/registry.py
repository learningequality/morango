"""
`SyncableModelRegistry` holds all syncable models for a project, on a per profile basis.
This class is registered at app load time for morango in `apps.py`.
"""
import sys
from collections import OrderedDict

from django.db.models.fields.related import ForeignKey
from django.utils import six

from morango.errors import InvalidMorangoModelConfiguration
from morango.errors import ModelRegistryNotReady
from morango.errors import UnsupportedFieldType


def _get_foreign_key_classes(m):
    return set(
        [field.rel.to for field in m._meta.fields if isinstance(field, ForeignKey)]
    )


def _multiple_self_ref_fk_check(class_model):
    """
    We check whether a class has more than 1 FK reference to itself.
    """
    self_fk = []
    for f in class_model._meta.concrete_fields:
        if f.related_model in self_fk:
            return True
        if f.related_model == class_model:
            self_fk.append(class_model)
    return False


class SyncableModelRegistry(object):
    def __init__(self):
        self.profile_models = {}
        self.ready = False
        self.models_ready = {}
        if hasattr(sys.modules[__name__], "syncable_models"):
            raise RuntimeError("Master registry has already been initialized.")

    def check_models_ready(self, profile):
        """Raise an exception if all models haven't been imported yet."""
        if not self.models_ready.get(profile):
            raise ModelRegistryNotReady(
                "Models for profile {} aren't loaded yet.".format(profile)
            )

    def get_model(self, profile, model_name):
        """
        Return the model matching the given profile and model_name.
        """
        self.check_models_ready(profile)
        return self.profile_models[profile][model_name]

    def get_models(self, profile):
        """
        Return a list of all syncable models for this profile.
        """
        self.check_models_ready(profile)
        return list(self.profile_models.get(profile, {}).values())

    def _insert_model_in_dependency_order(self, model, profile):
        # When we add models to be synced, we need to make sure
        #   that models that depend on other models are synced AFTER
        #   the model it depends on has been synced.

        # Get the dependencies of the new model
        foreign_key_classes = _get_foreign_key_classes(model)

        # add any more specified dependencies
        if hasattr(model, "morango_model_dependencies"):
            foreign_key_classes = foreign_key_classes | set(
                model.morango_model_dependencies
            )

        # Find all the existing models that this new model refers to.
        class_indices = [
            self.profile_models[profile].index(cls)
            for cls in foreign_key_classes
            if cls in self.profile_models[profile]
        ]

        # Insert just after the last dependency found,
        #   or at the front if no dependencies
        insert_after_idx = 1 + (max(class_indices) if class_indices else -1)

        # Now we're ready to insert.
        self.profile_models[profile].insert(insert_after_idx, model)

    def populate(self):  # noqa: C901
        if self.ready:
            return

        import django.apps
        from morango.models.core import SyncableModel
        from morango.models.manager import SyncableModelManager
        from morango.models.query import SyncableModelQuerySet

        model_list = []
        for model in django.apps.apps.get_models():
            # several validation checks to assert models will be syncing correctly
            if issubclass(model, SyncableModel):
                name = model.__name__
                if _multiple_self_ref_fk_check(model):
                    raise InvalidMorangoModelConfiguration(
                        "Syncing models with more than 1 self referential ForeignKey is not supported."
                    )
                try:
                    from mptt import models
                    from morango.models.morango_mptt import (
                        MorangoMPTTModel,
                        MorangoMPTTTreeManager,
                        MorangoTreeQuerySet,
                    )

                    # mptt syncable model checks
                    if issubclass(model, models.MPTTModel):
                        if not issubclass(model, MorangoMPTTModel):
                            raise InvalidMorangoModelConfiguration(
                                "{} that inherits from MPTTModel, should instead inherit from MorangoMPTTModel.".format(
                                    name
                                )
                            )
                        if not isinstance(model.objects, MorangoMPTTTreeManager):
                            raise InvalidMorangoModelConfiguration(
                                "Manager for {} must inherit from MorangoMPTTTreeManager.".format(
                                    name
                                )
                            )
                        if not isinstance(model.objects.none(), MorangoTreeQuerySet):
                            raise InvalidMorangoModelConfiguration(
                                "Queryset for {} model must inherit from MorangoTreeQuerySet.".format(
                                    name
                                )
                            )
                except ImportError:
                    pass
                # syncable model checks
                if not isinstance(model.objects, SyncableModelManager):
                    raise InvalidMorangoModelConfiguration(
                        "Manager for {} must inherit from SyncableModelManager.".format(
                            name
                        )
                    )
                if not isinstance(model.objects.none(), SyncableModelQuerySet):
                    raise InvalidMorangoModelConfiguration(
                        "Queryset for {} model must inherit from SyncableModelQuerySet.".format(
                            name
                        )
                    )
                if model._meta.many_to_many:
                    raise UnsupportedFieldType(
                        "{} model with a ManyToManyField is not supported in morango."
                    )
                if not hasattr(model, "morango_model_name"):
                    raise InvalidMorangoModelConfiguration(
                        "{} model must define a morango_model_name attribute".format(
                            name
                        )
                    )
                if not hasattr(model, "morango_profile"):
                    raise InvalidMorangoModelConfiguration(
                        "{} model must define a morango_profile attribute".format(name)
                    )

                # create empty list to hold model classes for profile if not yet created
                profile = model.morango_profile
                self.profile_models[profile] = self.profile_models.get(profile, [])

                # don't sync models where morango_model_name is None
                if model.morango_model_name is not None:
                    self._insert_model_in_dependency_order(model, profile)

        # for each profile, create a dict mapping from morango_model_name to model class
        for profile, model_list in six.iteritems(self.profile_models):
            mapping = OrderedDict()
            for model in model_list:
                mapping[model.morango_model_name] = model
            self.profile_models[profile] = mapping
            self.models_ready[profile] = True

        self.ready = True


syncable_models = SyncableModelRegistry()
