"""
`register_morango_profile` should be called when an app wants to create a class that they would
like to inherit from to make their data syncable. This method takes care of registering morango
data structures on a per-profile basis.
"""

from collections import OrderedDict
from django.db.models.fields.related import ForeignKey
from django.utils.six import iteritems

from morango.errors import (
    InvalidMorangoModelConfiguration, InvalidMPTTManager, InvalidMPTTQuerySet, InvalidSyncableManager, InvalidSyncableQueryset, UnsupportedFieldType
)

_profile_models = {}


def _get_foreign_key_classes(m):
    return set([field.rel.to for field in m._meta.fields if isinstance(field, ForeignKey)])

def _insert_model_into_profile_dict(model, profile):
    # When we add models to be synced, we need to make sure
    #   that models that depend on other models are synced AFTER
    #   the model it depends on has been synced.

    # Get the dependencies of the new model
    foreign_key_classes = _get_foreign_key_classes(model)

    # Find all the existing models that this new model refers to.
    class_indices = [_profile_models[profile].index(cls) for cls in foreign_key_classes if cls in _profile_models[profile]]

    # Insert just after the last dependency found,
    #   or at the front if no dependencies
    insert_after_idx = 1 + (max(class_indices) if class_indices else -1)

    # Now we're ready to insert.
    _profile_models[profile].insert(insert_after_idx, model)


def add_syncable_models():
    """
    Per profile, adds each model to a dictionary mapping the morango model name to its model class.
    We sort by ForeignKey dependencies to safely sync data.
    """

    import django.apps
    from morango.models import SyncableModel
    from morango.manager import SyncableModelManager
    from morango.query import SyncableModelQuerySet

    model_list = []
    for model_class in django.apps.apps.get_models():
        # several validation checks to assert models will be syncing correctly
        if issubclass(model_class, SyncableModel) and not model_class._meta.proxy:
            name = model_class.__name__
            try:
                from mptt import models
                from morango.utils.morango_mptt import MorangoMPTTModel, MorangoMPTTTreeManager, MorangoTreeQuerySet
                # mptt syncable model checks
                if issubclass(model_class, models.MPTTModel):
                    if not issubclass(model_class, MorangoMPTTModel):
                        raise InvalidMorangoModelConfiguration("{} that inherits from MPTTModel, should instead inherit from MorangoMPTTModel.".format(name))
                    if not isinstance(model_class.objects, MorangoMPTTTreeManager):
                        raise InvalidMPTTManager("Manager for {} must inherit from MorangoMPTTTreeManager.".format(name))
                    if not isinstance(model_class.objects.none(), MorangoTreeQuerySet):
                        raise InvalidMPTTQuerySet("Queryset for {} model must inherit from MorangoTreeQuerySet.".format(name))
            except ImportError:
                pass
            # syncable model checks
            if not isinstance(model_class.objects, SyncableModelManager):
                raise InvalidSyncableManager("Manager for {} must inherit from SyncableModelManager.".format(name))
            if not isinstance(model_class.objects.none(), SyncableModelQuerySet):
                raise InvalidSyncableQueryset("Queryset for {} model must inherit from SyncableModelQuerySet.".format(name))
            if model_class._meta.many_to_many:
                raise UnsupportedFieldType("{} model with a ManyToManyField is not supported in morango.")
            if not hasattr(model_class, 'morango_model_name'):
                raise InvalidMorangoModelConfiguration("{} model must define a morango_model_name attribute".format(name))
            if not hasattr(model_class, 'morango_profile'):
                raise InvalidMorangoModelConfiguration("{} model must define a morango_profile attribute".format(name))

            # create empty list to hold model classes for profile if not yet created
            profile = model_class.morango_profile
            _profile_models[profile] = _profile_models.get(profile, [])

            # special case for root proxy model
            if model_class._meta.proxied_children:
                model_class._meta.proxied_children.reverse()
                for proxy_model in model_class._meta.proxied_children:
                    _insert_model_into_profile_dict(proxy_model.model, profile)
            else:
                _insert_model_into_profile_dict(model_class, profile)

    # for each profile, create a dict mapping from morango model names to model class
    for profile, model_list in iteritems(_profile_models):
        syncable_models_dict = OrderedDict()
        for model_class in model_list:
            syncable_models_dict[model_class.morango_model_name] = model_class
        _profile_models[profile] = syncable_models_dict
