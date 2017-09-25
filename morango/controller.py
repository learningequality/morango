import json
import functools

from django.core.serializers.json import DjangoJSONEncoder
from django.db import transaction
from django.db.models import Q
from django.utils.six import iteritems
from morango.models import DeletedModels, InstanceIDModel, RecordMaxCounter, Store

from morango.utils.register_models import _profile_models


def _self_referential_fk(klass_model):
    """
    Return whether this model has a self ref FK, and the name for the field
    """
    for f in klass_model._meta.concrete_fields:
        if f.related_model == klass_model:
            return f.attname
    return None


class MorangoProfileController(object):

    def __init__(self, profile):
        assert profile, "profile needs to be defined."
        self.profile = profile

    def serialize_into_store(self, filter=None):
        """
        Takes data from app layer and serializes the models into the store.
        """
        # ensure that we write and retrieve the counter in one go for consistency
        current_id = InstanceIDModel.get_current_instance_and_increment_counter()

        with transaction.atomic():
            defaults = {'instance_id': current_id.id, 'counter': current_id.counter}

            # create Q objects for filtering by prefixes
            prefix_condition = None
            if filter:
                prefix_condition = functools.reduce(lambda x, y: x | y, [Q(_morango_partition__startswith=prefix) for prefix in filter])

            # filter through all models with the dirty bit turned on
            syncable_dict = _profile_models[self.profile]
            for (_, klass_model) in iteritems(syncable_dict):
                new_store_records = []
                new_rmc_records = []
                klass_queryset = klass_model.objects.filter(_morango_dirty_bit=True)
                if prefix_condition:
                    klass_queryset = klass_queryset.filter(prefix_condition)
                for app_model in klass_queryset:
                    try:
                        store_model = Store.objects.get(id=app_model.id)

                        # if store record dirty and app record dirty, append store serialized to conflicting data
                        if store_model.dirty_bit:
                            store_model.conflicting_serialized_data = store_model.serialized + "\n" + store_model.conflicting_serialized_data

                        # set new serialized data on this store model
                        ser_dict = json.loads(store_model.serialized)
                        ser_dict.update(app_model.serialize())
                        store_model.serialized = DjangoJSONEncoder().encode(ser_dict)

                        # create or update instance and counter on the record max counter for this store model
                        defaults.update({'store_model_id': store_model.id})
                        RecordMaxCounter.objects.update_or_create(defaults=defaults, instance_id=current_id.id, store_model_id=store_model.id)

                        # update last saved bys for this store model
                        store_model.last_saved_instance = current_id.id
                        store_model.last_saved_counter = current_id.counter

                        # update fields for this store model
                        store_model.save(update_fields=['serialized', 'last_saved_instance', 'last_saved_counter', 'conflicting_serialized_data'])

                    except Store.DoesNotExist:
                        kwargs = {
                            'id': app_model.id,
                            'serialized': DjangoJSONEncoder().encode(app_model.serialize()),
                            'last_saved_instance': current_id.id,
                            'last_saved_counter': current_id.counter,
                            'model_name': app_model.morango_model_name,
                            'profile': app_model.morango_profile,
                            'partition': app_model._morango_partition,
                        }
                        # check if model has FK pointing to itself, and add the value to a field on the store
                        self_ref_fk = _self_referential_fk(klass_model)
                        if self_ref_fk:
                            self_ref_fk_value = getattr(app_model, self_ref_fk)
                            kwargs.update({'_self_ref_fk': self_ref_fk_value or ''})
                        # create store model and record max counter for the app model
                        new_store_records.append(Store(**kwargs))
                        defaults.update({'store_model_id': app_model.id})
                        new_rmc_records.append(RecordMaxCounter(**defaults))

                # bulk create store and rmc records for this class
                Store.objects.bulk_create(new_store_records)
                RecordMaxCounter.objects.bulk_create(new_rmc_records)

                # set dirty bit to false for all instances of this model
                klass_queryset.update(update_dirty_bit_to=False)

            # update deleted flags based on DeletedModels
            deleted_ids = DeletedModels.objects.filter(profile=self.profile).values_list('id', flat=True)
            Store.objects.filter(id__in=deleted_ids).update(deleted=True)
            DeletedModels.objects.filter(profile=self.profile).delete()

    def deserialize_from_store(self):
        """
        Takes data from the store and integrates into the application.
        """
        # we first serialize to avoid deserialization merge conflicts
        self.serialize_into_store()

        with transaction.atomic():
            syncable_dict = _profile_models[self.profile]
            # iterate through classes which are in foreign key dependency order
            for model_name, klass_model in iteritems(syncable_dict):
                # handle cases where a class has a single FK reference to itself
                if _self_referential_fk(klass_model):
                    clean_parents = Store.objects.filter(dirty_bit=False, model_name=model_name, profile=self.profile).values_list("id", flat=True)
                    dirty_children = Store.objects.filter(dirty_bit=True, model_name=model_name, profile=self.profile) \
                                                  .filter(Q(_self_ref_fk__in=clean_parents) | Q(_self_ref_fk=''))

                    # keep iterating until size of dirty_children is 0
                    while len(dirty_children) > 0:
                        for store_model in dirty_children:
                            store_model._deserialize_store_model()
                            # we update a store model after we have deserialized it
                            store_model.dirty_bit = False
                            store_model.save(update_fields=['dirty_bit'])

                        # update lists with new clean parents and dirty children
                        clean_parents = Store.objects.filter(dirty_bit=False, model_name=model_name, profile=self.profile).values_list("id", flat=True)
                        dirty_children = Store.objects.filter(dirty_bit=True, model_name=model_name, profile=self.profile, _self_ref_fk__in=clean_parents)
                else:
                    for store_model in Store.objects.filter(model_name=model_name, profile=self.profile, dirty_bit=True):
                        store_model._deserialize_store_model()

            # clear dirty bit for all store models for this profile
            Store.objects.filter(profile=self.profile, dirty_bit=True).update(dirty_bit=False)
