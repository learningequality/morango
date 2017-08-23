import json

from django.core.exceptions import ObjectDoesNotExist
from django.core.serializers.json import DjangoJSONEncoder
from django.db import transaction
from django.utils.six import iteritems
from morango.models import DeletedModels, InstanceIDModel, RecordMaxCounter, Store

from morango.utils.register_models import _profile_models


class MorangoProfileController(object):

    def __init__(self, profile):
        assert profile, "profile needs to be defined."
        self.profile = profile

    def serialize_into_store(self):
        """
        Takes data from app layer and serializes the models into the store.
        """
        # ensure that we write and retrieve the counter in one go for consistency
        current_id = InstanceIDModel.get_current_instance_and_increment_counter()

        with transaction.atomic():
            defaults = {'instance_id': current_id.id, 'counter': current_id.counter}

            # filter through all models with the dirty bit turned on
            syncable_dict = _profile_models[self.profile]
            for (_, klass_model) in iteritems(syncable_dict):
                for app_model in klass_model.objects.filter(_morango_dirty_bit=True):
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
                        # create store model and record max counter for the app model
                        Store.objects.create(**kwargs)
                        defaults.update({'store_model_id': app_model.id})
                        RecordMaxCounter(**defaults).save()

                # set dirty bit to false for all instances of this model
                klass_model.objects.filter(_morango_dirty_bit=True).update(update_dirty_bit_to=False)

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
                for store_model in Store.objects.filter(model_name=model_name, profile=self.profile, dirty_bit=True):
                    if store_model.deleted:
                        klass_model.objects.filter(id=store_model.id).delete()
                    else:
                        app_model = klass_model.deserialize(json.loads(store_model.serialized))
                        try:
                            app_model.save(update_dirty_bit_to=False)
                        # when deserializing, we catch this exception in case we have a reference to a missing object (foreign key) not in the app layer
                        except ObjectDoesNotExist:
                            app_model._update_deleted_models()

            # clear dirty bit for all store models for this profile
            Store.objects.filter(profile=self.profile, dirty_bit=True).update(dirty_bit=False)
