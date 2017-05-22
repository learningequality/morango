import json

from django.core.serializers.json import DjangoJSONEncoder
from django.db import transaction
from django.db.models import F
from django.utils.six import iteritems
from morango.models import InstanceIDModel, RecordMaxCounter, Store

from .register_models import _profile_models


class MorangoProfileController(object):

    def __init__(self, profile):
        assert profile, "profile needs to be defined."
        self.profile = profile

    def _serialize_into_store(self):
        """
        Takes data from app layer and serializes the models into the store.
        """
        # ensure that we write and retrieve the counter in one go for consistency
        with transaction.atomic():
            InstanceIDModel.objects.filter(current=True).update(counter=F('counter') + 1)
            current_id = InstanceIDModel.objects.get(current=True)

        with transaction.atomic():
            defaults = {'instance_id': current_id.id, 'counter': current_id.counter}

            # filter through all models with the dirty bit turned on
            syncable_dict = _profile_models[self.profile]
            for (_, klass_model) in iteritems(syncable_dict):
                for app_model in klass_model.objects.filter(_morango_dirty_bit=True):
                    try:
                        # set new serialized data on this store model
                        store_model = Store.objects.get(id=app_model.id)
                        store_model.serialized = DjangoJSONEncoder().encode(app_model.serialize())

                        # create or update instance and counter on the record max counter for this store model
                        defaults.update({'store_model_id': store_model.id})
                        RecordMaxCounter.objects.update_or_create(defaults=defaults, instance_id=current_id.id, store_model_id=store_model.id)

                        # update last saved bys for this store model
                        store_model.last_saved_instance = current_id.id
                        store_model.last_saved_counter = current_id.counter

                        # update fields for this store model
                        store_model.save(update_fields=['serialized', 'last_saved_instance', 'last_saved_counter'])

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
                        store_model = Store.objects.create(**kwargs)
                        defaults.update({'store_model_id': store_model.id})
                        RecordMaxCounter(**defaults).save()

                    # set dirty bit to false for this model
                    app_model.save(update_dirty_bit_to=False, update_fields=['_morango_dirty_bit'])

    @transaction.atomic
    def _store_to_app(self):
        """
        Takes data from the store and integrates into the application.
        """
        syncable_dict = _profile_models[self.profile]
        # iterate through classes which are in foreign key dependency order
        for model_name, klass_model in iteritems(syncable_dict):
            for store_model in Store.objects.filter(model_name=model_name):
                concrete_store_model = klass_model.deserialize(json.loads(store_model.serialized))
                concrete_store_model.save(update_dirty_bit_to=False)

    def open_network_sync_connection(host, scope):
        pass

    def open_disk_sync_connection(path, scope):
        pass
