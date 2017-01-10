_syncing_models = {}  # dictionary of models with morango naming identifier


def add_syncing_models():
    from morango.models import SyncableModel
    import django.apps

    model_list = []
    for model_class in django.apps.apps.get_models():
        if issubclass(model_class, SyncableModel) and not model_class._meta.proxy:
            model_list.append(model_class)

    for model_class in model_list:
        _syncing_models.update({model_class.morango_model_name: model_class})
