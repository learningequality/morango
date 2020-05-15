import copy
import functools
import logging

from django.db import transaction
from rest_framework.exceptions import ValidationError

from morango.models.core import Buffer
from morango.models.core import RecordMaxCounterBuffer
from morango.models.core import SyncableModel
from morango.registry import syncable_models


logger = logging.getLogger(__name__)


# taken from https://github.com/FactoryBoy/factory_boy/blob/master/factory/django.py#L256
class mute_signals(object):
    """Temporarily disables and then restores any django signals.
    Args:
        *signals (django.dispatch.dispatcher.Signal): any django signals
    Examples:
        with mute_signals(pre_init):
            user = UserFactory.build()
            ...
        @mute_signals(pre_save, post_save)
        class UserFactory(factory.Factory):
            ...
        @mute_signals(post_save)
        def generate_users():
            UserFactory.create_batch(10)
    """

    def __init__(self, *signals):
        self.signals = signals
        self.paused = {}

    def __enter__(self):
        for signal in self.signals:
            logger.debug("mute_signals: Disabling signal handlers %r", signal.receivers)

            # Note that we're using implementation details of
            # django.signals, since arguments to signal.connect()
            # are lost in signal.receivers
            self.paused[signal] = signal.receivers
            signal.receivers = []

    def __exit__(self, exc_type, exc_value, traceback):
        for signal, receivers in self.paused.items():
            logger.debug("mute_signals: Restoring signal handlers %r", receivers)

            signal.receivers = receivers
            with signal.lock:
                # Django uses some caching for its signals.
                # Since we're bypassing signal.connect and signal.disconnect,
                # we have to keep messing with django's internals.
                signal.sender_receivers_cache.clear()
        self.paused = {}

    def copy(self):
        return mute_signals(*self.signals)

    def __call__(self, callable_obj):
        @functools.wraps(callable_obj)
        def wrapper(*args, **kwargs):
            # A mute_signals() object is not reentrant; use a copy every time.
            with self.copy():
                return callable_obj(*args, **kwargs)

        return wrapper


def validate_and_create_buffer_data(data, transfer_session, connection=None):  # noqa: C901
    data = copy.deepcopy(data)
    rmcb_list = []
    buffer_list = []
    for record in data:
        # ensure the provided model_uuid matches the expected/computed id
        try:
            Model = syncable_models.get_model(record["profile"], record["model_name"])
        except KeyError:
            Model = SyncableModel

        partition = record["partition"].replace(
            record["model_uuid"], Model.ID_PLACEHOLDER
        )
        expected_model_uuid = Model.compute_namespaced_id(
            partition, record["source_id"], record["model_name"]
        )
        if expected_model_uuid != record["model_uuid"]:
            raise ValidationError(
                {
                    "model_uuid": "Does not match results of calling {}.compute_namespaced_id".format(
                        Model.__class__.__name__
                    )
                }
            )

        # ensure the profile is marked onto the buffer record
        record["profile"] = transfer_session.sync_session.profile

        # ensure the partition is within the transfer session's filter
        if not transfer_session.get_filter().contains_partition(record["partition"]):
            raise ValidationError(
                {
                    "partition": "Partition {} is not contained within filter for TransferSession ({})".format(
                        record["partition"], transfer_session.filter
                    )
                }
            )

        # ensure that all nested RMCB models are properly associated with this record and transfer session
        for rmcb in record.pop("rmcb_list"):
            if rmcb["transfer_session"] != transfer_session.id:
                raise ValidationError(
                    {
                        "rmcb_list": "Transfer session on RMCB ({}) does not match Buffer's TransferSession ({})".format(
                            rmcb["transfer_session"], transfer_session
                        )
                    }
                )
            if rmcb["model_uuid"] != record["model_uuid"]:
                raise ValidationError(
                    {
                        "rmcb_list": "Model UUID on RMCB ({}) does not match Buffer's Model UUID ({})".format(
                            rmcb["model_uuid"], record["model_uuid"]
                        )
                    }
                )
            rmcb["transfer_session_id"] = rmcb.pop("transfer_session")
            rmcb_list += [RecordMaxCounterBuffer(**rmcb)]

        record["transfer_session_id"] = record.pop("transfer_session")
        buffer_list += [Buffer(**record)]

    with transaction.atomic():
        transfer_session.records_transferred += len(data)

        if connection is not None:
            transfer_session.bytes_sent = connection.bytes_sent
        if connection is not None:
            transfer_session.bytes_received = connection.bytes_received

        transfer_session.save()

        Buffer.objects.bulk_create(buffer_list)
        RecordMaxCounterBuffer.objects.bulk_create(rmcb_list)
