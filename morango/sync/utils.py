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


def validate_and_create_buffer_data(  # noqa: C901
    data, transfer_session, connection=None
):
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
                "Does not match results of calling {}.compute_namespaced_id".format(
                    Model.__class__.__name__
                )
            )

        # ensure the profile is marked onto the buffer record
        record["profile"] = transfer_session.sync_session.profile

        # ensure the partition is within the transfer session's filter
        if not transfer_session.get_filter().contains_partition(record["partition"]):
            raise ValidationError(
                "Partition {} is not contained within filter for TransferSession ({})".format(
                    record["partition"], transfer_session.filter
                )
            )

        # ensure that all nested RMCB models are properly associated with this record and transfer session
        for rmcb in record.pop("rmcb_list"):
            if rmcb["transfer_session"] != transfer_session.id:
                raise ValidationError(
                    "Transfer session on RMCB ({}) does not match Buffer's TransferSession ({})".format(
                        rmcb["transfer_session"], transfer_session
                    )
                )
            if rmcb["model_uuid"] != record["model_uuid"]:
                raise ValidationError(
                    "Model UUID on RMCB ({}) does not match Buffer's Model UUID ({})".format(
                        rmcb["model_uuid"], record["model_uuid"]
                    )
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


class SyncSignal(object):
    """
    Helper class for firing signals from the sync client
    """

    __slots__ = ("_handlers", "_defaults")

    def __init__(self, **kwargs_defaults):
        """
        Default keys/values that the signal consumer can depend on being present.
        """
        self._handlers = []
        self._defaults = kwargs_defaults

        # any class attributes defined as signals we'll automatically recreate with same args
        for attr_name, attr in self.__class__.__dict__.items():
            if isinstance(attr, SyncSignal):
                signal_attr = attr.clone(**kwargs_defaults)
                signal_attr.connect(self.fire)
                setattr(self, attr_name, signal_attr)

    def clone(self, **kwargs_defaults):
        """
        Clone the signal, it's defaults, and handlers
        """
        defaults = self._defaults.copy()
        defaults.update(kwargs_defaults)
        clone = self.__class__(**defaults)
        for handler in self._handlers:
            clone.connect(handler)
        return clone

    def connect(self, handler):
        """
        Adds a callable handler that will be called when the signal is fired.

        :type handler: function
        """
        self._handlers.append(handler)

    def fire(self, **kwargs):
        """
        Fires the handler functions connected via `connect`.
        """
        fire_kwargs = self._defaults.copy()
        fire_kwargs.update(kwargs)

        for handler in self._handlers:
            handler(**fire_kwargs)


class SyncSignalGroup(SyncSignal):
    """
    Breaks down a signal into `started`, `in_progress`, and `completed` stages. The
    `kwargs_defaults` are passed through to each signal stage.
    """

    started = SyncSignal()
    """The started signal, which will be fired at the beginning of the procedure."""
    in_progress = SyncSignal()
    """The in progress signal, which should be fired at least once during the procedure."""
    completed = SyncSignal()
    """The completed signal, which should be fired at the end of the procedure"""

    def send(self, **kwargs):
        """
        Context manager helper that will signal started and fired when entered and exited,
        and it'll fire those with the `kwargs`.

        :rtype: SyncSignalGroup
        """
        context_group = self.clone(**kwargs)
        context_group.started.connect(self.started.fire)
        context_group.in_progress.connect(self.in_progress.fire)
        context_group.completed.connect(self.completed.fire)
        return context_group

    def __enter__(self):
        """
        Fires the `started` signal.
        """
        self.started.fire()
        return self

    def __exit__(self, *args, **kwargs):
        """
        Fires the `completed` signal.
        """
        self.completed.fire()
