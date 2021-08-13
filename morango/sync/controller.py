import logging
from time import sleep

from morango.constants import transfer_stages
from morango.constants import transfer_statuses
from morango.registry import session_middleware

from morango.sync.operations import _deserialize_from_store
from morango.sync.operations import _serialize_into_store
from morango.sync.operations import OperationLogger
from morango.sync.utils import SyncSignalGroup
from morango.utils import _assert


logger = logging.getLogger(__name__)


def _self_referential_fk(klass_model):
    """
    Return whether this model has a self ref FK, and the name for the field
    """
    for f in klass_model._meta.concrete_fields:
        if f.related_model:
            if issubclass(klass_model, f.related_model):
                return f.attname
    return None


class MorangoProfileController(object):
    def __init__(self, profile):
        _assert(profile, "profile needs to be defined.")
        self.profile = profile

    def serialize_into_store(self, filter=None):
        """
        Takes data from app layer and serializes the models into the store.
        """
        with OperationLogger("Serializing records", "Serialization complete"):
            _serialize_into_store(self.profile, filter=filter)

    def deserialize_from_store(self, skip_erroring=False, filter=None):
        """
        Takes data from the store and integrates into the application.
        """
        with OperationLogger("Deserializing records", "Deserialization complete"):
            # we first serialize to avoid deserialization merge conflicts
            _serialize_into_store(self.profile, filter=filter)
            _deserialize_from_store(
                self.profile, filter=filter, skip_erroring=skip_erroring
            )

    def create_network_connection(self, base_url, **kwargs):
        from morango.sync.syncsession import NetworkSyncConnection

        kwargs.update(base_url=base_url)
        return NetworkSyncConnection(**kwargs)

    def create_disk_connection(path):
        raise NotImplementedError("Coming soon...")


class SessionControllerSignals(object):
    __slots__ = transfer_stages.ALL

    def __init__(self):
        """
        Initializes signal group for each transfer stage
        """
        for stage in transfer_stages.ALL:
            setattr(self, stage, SyncSignalGroup(context=None))

    def connect(self, handler):
        """
        Connects handler to every stage's signal
        :param handler: callable
        """
        for stage in transfer_stages.ALL:
            signal = getattr(self, stage)
            signal.connect(handler)


class SessionController(object):
    """
    Controller class that is used to execute transfer operations, like queuing and serializing,
    but does so through the middleware registry, which allows customization of how those transfer
    stage operations are handled
    """

    __slots__ = (
        "middleware",
        "signals",
        "context",
    )

    def __init__(self, middleware, signals, context=None):
        """
        :type middleware: morango.registry.SessionMiddlewareRegistry|list
        :type signals: SessionControllerSignals
        :type context: morango.sync.context.SessionContext|None
        """
        self.middleware = middleware
        self.signals = signals
        self.context = context

    @classmethod
    def build(cls, middleware=None, signals=None, context=None):
        """
        Factory method that instantiates the `SessionController` with the specified context and
        the global middleware registry `session_middleware`

        :type middleware: morango.registry.SessionMiddlewareRegistry|list|None
        :type signals: SessionControllerSignals|none
        :type context: morango.sync.context.SessionContext|None
        :return: A new transfer controller
        :rtype: SessionController
        """
        middleware = middleware or session_middleware
        signals = signals or SessionControllerSignals()
        return SessionController(middleware, signals, context=context)

    def proceed_to(self, target_stage, context=None):
        """
        Calls middleware that operates on stages between the current stage and the `target_stage`.
        The middleware are called incrementally, but in order to proceed to the next stage, the
        middleware must return a complete status. If the middleware does not return a complete
        status, that status is returned indicating that the method call has not reached
        `target_stage`. Therefore middleware can perform operations asynchronously and this can be
        repeatedly called to move forward through the transfer stages and their operations

        When invoking the middleware, if the status result is:
            PENDING: The controller will continue to invoke the middleware again when this method
                is called repeatedly, until the status result changes
            STARTED: The controller will not invoke any middleware until the the status changes,
                and assumes the "started" operation will update the state itself
            COMPLETED: The controller will proceed to invoke the middleware for the next stage
            ERRORED: The controller will not invoke any middleware until the the status changes,
                which would require codified resolution of the error outside of the controller

        :param target_stage: transfer_stage.* - The transfer stage to proceed to
        :type target_stage: str
        :param context: Override controller context, or provide it if missing
        :type context: morango.sync.context.SessionContext|None
        :return: transfer_status.* - The status of proceeding to that stage
        :rtype: str
        """
        if context is None:
            context = self.context
            if context is None:
                raise ValueError("Controller is missing required context object")

        target_stage = transfer_stages.stage(target_stage)

        # we can't 'proceed' to a stage we've already passed
        current_stage = transfer_stages.stage(context.stage)
        if current_stage > target_stage:
            return transfer_statuses.COMPLETED

        # See comments above, any of these statuses mean a no-op for proceeding
        if context.stage_status in (
            transfer_statuses.STARTED,
            transfer_statuses.ERRORED,
        ):
            return context.stage_status

        result = False
        # inside "session_middleware"
        for middleware in self.middleware:
            middleware_stage = transfer_stages.stage(middleware.related_stage)

            # break when we find middleware beyond proceed-to stage
            if middleware_stage > target_stage:
                break
            # execute middleware, up to and including the requested stage
            elif middleware_stage > current_stage or (
                context.stage_status == transfer_statuses.PENDING
                and middleware_stage == current_stage
            ):
                # if the result is not completed status, then break because that means we can't
                # proceed to the next stage (yet)
                result = self._invoke_middleware(context, middleware)
                # update local stage variable after completion
                current_stage = transfer_stages.stage(context.stage)
                if result != transfer_statuses.COMPLETED:
                    break

        # since the middleware must handle our request, or throw an unimplemented error, this
        # should always be a non-False status
        return result

    def proceed_to_and_wait_for(self, target_stage, context=None, max_interval=5):
        """
        Same as `proceed_to` but waits for a finished status to be returned by sleeping between
        calls to `proceed_to` if status is not complete

        :param target_stage: transfer_stage.* - The transfer stage to proceed to
        :type target_stage: str
        :param context: Override controller context, or provide it if missing
        :type context: morango.sync.context.SessionContext|None
        :param max_interval: The max time, in seconds, between repeat calls to `.proceed_to`
        :return: transfer_status.* - The status of proceeding to that stage,
            which should be `ERRORED` or `COMPLETE`
        :rtype: str
        """
        result = transfer_statuses.PENDING
        tries = 0
        while result not in transfer_statuses.FINISHED_STATES:
            if tries > 0:
                # exponential backoff up to max_interval
                sleep(min(0.3 * (2 ** tries - 1), max_interval))
            result = self.proceed_to(target_stage, context=context)
            tries += 1
        return result

    def _invoke_middleware(self, context, middleware):
        """
        Invokes middleware, with logging if enabled, and handles updating the transfer session state

        :param context: The context to invoke the middleware with
        :type context: morango.sync.context.SessionContext
        :type middleware: morango.registry.SessionMiddlewareOperations
        :return: transfer_status.* - The result of invoking the middleware
        :rtype: str
        """
        stage = middleware.related_stage
        signal = getattr(self.signals, stage)
        at_stage = context.stage == stage

        try:
            context.update(stage=stage, stage_status=transfer_statuses.PENDING)

            # only fire "started" when we first try to invoke the stage
            # NOTE: this means that signals.started is not equivalent to transfer_stage.STARTED
            if not at_stage:
                signal.started.fire(context=context)

            result = middleware(context)

            # don't update stage result if context's stage was updated during operation
            if context.stage == stage:
                context.update(stage_status=result)

            # fire signals based off middleware invocation result; the progress signal if incomplete
            if result == transfer_statuses.COMPLETED:
                signal.completed.fire(context=context)
            else:
                signal.in_progress.fire(context=context)

            return result
        except Exception as e:
            # always log the error itself
            logger.error(e)
            context.update(stage_status=transfer_statuses.ERRORED, error=e)
            # fire completed signal, after context update. handlers can use context to detect error
            signal.completed.fire(context=context)
            return transfer_statuses.ERRORED
