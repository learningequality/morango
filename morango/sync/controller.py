import logging
from time import sleep

from morango.constants import transfer_stage
from morango.constants import transfer_status
from morango.registry import session_middleware

from morango.sync.operations import _deserialize_from_store
from morango.sync.operations import _serialize_into_store
from morango.sync.operations import OperationLogger
from morango.sync.utils import SyncSignalGroup


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
        assert profile, "profile needs to be defined."
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

    def create_network_connection(self, base_url):
        from morango.sync.syncsession import NetworkSyncConnection

        return NetworkSyncConnection(base_url=base_url)

    def create_disk_connection(path):
        raise NotImplementedError("Coming soon...")


class SessionControllerSignals(object):
    __slots__ = transfer_stage.ALL

    def __init__(self):
        """
        Initializes signal group for each transfer stage
        """
        for stage in transfer_stage.ALL:
            setattr(self, stage, SyncSignalGroup(context=None))

    def connect(self, handler):
        """
        Connects handler to every stage's signal
        :param handler: callable
        """
        for stage in transfer_stage.ALL:
            signal = getattr(self, stage)
            signal.connect(handler)


class SessionController(object):
    """
    Controller class that is used to execute transfer operations, like queuing and serializing,
    but does so through the middleware registry, through which allows customization of how
    those transfer stage operations are handled
    """

    __slots__ = ("middleware", "signals", "context", "last_error",)

    def __init__(self, middleware, signals, context=None):
        """
        :type middleware: morango.registry.SessionMiddlewareRegistry|list
        :type signals: SessionControllerSignals
        :type context: morango.sync.context.SessionContext|None
        """
        self.middleware = middleware
        self.signals = signals
        self.context = context
        self.last_error = None

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

    def proceed_to(self, stage, context=None):
        """
        Calls middleware that operates on stages between the current stage and the `to_stage`. The
        middleware are called incrementally, but in order to proceed to the next stage, the
        middleware must return a complete status. If the middleware does not return a complete
        status, that status is returned indicating that the method call has not reached `to_stage`.
        Therefore middleware can perform operations asynchronously and this can be repeatedly called
        to move forward through the transfer stages and their operations

        When invoking the middleware, if the status result is:
            PENDING: The controller will continue to invoke the middleware again when this method
                is called repeatedly, until the status result changes
            STARTED: The controller will not invoke any middleware until the the status changes,
                and assumes the "started" operation will update the state itself
            COMPLETED: The controller will proceed to invoke the middleware for the next stage
            ERRORED: The controller will not invoke any middleware until the the status changes,
                which would require codified resolution of the error outside of the controller

        :param stage: transfer_stage.* - The transfer stage to proceed to
        :type stage: str
        :param context: Override controller context, or provide it if missing
        :type context: morango.sync.context.SessionContext|None
        :return: transfer_status.* - The status of proceeding to that stage
        :rtype: str
        """
        if context is None:
            context = self.context
            if context is None:
                raise ValueError("Controller is missing required context object")

        stage = transfer_stage.stage(stage)

        # we can't 'proceed' to a stage we've already passed
        current_stage = transfer_stage.stage(context.stage)
        if current_stage > stage:
            return transfer_status.COMPLETED

        # See comments above, any of these statuses mean a no-op for proceeding
        if context.stage_status in (
            transfer_status.STARTED,
            transfer_status.ERRORED,
        ):
            return context.stage_status

        result = False
        # inside "session_middleware"
        for middleware in self.middleware:
            middleware_stage = transfer_stage.stage(middleware.related_stage)

            # break when we find middleware beyond proceed-to stage
            if middleware_stage > stage:
                break
            # execute middleware, up to and including the requested stage
            elif middleware_stage > current_stage or (
                context.stage_status == transfer_status.PENDING
                and middleware_stage == current_stage
            ):
                # if the result is not completed status, then break because that means we can't
                # proceed to the next stage (yet)
                result = self._invoke_middleware(context, middleware)
                if result != transfer_status.COMPLETED:
                    break

        # since the middleware must handle our request, or throw an unimplemented error, this
        # should always be a non-False status
        return result

    def proceed_to_and_wait_for(self, stage, context=None, interval=5):
        """
        Same as `proceed_to` but waits for a finished status to be returned by sleeping between
        calls to `proceed_to` if status is not complete

        :param stage: transfer_stage.* - The transfer stage to proceed to
        :type stage: str
        :param context: Override controller context, or provide it if missing
        :type context: morango.sync.context.SessionContext|None
        :param interval: The time, in seconds, between repeat calls to `.proceed_to`
        :type stage: str
        :return: transfer_status.* - The status of proceeding to that stage,
            which should be `ERRORED` or `COMPLETE`
        :rtype: str
        """
        result = transfer_status.PENDING
        while result not in transfer_status.FINISHED_STATES:
            result = self.proceed_to(stage, context=context)
            sleep(interval)
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
            context.update(stage=stage, stage_status=transfer_status.PENDING)

            # only fire "started" when we first try to invoke the stage
            # NOTE: this means that signals.started is not equivalent to transfer_stage.STARTED
            if not at_stage:
                signal.started.fire(context=context)

            result = middleware(context)

            context.update(stage_status=result)

            # fire signals based off , progress signal if not completed
            if result == transfer_status.COMPLETED:
                signal.completed.fire(context=context)
            else:
                signal.in_progress.fire(context=context)

            return result
        except Exception as e:
            # always log the error itself
            logging.error(e)
            self.last_error = e
            context.update(stage_status=transfer_status.ERRORED)
            # fire completed signal, after context update. handlers can use context to detect error
            signal.completed.fire(context=context)
            return transfer_status.ERRORED
