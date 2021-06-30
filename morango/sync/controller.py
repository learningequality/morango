import logging
from time import sleep

from morango.constants import transfer_stage
from morango.constants import transfer_status
from morango.registry import session_middleware

from morango.sync.operations import _deserialize_from_store
from morango.sync.operations import _serialize_into_store
from morango.sync.operations import OperationLogger


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


class SessionController(object):
    """
    Controller class that is used to execute transfer operations, like queuing and serializing,
    but does so through the middleware registry, through which allows customization of how
    those transfer stage operations are handled
    """

    __slots__ = ("middleware", "context", "logging_enabled")

    def __init__(self, middleware, context=None, enable_logging=False):
        """
        :type middleware: morango.registry.SessionMiddlewareRegistry|list
        :type context: morango.sync.context.SessionContext|None
        :type enable_logging: bool
        """
        self.middleware = middleware
        self.context = context
        self.logging_enabled = enable_logging

    @classmethod
    def build(cls, context=None, enable_logging=False):
        """
        Factory method that instantiates the `SessionController` with the specified context and
        the global middleware registry `session_middleware`

        :type context: morango.sync.context.SessionContext|None
        :type enable_logging: bool
        :return: A new transfer controller
        :rtype: SessionController
        """
        return SessionController(
            session_middleware, context=context, enable_logging=enable_logging
        )

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

    def _log_invocation(self, stage, result=None):
        """
        Logs messages about middleware invocation, if logging is enabled. We avoid logging when
        used through the SyncClient to avoid messing up stdout
        """
        if not self.logging_enabled:
            return

        if result is None:
            logging.info("Starting stage '{}'".format(stage))
        if result == transfer_status.COMPLETED:
            logging.info("Completed stage '{}'".format(stage))
        elif result == transfer_status.STARTED:
            logging.info("Stage is in progress '{}' = '{}'".format(stage, result))
        elif result == transfer_status.ERRORED:
            logging.info("Encountered error during stage '{}'".format(stage))

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
        self._log_invocation(stage)

        try:
            context.update(stage=stage, stage_status=transfer_status.PENDING)
            result = middleware(context)
            self._log_invocation(stage, result=result)
            context.update(stage_status=result)
            return result
        except Exception as e:
            # always log the error itself
            logging.error(e)
            self._log_invocation(stage, result=transfer_status.ERRORED)
            context.update(stage_status=transfer_status.ERRORED)
            return transfer_status.ERRORED
