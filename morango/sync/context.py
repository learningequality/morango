from morango.constants import transfer_stages
from morango.constants import transfer_statuses
from morango.errors import MorangoContextUpdateError
from morango.models.certificates import Filter
from morango.models.core import SyncSession
from morango.models.core import TransferSession
from morango.utils import CAPABILITIES
from morango.utils import parse_capabilities_from_server_request


class SessionContext(object):
    """
    Class that holds the context of a transfer, for executing transfer ops through the middleware
    """

    __slots__ = (
        "sync_session",
        "transfer_session",
        "filter",
        "is_push",
        "capabilities",
        "error",
    )
    max_backoff_interval = 5

    def __init__(
        self,
        sync_session=None,
        transfer_session=None,
        sync_filter=None,
        is_push=None,
        capabilities=None,
    ):
        """
        :param sync_session: The sync session instance
        :type sync_session: SyncSession|None
        :param transfer_session: The current transfer session that will be operated against
        :type transfer_session: TransferSession|None
        :param sync_filter The sync filter to use for the TransferSession
        :type sync_filter Filter|None
        :param is_push: A boolean indicating whether or not the transfer is a push or pull
        :type is_push: bool
        :param capabilities: Capabilities set that is combined (intersected) with our own capabilities
        :type capabilities: set|None
        """
        self.sync_session = sync_session
        self.transfer_session = transfer_session
        self.filter = sync_filter
        self.is_push = is_push
        self.capabilities = set(capabilities or []) & CAPABILITIES
        self.error = None

        if self.transfer_session:
            self.sync_session = transfer_session.sync_session or self.sync_session
            self.is_push = transfer_session.push or self.is_push
            if transfer_session.filter:
                self.filter = transfer_session.get_filter()

    def prepare(self):
        """
        Perform any processing of the session context prior to passing it to the middleware,
        and return the context as it should be passed to the middleware
        """
        return self

    def update(
        self,
        transfer_session=None,
        sync_filter=None,
        is_push=None,
        stage=None,
        stage_status=None,
        capabilities=None,
        error=None,
    ):
        """
        Updates the context
        :type transfer_session: TransferSession|None
        :type sync_filter Filter|None
        :type is_push: bool
        :type stage: str|None
        :type stage_status: str|None
        :type capabilities: str[]|None
        :type error: BaseException|None
        """
        if transfer_session and self.transfer_session:
            raise MorangoContextUpdateError("Transfer session already exists")
        elif (
            transfer_session
            and self.sync_session
            and transfer_session.sync_session_id != self.sync_session.id
        ):
            raise MorangoContextUpdateError("Sync session mismatch")

        if sync_filter and self.filter:
            raise MorangoContextUpdateError("Filter already exists")

        if is_push is not None and self.is_push is not None:
            raise MorangoContextUpdateError("Push/pull method already exists")

        self.transfer_session = transfer_session or self.transfer_session
        self.filter = sync_filter or self.filter
        self.is_push = is_push if is_push is not None else self.is_push
        self.capabilities = set(capabilities or self.capabilities) & CAPABILITIES
        self.update_state(stage=stage, stage_status=stage_status)
        self.error = error or self.error

        # if transfer session was passed in, that takes precedence
        if transfer_session:
            self.sync_session = transfer_session.sync_session
            self.is_push = transfer_session.push
            if transfer_session.filter:
                self.filter = transfer_session.get_filter()

    @property
    def is_pull(self):
        """
        :rtype: bool
        """
        return not self.is_push

    @property
    def stage(self):
        """
        The stage of the transfer context
        :return: A transfer_stages.* constant
        :rtype: str
        """
        raise NotImplementedError("Context `stage` getter is missing")

    @property
    def stage_status(self):
        """
        The status of the transfer context's stage
        :return: A transfer_statuses.* constant
        :rtype: str
        """
        raise NotImplementedError("Context `stage_status` getter is missing")

    def update_state(self, stage=None, stage_status=None):
        """
        Updates the stage state
        :type stage: transfer_stages.*|None
        :type stage_status: transfer_statuses.*|None
        """
        raise NotImplementedError("Context `update_state` method is missing")

    def __getstate__(self):
        """Return dict of simplified data for serialization"""
        return dict(
            sync_session_id=self.sync_session.id if self.sync_session else None,
            transfer_session_id=(
                self.transfer_session.id if self.transfer_session else None
            ),
            filter=str(self.filter),
            is_push=self.is_push,
            stage=self.stage,
            stage_status=self.stage_status,
            capabilities=self.capabilities,
            error=self.error,
        )

    def __setstate__(self, state):
        """Re-apply dict state after serialization"""
        sync_session_id = state.get("sync_session_id", None)
        if sync_session_id is not None:
            self.sync_session = SyncSession.objects.get(pk=sync_session_id)

        transfer_session_id = state.get("transfer_session_id", None)
        if transfer_session_id is not None:
            self.transfer_session = TransferSession.objects.get(pk=transfer_session_id)
            if self.sync_session is None:
                self.sync_session = self.transfer_session.sync_session

        sync_filter = state.get("filter", None)
        if sync_filter is not None:
            self.filter = Filter(sync_filter)

        self.is_push = state.get("is_push", None)
        self.capabilities = state.get("capabilities", None)

        stage = state.get("stage", None)
        stage_status = state.get("stage_status", None)
        self.update_state(stage=stage, stage_status=stage_status)
        self.error = state.get("error", None)


class LocalSessionContext(SessionContext):
    """
    Class that holds the context for operating on a transfer locally
    """

    __slots__ = (
        "request",
        "is_server",
    )
    max_backoff_interval = 1

    def __init__(self, request=None, **kwargs):
        """
        :param request: If acting as the server, it should pass in the request, but the request
            is not serialized into context. See `is_server` prop for determining if request was
            passed in.
        :type request: django.http.request.HttpRequest
        """
        super(LocalSessionContext, self).__init__(**kwargs)
        self.request = request
        self.is_server = request is not None

    @classmethod
    def from_request(cls, request, **kwargs):
        """
        Parse capabilities from request and instantiate the LocalSessionContext
        :param request: The request object
        :type request: django.http.request.HttpRequest
        :param kwargs: Any other keyword args for the constructor
        :rtype: LocalSessionContext
        """
        kwargs.update(capabilities=parse_capabilities_from_server_request(request))
        return LocalSessionContext(request=request, **kwargs)

    @property
    def _has_transfer_session(self):
        """
        :rtype: bool
        """
        return getattr(self, "transfer_session", None) is not None

    @property
    def stage(self):
        """
        :return: A transfer_stage.* constant
        """
        stage = transfer_stages.INITIALIZING
        if self._has_transfer_session:
            stage = self.transfer_session.transfer_stage or stage
        return stage

    @property
    def stage_status(self):
        """
        :return: A transfer_statuses.* constant
        """
        stage_status = transfer_statuses.PENDING
        if self._has_transfer_session:
            stage_status = self.transfer_session.transfer_stage_status or stage_status
        return stage_status

    @property
    def is_receiver(self):
        """
        Whether or not the context indicates that the current local instance is receiving data,
        which means either:
            - A server context and a push transfer, or
            - A client context and a pull transfer
        :return: bool
        """
        return self.is_push == self.is_server

    @property
    def is_producer(self):
        """
        The opposite of `is_receiver`, meaning either:
            - A server context and a pull transfer, or
            - A client context and a push transfer
        :return: bool
        """
        return not self.is_receiver

    def update_state(self, stage=None, stage_status=None):
        """
        Passes through updating state to `TransferSession`, refreshing it from the DB in case it
        has changed during operation

        :param stage: Target stage for update
        :param stage_status: Target status for update
        """
        if self._has_transfer_session:
            self.transfer_session.refresh_from_db()
            self.transfer_session.update_state(stage=stage, stage_status=stage_status)

    def __getstate__(self):
        """Return dict of simplified data for serialization"""
        state = super(LocalSessionContext, self).__getstate__()
        state.update(is_server=self.is_server)
        return state

    def __setstate__(self, state):
        """Re-apply dict state after serialization"""
        self.is_server = state.pop("is_server", False)
        super(LocalSessionContext, self).__setstate__(state)


class NetworkSessionContext(SessionContext):
    """
    Class that holds the context for operating on a transfer remotely through network connection
    """

    __slots__ = ("connection", "_stage", "_stage_status")

    def __init__(self, connection, **kwargs):
        """
        :param connection: The sync client connection that allows operations to execute API calls
            against the remote Morango server instance
        :type connection: NetworkSyncConnection
        """
        self.connection = connection
        super(NetworkSessionContext, self).__init__(**kwargs)

        # since this is network context, keep local reference to state vars
        self._stage = transfer_stages.INITIALIZING
        self._stage_status = transfer_statuses.PENDING

    @property
    def stage(self):
        """
        :return: A transfer_stage.* constant
        """
        return self._stage

    @property
    def stage_status(self):
        """
        :return: A transfer_statuses.* constant
        """
        return self._stage_status

    def update_state(self, stage=None, stage_status=None):
        """
        :param stage: Target stage for update
        :param stage_status: Target status for update
        """
        self._stage = stage or self._stage
        self._stage_status = stage_status or self._stage_status


class CompositeSessionContext(SessionContext):
    """
    A composite context class that acts as a facade for more than one context, to facilitate
    "simpler" operation on local and remote contexts simultaneously
    """

    __slots__ = (
        "children",
        "_counter",
        "_stage",
        "_stage_status",
    )

    def __init__(self, contexts, *args, **kwargs):
        """
        :param contexts: A list of context objects
        :param args: Args to pass to the parent constructor
        :param kwargs: Keyword args to pass to the parent constructor
        """
        self.children = contexts
        self._counter = 0
        self._stage = transfer_stages.INITIALIZING
        self._stage_status = transfer_statuses.PENDING
        super(CompositeSessionContext, self).__init__(*args, **kwargs)
        self._update_attrs(**kwargs)

    @property
    def max_backoff_interval(self):
        """
        The maximum amount of time to wait between retries
        :return: A number of seconds
        """
        return self.prepare().max_backoff_interval

    @property
    def stage(self):
        """
        The stage of the transfer context
        :return: A transfer_stages.* constant
        :rtype: str
        """
        return self._stage

    @property
    def stage_status(self):
        """
        The status of the transfer context's stage
        :return: A transfer_statuses.* constant
        :rtype: str
        """
        return self._stage_status

    def _update_attrs(self, **kwargs):
        """
        Updates all contexts by applying key/value arguments as attributes. This avoids using the
        contexts' update methods because some validation is already handled in this class.
        """
        for context in self.children:
            for attr, value in kwargs.items():
                set_attr = "filter" if attr == "sync_filter" else attr
                setattr(context, set_attr, value)

    def prepare(self):
        """
        Preparing this context will return the current sub context that needs completion
        """
        return self.children[self._counter % len(self.children)]

    def update(self, stage=None, stage_status=None, **kwargs):
        """
        Updates the context object and its state
        :param stage: The str transfer stage
        :param stage_status: The str transfer stage status
        :param kwargs: Other arguments to update the context with
        """
        # update ourselves, but exclude stage and stage_status
        super(CompositeSessionContext, self).update(**kwargs)
        # update children contexts directly, but exclude stage and stage_status
        self._update_attrs(**kwargs)
        # handle state changes after updating children
        self.update_state(stage=stage, stage_status=stage_status)

        # During the initializing stage, we want to make sure to synchronize the transfer session
        # object between the composite and children contexts, using whatever child context's
        # transfer session object that was updated on the context during initialization
        current_stage = stage or self._stage
        if not self.transfer_session and current_stage == transfer_stages.INITIALIZING:
            try:
                transfer_session = next(
                    c.transfer_session for c in self.children if c.transfer_session
                )
                # prepare an updates dictionary, so we can update everything at once
                updates = dict(transfer_session=transfer_session)

                # if the transfer session is being resumed, we'd detect a different stage here,
                # and thus we reset the counter, so we can be sure to start fresh at that stage
                # on the next invocation of the middleware
                if (
                    transfer_session.transfer_stage
                    and transfer_session.transfer_stage != current_stage
                ):
                    self._counter = 0
                    updates.update(
                        stage=transfer_session.transfer_stage,
                        stage_status=transfer_statuses.PENDING,
                    )

                # recurse into update with transfer session and possibly state updates too
                self.update(**updates)
            except StopIteration:
                pass

    def update_state(self, stage=None, stage_status=None):
        """
        Updates the state of the transfer
        :type stage: transfer_stages.*|None
        :type stage_status: transfer_statuses.*|None
        """
        # parent's update method can pass through None values
        if stage is None and stage_status is None:
            return

        # advance the composite's stage when we move forward only
        if stage is not None and transfer_stages.stage(stage) > transfer_stages.stage(
            self._stage
        ):
            self._stage = stage

        # when finishing a stage without an error, we'll increment the counter by one such that
        # `prepare` returns the next context to process
        if stage_status == transfer_statuses.COMPLETED:
            self._counter += 1

        # when we've completed a loop through all contexts (modulus is zero), we want to bring
        # all the contexts' states up to date
        if (
            self._counter % len(self.children) == 0
            or stage_status == transfer_statuses.ERRORED
        ):
            for context in self.children:
                context.update_state(stage=stage, stage_status=stage_status)
            if stage_status is not None:
                self._stage_status = stage_status

    def __getstate__(self):
        """Return dict of simplified data for serialization"""
        return dict(
            children=self.children,
            counter=self._counter,
            stage=self._stage,
            stage_status=self._stage_status,
        )

    def __setstate__(self, state):
        """Re-apply dict state after serialization"""
        self.children = state.get("children", [])
        self._counter = state.get("counter", 0)
        self._stage = state.get("stage", None)
        self._stage_status = state.get("stage_status", None)
        self.error = state.get("error", None)
