from morango.constants import transfer_stage
from morango.constants import transfer_status
from morango.errors import MorangoContextUpdateError
from morango.models.certificates import Filter
from morango.models.core import SyncSession
from morango.models.core import TransferSession
from morango.utils import parse_capabilities_from_server_request
from morango.utils import CAPABILITIES


class SessionContext(object):
    """
    Class that holds the context of a transfer, for executing transfer ops through the middleware
    """

    __slots__ = (
        "sync_session",
        "transfer_session",
        "filter",
        "is_push",
        "stage",
        "stage_status",
        "capabilities",
    )

    def __init__(self, sync_session=None, transfer_session=None, sync_filter=None, is_push=None, capabilities=None):
        """
        :param sync_session: The sync session instance
        :type sync_session: SyncSession|None
        :param transfer_session: The current transfer session that will be operated against
        :type transfer_session: TransferSession|None
        :param sync_filter The sync filter to use for the TransferSession
        :type sync_filter Filter|None
        :param is_push: A boolean indicating whether or not the transfer is a push or pull
        :type is_push: bool
        :param capabilities: Capabilities set that is combined (union) against our own capabilities
        :type capabilities: set|None
        """
        self.sync_session = sync_session
        self.transfer_session = transfer_session
        self.filter = sync_filter
        self.is_push = is_push
        self.stage = transfer_stage.INITIALIZING
        self.stage_status = transfer_status.PENDING
        self.capabilities = set(capabilities or []) & CAPABILITIES

        if self.transfer_session:
            self.sync_session = transfer_session.sync_session or self.sync_session
            self.filter = transfer_session.get_filter() or self.filter
            self.is_push = transfer_session.push or self.is_push
            self.stage = transfer_session.transfer_stage or self.stage
            self.stage_status = (
                transfer_session.transfer_stage_status or self.stage_status
            )

    def update(
        self, transfer_session=None, sync_filter=None, is_push=None, stage=None, stage_status=None, capabilities=None
    ):
        """
        Updates the context
        :type transfer_session: TransferSession|None
        :type sync_filter Filter|None
        :type is_push: bool
        :type stage: str|None
        :type stage_status: str|None
        :type capabilities: str[]|None
        """
        if transfer_session and self.transfer_session:
            raise MorangoContextUpdateError("Transfer session already exists")

        if sync_filter and self.filter:
            raise MorangoContextUpdateError("Filter already exists")

        if is_push is not None and self.is_push is not None:
            raise MorangoContextUpdateError("Push/pull method already exists")

        self.transfer_session = transfer_session or self.transfer_session
        self.filter = sync_filter or self.filter
        self.is_push = is_push if is_push is not None else self.is_push
        self.stage = stage or self.stage
        self.stage_status = stage_status or self.stage_status
        self.capabilities = set(capabilities or self.capabilities) & CAPABILITIES

        # if transfer session was passed in, that takes precedence
        if transfer_session:
            self.filter = transfer_session.get_filter() or self.filter
            self.is_push = transfer_session.push or self.is_push
            self.stage = transfer_session.transfer_stage or self.stage
            self.stage_status = transfer_session.transfer_stage_status or self.stage_status

        # when updating, we go ahead and update the transfer session state too and ensure that
        # we also `refresh_from_db` too so context has the up-to-date instance
        if self.transfer_session:
            self.transfer_session.refresh_from_db()
            self.transfer_session.update_state(
                stage=self.stage, stage_status=self.stage_status
            )

    @property
    def is_pull(self):
        """
        :rtype: bool
        """
        return not self.is_push

    def __getstate__(self):
        """Return dict of simplified data for serialization"""
        return dict(
            sync_session_id=self.sync_session.id if self.sync_session else None,
            transfer_session_id=(
                self.transfer_session.id
                if self.transfer_session
                else None
            ),
            filter=str(self.filter),
            is_push=self.is_push,
            stage=self.stage,
            stage_status=self.stage_status,
            capabilities=self.capabilities,
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
        self.stage = state.get("stage", None)
        self.stage_status = state.get("stage_status", None)
        self.capabilities = state.get("capabilities", None)


class LocalSessionContext(SessionContext):
    """
    Class that holds the context for operating on a transfer locally
    """

    __slots__ = (
        "request",
        "is_server",
    )

    def __init__(self, request=None, **kwargs):
        """
        :param request: If acting as the server, it should pass in the request, but the request
            is not serialized into context. See `is_server` prop for determining if request was
            passed in.
        :type request: django.http.request.HttpRequest
        """
        capabilities = kwargs.pop("capabilities", [])
        if request is not None:
            capabilities = parse_capabilities_from_server_request(request)

        super(LocalSessionContext, self).__init__(capabilities=capabilities, **kwargs)
        self.request = request
        self.is_server = request is not None

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

    __slots__ = ("connection",)

    def __init__(self, connection, **kwargs):
        """
        :param connection: The sync client connection that allows operations to execute API calls
            against the remote Morango server instance
        :type connection: NetworkSyncConnection
        """
        self.connection = connection
        super(NetworkSessionContext, self).__init__(
            capabilities=self.connection.server_info.get("capabilities", []),
            **kwargs,
        )
