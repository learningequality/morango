"""
Characterization tests that pin the HTTP requests and the final state of a network push and pull
that use `ASYNC_OPERATIONS`. Refactors of the sync layer must pass these tests without edits.
"""

import gzip
import json
import re
import time

import mock
from facility_profile.models import InteractionLog
from facility_profile.models import SummaryLog

from morango.constants import transfer_stages
from morango.constants import transfer_statuses
from morango.models.core import Buffer
from morango.models.core import Store
from morango.models.core import TransferSession
from morango.sync.session import SessionWrapper

from .helpers import BasePushPullClientTestCase
from .helpers import second_environment

UUID_RE = re.compile(r"[0-9a-f]{32}")

# body fields whose values are stable between runs, so their values are pinned along with the keys
PINNED_VALUE_KEYS = ("filter", "push", "records_total", "records_transferred", "transfer_stage")

# a sync that stalls (for example, waiting on a remote stage that never completes) polls forever,
# so the recorder fails the test once it exceeds either budget
MAX_REQUESTS = 100
MAX_SECONDS = 120

CHUNK_SIZE = 3


def _normalize(value):
    if isinstance(value, str):
        return UUID_RE.sub("<id>", value)
    return value


def request(method, path, params=None, encoding=None, keys=(), values=None, retryable=False):
    """
    The comparable form of a single request.

    :param method: The HTTP method
    :param path: The URL path relative to the server's base URL, with ids replaced by `<id>`
    :param params: The query params as a sorted tuple of (name, value) pairs
    :param encoding: How the body was sent: "json", "gzip", or None for no body
    :param keys: The sorted body keys, or for a list body, the sorted keys of its records
    :param values: The values of `PINNED_VALUE_KEYS` in a dict body, as a sorted tuple of pairs,
        or for a list body, the number of records
    :param retryable: The `is_retryable` flag passed to `SessionWrapper.request`
    """
    return (method, path, params, encoding, tuple(keys), values, retryable)


class SyncStalledError(AssertionError):
    pass


class RequestRecorder(object):
    """
    Records each request sent through `SessionWrapper.request`, then passes the call through to
    the real method so that retries and errors behave as they do without the spy.
    """

    def __init__(self, baseurl):
        self.baseurl = baseurl
        self.requests = []
        self._original = SessionWrapper.request
        self._started = time.monotonic()

    def __call__(self, session, method, url, **kwargs):
        self._check_budget()
        encoding, body = self._body(kwargs)
        self.requests.append(
            request(
                method.upper(),
                self._path(url),
                params=self._params(kwargs),
                encoding=encoding,
                keys=self._keys(body),
                values=self._values(body),
                retryable=kwargs.get("is_retryable", False),
            )
        )
        return self._original(session, method, url, **kwargs)

    def _check_budget(self):
        if len(self.requests) >= MAX_REQUESTS:
            raise SyncStalledError(
                "Sync sent more than {} requests: {}".format(MAX_REQUESTS, self.requests[-5:])
            )
        if time.monotonic() - self._started > MAX_SECONDS:
            raise SyncStalledError(
                "Sync ran longer than {}s: {}".format(MAX_SECONDS, self.requests[-5:])
            )

    def _path(self, url):
        path = url[len(self.baseurl) :] if url.startswith(self.baseurl) else url
        return _normalize(path)

    def _params(self, kwargs):
        params = kwargs.get("params")
        if not params:
            return None
        return tuple(sorted((name, _normalize(value)) for name, value in params.items()))

    def _body(self, kwargs):
        if kwargs.get("json") is not None:
            return "json", kwargs["json"]
        data = kwargs.get("data")
        if isinstance(data, bytes):
            return "gzip", json.loads(gzip.decompress(data).decode("utf-8"))
        if data is not None:
            return "data", data
        return None, None

    def _keys(self, body):
        if isinstance(body, dict):
            return sorted(body.keys())
        if isinstance(body, list):
            # a chunk of serialized buffers, so record the keys shared by its records
            return sorted(set().union(*(record.keys() for record in body)))
        return ()

    def _values(self, body):
        if isinstance(body, dict):
            return tuple(
                sorted((key, _normalize(body[key])) for key in PINNED_VALUE_KEYS if key in body)
            )
        if isinstance(body, list):
            return len(body)
        return None

    @property
    def patch(self):
        return mock.patch.object(SessionWrapper, "request", autospec=True, side_effect=self)


TRANSFER_SESSIONS = "api/morango/v1/transfersessions/"
TRANSFER_SESSION = "api/morango/v1/transfersessions/<id>/"
BUFFERS = "api/morango/v1/buffers/"
FILTER = "<id>:user"

CREATE_TRANSFER_SESSION_KEYS = ("client_fsic", "filter", "id", "push", "sync_session_id")
BUFFER_KEYS = (
    "_self_ref_fk",
    "_self_ref_order",
    "conflicting_serialized_data",
    "deleted",
    "hard_deleted",
    "last_saved_counter",
    "last_saved_instance",
    "model_name",
    "model_uuid",
    "partition",
    "profile",
    "rmcb_list",
    "serialized",
    "source_id",
    "transfer_session",
)
PULL_TRANSFER_KEYS = ("bytes_received", "bytes_sent", "records_transferred", "transfer_stage")


def create_transfer_session(push):
    return request(
        "POST",
        TRANSFER_SESSIONS,
        encoding="json",
        keys=CREATE_TRANSFER_SESSION_KEYS,
        values=(("filter", FILTER), ("push", push)),
        retryable=True,
    )


def get_transfer_session():
    return request("GET", TRANSFER_SESSION, retryable=True)


def update_transfer_session(keys, **values):
    return request(
        "PATCH",
        TRANSFER_SESSION,
        encoding="json",
        keys=keys,
        values=tuple(sorted(values.items())),
        retryable=True,
    )


def close_transfer_session():
    return request("DELETE", TRANSFER_SESSION, retryable=True)


def push_buffers(count):
    return request("POST", BUFFERS, encoding="gzip", keys=BUFFER_KEYS, values=count, retryable=True)


def pull_buffers(offset):
    return request(
        "GET",
        BUFFERS,
        params=(("limit", CHUNK_SIZE), ("offset", offset), ("transfer_session_id", "<id>")),
        retryable=True,
    )


# 10 records in chunks of 3. Before each stage update, the async flow first reads the remote
# transfer session to check whether the remote is already at that stage.
PUSH_REQUESTS = [
    # initializing
    create_transfer_session(push=True),
    # serializing
    get_transfer_session(),
    update_transfer_session(
        ("client_fsic", "transfer_stage"), transfer_stage=transfer_stages.SERIALIZING
    ),
    # queuing
    get_transfer_session(),
    update_transfer_session(
        ("records_total", "transfer_stage"),
        records_total=10,
        transfer_stage=transfer_stages.QUEUING,
    ),
    # transferring, where the server advances its transfer session as it receives each chunk
    push_buffers(3),
    push_buffers(3),
    push_buffers(3),
    push_buffers(1),
    # dequeuing
    get_transfer_session(),
    update_transfer_session(("transfer_stage",), transfer_stage=transfer_stages.DEQUEUING),
    # deserializing
    get_transfer_session(),
    update_transfer_session(("transfer_stage",), transfer_stage=transfer_stages.DESERIALIZING),
    # cleanup
    close_transfer_session(),
]

# 10 records in chunks of 3. Before each stage update, the async flow first reads the remote
# transfer session to check whether the remote is already at that stage.
PULL_REQUESTS = [
    # initializing
    create_transfer_session(push=False),
    # serializing
    get_transfer_session(),
    update_transfer_session(
        ("client_fsic", "transfer_stage"), transfer_stage=transfer_stages.SERIALIZING
    ),
    # queuing
    get_transfer_session(),
    update_transfer_session(("transfer_stage",), transfer_stage=transfer_stages.QUEUING),
    # transferring, where the client reports its progress after each chunk
    pull_buffers(offset=0),
    update_transfer_session(
        PULL_TRANSFER_KEYS, records_transferred=3, transfer_stage=transfer_stages.TRANSFERRING
    ),
    pull_buffers(offset=3),
    update_transfer_session(
        PULL_TRANSFER_KEYS, records_transferred=6, transfer_stage=transfer_stages.TRANSFERRING
    ),
    pull_buffers(offset=6),
    update_transfer_session(
        PULL_TRANSFER_KEYS, records_transferred=9, transfer_stage=transfer_stages.TRANSFERRING
    ),
    pull_buffers(offset=9),
    update_transfer_session(
        PULL_TRANSFER_KEYS, records_transferred=10, transfer_stage=transfer_stages.TRANSFERRING
    ),
    # dequeuing
    get_transfer_session(),
    update_transfer_session(("transfer_stage",), transfer_stage=transfer_stages.DEQUEUING),
    # deserializing
    get_transfer_session(),
    update_transfer_session(("transfer_stage",), transfer_stage=transfer_stages.DESERIALIZING),
    # cleanup
    close_transfer_session(),
]


class NetworkCharacterizationTestCase(BasePushPullClientTestCase):
    def setUp(self):
        super(NetworkCharacterizationTestCase, self).setUp()
        # the expected requests depend on the chunk size, so pin it here rather than rely on
        # the harness
        self.conn.chunk_size = CHUNK_SIZE

    def _sync(self, client):
        recorder = RequestRecorder(self.server.baseurl)
        with recorder.patch:
            client.initialize(self.filter)
            client.run()
            client.finalize()
        return recorder.requests

    def _store_count(self):
        return Store.objects.filter(
            partition__startswith=str(self.filter), profile=self.profile
        ).count()

    def _remote_transfer_session(self, transfer_session):
        with second_environment():
            return TransferSession.objects.get(id=transfer_session.id)

    def assertTransferSessionFinalState(self, transfer_session, records_total):
        transfer_session.refresh_from_db()
        self.assertEqual(records_total, transfer_session.records_total)
        self.assertEqual(records_total, transfer_session.records_transferred)
        self.assertEqual(transfer_stages.CLEANUP, transfer_session.transfer_stage)
        self.assertEqual(transfer_statuses.COMPLETED, transfer_session.transfer_stage_status)
        self.assertFalse(transfer_session.active)
        self.assertEqual(0, Buffer.objects.filter(transfer_session=transfer_session).count())

    def test_async_push(self):
        for _ in range(5):
            SummaryLog.objects.create(user=self.local_user)
            InteractionLog.objects.create(user=self.local_user)

        client = self.client.get_push_client()
        requests = self._sync(client)
        self.assertEqual(PUSH_REQUESTS, requests)

        transfer_session = client.context.transfer_session
        self.assertTransferSessionFinalState(transfer_session, records_total=10)

        # the client does not report its byte counts to the server during a push
        remote_transfer_session = self._remote_transfer_session(transfer_session)
        self.assertEqual(0, remote_transfer_session.bytes_sent)
        self.assertEqual(0, remote_transfer_session.bytes_received)

        with second_environment():
            # the 10 pushed logs, plus the user
            self.assertEqual(11, self._store_count())
            self.assertEqual(5, SummaryLog.objects.filter(user=self.remote_user).count())
            self.assertEqual(5, InteractionLog.objects.filter(user=self.remote_user).count())

    def test_async_pull(self):
        with second_environment():
            for _ in range(5):
                SummaryLog.objects.create(user=self.remote_user)
                InteractionLog.objects.create(user=self.remote_user)

        client = self.client.get_pull_client()
        requests = self._sync(client)
        self.assertEqual(PULL_REQUESTS, requests)

        transfer_session = client.context.transfer_session
        self.assertTransferSessionFinalState(transfer_session, records_total=10)

        # the client reports its byte counts to the server, flipped to the server's point of view
        remote_transfer_session = self._remote_transfer_session(transfer_session)
        self.assertEqual(transfer_session.bytes_received, remote_transfer_session.bytes_sent)
        self.assertEqual(transfer_session.bytes_sent, remote_transfer_session.bytes_received)

        # the 10 pulled logs, plus the user from the initial pull in `setUp`
        self.assertEqual(11, self._store_count())
        self.assertEqual(5, SummaryLog.objects.filter(user=self.local_user).count())
        self.assertEqual(5, InteractionLog.objects.filter(user=self.local_user).count())
