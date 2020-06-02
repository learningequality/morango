import json
import logging
import socket
import uuid
from io import BytesIO

from django.conf import settings
from django.core.paginator import Paginator
from django.utils import timezone
from django.utils.six import iteritems
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from rest_framework.exceptions import ValidationError
from django.utils.six.moves.urllib.parse import urljoin
from django.utils.six.moves.urllib.parse import urlparse

from .operations import _dequeue_into_store
from .operations import _queue_into_buffer
from .operations import _serialize_into_store
from .session import SessionWrapper
from .utils import validate_and_create_buffer_data
from morango.api.serializers import BufferSerializer
from morango.api.serializers import CertificateSerializer
from morango.api.serializers import InstanceIDSerializer
from morango.constants import api_urls
from morango.constants.capabilities import ALLOW_CERTIFICATE_PUSHING
from morango.constants.capabilities import GZIP_BUFFER_POST
from morango.errors import CertificateSignatureInvalid
from morango.errors import MorangoError
from morango.errors import MorangoServerDoesNotAllowNewCertPush
from morango.models.certificates import Certificate
from morango.models.certificates import Filter
from morango.models.certificates import Key
from morango.models.core import Buffer
from morango.models.core import DatabaseMaxCounter
from morango.models.core import InstanceIDModel
from morango.models.core import RecordMaxCounterBuffer
from morango.models.core import SyncSession
from morango.models.core import TransferSession
from morango.utils import CAPABILITIES

if GZIP_BUFFER_POST in CAPABILITIES:
    from gzip import GzipFile


logger = logging.getLogger(__name__)


def _join_with_logical_operator(lst, operator):
    op = ") {operator} (".format(operator=operator)
    return "(({items}))".format(items=op.join(lst))


def _get_server_ip(hostname):
    try:
        return socket.gethostbyname(hostname)
    except:  # noqa: E722
        return ""


def _get_client_ip_for_server(server_host, server_port):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((server_host, server_port))
        IP = s.getsockname()[0]
    except:  # noqa: E722
        IP = "127.0.0.1"
    finally:
        s.close()
    return IP


# borrowed from https://github.com/django/django/blob/1.11.20/django/utils/text.py#L295
def compress_string(s, compresslevel=9):
    zbuf = BytesIO()
    with GzipFile(
        mode="wb", compresslevel=compresslevel, fileobj=zbuf, mtime=0
    ) as zfile:
        zfile.write(s)
    return zbuf.getvalue()


class Connection(object):
    """
    Abstraction around a connection with a syncing peer (network or disk),
    supporting interactions with that peer. This may be used by a SyncClient,
    but also supports other operations (e.g. querying certificates) outside
    the context of syncing.

    This class should be subclassed for particular transport mechanisms,
    and the necessary methods overridden.
    """

    pass


class NetworkSyncConnection(Connection):
    def __init__(self, base_url="", compresslevel=9, retries=7, backoff_factor=0.3):
        self.base_url = base_url
        self.compresslevel = compresslevel
        # set up requests session with retry logic
        self.session = SessionWrapper()
        # sleep for {backoff factor} * (2 ^ ({number of total retries} - 1)) between requests
        # with 7 retry attempts, sleep escalation becomes (0.6s, 1.2s, ..., 38.4s)
        retry = Retry(total=retries, backoff_factor=backoff_factor)
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        # get morango information about server
        self.server_info = self.session.get(
            urljoin(self.base_url, api_urls.INFO)
        ).json()
        self.capabilities = self.server_info.get("capabilities", [])

    @property
    def bytes_sent(self):
        return self.session.bytes_sent

    @property
    def bytes_received(self):
        return self.session.bytes_received

    def urlresolve(self, endpoint, lookup=None):
        if lookup:
            lookup = lookup + "/"
        url = urljoin(urljoin(self.base_url, endpoint), lookup)
        return url

    def create_sync_session(self, client_cert, server_cert, chunk_size=500):
        # if server cert does not exist locally, retrieve it from server
        if not Certificate.objects.filter(id=server_cert.id).exists():
            cert_chain_response = self._get_certificate_chain(
                params={"ancestors_of": server_cert.id}
            )

            # upon receiving cert chain from server, we attempt to save the chain into our records
            Certificate.save_certificate_chain(
                cert_chain_response.json(), expected_last_id=server_cert.id
            )

        # request the server for a one-time-use nonce
        nonce_resp = self._get_nonce()
        nonce = nonce_resp.json()["id"]

        # if no hostname then url is actually an ip
        url = urlparse(self.base_url)
        hostname = url.hostname or self.base_url
        port = url.port or (80 if url.scheme == "http" else 443)

        # prepare the data to send in the syncsession creation request
        data = {
            "id": uuid.uuid4().hex,
            "server_certificate_id": server_cert.id,
            "client_certificate_id": client_cert.id,
            "profile": client_cert.profile,
            "certificate_chain": json.dumps(
                CertificateSerializer(
                    client_cert.get_ancestors(include_self=True), many=True
                ).data
            ),
            "connection_path": self.base_url,
            "instance": json.dumps(
                InstanceIDSerializer(
                    InstanceIDModel.get_or_create_current_instance()[0]
                ).data
            ),
            "nonce": nonce,
            "client_ip": _get_client_ip_for_server(hostname, port),
            "server_ip": _get_server_ip(hostname),
        }

        # sign the nonce/ID combo to attach to the request
        message = "{nonce}:{id}".format(**data)
        data["signature"] = client_cert.sign(message)

        # Sync Session creation request
        session_resp = self._create_sync_session(data)

        # check that the nonce/id were properly signed by the server cert
        if not server_cert.verify(message, session_resp.json().get("signature")):
            raise CertificateSignatureInvalid()

        # build the data to be used for creating our own syncsession
        data = {
            "id": data["id"],
            "start_timestamp": timezone.now(),
            "last_activity_timestamp": timezone.now(),
            "active": True,
            "is_server": False,
            "client_certificate": client_cert,
            "server_certificate": server_cert,
            "profile": client_cert.profile,
            "connection_kind": "network",
            "connection_path": self.base_url,
            "client_ip": data["client_ip"],
            "server_ip": data["server_ip"],
            "client_instance": json.dumps(
                InstanceIDSerializer(
                    InstanceIDModel.get_or_create_current_instance()[0]
                ).data
            ),
            "server_instance": session_resp.json().get("server_instance") or "{}",
        }
        sync_session = SyncSession.objects.create(**data)

        return SyncClient(self, sync_session, chunk_size=chunk_size)

    def get_remote_certificates(self, primary_partition, scope_def_id=None):
        remote_certs = []
        # request certs for this primary partition, where the server also has a private key for
        remote_certs_resp = self._get_certificate_chain(
            params={"primary_partition": primary_partition}
        )

        # inflate remote certs into a list of unsaved models
        for cert in remote_certs_resp.json():
            remote_certs.append(
                Certificate.deserialize(cert["serialized"], cert["signature"])
            )

        # filter certs by scope definition id, if provided
        if scope_def_id:
            remote_certs = [
                cert
                for cert in remote_certs
                if cert.scope_definition_id == scope_def_id
            ]

        return remote_certs

    def certificate_signing_request(
        self,
        parent_cert,
        scope_definition_id,
        scope_params,
        userargs=None,
        password=None,
    ):
        # if server cert does not exist locally, retrieve it from server
        if not Certificate.objects.filter(id=parent_cert.id).exists():
            cert_chain_response = self._get_certificate_chain(
                params={"ancestors_of": parent_cert.id}
            )

            # upon receiving cert chain from server, we attempt to save the chain into our records
            Certificate.save_certificate_chain(
                cert_chain_response.json(), expected_last_id=parent_cert.id
            )

        csr_key = Key()
        # build up data for csr
        data = {
            "parent": parent_cert.id,
            "profile": parent_cert.profile,
            "scope_definition": scope_definition_id,
            "scope_version": parent_cert.scope_version,
            "scope_params": json.dumps(scope_params),
            "public_key": csr_key.get_public_key_string(),
        }
        csr_resp = self._certificate_signing(data, userargs, password)
        csr_data = csr_resp.json()

        # verify cert returned from server, and proceed to save into our records
        csr_cert = Certificate.deserialize(
            csr_data["serialized"], csr_data["signature"]
        )
        csr_cert.private_key = csr_key
        csr_cert.check_certificate()
        csr_cert.save()
        return csr_cert

    def push_signed_client_certificate_chain(
        self, local_parent_cert, scope_definition_id, scope_params
    ):
        if ALLOW_CERTIFICATE_PUSHING not in self.capabilities:
            raise MorangoServerDoesNotAllowNewCertPush(
                "Server does not allow certificate pushing"
            )

        # grab shared public key of server
        publickey_response = self._get_public_key()

        # request the server for a one-time-use nonce
        nonce_response = self._get_nonce()

        # build up data for csr
        certificate = Certificate(
            parent_id=local_parent_cert.id,
            profile=local_parent_cert.profile,
            scope_definition_id=scope_definition_id,
            scope_version=local_parent_cert.scope_version,
            scope_params=json.dumps(scope_params),
            public_key=Key(
                public_key_string=publickey_response.json()[0]["public_key"]
            ),
            salt=nonce_response.json()[
                "id"
            ],  # for pushing signed certs, we use nonce as salt
        )

        # add ID and signature to the certificate
        certificate.id = certificate.calculate_uuid()
        certificate.parent.sign_certificate(certificate)

        # serialize the chain for sending to server
        certificate_chain = list(local_parent_cert.get_ancestors(include_self=True)) + [
            certificate
        ]
        data = json.dumps(CertificateSerializer(certificate_chain, many=True).data)

        # client sends signed certificate chain to server
        self._push_certificate_chain(data)

        # if there are no errors, we can save the pushed certificate
        certificate.save()
        return certificate

    def _get_public_key(self):
        return self.session.get(self.urlresolve(api_urls.PUBLIC_KEY))

    def _get_nonce(self):
        return self.session.post(self.urlresolve(api_urls.NONCE))

    def _get_certificate_chain(self, params):
        return self.session.get(self.urlresolve(api_urls.CERTIFICATE), params=params)

    def _certificate_signing(self, data, userargs, password):
        # convert user arguments into query str for passing to auth layer
        if isinstance(userargs, dict):
            userargs = "&".join(
                ["{}={}".format(key, val) for (key, val) in iteritems(userargs)]
            )
        return self.session.post(
            self.urlresolve(api_urls.CERTIFICATE), json=data, auth=(userargs, password)
        )

    def _push_certificate_chain(self, data):
        return self.session.post(self.urlresolve(api_urls.CERTIFICATE_CHAIN), json=data)

    def _create_sync_session(self, data):
        return self.session.post(self.urlresolve(api_urls.SYNCSESSION), json=data)

    def _create_transfer_session(self, data):
        return self.session.post(self.urlresolve(api_urls.TRANSFERSESSION), json=data)

    def _update_transfer_session(self, data, transfer_session):
        return self.session.patch(
            self.urlresolve(api_urls.TRANSFERSESSION, lookup=transfer_session.id),
            json=data,
        )

    def _close_transfer_session(self, transfer_session):
        return self.session.delete(
            self.urlresolve(api_urls.TRANSFERSESSION, lookup=transfer_session.id)
        )

    def _close_sync_session(self, sync_session):
        return self.session.delete(
            self.urlresolve(api_urls.SYNCSESSION, lookup=sync_session.id)
        )

    def _push_record_chunk(self, data):
        # gzip the data if both client and server have gzipping capabilities
        if GZIP_BUFFER_POST in self.capabilities and GZIP_BUFFER_POST in CAPABILITIES:
            json_data = json.dumps([dict(el) for el in data])
            gzipped_data = compress_string(
                bytes(json_data.encode("utf-8")), compresslevel=self.compresslevel
            )
            return self.session.post(
                self.urlresolve(api_urls.BUFFER),
                data=gzipped_data,
                headers={"content-type": "application/gzip"},
            )
        else:
            return self.session.post(self.urlresolve(api_urls.BUFFER), json=data)

    def _pull_record_chunk(self, chunk_size, transfer_session):
        # pull records from server for given transfer session
        params = {
            "limit": chunk_size,
            "offset": transfer_session.records_transferred,
            "transfer_session_id": transfer_session.id,
        }
        return self.session.get(self.urlresolve(api_urls.BUFFER), params=params)


class SyncSignal(object):
    """
    Helper class for firing signals from the sync client
    """

    def __init__(self, **kwargs_defaults):
        """
        Default keys/values that the signal consumer can depend on being present.
        """
        self._handlers = []
        self._defaults = kwargs_defaults

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

    def __init__(self, **kwargs_defaults):
        super(SyncSignalGroup, self).__init__(**kwargs_defaults)
        self.started = SyncSignal(**kwargs_defaults)
        self.in_progress = SyncSignal(**kwargs_defaults)
        self.completed = SyncSignal(**kwargs_defaults)

        self.started.connect(self.fire)
        self.in_progress.connect(self.fire)
        self.completed.connect(self.fire)

    def send(self, **kwargs):
        """
        Context manager helper that will signal started and fired when entered and exited,
        and it'll fire those with the `kwargs`.

        :rtype: SyncSignalGroup
        """
        defaults = self._defaults.copy()
        defaults.update(kwargs)
        context_group = SyncSignalGroup(**defaults)
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


class SyncClientSignals(object):
    """
    Class for holding all signal types, attached to `SyncClient` as attribute. All groups
    are sent the `TransferSession` object via the `transfer_session` keyword argument.
    """

    session = SyncSignalGroup(transfer_session=None)
    """Signal group firing for each push and pull `TransferSession`."""
    queuing = SyncSignalGroup(transfer_session=None)
    """Queuing signal group for locally or remotely queuing data before transfer."""
    pushing = SyncSignalGroup(transfer_session=None)
    """Pushing signal group for tracking push progress of `TransferSession`."""
    pulling = SyncSignalGroup(transfer_session=None)
    """Pulling signal group for tracking pull progress of `TransferSession`."""
    dequeuing = SyncSignalGroup(transfer_session=None)
    """Dequeuing signal group for locally or remotely dequeuing data after transfer."""


class SyncClient(object):
    """
    Controller to support client in initiating syncing and performing related operations.
    """

    signals = SyncClientSignals()
    """A namespacing attribute clearly separating signal groups"""

    def __init__(self, sync_connection, sync_session, chunk_size=500):
        """
        :type sync_connection: NetworkSyncConnection
        :type sync_session: SessionWrapper
        :type chunk_size: int
        """
        self.sync_connection = sync_connection
        self.sync_session = sync_session
        self.chunk_size = chunk_size
        self.current_transfer_session = None

    def initiate_push(self, sync_filter):
        self._create_transfer_session(True, sync_filter)

        with self.signals.queuing.send(transfer_session=self.current_transfer_session):
            _queue_into_buffer(self.current_transfer_session)

        # update the records_total for client and server transfer session
        records_total = Buffer.objects.filter(
            transfer_session=self.current_transfer_session
        ).count()

        self.current_transfer_session.records_total = records_total
        self.current_transfer_session.save()

        if records_total == 0:
            self._close_transfer_session()
            return

        self.sync_connection._update_transfer_session(
            {"records_total": records_total}, self.current_transfer_session
        )

        with self.signals.pushing.send(
            transfer_session=self.current_transfer_session
        ) as status:
            self._push_records(callback=status.in_progress.fire)

        # upon successful completion of pushing records, proceed to delete buffered records
        Buffer.objects.filter(transfer_session=self.current_transfer_session).delete()
        RecordMaxCounterBuffer.objects.filter(
            transfer_session=self.current_transfer_session
        ).delete()

        # close client and server transfer session
        self._close_transfer_session()

    def initiate_pull(self, sync_filter):
        self._create_transfer_session(False, sync_filter)

        if self.current_transfer_session.records_total == 0:
            self._close_transfer_session()
            return

        with self.signals.pulling.send(
            transfer_session=self.current_transfer_session
        ) as status:
            self._pull_records(callback=status.in_progress.fire)

        with self.signals.dequeuing.send(transfer_session=self.current_transfer_session):
            _dequeue_into_store(self.current_transfer_session)

        # update database max counters but use latest fsics on client
        DatabaseMaxCounter.update_fsics(
            json.loads(self.current_transfer_session.server_fsic), sync_filter
        )

        self._close_transfer_session()

    def _pull_records(self, callback=None):
        while (
            self.current_transfer_session.records_transferred
            < self.current_transfer_session.records_total
        ):
            buffers_resp = self.sync_connection._pull_record_chunk(
                self.chunk_size, self.current_transfer_session
            )

            # load the returned data from JSON
            data = buffers_resp.json()

            # parse out the results from a paginated set, if needed
            if isinstance(data, dict) and "results" in data:
                data = data["results"]

            # ensure the transfer session allows pulls, and is same across records
            transfer_session = TransferSession.objects.get(
                id=data[0]["transfer_session"]
            )
            if transfer_session.push:
                raise ValidationError(
                    "Specified TransferSession does not allow pulling."
                )

            if len(set(rec["transfer_session"] for rec in data)) > 1:
                raise ValidationError(
                    "All pulled records must be associated with the same TransferSession."
                )

            if self.current_transfer_session.id != transfer_session.id:
                raise ValidationError(
                    "Specified TransferSession does not match this SyncClient's current TransferSession."
                )

            validate_and_create_buffer_data(
                data, self.current_transfer_session, connection=self.sync_connection
            )
            # update the records transferred so client and server are in agreement
            self.sync_connection._update_transfer_session(
                {
                    "records_transferred": self.current_transfer_session.records_transferred,
                    # flip each of these since we're talking about the remote instance
                    "bytes_received": self.current_transfer_session.bytes_sent,
                    "bytes_sent": self.current_transfer_session.bytes_received,
                },
                self.current_transfer_session,
            )

            if callback is not None:
                callback()

    def _push_records(self, callback=None):
        # paginate buffered records so we do not load them all into memory
        buffered_records = Buffer.objects.filter(
            transfer_session=self.current_transfer_session
        ).order_by("pk")
        buffered_pages = Paginator(buffered_records, self.chunk_size)
        for count in buffered_pages.page_range:

            # serialize and send records to server
            serialized_recs = BufferSerializer(
                buffered_pages.page(count).object_list, many=True
            )
            self.sync_connection._push_record_chunk(serialized_recs.data)
            # update records_transferred upon successful request
            self.current_transfer_session.records_transferred = min(
                self.current_transfer_session.records_transferred + self.chunk_size,
                self.current_transfer_session.records_total,
            )
            self.current_transfer_session.bytes_sent = self.sync_connection.bytes_sent
            self.current_transfer_session.bytes_received = (
                self.sync_connection.bytes_received
            )
            self.current_transfer_session.save()

            if callback is not None:
                callback()

    def close_sync_session(self):
        # "delete" sync session on server side
        self.sync_connection._close_sync_session(self.sync_session)

        # "delete" our own local sync session
        if self.current_transfer_session is not None:
            raise MorangoError(
                "Transfer Session must be closed before closing sync session."
            )
        self.sync_session.active = False
        self.sync_session.save()
        self.sync_session = None
        # close adapters on requests session object
        self.sync_connection.session.close()

    def _create_transfer_session(self, push, filter):
        # build data for creating transfer session on server side
        data = {
            "id": uuid.uuid4().hex,
            "filter": str(filter),
            "push": push,
            "sync_session_id": self.sync_session.id,
        }

        data["last_activity_timestamp"] = timezone.now()
        self.current_transfer_session = TransferSession.objects.create(**data)
        self.signals.session.started.fire(
            transfer_session=self.current_transfer_session
        )
        data.pop("last_activity_timestamp")

        if push:
            # before pushing, we want to serialize the most recent data and update database max counters
            if getattr(settings, "MORANGO_SERIALIZE_BEFORE_QUEUING", True):
                _serialize_into_store(
                    self.current_transfer_session.sync_session.profile,
                    filter=Filter(self.current_transfer_session.filter),
                )

        data["client_fsic"] = json.dumps(
            DatabaseMaxCounter.calculate_filter_max_counters(filter)
        )
        self.current_transfer_session.client_fsic = data["client_fsic"]

        # save transfersession locally before creating transfersession server side
        self.current_transfer_session.save()

        # the next call to create the session starts queueing on server side
        if not push:
            self.signals.queuing.started.fire(
                transfer_session=self.current_transfer_session
            )

        # create transfer session on server side
        transfer_resp = self.sync_connection._create_transfer_session(data)

        # the above call to create the session started queueing on server side, so it's done
        if not push:
            self.signals.queuing.completed.fire(
                transfer_session=self.current_transfer_session
            )

        self.current_transfer_session.server_fsic = (
            transfer_resp.json().get("server_fsic") or "{}"
        )
        if not push:
            self.current_transfer_session.records_total = transfer_resp.json().get(
                "records_total"
            )
        self.current_transfer_session.save()

    def _close_transfer_session(self):
        # deleting the server's transfer session also dequeues
        if self.current_transfer_session.push:
            self.signals.dequeuing.started.fire(
                transfer_session=self.current_transfer_session
            )

        # "delete" transfer session on server side
        self.sync_connection._close_transfer_session(self.current_transfer_session)

        if self.current_transfer_session.push:
            self.signals.dequeuing.completed.fire(
                transfer_session=self.current_transfer_session
            )

        # "delete" our own local transfer session
        self.current_transfer_session.active = False
        self.current_transfer_session.save()
        self.signals.session.completed.fire(
            transfer_session=self.current_transfer_session
        )
        self.current_transfer_session = None
