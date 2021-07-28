"""
The main module to be used for initiating the synchronization of data between morango instances.
"""
import os
import json
import logging
import socket
import uuid
from io import BytesIO

from django.utils import timezone
from django.utils.six import iteritems
from django.utils.six import raise_from
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from requests.packages.urllib3.util.retry import Retry
from django.utils.six.moves.urllib.parse import urljoin
from django.utils.six.moves.urllib.parse import urlparse

from .session import SessionWrapper
from morango.api.serializers import CertificateSerializer
from morango.api.serializers import InstanceIDSerializer
from morango.constants import api_urls
from morango.constants import transfer_stages
from morango.constants import transfer_statuses
from morango.constants.capabilities import ALLOW_CERTIFICATE_PUSHING
from morango.constants.capabilities import GZIP_BUFFER_POST
from morango.errors import CertificateSignatureInvalid
from morango.errors import MorangoError
from morango.errors import MorangoResumeSyncError
from morango.errors import MorangoServerDoesNotAllowNewCertPush
from morango.models.certificates import Certificate
from morango.models.certificates import Key
from morango.models.core import InstanceIDModel
from morango.models.core import SyncSession
from morango.sync.controller import SessionController
from morango.sync.context import LocalSessionContext
from morango.sync.context import NetworkSessionContext
from morango.sync.utils import SyncSignal
from morango.sync.utils import SyncSignalGroup
from morango.utils import CAPABILITIES
from morango.utils import pid_exists

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
    __slots__ = (
        "base_url",
        "compresslevel",
        "session",
        "server_info",
        "capabilities",
        "chunk_size",
    )

    default_chunk_size = 500

    def __init__(
        self,
        base_url="",
        compresslevel=9,
        retries=7,
        backoff_factor=0.3,
        chunk_size=default_chunk_size,
    ):
        """
        The underlying network connection with a syncing peer. Any network requests
        (such as certificate querying or syncing related) will be done through this class.
        """
        if base_url == "":
            raise AssertionError("Network connection `base_url` cannot be empty")

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
        self.chunk_size = chunk_size

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

    def create_sync_session(self, client_cert, server_cert, chunk_size=None):
        """
        Starts a sync session by creating it on the server side and returning a client to use
        for initiating transfer operations

        :param client_cert: The local certificate to use, already registered with the server
        :type client_cert: Certificate
        :param server_cert: The server's certificate that relates to the same profile as local
        :type server_cert: Certificate
        :param chunk_size: An optional parameter specifying the size for each transferred chunk
        :type chunk_size: int
        :return: A SyncSessionClient instance
        :rtype: SyncSessionClient
        """
        if chunk_size is not None:
            self.chunk_size = chunk_size

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
            "process_id": os.getpid(),
        }
        sync_session = SyncSession.objects.create(**data)
        return SyncSessionClient(self, sync_session)

    def resume_sync_session(self, sync_session_id, chunk_size=None):
        """
        Resumes an existing sync session given an ID

        :param sync_session_id: The UUID of the `SyncSession` to resume
        :param chunk_size: An optional parameter specifying the size for each transferred chunk
        :return: A SyncSessionClient instance
        :rtype: SyncSessionClient
        """
        if chunk_size is not None:
            self.chunk_size = chunk_size

        try:
            sync_session = SyncSession.objects.get(pk=sync_session_id, active=True)
        except SyncSession.DoesNotExist:
            raise MorangoResumeSyncError(
                "Session for ID '{}' not found".format(sync_session_id)
            )

        # check that process of existing session isn't still running
        if (
            sync_session.process_id
            and sync_session.process_id != os.getpid()
            and pid_exists(sync_session.process_id)
        ):
            raise MorangoResumeSyncError(
                "Session process '{}' is still running".format(sync_session.process_id)
            )

        # In order to resume, we need sync sessions on both server and client, otherwise resuming
        # wouldn't have any benefit
        try:
            self._get_sync_session(sync_session)
        except HTTPError as e:
            raise_from(MorangoResumeSyncError("Failure resuming sync session"), e)

        # update process id
        sync_session.process_id = os.getpid()
        sync_session.save()
        return SyncSessionClient(self, sync_session)

    def close_sync_session(self, sync_session):
        # "delete" sync session on server side
        self._close_sync_session(sync_session)

        sync_session.active = False
        sync_session.save()

    def close(self):
        # close adapters on requests session object
        self.session.close()

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

    def _get_sync_session(self, sync_session):
        return self.session.get(
            self.urlresolve(api_urls.SYNCSESSION, lookup=sync_session.id)
        )

    def _create_transfer_session(self, data):
        return self.session.post(self.urlresolve(api_urls.TRANSFERSESSION), json=data)

    def _get_transfer_session(self, transfer_session):
        return self.session.get(
            self.urlresolve(api_urls.TRANSFERSESSION, lookup=transfer_session.id)
        )

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

    def _pull_record_chunk(self, transfer_session):
        # pull records from server for given transfer session
        params = {
            "limit": self.chunk_size,
            "offset": transfer_session.records_transferred,
            "transfer_session_id": transfer_session.id,
        }
        return self.session.get(self.urlresolve(api_urls.BUFFER), params=params)


class SyncClientSignals(SyncSignal):
    """
    Class for holding all signal types, attached to `SyncClient` as attribute. All groups
    are sent the `TransferSession` object via the `transfer_session` keyword argument.
    """

    session = SyncSignalGroup(transfer_session=None)
    """Signal group firing for each push and pull `TransferSession`."""
    queuing = SyncSignalGroup(transfer_session=None)
    """Queuing signal group for locally or remotely queuing data before transfer."""
    transferring = SyncSignalGroup(transfer_session=None)
    """Transferring signal group for tracking progress of push/pull on `TransferSession`."""
    dequeuing = SyncSignalGroup(transfer_session=None)
    """Dequeuing signal group for locally or remotely dequeuing data after transfer."""


class SyncSessionClient(object):
    __slots__ = (
        "sync_connection",
        "sync_session",
        "signals",
        "controller",
    )

    def __init__(self, sync_connection, sync_session, controller=None):
        """
        :param sync_connection: NetworkSyncConnection
        :param sync_session: SyncSession
        :param controller: SessionController
        """
        self.sync_connection = sync_connection
        self.sync_session = sync_session
        self.signals = SyncClientSignals()
        self.controller = controller or SessionController.build()

    def get_pull_client(self):
        """
        returns ``PullClient``
        """
        return PullClient(self.sync_connection, self.sync_session, self.controller)

    def get_push_client(self):
        """
        returns ``PushClient``
        """
        return PushClient(self.sync_connection, self.sync_session, self.controller)

    def initiate_pull(self, sync_filter):
        """
        Deprecated - Please use ``get_pull_client`` and use the client
        :param sync_filter: Filter
        """
        client = self.get_pull_client()
        client.signals = self.signals
        client.initialize(sync_filter)
        client.run()
        client.finalize()

    def initiate_push(self, sync_filter):
        """
        Deprecated - Please use ``get_push_client`` and use the client
        """
        client = self.get_push_client()
        client.signals = self.signals
        client.initialize(sync_filter)
        client.run()
        client.finalize()

    def close_sync_session(self):
        """
        Deprecated - Please use ``NetworkSyncConnection.close_sync_session`` and ``NetworkSyncConnection.close``
        """
        self.sync_connection.close_sync_session(self.sync_session)
        self.sync_connection.close()


class TransferClient(object):
    """
    Base class for handling common operations for initiating syncing and other related operations.
    """

    __slots__ = (
        "sync_connection",
        "sync_session",
        "controller",
        "current_transfer_session",
        "signals",
        "local_context",
        "remote_context",
    )

    def __init__(self, sync_connection, sync_session, controller):
        """
        :param sync_connection: NetworkSyncConnection
        :param sync_session: SyncSession
        :param controller: SessionController
        """
        self.sync_connection = sync_connection
        self.sync_session = sync_session
        self.controller = controller
        self.current_transfer_session = None
        self.signals = SyncClientSignals()

        # TODO: come up with strategy to use only one context here
        self.local_context = LocalSessionContext(sync_session=sync_session)
        self.remote_context = NetworkSessionContext(
            sync_connection, sync_session=sync_session
        )

    def proceed_to_and_wait_for(self, stage):
        contexts = (self.local_context, self.remote_context)
        for context in contexts:
            max_interval = 1 if context is self.local_context else 5
            result = self.controller.proceed_to_and_wait_for(
                stage, context=context, max_interval=max_interval
            )
            if result == transfer_statuses.ERRORED:
                raise_from(
                    MorangoError("Stage `{}` failed".format(stage)),
                    context.error,
                )

    def initialize(self, sync_filter):
        """
        :param sync_filter: Filter
        """
        # set filter on controller
        self.local_context.update(sync_filter=sync_filter)
        self.remote_context.update(sync_filter=sync_filter)

        # initialize the transfer session locally
        status = self.controller.proceed_to_and_wait_for(
            transfer_stages.INITIALIZING, context=self.local_context, max_interval=1
        )
        if status == transfer_statuses.ERRORED:
            raise_from(
                MorangoError("Failed to initialize transfer session"),
                self.local_context.error,
            )

        # copy the transfer session to local state and update remote controller context
        self.current_transfer_session = self.local_context.transfer_session
        self.remote_context.update(transfer_session=self.current_transfer_session)

        self.signals.session.started.fire(
            transfer_session=self.current_transfer_session
        )

        # backwards compatibility for the queuing signal as it included both serialization
        # and queuing originally
        with self.signals.queuing.send(transfer_session=self.current_transfer_session):
            # proceeding to serialization on remote will trigger initialization as well
            self.proceed_to_and_wait_for(transfer_stages.SERIALIZING)
            self.proceed_to_and_wait_for(transfer_stages.QUEUING)

    def run(self):
        with self.signals.transferring.send(
            transfer_session=self.current_transfer_session
        ) as status:
            self._transfer(callback=status.in_progress.fire)

    def finalize(self):
        # if not initialized, we don't need to finalize
        if not self.current_transfer_session:
            return

        with self.signals.dequeuing.send(
            transfer_session=self.current_transfer_session
        ):
            self.proceed_to_and_wait_for(transfer_stages.DESERIALIZING)

        self.proceed_to_and_wait_for(transfer_stages.CLEANUP)
        self.signals.session.completed.fire(
            transfer_session=self.current_transfer_session
        )
        self.current_transfer_session = None

    def _transfer(self, callback=None):
        result = transfer_statuses.PENDING

        while result not in transfer_statuses.FINISHED_STATES:
            result = self.controller.proceed_to(
                transfer_stages.TRANSFERRING, context=self.remote_context
            )
            self.local_context.update(
                stage=transfer_stages.TRANSFERRING, stage_status=result
            )
            if callback is not None:
                callback()

        if result == transfer_statuses.ERRORED:
            raise_from(
                MorangoError("Failure occurred during transfer"),
                self.remote_context.error,
            )


class PushClient(TransferClient):
    """
    Sync client for pushing to a server
    """

    def __init__(self, *args, **kwargs):
        super(PushClient, self).__init__(*args, **kwargs)
        self.local_context.update(is_push=True)
        self.remote_context.update(is_push=True)


class PullClient(TransferClient):
    """
    Sync class to pull from server
    """

    def __init__(self, *args, **kwargs):
        super(PullClient, self).__init__(*args, **kwargs)
        self.local_context.update(is_push=False)
        self.remote_context.update(is_push=False)
