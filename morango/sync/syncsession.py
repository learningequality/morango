import json
import logging
import socket
import uuid

import requests
from django.conf import settings
from django.utils import timezone
from django.utils.six import iteritems
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from rest_framework.exceptions import ValidationError
from six.moves.urllib.parse import urljoin
from six.moves.urllib.parse import urlparse

from .operations import _dequeue_into_store
from .operations import _queue_into_buffer
from .operations import _serialize_into_store
from .session import SessionWrapper
from .utils import compress_string
from .utils import validate_and_create_buffer_data
from morango.api.serializers import BufferSerializer
from morango.api.serializers import CertificateSerializer
from morango.api.serializers import InstanceIDSerializer
from morango.constants import api_urls
from morango.constants import transfer_status
from morango.constants.capabilities import ALLOW_CERTIFICATE_PUSHING
from morango.constants.capabilities import GZIP_BUFFER_POST
from morango.errors import CertificateSignatureInvalid
from morango.errors import MorangoError
from morango.errors import MorangoServerDoesNotAllowNewCertPush
from morango.models.certificates import Certificate
from morango.models.certificates import Key
from morango.models.core import Buffer
from morango.models.core import DatabaseMaxCounter
from morango.models.core import InstanceIDModel
from morango.models.core import RecordMaxCounterBuffer
from morango.models.core import SyncSession
from morango.models.core import TransferSession
from morango.utils import CAPABILITIES


logger = logging.getLogger(__name__)


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

    def urlresolve(self, endpoint, lookup=None):
        if lookup:
            lookup = lookup + "/"
        url = urljoin(urljoin(self.base_url, endpoint), lookup)
        return url

    def _retrieve_server_cert_if_needed(self, cert):
        # if server cert does not exist locally, retrieve it from server
        if not Certificate.objects.filter(id=cert.id).exists():
            cert_chain_response = self._get_certificate_chain(
                params={"ancestors_of": cert.id}
            )

            # upon receiving cert chain from server, we attempt to save the chain into our records
            Certificate.save_certificate_chain(
                cert_chain_response.json(), expected_last_id=cert.id
            )

    def create_sync_session(self, client_cert, server_cert, chunk_size=500):
        resumable_ss = SyncSession.objects.filter(
            active=True,
            client_certificate=client_cert,
            server_certificate=server_cert,
            is_server=False,
        ).first()
        # attempt to resume an active sync session
        if resumable_ss:
            # if instance ids match (ensuring syncing with same server), resume sync session
            if (
                self.server_info["instance_id"]
                == json.loads(resumable_ss.server_instance)["id"]
            ):
                try:
                    ss_response = self._get_sync_session(resumable_ss.id)
                except requests.exceptions.HTTPError as e:
                    # if non existent server ss, continue to create one on server
                    if e.response.status_code == 404:
                        pass
                    else:
                        raise
                # only gets executed if no exception was caught above
                else:
                    # if server session still active, resume session
                    if ss_response.json()["active"] is True:
                        return SyncClient(self, resumable_ss, chunk_size=chunk_size)

        # if server cert does not exist locally, retrieve it from server
        self._retrieve_server_cert_if_needed(server_cert)

        # request the server for a one-time-use nonce
        nonce = self._get_nonce().json()["id"]

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

    def get_remote_certificates(
        self, primary_partition, scope_def_id=None, scope_params=None
    ):
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

        if scope_params:
            if isinstance(scope_params, dict):
                scope_params = json.dumps(scope_params)
            remote_certs = [
                cert for cert in remote_certs if cert.scope_params == scope_params
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
        self._retrieve_server_cert_if_needed(parent_cert)

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
        certificate_chain = list(
            local_parent_cert.get_descendants(include_self=True)
        ) + [certificate]
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

    def _get_transfer_session(self, ident):
        return self.session.get(self.urlresolve(api_urls.TRANSFERSESSION, lookup=ident))

    def _get_sync_session(self, ident):
        return self.session.get(self.urlresolve(api_urls.SYNCSESSION, lookup=ident))

    def _create_sync_session(self, data):
        return self.session.post(self.urlresolve(api_urls.SYNCSESSION), json=data)

    def _create_transfer_session(self, data):
        return self.session.post(self.urlresolve(api_urls.TRANSFERSESSION), json=data)

    def _update_transfer_session(self, data, transfer_session):
        return self.session.patch(
            self.urlresolve(api_urls.TRANSFERSESSION, lookup=transfer_session.id),
            json=data,
        )

    def _close_transfer_session(self, ident):
        return self.session.delete(
            self.urlresolve(api_urls.TRANSFERSESSION, lookup=ident)
        )

    def _close_sync_session(self, ident):
        return self.session.delete(self.urlresolve(api_urls.SYNCSESSION, lookup=ident))

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


class SyncClient(object):
    """
    Controller to support client in initiating syncing and performing related operations.
    """

    def __init__(self, sync_connection, sync_session, chunk_size=500):
        self.sync_connection = sync_connection
        self.sync_session = sync_session
        self.chunk_size = chunk_size
        self.current_transfer_session = None

    def _starting_transfer_session(self, sync_filter, push):
        data = None
        # transfer session may or may not exist
        resumable_ts = self.sync_session.transfersession_set.filter(
            filter=sync_filter, active=True, push=push
        ).first()
        if resumable_ts:
            try:
                ts_response = self.sync_connection._get_transfer_session(
                    resumable_ts.id
                )
            except requests.exceptions.HTTPError as e:
                # if non existent server ts, continue to create one on server
                if e.response.status_code == 404:
                    pass
                else:
                    raise
            # only gets executed if no exception was caught above
            else:
                ts_data = ts_response.json()
                # if transfer session still active, resume session
                if ts_data["active"] is True and ts_data["push"] == resumable_ts.push:
                    # grab active transfer session
                    self.current_transfer_session = resumable_ts
                    # set to server records transferred in case client did not receive response from server
                    if (
                        self.current_transfer_session.transfer_stage
                        == transfer_status.PUSHING
                    ):
                        self.current_transfer_session.records_transferred = ts_data[
                            "records_transferred"
                        ]
                        self.current_transfer_session.save()
                    # turn off any other active transfer sessions attached to this syncsession
                    # this also deletes buffer and rmcb records associated with those transfersessions
                    self.sync_session.transfersession_set.filter(active=True).exclude(
                        id=self.current_transfer_session.id
                    ).delete(soft=True)
                    # resume transfer session
                    if push:
                        logger.info(
                            "Resuming sync push from {} stage".format(
                                self.current_transfer_session.transfer_stage
                            )
                        )
                    else:  # pull
                        logger.info(
                            "Resuming sync pull from {} stage".format(
                                self.current_transfer_session.transfer_stage
                            )
                        )
                        # we need to pass data dict onto QUEUING stage which creates transfer session server side
                        data = {
                            "id": self.current_transfer_session.id,
                            "filter": self.current_transfer_session.filter,
                            "push": self.current_transfer_session.push,
                            "sync_session_id": self.current_transfer_session.sync_session.id,
                            "transfer_stage": transfer_status.QUEUING,
                            "client_fsic": self.current_transfer_session.client_fsic,
                        }
                    return data

        # execute as normal if no resumable ts or non existent ts on server
        if push:
            data = self._generate_transfer_session_data(True, sync_filter)
            data.pop("last_activity_timestamp")
            # create transfer session server side
            ts_response = self.sync_connection._create_transfer_session(data)

            # create transfer session locally
            data["server_fsic"] = ts_response.json().get("server_fsic") or "{}"
            data["last_activity_timestamp"] = timezone.now()
            data["transfer_stage"] = transfer_status.QUEUING
            self.current_transfer_session = TransferSession.objects.create(**data)
        else:  # pull
            # create transfer session locally
            data = self._generate_transfer_session_data(False, sync_filter)
            data["last_activity_timestamp"] = timezone.now()
            data["transfer_stage"] = transfer_status.QUEUING
            self.current_transfer_session = TransferSession.objects.create(**data)
            data.pop("last_activity_timestamp")

        return data

    def _queuing(self, data, push):
        if push:
            _queue_into_buffer(self.current_transfer_session)
            # update the records_total for client and server transfer session
            records_total = Buffer.objects.filter(
                transfer_session=self.current_transfer_session
            ).count()
            self.current_transfer_session.records_total = records_total
            self.current_transfer_session.transfer_stage = transfer_status.PUSHING
            self.current_transfer_session.save()
        else:
            # creating transfer session on pull also queues data server side
            try:
                response = self.sync_connection._create_transfer_session(data)
            except requests.HTTPError:
                self.current_transfer_session.transfer_stage = transfer_status.ERROR
                self.current_transfer_session.delete(soft=True)
                raise

            self.current_transfer_session.server_fsic = response.json().get(
                "server_fsic", {}
            )
            self.current_transfer_session.records_total = response.json().get(
                "records_total", 0
            )
            self.current_transfer_session.transfer_stage = transfer_status.PULLING
            self.current_transfer_session.save()

        logger.info(
            "{} records have been queued for transfer".format(
                self.current_transfer_session.records_total
            )
        )

    def _pushing(self):
        try:
            self.sync_connection._update_transfer_session(
                {"records_total": self.current_transfer_session.records_total},
                self.current_transfer_session,
            )
        except requests.HTTPError:
            self._close_transfer_session(error=True)
            raise
        # push records to server
        self._push_records()

        # upon successful completion of pushing records, proceed to delete buffered records
        Buffer.objects.filter(transfer_session=self.current_transfer_session).delete()
        RecordMaxCounterBuffer.objects.filter(
            transfer_session=self.current_transfer_session
        ).delete()
        self.current_transfer_session.transfer_stage = transfer_status.DEQUEUING
        self.current_transfer_session.save()

    def _pulling(self):
        # pull records and close transfer session upon completion
        self._pull_records()
        self.current_transfer_session.transfer_stage = transfer_status.DEQUEUING
        self.current_transfer_session.save()

    def _dequeuing(self, push):
        if push:
            # close client and server transfer session
            # closing server transfer session triggers a dequeue
            self._close_transfer_session()
        else:
            _dequeue_into_store(self.current_transfer_session)
            # update database max counters but use latest fsics on client
            DatabaseMaxCounter.update_fsics(
                json.loads(self.current_transfer_session.server_fsic),
                self.current_transfer_session.filter,
            )

            self._close_transfer_session()

    def initiate_push(self, sync_filter):
        logger.info("Initiating push sync")
        data = self._starting_transfer_session(sync_filter, push=True)

        if self.current_transfer_session.transfer_stage == transfer_status.QUEUING:
            self._queuing(data, push=True)

            if self.current_transfer_session.records_total == 0:
                logger.info("There are no records to transfer")
                self._close_transfer_session()
                return

        if self.current_transfer_session.transfer_stage == transfer_status.PUSHING:
            self._pushing()

        if self.current_transfer_session.transfer_stage == transfer_status.DEQUEUING:
            self._dequeuing(push=True)

    def initiate_pull(self, sync_filter):
        logger.info("Initiating pull sync")
        data = self._starting_transfer_session(sync_filter, push=False)

        if self.current_transfer_session.transfer_stage == transfer_status.QUEUING:
            self._queuing(data, push=False)

            if self.current_transfer_session.records_total == 0:
                logger.info("There are no records to transfer")
                self._close_transfer_session()
                return

        if self.current_transfer_session.transfer_stage == transfer_status.PULLING:
            self._pulling()

        if self.current_transfer_session.transfer_stage == transfer_status.DEQUEUING:
            self._dequeuing(push=False)

    def _pull_records(self, callback=None):
        logger.info("Beginning pulling of data...")
        while (
            self.current_transfer_session.records_transferred
            < self.current_transfer_session.records_total
        ):
            try:
                buffers_resp = self.sync_connection._pull_record_chunk(
                    self.chunk_size, self.current_transfer_session
                )
            except requests.HTTPError:
                self._close_transfer_session(error=True)
                raise

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

            validate_and_create_buffer_data(data, self.current_transfer_session)
            # update the records transferred so client and server are in agreement
            try:
                self.sync_connection._update_transfer_session(
                    {
                        "records_transferred": self.current_transfer_session.records_transferred
                    },
                    self.current_transfer_session,
                )
            except requests.HTTPError:
                self._close_transfer_session(error=True)
                raise

            logger.info(
                "Received {}/{} records".format(
                    self.current_transfer_session.records_transferred,
                    self.current_transfer_session.records_total,
                )
            )
        logger.info("Completed pull of data")

    def _push_records(self, callback=None):
        logger.info("Beginning pushing of data...")
        # paginate buffered records so we do not load them all into memory
        buffered_records = Buffer.objects.filter(
            transfer_session=self.current_transfer_session
        ).order_by("pk")

        while (
            self.current_transfer_session.records_transferred
            < self.current_transfer_session.records_total
        ):
            chunk = buffered_records[
                self.current_transfer_session.records_transferred : self.current_transfer_session.records_transferred
                + self.chunk_size
            ]

            # serialize and send records to server
            serialized_recs = BufferSerializer(chunk, many=True)
            try:
                self.sync_connection._push_record_chunk(serialized_recs.data)
            except requests.HTTPError:
                self._close_transfer_session(error=True)
                raise
            # update records_transferred upon successful request
            self.current_transfer_session.records_transferred = min(
                self.current_transfer_session.records_transferred + self.chunk_size,
                self.current_transfer_session.records_total,
            )
            self.current_transfer_session.save()
            logger.info(
                "Sent {}/{} records".format(
                    self.current_transfer_session.records_transferred,
                    self.current_transfer_session.records_total,
                )
            )
        logger.info("Completed push of data")

    def close_sync_session(self):

        logger.info("Closing sync session")

        # "delete" our own local sync session
        if self.current_transfer_session is not None:
            raise MorangoError(
                "Transfer Session must be closed before closing sync session."
            )

        self.sync_session.active = False
        self.sync_session.save()
        ident = self.sync_session.id
        self.sync_session = None
        # close adapters on requests session object
        self.sync_connection.session.close()

        # "delete" sync session on server side
        self.sync_connection._close_sync_session(ident)

    def _generate_transfer_session_data(self, push, sync_filter):
        # build data for creating transfer session on server side
        data = {
            "id": uuid.uuid4().hex,
            "filter": str(sync_filter),
            "push": push,
            "sync_session_id": self.sync_session.id,
        }

        if push:
            # before pushing, we want to serialize the most recent data and update database max counters
            if getattr(settings, "MORANGO_SERIALIZE_BEFORE_QUEUING", True):
                _serialize_into_store(self.sync_session.profile, filter=sync_filter)

        data["last_activity_timestamp"] = timezone.now()

        data["client_fsic"] = json.dumps(
            DatabaseMaxCounter.calculate_filter_max_counters(sync_filter)
        )
        return data

    def _close_transfer_session(self, error=False):

        logger.info("Closing transfer session")

        # "delete" transfer session on server side
        try:
            self.sync_connection._close_transfer_session(
                self.current_transfer_session.id
            )
        except requests.HTTPError:
            self.current_transfer_session.transfer_stage = transfer_status.ERROR
            self.current_transfer_session.delete(soft=True)
            raise

        self.current_transfer_session.transfer_stage = transfer_status.COMPLETED
        if error:
            self.current_transfer_session.transfer_stage = transfer_status.ERROR
        # "delete" our own local transfer session
        self.current_transfer_session.delete(soft=True)
        self.current_transfer_session = None
