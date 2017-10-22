import json
import requests
import socket
import uuid

from django.utils import timezone
from django.utils.six import iteritems
from morango.api.serializers import BufferSerializer, CertificateSerializer, InstanceIDSerializer
from morango.certificates import Certificate, Key
from morango.constants import api_urls
from morango.errors import CertificateSignatureInvalid
from morango.models import Buffer, InstanceIDModel, RecordMaxCounterBuffer, SyncSession, TransferSession, DatabaseMaxCounter
from morango.utils.sync_utils import _queue_into_buffer, _dequeue_into_store
from six.moves.urllib.parse import urljoin, urlparse

from django.core.paginator import Paginator


def _join_with_logical_operator(lst, operator):
    op = ") {operator} (".format(operator=operator)
    return "(({items}))".format(items=op.join(lst))


class Connection(object):
    """
    Abstraction around a connection with a syncing peer (network or disk),
    supporting interactions with that peer. This may be used by a SyncClient,
    but also supports other operations (e.g. querying certificates) outside
    the context of syncing.

    This class should be subclassed for particular transport mechanisms,
    and the necessary methods overridden.
    """

    def __init__(self, profile):
        self.profile = profile


class NetworkSyncConnection(Connection):

    def __init__(self, base_url='', profile=''):
        self.base_url = base_url
        super(NetworkSyncConnection, self).__init__(profile)

    def _request(self, endpoint, method="GET", lookup=None, data={}, params={}, userargs=None, password=None):
        """
        Generic request method designed to handle any morango endpoint.

        :param endpoint: constant representing which morango endpoint we are querying
        :param method: HTTP verb/method for request
        :param lookup: the pk value for the specific object we are querying
        :param data: dict that will be form-encoded in request
        :param params: dict to be sent as part of URL's query string
        :param userargs: Authorization credentials
        :param password:
        :return: ``Response`` object from request
        """
        # convert user arguments into query str for passing to auth layer
        if isinstance(userargs, dict):
            userargs = "&".join(["{}={}".format(key, val) for (key, val) in iteritems(userargs)])

        # build up url and send request
        url = urljoin(urljoin(self.base_url, endpoint), lookup)
        resp = requests.request(method, url, data=data, params=params, auth=(userargs, password))
        resp.raise_for_status()
        return resp

    def create_sync_session(self, client_cert, server_cert):
        # if server cert does not exist locally, retrieve it from server
        if not Certificate.objects.filter(id=server_cert.id).exists():
            self._get_certificate_chain(server_cert)

        # request the server for a one-time-use nonce
        nonce_resp = self._request(api_urls.NONCE, method="POST")
        nonce = json.loads(nonce_resp.content.decode())["id"]

        # prepare the data to send in the syncsession creation request
        data = {
            "id": uuid.uuid4().hex,
            "server_certificate_id": server_cert.id,
            "client_certificate_id": client_cert.id,
            "profile": client_cert.profile,
            "certificate_chain": json.dumps(CertificateSerializer(client_cert.get_ancestors(include_self=True), many=True).data),
            "connection_path": self.base_url,
            "instance": json.dumps(InstanceIDSerializer(InstanceIDModel.get_or_create_current_instance()[0]).data),
            "nonce": nonce,
        }

        # sign the nonce/ID combo to attach to the request
        message = "{nonce}:{id}".format(**data)
        data["signature"] = client_cert.sign(message)

        # Sync Session creation request
        session_resp = self._request(api_urls.SYNCSESSION, method="POST", data=data)

        # check that the nonce/id were properly signed by the server cert
        if not server_cert.verify(message, session_resp.data.pop("signature")):
            raise CertificateSignatureInvalid()

        # build the data to be used for creating our own syncsession
        data = {
            "id": data['id'],
            "start_timestamp": timezone.now(),
            "last_activity_timestamp": timezone.now(),
            "active": True,
            "is_server": False,
            "local_certificate": client_cert,
            "remote_certificate": server_cert,
            "profile": client_cert.profile,
            "connection_kind": "network",
            "connection_path": self.base_url,
            "local_ip": socket.gethostbyname(socket.gethostname()),
            "remote_ip": socket.gethostbyname(self.base_url),
            "local_instance": json.dumps(InstanceIDSerializer(InstanceIDModel.get_or_create_current_instance()[0]).data),
            "remote_instance": session_resp.data.get("instance") or "{}",
        }
        sync_session = SyncSession.objects.create(**data)

        return SyncClient(self, sync_session, profile=self.profile)

    def get_remote_certificates(self, primary_partition):
        remote_certs = []
        # request certs for this primary partition, where the server also has a private key for
        remote_certs_resp = self._request(api_urls.CERTIFICATE, params={'primary_partition': primary_partition})

        # inflate remote certs into a list of unsaved models
        for cert in json.loads(remote_certs_resp.data):
            remote_certs.append(Certificate.deserialize(cert["serialized"], cert["signature"]))
        return remote_certs

    def certificate_signing_request(self, parent_cert, scope_definition_id, scope_params, userargs=None, password=None):
        csr_key = Key()
        # build up data for csr
        data = {
            "parent": parent_cert.id,
            "profile": self.profile,
            "scope_definition": scope_definition_id,
            "scope_version": parent_cert.scope_version,
            "scope_params": scope_params,
            "public_key": csr_key.get_public_key_string()
        }
        csr_resp = self._request(api_urls.CERTIFICATE, method="POST", data=data, userargs=userargs, password=password)
        csr_data = json.loads(csr_resp.data)

        # verify cert returned from server, and proceed to save into our records
        csr_cert = Certificate.deserialize(csr_data["serialized"], csr_data["signature"])
        csr_cert.private_key = csr_key
        csr_cert.check_certificate()
        csr_cert.save()
        return csr_cert

    def _get_certificate_chain(self, server_cert):
        # get ancestors certificate chain for this server cert
        cert_chain_resp = self._request(api_urls.CERTIFICATE, params={'ancestors_of': server_cert.id})

        # upon receiving cert chain from server, we attempt to save the chain into our records
        Certificate.save_certificate_chain(cert_chain_resp.data, expected_last_id=server_cert.id)

    def _create_transfer_session(self, data):
        # create transfer session on server
        return self._request(api_urls.TRANSFERSESSION, method="POST", data=data)

    def _update_transfer_session(self, data, transfer_session):
        # update transfer session on server side with kwargs
        return self._request(api_urls.TRANSFERSESSION, method="PATCH", lookup=transfer_session.id, data=data)

    def _close_transfer_session(self, transfer_session):
        # "delete" transfer session on server side
        return self._request(api_urls.TRANSFERSESSION, method="DELETE", lookup=transfer_session.id)

    def _close_sync_session(self, sync_session):
        # "delete" sync session on server side
        return self._request(api_urls.SYNCSESSION, method="DELETE", lookup=sync_session.id)


class SyncClient(object):
    """
    Controller to support client in initiating syncing and performing related operations.
    """
    def __init__(self, sync_connection, sync_session, profile=''):
        self.sync_connection = sync_connection
        self.sync_session = sync_session
        self.current_transfer_session = None
        self.profile = profile

    def close_sync_session(self):

        # "delete" sync session on server side
        self.sync_connection._close_sync_session(self.sync_session)

        # "delete" our own local sync session
        self.sync_connection.sync_session.active = False
        self.sync_connection.sync_session.save()
        self.sync_connection.sync_session = None
        self = None

    def _create_transfer_session(self, push, filter):

        # build data for creating transfer session on server side
        data = {
            'id': uuid.uuid4().hex,
            'filter': filter,
            'push': push,
            'sync_session_id': self.sync_session.id,
            'local_fsic': DatabaseMaxCounter.calculate_filter_max_counters(filter)
        }
        data['start_timestamp'] = timezone.now()
        data['last_activity_timestamp'] = timezone.now()
        self.current_transfer_session = TransferSession.objects.create(**data)

        # create transfer session on server side
        transfer_resp = self.sync_connection._create_transfer_session(data)

        self.current_transfer_session.remote_fsic = transfer_resp.data.get('local_fsic')
        if not push:
            self.current_transfer_session.records_total = transfer_resp.data.get('records_total')
        self.current_transfer_session.save()

    def _close_transfer_session(self):

        # "delete" transfer session on server side
        self.sync_connection._close_transfer_session(self.current_transfer_session)

        # delete local buffered objects if pushing records
        if self.current_transfer_session.push:
            Buffer.objects.filter(transfer_session=self.current_transfer_session).delete()
            RecordMaxCounterBuffer.objects.filter(transfer_session=self.current_transfer_session).delete()

        # "delete" our own local transfer session
        self.current_transfer_session.active = False
        self.current_transfer_session.save()
        self.current_transfer_session = None

    def _queue_into_buffer(self):
        _queue_into_buffer(self.current_transfer_session)

    def _dequeue_into_store(self):
        """
        Takes data from the buffers and merges into the store and record max counters.
        """
        _dequeue_into_store(self.current_transfer_session)
