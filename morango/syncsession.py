import json
import requests
import socket
import uuid

from django.db import connection, transaction
from rest_framework import status
from django.utils import timezone
from django.utils.six import iteritems
from morango.api.serializers import BufferSerializer, CertificateSerializer, InstanceIDSerializer
from morango.certificates import Certificate, Key
from morango.constants import api_urls
from morango.errors import CertificateSignatureInvalid
from morango.models import Buffer, InstanceIDModel, RecordMaxCounter, RecordMaxCounterBuffer, Store, SyncSession, TransferSession, DatabaseMaxCounter
from six.moves.urllib.parse import urljoin

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

    @transaction.atomic
    def _queue_into_buffer(self, filter_prefixes, fsics):
        """
        Takes a chunk of data from the store to be put into the buffer to be sent to another morango instance.
        """
        last_saved_by_conditions = []
        # create condition for all push FSICs where instance_ids are equal, but internal counters are higher than FSICs counters
        for instance, counter in iteritems(fsics):
            last_saved_by_conditions += ["(last_saved_instance = '{0}' AND last_saved_counter > {1})".format(instance, counter)]
        if fsics:
            last_saved_by_conditions = [_join_with_logical_operator(last_saved_by_conditions, 'OR')]

        partition_conditions = []
        # create condition for filtering by partitions
        for prefix in filter_prefixes:
            partition_conditions += ["partition LIKE '{}%'".format(prefix)]
        if filter_prefixes:
            partition_conditions = [_join_with_logical_operator(partition_conditions, 'OR')]

        # combine conditions
        fsic_and_partition_conditions = _join_with_logical_operator(last_saved_by_conditions + partition_conditions, 'AND')

        # filter by profile
        where_condition = _join_with_logical_operator([fsic_and_partition_conditions, "profile = '{}'".format(self.profile)], 'AND')

        # execute raw sql to take all records that match condition, to be put into buffer for transfer
        with connection.cursor() as cursor:
            queue_buffer = """INSERT INTO {outgoing_buffer}
                            (model_uuid, serialized, deleted, last_saved_instance, last_saved_counter,
                             model_name, profile, partition, source_id, conflicting_serialized_data, transfer_session_id, _self_ref_fk)
                            SELECT id, serialized, deleted, last_saved_instance, last_saved_counter,
                            model_name, profile, partition, source_id, conflicting_serialized_data, '{transfer_session_id}', _self_ref_fk
                            FROM {store}
                            WHERE {condition}""".format(outgoing_buffer=Buffer._meta.db_table,
                                                        transfer_session_id=self.current_transfer_session.id,
                                                        condition=where_condition,
                                                        store=Store._meta.db_table)
            cursor.execute(queue_buffer)
            # take all record max counters that are foreign keyed onto store models, which were queued into the buffer
            queue_rmc_buffer = """INSERT INTO {outgoing_rmcb}
                                (instance_id, counter, transfer_session_id, model_uuid)
                                SELECT instance_id, counter, '{transfer_session_id}', store_model_id
                                FROM {record_max_counter} AS rmc, {outgoing_buffer} AS buffer
                                WHERE EXISTS (SELECT 1
                                              FROM {outgoing_buffer}
                                              WHERE buffer.model_uuid = rmc.store_model_id)
                                              AND buffer.transfer_session_id = '{transfer_session_id}'
                                """.format(outgoing_rmcb=RecordMaxCounterBuffer._meta.db_table,
                                           transfer_session_id=self.current_transfer_session.id,
                                           record_max_counter=RecordMaxCounter._meta.db_table,
                                           outgoing_buffer=Buffer._meta.db_table)
            cursor.execute(queue_rmc_buffer)

    # START of dequeuing methods
    def _dequeuing_delete_rmcb_records(self, cursor):
        # delete all RMCBs which are a reverse FF (store version newer than buffer version)
        delete_rmcb_records = """DELETE FROM {rmcb}
                                 WHERE model_uuid IN
                                 (SELECT rmcb.model_uuid FROM {store} as store, {buffer} as buffer, {rmc} as rmc, {rmcb} as rmcb
                                 /*Scope to a single record*/
                                 WHERE store.id = buffer.model_uuid
                                 AND  store.id = rmc.store_model_id
                                 AND  store.id = rmcb.model_uuid
                                 /*Checks whether LSB of buffer or less is in RMC of store*/
                                 AND buffer.last_saved_instance = rmc.instance_id
                                 AND buffer.last_saved_counter <= rmc.counter
                                 AND rmcb.transfer_session_id = '{transfer_session_id}'
                                 AND buffer.transfer_session_id = '{transfer_session_id}')
                                  """.format(buffer=Buffer._meta.db_table,
                                             store=Store._meta.db_table,
                                             rmc=RecordMaxCounter._meta.db_table,
                                             rmcb=RecordMaxCounterBuffer._meta.db_table,
                                             transfer_session_id=self.current_transfer_session.id)

        cursor.execute(delete_rmcb_records)

    def _dequeuing_delete_buffered_records(self, cursor):
        # delete all buffer records which are a reverse FF (store version newer than buffer version)
        delete_buffered_records = """DELETE FROM {buffer}
                                     WHERE model_uuid in
                                     (SELECT buffer.model_uuid FROM {store} as store, {buffer} as buffer, {rmc} as rmc
                                     /*Scope to a single record*/
                                     WHERE store.id = buffer.model_uuid
                                     AND rmc.store_model_id = buffer.model_uuid
                                     /*Checks whether LSB of buffer or less is in RMC of store*/
                                     AND buffer.last_saved_instance = rmc.instance_id
                                     AND buffer.last_saved_counter <= rmc.counter
                                     AND buffer.transfer_session_id = '{transfer_session_id}')
                                  """.format(buffer=Buffer._meta.db_table,
                                             store=Store._meta.db_table,
                                             rmc=RecordMaxCounter._meta.db_table,
                                             rmcb=RecordMaxCounterBuffer._meta.db_table,
                                             transfer_session_id=self.current_transfer_session.id)
        cursor.execute(delete_buffered_records)

    def _dequeuing_merge_conflict_rmcb(self, cursor):
        # transfer record max counters for records with merge conflicts + perform max
        merge_conflict_rmc = """REPLACE INTO {rmc} (instance_id, counter, store_model_id)
                                    SELECT rmcb.instance_id, rmcb.counter, rmcb.model_uuid
                                    FROM {rmcb} AS rmcb, {store} AS store, {rmc} AS rmc, {buffer} AS buffer
                                    /*Scope to a single record.*/
                                    WHERE store.id = rmcb.model_uuid
                                    AND store.id = rmc.store_model_id
                                    AND store.id = buffer.model_uuid
                                    /*Where buffer rmc is greater than store rmc*/
                                    AND rmcb.instance_id = rmc.instance_id
                                    AND rmcb.counter > rmc.counter
                                    AND rmcb.transfer_session_id = '{transfer_session_id}'
                                    /*Exclude fast-forwards*/
                                    AND NOT EXISTS (SELECT 1 FROM {rmcb} AS rmcb2 WHERE store.id = rmcb2.model_uuid
                                                                                  AND store.last_saved_instance = rmcb2.instance_id
                                                                                  AND store.last_saved_counter <= rmcb2.counter
                                                                                  AND rmcb2.transfer_session_id = '{transfer_session_id}')
                               """.format(buffer=Buffer._meta.db_table,
                                          store=Store._meta.db_table,
                                          rmc=RecordMaxCounter._meta.db_table,
                                          rmcb=RecordMaxCounterBuffer._meta.db_table,
                                          transfer_session_id=self.current_transfer_session.id)
        cursor.execute(merge_conflict_rmc)

    def _dequeuing_merge_conflict_buffer(self, cursor, current_id):
        # transfer buffer serialized into conflicting store
        merge_conflict_store = """REPLACE INTO {store} (id, serialized, deleted, last_saved_instance, last_saved_counter, model_name,
                                                        profile, partition, source_id, conflicting_serialized_data, dirty_bit, _self_ref_fk)
                                            SELECT store.id, store.serialized, store.deleted OR buffer.deleted, '{current_instance_id}',
                                                   {current_instance_counter}, store.model_name, store.profile, store.partition, store.source_id,
                                                   buffer.serialized || '\n' || store.conflicting_serialized_data, 1, store._self_ref_fk
                                            FROM {buffer} AS buffer, {store} AS store
                                            /*Scope to a single record.*/
                                            WHERE store.id = buffer.model_uuid
                                            AND buffer.transfer_session_id = '{transfer_session_id}'
                                            /*Exclude fast-forwards*/
                                            AND NOT EXISTS (SELECT 1 FROM {rmcb} AS rmcb2 WHERE store.id = rmcb2.model_uuid
                                                                                          AND store.last_saved_instance = rmcb2.instance_id
                                                                                          AND store.last_saved_counter <= rmcb2.counter
                                                                                          AND rmcb2.transfer_session_id = '{transfer_session_id}')
                                      """.format(buffer=Buffer._meta.db_table,
                                                 rmcb=RecordMaxCounterBuffer._meta.db_table,
                                                 store=Store._meta.db_table,
                                                 rmc=RecordMaxCounter._meta.db_table,
                                                 transfer_session_id=self.current_transfer_session.id,
                                                 current_instance_id=current_id.id,
                                                 current_instance_counter=current_id.counter)
        cursor.execute(merge_conflict_store)

    def _dequeuing_update_rmcs_last_saved_by(self, cursor, current_id):
        # update or create rmc for merge conflicts with local instance id
        merge_conflict_store = """REPLACE INTO {rmc} (instance_id, counter, store_model_id)
                                SELECT '{current_instance_id}', {current_instance_counter}, store.id
                                FROM {store} as store, {buffer} as buffer
                                /*Scope to a single record.*/
                                WHERE store.id = buffer.model_uuid
                                AND buffer.transfer_session_id = '{transfer_session_id}'
                                /*Exclude fast-forwards*/
                                AND NOT EXISTS (SELECT 1 FROM {rmcb} AS rmcb2 WHERE store.id = rmcb2.model_uuid
                                                                              AND store.last_saved_instance = rmcb2.instance_id
                                                                              AND store.last_saved_counter <= rmcb2.counter
                                                                              AND rmcb2.transfer_session_id = '{transfer_session_id}')
                                      """.format(buffer=Buffer._meta.db_table,
                                                 rmcb=RecordMaxCounterBuffer._meta.db_table,
                                                 store=Store._meta.db_table,
                                                 rmc=RecordMaxCounter._meta.db_table,
                                                 transfer_session_id=self.current_transfer_session.id,
                                                 current_instance_id=current_id.id,
                                                 current_instance_counter=current_id.counter)
        cursor.execute(merge_conflict_store)

    def _dequeuing_delete_mc_buffer(self, cursor):
        # delete records with merge conflicts from buffer
        delete_mc_buffer = """DELETE FROM {buffer}
                                    WHERE EXISTS
                                    (SELECT 1 FROM {store} AS store, {buffer} AS buffer
                                    /*Scope to a single record.*/
                                    WHERE store.id = {buffer}.model_uuid
                                    AND {buffer}.transfer_session_id = '{transfer_session_id}'
                                    /*Exclude fast-forwards*/
                                    AND NOT EXISTS (SELECT 1 FROM {rmcb} AS rmcb2 WHERE store.id = rmcb2.model_uuid
                                                                                  AND store.last_saved_instance = rmcb2.instance_id
                                                                                  AND store.last_saved_counter <= rmcb2.counter
                                                                                  AND rmcb2.transfer_session_id = '{transfer_session_id}'))
                               """.format(buffer=Buffer._meta.db_table,
                                          store=Store._meta.db_table,
                                          rmc=RecordMaxCounter._meta.db_table,
                                          rmcb=RecordMaxCounterBuffer._meta.db_table,
                                          transfer_session_id=self.current_transfer_session.id)
        cursor.execute(delete_mc_buffer)

    def _dequeuing_delete_mc_rmcb(self, cursor):
        # delete rmcb records with merge conflicts
        delete_mc_rmc = """DELETE FROM {rmcb}
                                    WHERE EXISTS
                                    (SELECT 1 FROM {store} AS store, {rmc} AS rmc
                                    /*Scope to a single record.*/
                                    WHERE store.id = {rmcb}.model_uuid
                                    AND store.id = rmc.store_model_id
                                    /*Where buffer rmc is greater than store rmc*/
                                    AND {rmcb}.instance_id = rmc.instance_id
                                    AND {rmcb}.transfer_session_id = '{transfer_session_id}'
                                    /*Exclude fast fast-forwards*/
                                    AND NOT EXISTS (SELECT 1 FROM {rmcb} AS rmcb2 WHERE store.id = rmcb2.model_uuid
                                                                                  AND store.last_saved_instance = rmcb2.instance_id
                                                                                  AND store.last_saved_counter <= rmcb2.counter
                                                                                  AND rmcb2.transfer_session_id = '{transfer_session_id}'))
                               """.format(buffer=Buffer._meta.db_table,
                                          store=Store._meta.db_table,
                                          rmc=RecordMaxCounter._meta.db_table,
                                          rmcb=RecordMaxCounterBuffer._meta.db_table,
                                          transfer_session_id=self.current_transfer_session.id)
        cursor.execute(delete_mc_rmc)

    def _dequeuing_insert_remaining_buffer(self, cursor):
        # insert remaining records into store
        insert_remaining_buffer = """REPLACE INTO {store} (id, serialized, deleted, last_saved_instance, last_saved_counter,
                                                               model_name, profile, partition, source_id, conflicting_serialized_data, dirty_bit, _self_ref_fk)
                                    SELECT buffer.model_uuid, buffer.serialized, buffer.deleted, buffer.last_saved_instance, buffer.last_saved_counter,
                                           buffer.model_name, buffer.profile, buffer.partition, buffer.source_id, buffer.conflicting_serialized_data, 1,
                                           buffer._self_ref_fk
                                    FROM {buffer} AS buffer
                                    WHERE buffer.transfer_session_id = '{transfer_session_id}'
                           """.format(buffer=Buffer._meta.db_table,
                                      store=Store._meta.db_table,
                                      transfer_session_id=self.current_transfer_session.id)

        cursor.execute(insert_remaining_buffer)

    def _dequeuing_insert_remaining_rmcb(self, cursor):
        # insert remaining records into rmc
        insert_remaining_rmcb = """REPLACE INTO {rmc} (instance_id, counter, store_model_id)
                                    SELECT rmcb.instance_id, rmcb.counter, rmcb.model_uuid
                                    FROM {rmcb} AS rmcb
                                    WHERE rmcb.transfer_session_id = '{transfer_session_id}'
                           """.format(rmc=RecordMaxCounter._meta.db_table,
                                      rmcb=RecordMaxCounterBuffer._meta.db_table,
                                      transfer_session_id=self.current_transfer_session.id)

        cursor.execute(insert_remaining_rmcb)

    def _dequeuing_delete_remaining_rmcb(self, cursor):
        # delete the rest for this transfer session
        delete_remaining_rmcb = """DELETE FROM {rmcb}
                                  WHERE {rmcb}.transfer_session_id = '{transfer_session_id}'
                               """.format(rmcb=RecordMaxCounterBuffer._meta.db_table,
                                          transfer_session_id=self.current_transfer_session.id)

        cursor.execute(delete_remaining_rmcb)

    def _dequeuing_delete_remaining_buffer(self, cursor):
        delete_remaining_buffer = """DELETE FROM {buffer}
                                 WHERE {buffer}.transfer_session_id = '{transfer_session_id}'
                              """.format(buffer=Buffer._meta.db_table,
                                         transfer_session_id=self.current_transfer_session.id)

        cursor.execute(delete_remaining_buffer)

    @transaction.atomic
    def _dequeue_into_store(self):
        """
        Takes data from the buffers and merges into the store and record max counters.
        """
        with connection.cursor() as cursor:
            self._dequeuing_delete_rmcb_records(cursor)
            self._dequeuing_delete_buffered_records(cursor)
            current_id = InstanceIDModel.get_current_instance_and_increment_counter()
            self._dequeuing_merge_conflict_buffer(cursor, current_id)
            self._dequeuing_merge_conflict_rmcb(cursor)
            self._dequeuing_update_rmcs_last_saved_by(cursor, current_id)
            self._dequeuing_delete_mc_rmcb(cursor)
            self._dequeuing_delete_mc_buffer(cursor)
            self._dequeuing_insert_remaining_buffer(cursor)
            self._dequeuing_insert_remaining_rmcb(cursor)
            self._dequeuing_delete_remaining_rmcb(cursor)
            self._dequeuing_delete_remaining_buffer(cursor)
