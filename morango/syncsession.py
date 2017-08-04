import uuid

from django.db import connection, transaction
from django.utils.six import iteritems
from morango.models import Buffer, RecordMaxCounter, RecordMaxCounterBuffer, Store, SyncSession


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

    def __init__(self, host, profile):
        self.host = host
        self.profile = profile
        self.session_id = uuid.uuid4()
        self.current_transfer_session = None
        # self.connection =

    def get_remote_certificates():
        pass

    def request_certificate(self, unsaved_cert, parent_cert, username=None, password=None):
        pass

    def _push_chunk(self, data):
        raise NotImplementedError


class SyncClient(object):
    """
    Controller to support client in initiating syncing and performing related operations.
    """
    def __init__(self, host, profile):
        self.host = host
        self.profile = profile
        self.transfer_session_id = uuid.uuid4().hex

    def initiate_push(self, sync_filter, chunksize):
        pass

    def initiate_pull(self, sync_filter, fsics, transfer_id):
        pass

    def close(self):
        pass

    @transaction.atomic
    def _queue_into_buffer(self, filter_prefixes, fsics):
        """
        Takes a chunk of data from the store to be put into the buffer to be sent to another morango instance.
        """
        where_statements = []
        # create condition for all incoming FSICs where instance_ids are equal, but internal counters are higher then FSICs counters
        for instance, counter in iteritems(fsics):
            where_statements += ["(last_saved_instance = '{0}' AND last_saved_counter > {1})".format(instance, counter)]
        # create condition for filtering by partitions
        for prefix in filter_prefixes:
            where_statements += ["partition LIKE '{}%'".format(prefix)]
        condition = _join_with_logical_operator(where_statements, 'OR')
        # filter by profile
        condition = _join_with_logical_operator([condition, "profile = '{}'".format(self.profile)], 'AND')

        # execute raw sql to take all records that match condition, to be put into buffer for transfer
        with connection.cursor() as cursor:
            queue_buffer = """INSERT INTO {outgoing_buffer}
                            (model_uuid, serialized, deleted, last_saved_instance, last_saved_counter,
                             model_name, profile, partition, conflicting_serialized_data, transfer_session_id)
                            SELECT id, serialized, deleted, last_saved_instance, last_saved_counter,
                            model_name, profile, partition, conflicting_serialized_data, '{transfer_session_id}'
                            FROM {store}
                            WHERE {condition}""".format(outgoing_buffer=Buffer._meta.db_table,
                                                        transfer_session_id=self.transfer_session_id,
                                                        condition=condition,
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
                                           transfer_session_id=self.transfer_session_id,
                                           record_max_counter=RecordMaxCounter._meta.db_table,
                                           outgoing_buffer=Buffer._meta.db_table)
            cursor.execute(queue_rmc_buffer)
