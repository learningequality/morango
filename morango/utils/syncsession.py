import uuid

from django.db import connection, transaction
from django.forms.models import model_to_dict
from django.utils.six import iteritems
from morango.models import DataTransferBuffer, RecordMaxCounter, RecordMaxCounterBuffer, Store, SyncSession


def _join_with_logical_operator(lst, operator):
    op = ") {operator} (".format(operator=operator)
    return "(({items}))".format(items=op.join(lst))


class SyncConnection(object):

    def get_remote_certifcates():
        pass

    def get_local_certificates():
        pass

    def start_sync(local_certificate, remote_certificate=None):
        pass

    def request_certificate(username, password):
        pass

    def provide_certificate(role):
        pass


class SyncController(object):

    def __init__(self, host, profile):
        self.host = host
        self.profile = profile
        self.session_id = uuid.uuid4()
        self.transfer_session_id = None
        self.symmetric_key = 'xxx'
        self.sync_session = SyncSession(host=host, id=self.session_id, symmetric_key=self.symmetric_key)

    def initiate_push_request(self, sync_filter, chunksize):
        pass

    def initiate_pull_request(self, sync_filter, fsics, transfer_id):
        pass

    def close(self):
        pass

    @transaction.atomic
    def _queue_into_buffer(self, fsics):
        """
        Takes a chunk of data from the store to be put into the buffer to be sent to another morango instance.
        """
        where_statements = []
        # create condition for all incoming FSICs where instance_ids are equal, but internal counters are higher then FSICs counters
        for instance, counter in iteritems(fsics):
            where_statements += ['(last_saved_instance == {0} AND last_saved_counter > {1})'.format(instance, counter)]
        condition = _join_with_logical_operator(where_statements, 'OR')
        # execute raw sql to take all records that match condition, to be put into buffer for transfer
        with connection.cursor() as cursor:
            queue_buffer = '''INSERT INTO {outgoing}
                            (serialized, deleted, last_saved_instance, last_saved_counter,
                             model_name, profile, partitions, transfer_session_id, incoming_buffer)
                            SELECT serialized, deleted, last_saved_instance, last_saved_counter,
                            model_name, profile, partitions, {transfer_session_id}, {incoming_buffer}
                            FROM {store}
                            [WHERE {condition}]'''.format(outgoing=DataTransferBuffer._meta.db_table,
                                                          transfer_session_id=self.transfer_session_id,
                                                          condition=condition,
                                                          store=Store._meta.db_table,
                                                          incoming_buffer=False)
            cursor.execute(queue_buffer)
            # take all record max counters that are foreign keyed onto store models, which were queued into the buffer
            queue_rmc_buffer = '''INSERT INTO {outgoing_rmc}
                                (instance_id, counter, incoming_buffer, transfer_session_id, store_model_id)
                                SELECT instance_id, counter, {incoming_buffer}, {transfer_session_id}, store_model_id
                                FROM {record_max_counter} as rmc
                                WHERE EXISTS (SELECT *
                                              FROM {outgoing_buffer} as buffer
                                              WHERE buffer.store_model_id == rmc.store_model_id)
                                '''.format(outgoing_rmc=RecordMaxCounterBuffer._meta.db_table,
                                           incoming_buffer=False,
                                           transfer_session_id=self.transfer_session_id,
                                           record_max_counter=RecordMaxCounter._meta.db_table,
                                           outgoing_buffer=DataTransferBuffer._meta.db_table)
            cursor.execute(queue_rmc_buffer)

    @transaction.atomic
    def _integrate_into_store(self, session):
        """
        Takes data from the buffers and merges into the store and record max counters.
        """
        incoming_data = DataTransferBuffer.objects.filter(transfer_session_id=self.transfer_session_id)
        for incoming_row in incoming_data:
            incoming_dict = model_to_dict(incoming_row)

            # remove buffer related attributes
            del incoming_dict['transfer_session_id']

            # attempt to integrate data based on record max counters
            try:
                if 'forward_ff':  # update with incoming row
                    pass
                elif 'merge_conflict':  # in this case, we just overwrite with one of the incoming records
                    pass
                else:  # do nothing (can not update)
                    pass
            except self._StoreModel.DoesNotExist:
                self._StoreModel.objects.create(**incoming_dict)
