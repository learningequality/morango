from morango.models.core import Buffer
from morango.models.core import RecordMaxCounter
from morango.models.core import RecordMaxCounterBuffer
from morango.models.core import Store


class BaseSQLWrapper(object):
    create_temporary_table_template = "CREATE TEMP TABLE {name} ({fields})"

    def __init__(self, connection):
        self.connection = connection

    def _set_transaction_repeatable_read(self):
        """Set the current transaction isolation level"""
        pass

    def _create_placeholder_list(self, fields, db_values):
        # number of rows to update
        num_of_rows = len(db_values) // len(fields)
        # create '%s' placeholders for a single row
        placeholder_tuple = tuple(["%s" for _ in range(len(fields))])
        # create list of the '%s' tuple placeholders based on number of rows to update
        return [str(placeholder_tuple) for _ in range(num_of_rows)]

    def _bulk_full_record_upsert(self, cursor, table_name, fields, db_values):
        raise NotImplementedError("Subclass must implement this method.")

    def _bulk_insert(self, cursor, table_name, fields, db_values):
        placeholder_str = ", ".join(
            self._create_placeholder_list(fields, db_values)
        ).replace("'", "")
        fields_str = str(tuple(str(f.attname) for f in fields)).replace("'", "")
        insert = """
            INSERT INTO {table_name} {fields}
            VALUES {placeholder_str}
        """.format(
            table_name=table_name, fields=fields_str, placeholder_str=placeholder_str
        )
        cursor.execute(insert, db_values)

    def _bulk_update(self, cursor, table_name, fields, db_values):
        raise NotImplementedError("Subclass must implement this method.")

    def _dequeuing_delete_rmcb_records(self, cursor, transfersession_id):
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
                                  """.format(
            buffer=Buffer._meta.db_table,
            store=Store._meta.db_table,
            rmc=RecordMaxCounter._meta.db_table,
            rmcb=RecordMaxCounterBuffer._meta.db_table,
            transfer_session_id=transfersession_id,
        )

        cursor.execute(delete_rmcb_records)

    def _dequeuing_delete_buffered_records(self, cursor, transfersession_id):
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
                                  """.format(
            buffer=Buffer._meta.db_table,
            store=Store._meta.db_table,
            rmc=RecordMaxCounter._meta.db_table,
            transfer_session_id=transfersession_id,
        )
        cursor.execute(delete_buffered_records)

    def _dequeuing_merge_conflict_rmcb(self, cursor, transfersession_id):
        raise NotImplementedError("Subclass must implement this method.")

    def _dequeuing_merge_conflict_buffer(self, cursor, current_id, transfersession_id):
        raise NotImplementedError("Subclass must implement this method.")

    def _dequeuing_update_rmcs_last_saved_by(
        self, cursor, current_id, transfersession_id
    ):
        raise NotImplementedError("Subclass must implement this method.")

    def _dequeuing_delete_mc_buffer(self, cursor, transfersession_id):
        # delete records with merge conflicts from buffer
        delete_mc_buffer = """DELETE FROM {buffer}
                                    WHERE EXISTS
                                    (SELECT 1 FROM {store} AS store, {buffer} AS buffer
                                    /*Scope to a single record.*/
                                    WHERE store.id = {buffer}.model_uuid
                                    AND {buffer}.transfer_session_id = '{transfer_session_id}'
                                    /*Exclude fast-forwards*/
                                    AND NOT EXISTS (SELECT 1 FROM {rmcb} AS rmcb WHERE store.id = rmcb.model_uuid
                                                                                  AND store.last_saved_instance = rmcb.instance_id
                                                                                  AND store.last_saved_counter <= rmcb.counter
                                                                                  AND rmcb.transfer_session_id = '{transfer_session_id}'))
                               """.format(
            buffer=Buffer._meta.db_table,
            store=Store._meta.db_table,
            rmcb=RecordMaxCounterBuffer._meta.db_table,
            transfer_session_id=transfersession_id,
        )
        cursor.execute(delete_mc_buffer)

    def _dequeuing_delete_mc_rmcb(self, cursor, transfersession_id):
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
                               """.format(
            store=Store._meta.db_table,
            rmc=RecordMaxCounter._meta.db_table,
            rmcb=RecordMaxCounterBuffer._meta.db_table,
            transfer_session_id=transfersession_id,
        )
        cursor.execute(delete_mc_rmc)

    def _dequeuing_insert_remaining_buffer(self, cursor, transfersession_id):
        raise NotImplementedError("Subclass must implement this method.")

    def _dequeuing_insert_remaining_rmcb(self, cursor, transfersession_id):
        raise NotImplementedError("Subclass must implement this method.")

    def _dequeuing_delete_remaining_rmcb(self, cursor, transfersession_id):
        # delete the remaining rmcb for this transfer session
        delete_remaining_rmcb = """
                                DELETE FROM {rmcb}
                                WHERE {rmcb}.transfer_session_id = '{transfer_session_id}'
                                """.format(
            rmcb=RecordMaxCounterBuffer._meta.db_table,
            transfer_session_id=transfersession_id,
        )

        cursor.execute(delete_remaining_rmcb)

    def _dequeuing_delete_remaining_buffer(self, cursor, transfersession_id):
        # delete the remaining buffer for this transfer session
        delete_remaining_buffer = """
                                  DELETE FROM {buffer}
                                  WHERE {buffer}.transfer_session_id = '{transfer_session_id}'
                                  """.format(
            buffer=Buffer._meta.db_table, transfer_session_id=transfersession_id
        )
        cursor.execute(delete_remaining_buffer)

    def _create_temporary_table(self, cursor, name, field_sqls, fields_params):
        """
        :param cursor: The database connection cursor
        :param name: The str name of the temp table
        :param field_sqls: A list of SQL strings representing the fields
        :param fields_params: A list of SQL parameters if necessary for the fields SQL
        """
        sql = self.create_temporary_table_template.format(
            name=name, fields=", ".join(field_sqls)
        )
        cursor.execute(sql, fields_params)

    def _lock_all_partitions(self, shared=False):
        """
        Execute a lock within the database for all partitions, if the backend supports it. The lock
        should block until acquired

        :param shared: Whether or not the lock is exclusive or shared
        """
        pass

    def _lock_partition(self, partition, shared=False):
        """
        Execute a lock within the database for a specific partition, if the database supports it.
        The lock should block until acquired

        :param partition: The partition prefix string to lock
        :param shared: Whether or not the lock is exclusive or shared
        """
        pass
