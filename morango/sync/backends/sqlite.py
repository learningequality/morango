from .base import BaseSQLWrapper
from .utils import calculate_max_sqlite_variables
from morango.models.core import Buffer
from morango.models.core import RecordMaxCounter
from morango.models.core import RecordMaxCounterBuffer
from morango.models.core import Store


class SQLWrapper(BaseSQLWrapper):
    backend = "sqlite"

    def _bulk_insert_into_app_models(
        self, cursor, app_model, fields, db_values, placeholder_list
    ):
        """
        Example query:
        `REPLACE INTO model (F1,F2,F3) VALUES (%s, %s, %s), (%s, %s, %s), (%s, %s, %s)`
        where values=[1,2,3,4,5,6,7,8,9]
        """
        # calculate and create equal sized chunks of data to insert incrementally
        num_of_rows_able_to_insert = calculate_max_sqlite_variables() // len(fields)
        num_of_values_able_to_insert = num_of_rows_able_to_insert * len(fields)
        value_chunks = [
            db_values[x : x + num_of_values_able_to_insert]
            for x in range(0, len(db_values), num_of_values_able_to_insert)
        ]
        placeholder_chunks = [
            placeholder_list[x : x + num_of_rows_able_to_insert]
            for x in range(0, len(placeholder_list), num_of_rows_able_to_insert)
        ]
        # insert data chunks
        fields = str(tuple(str(f.attname) for f in fields)).replace("'", "")
        for values, params in zip(value_chunks, placeholder_chunks):
            placeholder_str = ", ".join(params).replace("'", "")
            insert = """REPLACE INTO {app_model} {fields}
                        VALUES {placeholder_str}
            """.format(
                app_model=app_model, fields=fields, placeholder_str=placeholder_str
            )
            # use DB-APIs parameter substitution (2nd parameter expects a sequence)
            cursor.execute(insert, values)

    def _dequeuing_merge_conflict_rmcb(self, cursor, transfersession_id):
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
                               """.format(
            buffer=Buffer._meta.db_table,
            store=Store._meta.db_table,
            rmc=RecordMaxCounter._meta.db_table,
            rmcb=RecordMaxCounterBuffer._meta.db_table,
            transfer_session_id=transfersession_id,
        )
        cursor.execute(merge_conflict_rmc)

    def _dequeuing_merge_conflict_buffer(self, cursor, current_id, transfersession_id):
        # transfer buffer serialized into conflicting store
        merge_conflict_store = """REPLACE INTO {store} (id, serialized, deleted, last_saved_instance, last_saved_counter, hard_deleted, model_name, profile, partition,
                                                        source_id, conflicting_serialized_data, dirty_bit, _self_ref_fk, deserialization_error, last_transfer_session_id)
                                            SELECT store.id, CASE buffer.hard_deleted WHEN 1 THEN '' ELSE store.serialized END, store.deleted OR buffer.deleted, '{current_instance_id}',
                                                   {current_instance_counter}, store.hard_deleted OR buffer.hard_deleted, store.model_name, store.profile, store.partition, store.source_id,
                                                   CASE buffer.hard_deleted WHEN 1 THEN '' ELSE buffer.serialized || '\n' || store.conflicting_serialized_data END, 1, store._self_ref_fk,
                                                   '', '{transfer_session_id}'
                                            FROM {buffer} AS buffer, {store} AS store
                                            /*Scope to a single record.*/
                                            WHERE store.id = buffer.model_uuid
                                            AND buffer.transfer_session_id = '{transfer_session_id}'
                                            /*Exclude fast-forwards*/
                                            AND NOT EXISTS (SELECT 1 FROM {rmcb} AS rmcb2 WHERE store.id = rmcb2.model_uuid
                                                                                          AND store.last_saved_instance = rmcb2.instance_id
                                                                                          AND store.last_saved_counter <= rmcb2.counter
                                                                                          AND rmcb2.transfer_session_id = '{transfer_session_id}')
                                      """.format(
            buffer=Buffer._meta.db_table,
            rmcb=RecordMaxCounterBuffer._meta.db_table,
            store=Store._meta.db_table,
            rmc=RecordMaxCounter._meta.db_table,
            transfer_session_id=transfersession_id,
            current_instance_id=current_id.id,
            current_instance_counter=current_id.counter,
        )
        cursor.execute(merge_conflict_store)

    def _dequeuing_update_rmcs_last_saved_by(
        self, cursor, current_id, transfersession_id
    ):
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
                                      """.format(
            buffer=Buffer._meta.db_table,
            rmcb=RecordMaxCounterBuffer._meta.db_table,
            store=Store._meta.db_table,
            rmc=RecordMaxCounter._meta.db_table,
            transfer_session_id=transfersession_id,
            current_instance_id=current_id.id,
            current_instance_counter=current_id.counter,
        )
        cursor.execute(merge_conflict_store)

    def _dequeuing_insert_remaining_buffer(self, cursor, transfersession_id):
        # insert remaining records into store
        insert_remaining_buffer = """REPLACE INTO {store} (id, serialized, deleted, last_saved_instance, last_saved_counter, hard_deleted, model_name, profile, partition,
                                                           source_id, conflicting_serialized_data, dirty_bit, _self_ref_fk, deserialization_error, last_transfer_session_id)
                                    SELECT buffer.model_uuid, buffer.serialized, buffer.deleted, buffer.last_saved_instance, buffer.last_saved_counter, buffer.hard_deleted,
                                           buffer.model_name, buffer.profile, buffer.partition, buffer.source_id, buffer.conflicting_serialized_data, 1,
                                           buffer._self_ref_fk, '', '{transfer_session_id}'
                                    FROM {buffer} AS buffer
                                    WHERE buffer.transfer_session_id = '{transfer_session_id}'
                           """.format(
            buffer=Buffer._meta.db_table,
            store=Store._meta.db_table,
            transfer_session_id=transfersession_id,
        )

        cursor.execute(insert_remaining_buffer)

    def _dequeuing_insert_remaining_rmcb(self, cursor, transfersession_id):
        # insert remaining records into rmc
        insert_remaining_rmcb = """REPLACE INTO {rmc} (instance_id, counter, store_model_id)
                                    SELECT rmcb.instance_id, rmcb.counter, rmcb.model_uuid
                                    FROM {rmcb} AS rmcb
                                    WHERE rmcb.transfer_session_id = '{transfer_session_id}'
                           """.format(
            rmc=RecordMaxCounter._meta.db_table,
            rmcb=RecordMaxCounterBuffer._meta.db_table,
            transfer_session_id=transfersession_id,
        )

        cursor.execute(insert_remaining_rmcb)
