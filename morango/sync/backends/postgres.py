import binascii
import logging

from .base import BaseSQLWrapper
from .utils import get_pk_field
from morango.models.core import Buffer
from morango.models.core import RecordMaxCounter
from morango.models.core import RecordMaxCounterBuffer
from morango.models.core import Store
from morango.utils import SETTINGS


# advisory lock integers for locking partitions
LOCK_ALL_PARTITIONS = 1
LOCK_PARTITION = 2

SIGNED_MAX_INTEGER = 2147483647

logger = logging.getLogger(__name__)


class SQLWrapper(BaseSQLWrapper):
    backend = "postgresql"
    create_temporary_table_template = (
        "CREATE TEMP TABLE {name} ({fields}) ON COMMIT DROP"
    )

    def _transaction_has_savepoint(self):
        """
        Determine if we're in a transaction and whether savepoints have been created during it
        :return:
        """
        if not self.connection.in_atomic_block:
            return False
        for savepoint_id in self.connection.savepoint_ids:
            if savepoint_id is not None:
                return True
        return False

    def _set_transaction_repeatable_read(self):
        """Set the current transaction isolation level"""
        from psycopg2.extensions import ISOLATION_LEVEL_REPEATABLE_READ

        # setting the transaction isolation must be either done at the BEGIN statement, or before
        # any reading/writing operations have taken place, which includes creating savepoints
        if self._transaction_has_savepoint():
            # if we're running tests, we should simply ignore this warning, since the test suites
            # manage their own connections
            if not SETTINGS.MORANGO_TEST_POSTGRESQL:
                logger.warning("Unable to set transaction isolation when savepoints have been created")
        else:
            self.connection.connection.set_isolation_level(ISOLATION_LEVEL_REPEATABLE_READ)

    def _prepare_with_values(self, name, fields, db_values):
        placeholder_list = self._create_placeholder_list(fields, db_values)
        # convert this list to a string to be passed into raw sql query
        placeholder_str = ", ".join(placeholder_list).replace("'", "")
        return """
            WITH {name} {fields} as
            (
                VALUES {placeholder_str}
            )
        """.format(
            name=name,
            fields=str(tuple(str(f.column) for f in fields)).replace("'", ""),
            placeholder_str=placeholder_str,
        )

    def _prepare_casted_fields(self, fields):
        return ", ".join(
            map(
                lambda f: "{f}::{type}".format(
                    f=f.column, type=f.rel_db_type(self.connection)
                ),
                fields,
            )
        )

    def _prepare_set_casted_values(self, fields, source_table):
        return ", ".join(
            map(
                lambda f: "{f} = {src}.{f}::{type}".format(
                    f=f.attname,
                    type=f.rel_db_type(self.connection),
                    src=source_table,
                ),
                fields,
            )
        )

    def _bulk_full_record_upsert(self, cursor, table_name, fields, db_values):
        pk = get_pk_field(fields)

        cte_name = "new_values"
        upsert = """
            {cte},
            updated as
            (
                UPDATE {table_name} model
                SET {set_values}
                FROM {cte_name} cte
                WHERE model.id = cte.{pk_field}::{pk_type}
                RETURNING model.{pk_field}
            )
            INSERT INTO {table_name} {fields}
            SELECT {select_fields}
            FROM {cte_name} cte
            WHERE cte.{pk_field}::{pk_type} NOT IN (SELECT {pk_field} FROM updated)
        """.format(
            cte=self._prepare_with_values(cte_name, fields, db_values),
            cte_name=cte_name,
            table_name=table_name,
            fields=str(tuple(str(f.column) for f in fields)).replace("'", ""),
            set_values=self._prepare_set_casted_values(fields, "cte"),
            select_fields=self._prepare_casted_fields(fields),
            pk_field=pk.column,
            pk_type=pk.rel_db_type(self.connection),
        )
        # use DB-APIs parameter substitution (2nd parameter expects a sequence)
        cursor.execute(upsert, db_values)

    def _bulk_update(self, cursor, table_name, fields, db_values):
        pk = get_pk_field(fields)

        insert = """
            {cte}
            UPDATE {table_name} model
            SET {set_values}
            FROM {cte_name} cte
            WHERE model.{pk_field} = cte.{pk_field}::{pk_type}
        """

        cte_name = "new_values"
        insert = insert.format(
            cte=self._prepare_with_values(cte_name, fields, db_values),
            cte_name=cte_name,
            table_name=table_name,
            fields=str(tuple(str(f.column) for f in fields)).replace("'", ""),
            set_values=self._prepare_set_casted_values(fields, "cte"),
            pk_field=pk.column,
            pk_type=pk.rel_db_type(self.connection),
        )
        # use DB-APIs parameter substitution (2nd parameter expects a sequence)
        cursor.execute(insert, db_values)

    def _dequeuing_merge_conflict_rmcb(self, cursor, transfersession_id):
        # transfer record max counters for records with merge conflicts + perform max
        merge_conflict_rmc = """UPDATE {rmc} as rmc SET counter
                                    = rmcb.counter
                                    FROM {rmcb} AS rmcb, {store} AS store, {buffer} AS buffer
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
        merge_conflict_store = """UPDATE {store} as store SET (serialized, deleted, last_saved_instance, last_saved_counter, hard_deleted, model_name,
                                                        profile, partition, source_id, conflicting_serialized_data, dirty_bit, _self_ref_fk, deserialization_error, last_transfer_session_id)
                                            = (CASE buffer.hard_deleted WHEN TRUE THEN '' ELSE store.serialized END, store.deleted OR buffer.deleted, '{current_instance_id}',
                                                   {current_instance_counter}, store.hard_deleted, store.model_name, store.profile, store.partition, store.source_id,
                                                   CASE buffer.hard_deleted WHEN TRUE THEN '' ELSE buffer.serialized || '\n' || store.conflicting_serialized_data END, TRUE, store._self_ref_fk,
                                                   '', '{transfer_session_id}')
                                            /*Scope to a single record.*/
                                            FROM {buffer} AS buffer
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
        merge_conflict_store = """
                WITH new_values as
            (
                SELECT '{current_instance_id}'::uuid curr_id, {current_instance_counter} curr_counter, store.id
                FROM {store} as store, {buffer} as buffer
                /*Scope to a single record.*/
                WHERE store.id = buffer.model_uuid
                AND buffer.transfer_session_id = '{transfer_session_id}'
                /*Exclude fast-forwards*/
                AND NOT EXISTS (SELECT 1 FROM {rmcb} AS rmcb2 WHERE store.id = rmcb2.model_uuid
                                                              AND store.last_saved_instance = rmcb2.instance_id
                                                              AND store.last_saved_counter <= rmcb2.counter
                                                              AND rmcb2.transfer_session_id = '{transfer_session_id}')
            ),
            updated as
            (
                UPDATE {rmc} rmc
                SET counter = nv.curr_counter
                FROM new_values nv
                WHERE store_model_id = nv.id AND instance_id = nv.curr_id
                returning rmc.*
            )
            INSERT INTO {rmc}(instance_id, counter, store_model_id)
            SELECT '{current_instance_id}'::uuid, {current_instance_counter}, ut.id
            FROM new_values ut
            WHERE ut.id not in (SELECT store_model_id FROM updated)
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
        insert_remaining_buffer = """
            WITH new_values as
            (
                SELECT buffer.model_uuid, buffer.serialized, buffer.deleted, buffer.last_saved_instance, buffer.last_saved_counter, buffer.hard_deleted,
                       buffer.model_name, buffer.profile, buffer.partition, buffer.source_id, buffer.conflicting_serialized_data, buffer._self_ref_fk
                FROM {buffer} as buffer
                WHERE buffer.transfer_session_id = '{transfer_session_id}'
            ),
            updated as
            (
                UPDATE {store} store SET (serialized, deleted, last_saved_instance, last_saved_counter, hard_deleted, model_name, profile,
                                     partition, source_id, conflicting_serialized_data, dirty_bit, _self_ref_fk, deserialization_error, last_transfer_session_id)
                                    = (nv.serialized, nv.deleted, nv.last_saved_instance, nv.last_saved_counter, nv.hard_deleted,
                                       nv.model_name, nv.profile, nv.partition, nv.source_id, nv.conflicting_serialized_data, TRUE,
                                       nv._self_ref_fk, '', '{transfer_session_id}')
                FROM new_values nv
                WHERE nv.model_uuid = store.id
                returning store.*
            )
            INSERT INTO {store}(id, serialized, deleted, last_saved_instance, last_saved_counter, hard_deleted, model_name, profile,
                                partition, source_id, conflicting_serialized_data, dirty_bit, _self_ref_fk, deserialization_error, last_transfer_session_id)
            SELECT ut.model_uuid, ut.serialized, ut.deleted, ut.last_saved_instance, ut.last_saved_counter, ut.hard_deleted,
                       ut.model_name, ut.profile, ut.partition, ut.source_id, ut.conflicting_serialized_data, TRUE,
                       ut._self_ref_fk, '', '{transfer_session_id}'
            FROM new_values ut
            WHERE ut.model_uuid not in (SELECT id FROM updated)
        """.format(
            buffer=Buffer._meta.db_table,
            store=Store._meta.db_table,
            transfer_session_id=transfersession_id,
        )

        cursor.execute(insert_remaining_buffer)

    def _dequeuing_insert_remaining_rmcb(self, cursor, transfersession_id):
        # insert remaining records into rmc
        insert_remaining_rmcb = """
                WITH new_values as
            (
                SELECT rmcb.instance_id rmcb_instance_id, rmcb.counter, rmcb.model_uuid
                FROM {rmcb} as rmcb
                WHERE rmcb.transfer_session_id = '{transfer_session_id}'
            ),
            updated as
            (
                UPDATE {rmc} rmc
                SET counter = nv.counter
                FROM new_values nv
                WHERE store_model_id = nv.model_uuid AND instance_id = nv.rmcb_instance_id
                returning rmc.*
            )
            INSERT INTO {rmc}(instance_id, counter, store_model_id)
            SELECT ut.rmcb_instance_id, ut.counter, ut.model_uuid
            FROM new_values ut
            WHERE (ut.model_uuid, ut.rmcb_instance_id)
            not in (SELECT store_model_id, instance_id FROM updated)
            """.format(
            rmc=RecordMaxCounter._meta.db_table,
            rmcb=RecordMaxCounterBuffer._meta.db_table,
            transfer_session_id=transfersession_id,
        )

        cursor.execute(insert_remaining_rmcb)

    def _execute_lock(self, key1, key2=None, unlock=False, session=False, shared=False, wait=True):
        """
        Creates or destroys an advisory lock within postgres
        :param key1: An int sent to the PG lock function
        :param key2: A 2nd int sent to the PG lock function
        :param unlock: A bool representing whether query should use `unlock`
        :param session: A bool indicating if this should persist outside of transaction
        :param shared: A bool indicating if this should be shared, otherwise exclusive
        :param wait: A bool indicating if it should use a `try` PG function
        """
        if not session:
            if not self.connection.in_atomic_block:
                raise NotImplementedError("Advisory lock requires transaction")
            if unlock:
                raise NotImplementedError("Transaction level locks unlock automatically")

        keys = [key1]
        if key2 is not None:
            keys.append(key2)

        query = "SELECT pg{_try}_advisory_{xact_}{lock}{_shared}({keys}) AS lock;".format(
            _try="" if wait else "_try",
            xact_="" if session else "xact_",
            lock="unlock" if unlock else "lock",
            _shared="_shared" if shared else "",
            keys=", ".join(["%s"] * len(keys))
        )

        with self.connection.cursor() as c:
            c.execute(query, keys)

    def _lock_all_partitions(self, shared=False):
        """
        Execute a lock within the database for all partitions, if the database supports it.

        :param shared: Whether the lock is exclusive or shared
        """
        self._execute_lock(LOCK_ALL_PARTITIONS, shared=shared)

    def _lock_partition(self, partition, shared=False):
        """
        Execute a lock within the database for a specific partition, if the database supports it.

        :param partition: The partition prefix string to lock
        :param shared: Whether the lock is exclusive or shared
        """
        # first we open a shared lock on all partitions, so that we don't interfere with concurrent
        # locks on all partitions or operations that could attempt to open a lock on all partitions
        # while we've locked only some partitions
        self._lock_all_partitions(shared=True)

        # Postgres advisory locks use integers, so we have to convert the partition string into
        # an integer. To do this we use crc32, which returns an unsigned integer. When using two
        # keys for advisory locks, the two keys are signed integers, so we have to adjust the crc32
        # value so that it doesn't exceed the maximum signed integer. Turning the partition str into
        # a crc32 value could produce the same integer for different partitions, but for the
        # purposes of locking to manage concurrency, this shouldn't be an issue.
        partition_int = binascii.crc32(partition.encode("utf-8")) - SIGNED_MAX_INTEGER
        self._execute_lock(LOCK_PARTITION, key2=partition_int, shared=shared)
