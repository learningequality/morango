import json
import uuid

import mock
from django.core.exceptions import ValidationError
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection
from django.db.utils import IntegrityError
from django.test import SimpleTestCase
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from facility_profile.models import ConditionalLog
from facility_profile.models import Facility
from facility_profile.models import MyUser
from facility_profile.models import SummaryLog

from morango.errors import MorangoDirtyParent
from morango.errors import MorangoMissingParent
from morango.models.certificates import Filter
from morango.models.core import DeletedModels
from morango.models.core import HardDeletedModels
from morango.models.core import Store
from morango.models.core import SyncableModel
from morango.sync.operations import _deserialize_from_store
from morango.sync.stream.core import Sink
from morango.sync.stream.deserialize import AppModelDeserialize
from morango.sync.stream.deserialize import AppModelValidate
from morango.sync.stream.deserialize import DeserializeOutcome
from morango.sync.stream.deserialize import DeserializeTask
from morango.sync.stream.deserialize import StoreModelSource
from morango.utils import exception_path

from ...helpers import StoreFactory


class DeserializeTaskTestCase(SimpleTestCase):
    def setUp(self):
        self.store = mock.Mock(spec_set=Store)
        self.store.profile = "test"
        self.store.model_name = "testmodel"
        self.store.deleted = False
        self.task = DeserializeTask(self.store, {})

    @mock.patch("morango.sync.stream.deserialize.syncable_models.get_model")
    def test_model(self, mock_get_model):
        model = mock.Mock(spec_set=SyncableModel)
        mock_get_model.return_value = model

        self.assertEqual(self.task.model, model)
        mock_get_model.assert_called_once_with("test", "testmodel")

    def test_has_errors(self):
        self.assertFalse(self.task.has_errors)
        self.task.add_error(ValueError("bad data"))
        self.assertTrue(self.task.has_errors)

    def test_set_app_model(self):
        app_model = mock.Mock(spec_set=SyncableModel)
        self.task.set_app_model(app_model)
        self.assertEqual(self.task.app_model, app_model)

    @mock.patch("morango.sync.stream.deserialize.syncable_models.get_self_referential_fk")
    @mock.patch("morango.sync.stream.deserialize.syncable_models.get_model")
    def test_self_referential_fk(self, mock_get_model, mock_get_self_referential_fk):
        model = mock.Mock(spec_set=SyncableModel)
        mock_get_model.return_value = model
        mock_get_self_referential_fk.return_value = "parent_id"

        self.assertEqual(self.task.self_referential_fk(), "parent_id")
        mock_get_self_referential_fk.assert_called_once_with(model)

    def test_outcome__save(self):
        self.task.set_app_model(mock.Mock(spec_set=SyncableModel))
        self.assertEqual(self.task.outcome, DeserializeOutcome.SAVE)

    def test_outcome__delete(self):
        self.store.deleted = True
        self.assertEqual(self.task.outcome, DeserializeOutcome.DELETE)

    def test_outcome__propagate_delete(self):
        self.task.set_propagated_deletion(hard=False)
        self.assertEqual(self.task.outcome, DeserializeOutcome.PROPAGATE_DELETE)

    def test_outcome__propagate_hard_delete(self):
        self.task.set_propagated_deletion(hard=True)
        self.assertEqual(self.task.outcome, DeserializeOutcome.PROPAGATE_HARD_DELETE)

    def test_outcome__error(self):
        self.task.add_error(ValueError("bad data"))
        self.assertEqual(self.task.outcome, DeserializeOutcome.ERROR)

    def test_outcome__error_precedes_delete(self):
        self.store.deleted = True
        self.task.add_error(ValueError("bad data"))
        self.assertEqual(self.task.outcome, DeserializeOutcome.ERROR)

    def test_outcome__error_precedes_propagated_deletion(self):
        self.task.set_propagated_deletion(hard=True)
        self.task.add_error(ValueError("bad data"))
        self.assertEqual(self.task.outcome, DeserializeOutcome.ERROR)

    def test_outcome__delete_precedes_propagated_deletion(self):
        self.store.deleted = True
        self.task.set_propagated_deletion(hard=True)
        self.assertEqual(self.task.outcome, DeserializeOutcome.DELETE)

    def test_outcome__propagated_deletion_precedes_save(self):
        self.task.set_app_model(mock.Mock(spec_set=SyncableModel))
        self.task.set_propagated_deletion(hard=False)
        self.assertEqual(self.task.outcome, DeserializeOutcome.PROPAGATE_DELETE)

    def test_outcome__unresolved(self):
        with self.assertRaises(AssertionError):
            _ = self.task.outcome


def _facility_store(facility_id=None, parent_id=None, **kwargs):
    """An unsaved `Store` record holding a serialized `Facility`"""
    facility_id = facility_id or uuid.uuid4().hex
    facility = Facility(id=facility_id, name="Facility {}".format(facility_id), parent_id=parent_id)
    store_kwargs = dict(
        id=facility_id,
        profile="facilitydata",
        model_name="facility",
        partition=facility_id,
        source_id=facility.name,
        serialized=DjangoJSONEncoder().encode(facility.serialize()),
        last_saved_instance=uuid.uuid4().hex,
        last_saved_counter=1,
    )
    store_kwargs.update(kwargs)
    return Store(**store_kwargs)


class AppModelDeserializeTestCase(SimpleTestCase):
    def setUp(self):
        self.store = _facility_store()
        self.task = DeserializeTask(self.store, {})

    def test_transform__deserializes_app_model(self):
        task = AppModelDeserialize().transform(self.task)

        self.assertIs(task, self.task)
        self.assertIsInstance(task.app_model, Facility)
        self.assertEqual(task.app_model.id, self.store.id)
        self.assertEqual(task.outcome, DeserializeOutcome.SAVE)

    def test_transform__sets_morango_fields_from_store(self):
        task = AppModelDeserialize().transform(self.task)

        self.assertEqual(task.app_model._morango_source_id, self.store.source_id)
        self.assertEqual(task.app_model._morango_partition, self.store.partition)
        self.assertFalse(task.app_model._morango_dirty_bit)

    def test_transform__passes_sync_filter(self):
        sync_filter = Filter(self.store.partition)
        with mock.patch.object(Facility, "deserialize", wraps=Facility.deserialize) as deserialize:
            AppModelDeserialize(sync_filter=sync_filter).transform(self.task)

        deserialize.assert_called_once_with(mock.ANY, sync_filter=sync_filter)

    def test_transform__deleted_store(self):
        """Deleting the app model is the sink's job, so the deleted record passes through as-is"""
        self.store.deleted = True
        with mock.patch.object(Facility, "deserialize") as deserialize:
            task = AppModelDeserialize().transform(self.task)

        deserialize.assert_not_called()
        self.assertIsNone(task.app_model)
        self.assertEqual(task.outcome, DeserializeOutcome.DELETE)

    def test_transform__invalid_json(self):
        self.store.serialized = "{bad"
        task = AppModelDeserialize().transform(self.task)

        self.assertIsNone(task.app_model)
        self.assertIsInstance(task.errors[0], ValueError)
        self.assertEqual(task.outcome, DeserializeOutcome.ERROR)

    def test_transform__validation_error(self):
        error = ValidationError("bad value")
        with mock.patch.object(Facility, "deserialize", side_effect=error):
            task = AppModelDeserialize().transform(self.task)

        self.assertIsNone(task.app_model)
        self.assertEqual(task.errors, [error])

    def test_transform__unexpected_error_propagates(self):
        """Only per-record data errors are recorded; anything else is a defect and must surface"""
        with mock.patch.object(Facility, "deserialize", side_effect=RuntimeError("defect")):
            with self.assertRaises(RuntimeError):
                AppModelDeserialize().transform(self.task)


def _validate_task(app_model, fk_cache=None, **store_kwargs):
    """A task carrying an already deserialized `app_model`, as `AppModelDeserialize` leaves it"""
    store = Store(
        id=app_model.id,
        profile=app_model.morango_profile,
        model_name=app_model.morango_model_name,
        **store_kwargs,
    )
    task = DeserializeTask(store, fk_cache if fk_cache is not None else {})
    task.set_app_model(app_model)
    return task


class AppModelValidateTestCase(SimpleTestCase):
    """
    Covers the in-run checks that run ahead of model validation. Model validation itself is
    patched out, so these never reach the database.
    """

    def setUp(self):
        self.validate = AppModelValidate()
        self.parent_id = uuid.uuid4().hex
        self.user_id = uuid.uuid4().hex

        patcher = mock.patch.object(SyncableModel, "cached_clean_fields")
        self.cached_clean_fields = patcher.start()
        self.addCleanup(patcher.stop)

    def _facility_task(self, parent_id=None):
        return _validate_task(Facility(id=uuid.uuid4().hex, parent_id=parent_id))

    def _summary_log_task(self):
        return _validate_task(SummaryLog(id=uuid.uuid4().hex, user_id=self.user_id))

    def test_transform__valid(self):
        task = self._facility_task(parent_id=self.parent_id)

        self.assertIs(self.validate.transform(task), task)
        self.assertEqual(task.outcome, DeserializeOutcome.SAVE)
        self.assertIn(task.id, self.validate.accepted_ids)

    def test_transform__validates_with_fk_cache_and_sync_filter(self):
        sync_filter = Filter("abc")
        task = self._facility_task(parent_id=self.parent_id)

        AppModelValidate(sync_filter=sync_filter).transform(task)

        self.cached_clean_fields.assert_called_once_with(task.fk_cache, sync_filter=sync_filter)

    def test_transform__errored_task_recorded_as_failed(self):
        task = self._facility_task()
        task.add_error(ValueError("bad data"))

        self.validate.transform(task)

        self.cached_clean_fields.assert_not_called()
        self.assertIn(task.id, self.validate.failed_ids)
        self.assertEqual(task.outcome, DeserializeOutcome.ERROR)

    def test_transform__deleted_task_recorded_as_deleted(self):
        task = _validate_task(Facility(id=uuid.uuid4().hex), deleted=True)

        self.validate.transform(task)

        self.cached_clean_fields.assert_not_called()
        self.assertEqual(self.validate.deleted_ids, {task.id: False})
        self.assertEqual(task.outcome, DeserializeOutcome.DELETE)

    def test_transform__hard_deleted_task_recorded_as_hard_deleted(self):
        task = _validate_task(Facility(id=uuid.uuid4().hex), deleted=True, hard_deleted=True)

        self.validate.transform(task)

        self.assertEqual(self.validate.deleted_ids, {task.id: True})

    def test_transform__target_deleted_in_run(self):
        """
        The sink may not have deleted the target's app row yet, so validation could wrongly pass
        """
        self.validate.deleted_ids[self.user_id] = False
        task = self._summary_log_task()

        self.validate.transform(task)

        self.cached_clean_fields.assert_not_called()
        self.assertEqual(task.outcome, DeserializeOutcome.PROPAGATE_DELETE)
        self.assertEqual(self.validate.deleted_ids[task.id], False)

    def test_transform__target_hard_deleted_in_run(self):
        self.validate.deleted_ids[self.user_id] = True
        task = self._summary_log_task()

        self.validate.transform(task)

        self.assertEqual(task.outcome, DeserializeOutcome.PROPAGATE_HARD_DELETE)
        self.assertEqual(self.validate.deleted_ids[task.id], True)

    def test_transform__parent_deleted_in_run(self):
        """Models with a self-referential FK fail instead, as legacy does once the row is gone"""
        self.validate.deleted_ids[self.parent_id] = True
        task = self._facility_task(parent_id=self.parent_id)

        self.validate.transform(task)

        self.cached_clean_fields.assert_not_called()
        self.assertIsInstance(task.errors[0], ValidationError)
        self.assertIn(self.parent_id, str(task.errors[0]))
        self.assertIn(task.id, self.validate.failed_ids)
        self.assertNotIn(task.id, self.validate.deleted_ids)

    def test_transform__parent_deleted_in_run__grandchildren_fail(self):
        self.validate.deleted_ids[self.parent_id] = False
        child = self._facility_task(parent_id=self.parent_id)
        grandchild = self._facility_task(parent_id=child.id)

        self.validate.transform(child)
        self.validate.transform(grandchild)

        self.assertIsInstance(grandchild.errors[0], MorangoDirtyParent)

    def test_transform__parent_failed_in_run(self):
        self.validate.failed_ids.add(self.parent_id)
        task = self._facility_task(parent_id=self.parent_id)

        self.validate.transform(task)

        self.cached_clean_fields.assert_not_called()
        self.assertIsInstance(task.errors[0], MorangoDirtyParent)
        self.assertEqual(str(task.errors[0]), "Parent is dirty; could not deserialize.")
        self.assertIn(task.id, self.validate.failed_ids)

    def test_transform__failure_propagates_to_grandchildren(self):
        self.validate.failed_ids.add(self.parent_id)
        child = self._facility_task(parent_id=self.parent_id)
        grandchild = self._facility_task(parent_id=child.id)

        self.validate.transform(child)
        self.validate.transform(grandchild)

        self.assertIsInstance(grandchild.errors[0], MorangoDirtyParent)

    def test_transform__target_failed_in_run(self):
        self.validate.failed_ids.add(self.user_id)
        task = self._summary_log_task()

        self.validate.transform(task)

        self.assertIsInstance(task.errors[0], ValidationError)
        self.assertIn(self.user_id, str(task.errors[0]))
        self.assertIn(task.id, self.validate.failed_ids)

    def test_transform__deleted_target_precedes_failed_target(self):
        task = _validate_task(
            ConditionalLog(id=uuid.uuid4().hex, facility_id=self.parent_id, user_id=self.user_id)
        )
        self.validate.failed_ids.add(self.parent_id)
        self.validate.deleted_ids[self.user_id] = False

        self.validate.transform(task)

        self.assertEqual(task.outcome, DeserializeOutcome.PROPAGATE_DELETE)

    def test_transform__null_fk_ignored(self):
        """A null FK has no target, so the in-run checks must not match it"""
        self.validate.failed_ids.add(None)
        task = self._facility_task(parent_id=None)

        self.validate.transform(task)

        self.assertEqual(task.outcome, DeserializeOutcome.SAVE)


class AppModelValidateClassifyFailureTestCase(TestCase):
    """
    When model validation fails, the FK targets' store records determine how the failure is
    resolved and reported
    """

    def setUp(self):
        self.validate = AppModelValidate()
        self.parent_id = uuid.uuid4().hex
        self.user_id = uuid.uuid4().hex
        self.error = ValidationError("invalid")

        patcher = mock.patch.object(SyncableModel, "cached_clean_fields", side_effect=self.error)
        self.cached_clean_fields = patcher.start()
        self.addCleanup(patcher.stop)

    def _target_store(self, target_id, model_name, **kwargs):
        return StoreFactory(
            id=target_id,
            profile="facilitydata",
            model_name=model_name,
            partition=target_id,
            serialized="{}",
            last_saved_instance=uuid.uuid4().hex,
            last_saved_counter=1,
            **kwargs,
        )

    def _facility_task(self):
        return _validate_task(Facility(id=uuid.uuid4().hex, parent_id=self.parent_id))

    def _summary_log_task(self):
        return _validate_task(SummaryLog(id=uuid.uuid4().hex, user_id=self.user_id))

    def test_target_deleted(self):
        """The target was deleted in an earlier run, so its app row is already gone"""
        self._target_store(self.user_id, "user", deleted=True)
        task = self._summary_log_task()

        self.validate.transform(task)

        self.assertEqual(task.outcome, DeserializeOutcome.PROPAGATE_DELETE)
        self.assertEqual(self.validate.deleted_ids, {task.id: False})
        self.assertNotIn(task.id, self.validate.failed_ids)

    def test_parent_deleted__object_does_not_exist(self):
        """Models with a self-referential FK only propagate the deletion on this error"""
        self.cached_clean_fields.side_effect = Facility.DoesNotExist()
        self._target_store(self.parent_id, "facility", deleted=True)
        task = self._facility_task()

        self.validate.transform(task)

        self.assertEqual(task.outcome, DeserializeOutcome.PROPAGATE_DELETE)
        self.assertEqual(self.validate.deleted_ids, {task.id: False})

    def test_parent_deleted__validation_error(self):
        self._target_store(self.parent_id, "facility", deleted=True, hard_deleted=True)
        task = self._facility_task()

        self.validate.transform(task)

        self.assertEqual(task.errors, [self.error])
        self.assertIn(task.id, self.validate.failed_ids)
        self.assertEqual(self.validate.deleted_ids, {})

    def test_target_hard_deleted(self):
        self._target_store(self.user_id, "user", deleted=True, hard_deleted=True)
        task = self._summary_log_task()

        self.validate.transform(task)

        self.assertEqual(task.outcome, DeserializeOutcome.PROPAGATE_HARD_DELETE)
        self.assertEqual(self.validate.deleted_ids, {task.id: True})

    def test_target_hard_deleted_only(self):
        """Serialization's hard deletion only sets `hard_deleted`, which still counts as deleted"""
        self._target_store(self.user_id, "user", hard_deleted=True)
        task = self._summary_log_task()

        self.validate.transform(task)

        self.assertEqual(task.outcome, DeserializeOutcome.PROPAGATE_HARD_DELETE)

    def test_target_deleted_precedes_missing_target(self):
        """The hard flag comes from the deleted target, regardless of the other targets"""
        self._target_store(self.user_id, "user", deleted=True, hard_deleted=True)
        task = _validate_task(
            ConditionalLog(id=uuid.uuid4().hex, facility_id=self.parent_id, user_id=self.user_id)
        )

        self.validate.transform(task)

        self.assertEqual(task.outcome, DeserializeOutcome.PROPAGATE_HARD_DELETE)

    def test_parent_missing(self):
        task = self._facility_task()

        self.validate.transform(task)

        self.assertIsInstance(task.errors[0], MorangoMissingParent)
        self.assertEqual(
            str(task.errors[0]), "Parent does not exist in Store; could not deserialize."
        )
        self.assertIn(task.id, self.validate.failed_ids)

    def test_parent_dirty(self):
        """The parent is dirty but was not deserialized in this run, e.g. it was skipped"""
        self._target_store(self.parent_id, "facility", dirty_bit=True)
        task = self._facility_task()

        self.validate.transform(task)

        self.assertIsInstance(task.errors[0], MorangoDirtyParent)
        self.assertEqual(str(task.errors[0]), "Parent is dirty; could not deserialize.")
        self.assertIn(task.id, self.validate.failed_ids)

    def test_parent_accepted_in_run(self):
        """The parent's store record stays dirty until the sink writes it, so the error is the
        record's own"""
        self._target_store(self.parent_id, "facility", dirty_bit=True)
        self.validate.accepted_ids.add(self.parent_id)
        task = self._facility_task()

        self.validate.transform(task)

        self.assertEqual(task.errors, [self.error])

    def test_parent_accepted_in_run__object_does_not_exist(self):
        """Loading the parent fails because the sink has not written it yet"""
        self.cached_clean_fields.side_effect = Facility.DoesNotExist()
        self._target_store(self.parent_id, "facility", dirty_bit=True)
        self.validate.accepted_ids.add(self.parent_id)
        task = self._facility_task()

        self.validate.transform(task)

        self.assertIsInstance(task.errors[0], MorangoDirtyParent)

    def test_target_missing(self):
        task = self._summary_log_task()

        self.validate.transform(task)

        self.assertIsInstance(task.errors[0], IntegrityError)
        self.assertEqual(
            str(task.errors[0]),
            "SummaryLog.user_id references non-existent my user instance with id '{}'".format(
                self.user_id
            ),
        )
        self.assertIn(task.id, self.validate.failed_ids)

    def test_parent_clean(self):
        """With nothing wrong with the target, the failure is the record's own"""
        self._target_store(self.parent_id, "facility", dirty_bit=False)
        task = self._facility_task()

        self.validate.transform(task)

        self.assertEqual(task.errors, [self.error])
        self.assertIn(task.id, self.validate.failed_ids)

    def test_target_dirty(self):
        """Only the self-referential parent is checked for dirtiness, matching legacy behavior"""
        self._target_store(self.user_id, "user", dirty_bit=True)
        task = self._summary_log_task()

        self.validate.transform(task)

        self.assertEqual(task.errors, [self.error])

    def test_no_fk_references(self):
        task = _validate_task(Facility(id=uuid.uuid4().hex))

        with self.assertNumQueries(0):
            self.validate.transform(task)

        self.assertEqual(task.errors, [self.error])

    def test_object_does_not_exist(self):
        """e.g. `clean_fields` loading a related object whose row does not exist"""
        self.cached_clean_fields.side_effect = Facility.DoesNotExist()
        task = self._facility_task()

        self.validate.transform(task)

        self.assertIsInstance(task.errors[0], MorangoMissingParent)

    def test_value_error(self):
        """e.g. `clean_fields` failing to coerce bad synced data"""
        error = ValueError("bad data")
        self.cached_clean_fields.side_effect = error
        self._target_store(self.parent_id, "facility", dirty_bit=False)
        task = self._facility_task()

        self.validate.transform(task)

        self.assertEqual(task.errors, [error])
        self.assertIn(task.id, self.validate.failed_ids)

    def test_failure_queries_store_once(self):
        task = _validate_task(
            ConditionalLog(id=uuid.uuid4().hex, facility_id=self.parent_id, user_id=self.user_id)
        )

        with self.assertNumQueries(1):
            self.validate.transform(task)

    def test_success_queries_store_never(self):
        self.cached_clean_fields.side_effect = None
        task = self._facility_task()

        with self.assertNumQueries(0):
            self.validate.transform(task)

        self.assertEqual(task.outcome, DeserializeOutcome.SAVE)


class StoreModelSourcePrefixConditionsTestCase(SimpleTestCase):
    """`prefix_conditions` is pure ordering logic over the sync filter and touches no ORM"""

    def test_prefix_conditions__none(self):
        source = StoreModelSource(profile="test")
        self.assertEqual(list(source.prefix_conditions()), [None])

    def test_prefix_conditions__with_filter_asc(self):
        source = StoreModelSource(profile="test", sync_filter=Filter("b\na"), partition_order="asc")
        self.assertEqual(list(source.prefix_conditions()), ["a", "b"])

    def test_prefix_conditions__with_filter_asc__realistic(self):
        """
        Partitions often take the form of `{id}:{additional specificity}`, so the important aspect
        of partition ordering is that the least specific filter is generally first
        """
        source = StoreModelSource(
            profile="test", sync_filter=Filter("a:test:z\na\na:initial"), partition_order="asc"
        )
        self.assertEqual(list(source.prefix_conditions()), ["a", "a:initial", "a:test:z"])

    def test_prefix_conditions__with_filter_desc(self):
        source = StoreModelSource(
            profile="test", sync_filter=Filter("a\nb"), partition_order="desc"
        )
        self.assertEqual(list(source.prefix_conditions()), ["b", "a"])


class StoreModelSourceBeginTestCase(SimpleTestCase):
    def test_begin_clears_fk_cache(self):
        """The FK cache is only valid within a single run, since the app models can change"""
        fk_cache = {"facility": "stale"}
        source = StoreModelSource(profile="test", fk_cache=fk_cache)

        source.begin()

        self.assertEqual(fk_cache, {})
        self.assertIs(source.fk_cache, fk_cache)

    def test_begin_initializes_seen(self):
        """Delegates to the base source, which owns the seen set"""
        source = StoreModelSource(profile="test")
        self.assertIsNone(source._seen)

        source.begin()

        self.assertEqual(source._seen, set())


class StoreModelSourceStreamTestCase(TestCase):
    """
    Streams against real `Store` rows and the real registry, so that the filters are validated as
    queries the database actually accepts, and not merely as the keyword arguments the source
    happened to pass along
    """

    profile = "facilitydata"

    def _store(
        self,
        model_name="user",
        partition="a",
        dirty_bit=True,
        deserialization_error=None,
        self_ref_order=None,
    ):
        return StoreFactory(
            id=uuid.uuid4().hex,
            profile=self.profile,
            model_name=model_name,
            partition=partition,
            serialized="{}",
            last_saved_instance=uuid.uuid4().hex,
            last_saved_counter=1,
            dirty_bit=dirty_bit,
            deserialization_error=deserialization_error,
            _self_ref_order=self_ref_order,
        )

    def _stream_ids(self, **kwargs):
        source = StoreModelSource(profile=self.profile, **kwargs)
        source.begin()
        return [task.store.id for task in source.stream()]

    def test_stream__dirty_only(self):
        dirty = self._store(dirty_bit=True)
        self._store(dirty_bit=False)

        self.assertEqual(self._stream_ids(), [dirty.id])

    def test_stream__dirty_only_false(self):
        dirty = self._store(dirty_bit=True)
        clean = self._store(dirty_bit=False)

        self.assertEqual(sorted(self._stream_ids(dirty_only=False)), sorted([dirty.id, clean.id]))

    def test_stream__skip_errored(self):
        """
        `deserialization_error` was historically non-nullable and set to an empty string, so both
        an empty string and null have to be treated as "this record has not errored"
        """
        null_error = self._store(deserialization_error=None)
        empty_error = self._store(deserialization_error="")
        self._store(deserialization_error="it broke")

        self.assertEqual(
            sorted(self._stream_ids(skip_errored=True)),
            sorted([null_error.id, empty_error.id]),
        )

    def test_stream__errored_included_by_default(self):
        """Errored records are retried unless the caller opts into skipping them"""
        errored = self._store(deserialization_error="it broke")
        clean = self._store(deserialization_error=None)

        self.assertEqual(sorted(self._stream_ids()), sorted([errored.id, clean.id]))

    def test_stream__no_partition_filter(self):
        first = self._store(partition="a")
        second = self._store(partition="zzz")

        self.assertEqual(sorted(self._stream_ids()), sorted([first.id, second.id]))

    def test_stream__partition_filter(self):
        included = self._store(partition="a:one")
        self._store(partition="b:two")

        self.assertEqual(self._stream_ids(sync_filter=Filter("a")), [included.id])

    def test_stream__deduplicates_across_partition_passes(self):
        """
        A less specific partition prefix already matches everything beneath it, so the same record
        turns up in more than one pass and must only be yielded once
        """
        shared = self._store(partition="a:b")

        self.assertEqual(self._stream_ids(sync_filter=Filter("a\na:b")), [shared.id])

    def test_stream__parents_before_children(self):
        """`facility` is the profile's self-referential model, so its records carry a tree depth"""
        child = self._store(model_name="facility", self_ref_order=2)
        root = self._store(model_name="facility", self_ref_order=0)
        middle = self._store(model_name="facility", self_ref_order=1)

        self.assertEqual(self._stream_ids(), [root.id, middle.id, child.id])

    def test_stream__unresolved_parents_last(self):
        unresolved = self._store(model_name="facility", self_ref_order=None)
        root = self._store(model_name="facility", self_ref_order=0)

        self.assertEqual(self._stream_ids(), [root.id, unresolved.id])

    def test_stream__models_in_dependency_order(self):
        """
        The registry orders models so that a record's foreign key targets precede it, and the
        source must preserve that. `user` is registered ahead of `facility` for this profile.
        """
        facility = self._store(model_name="facility")
        user = self._store(model_name="user")

        self.assertEqual(self._stream_ids(), [user.id, facility.id])

    def test_stream__ignores_other_profiles(self):
        included = self._store()
        StoreFactory(
            id=uuid.uuid4().hex,
            profile="otherprofile",
            model_name="user",
            partition="a",
            serialized="{}",
            last_saved_instance=uuid.uuid4().hex,
            last_saved_counter=1,
            dirty_bit=True,
        )

        self.assertEqual(self._stream_ids(), [included.id])


class CollectingSink(Sink[DeserializeTask]):
    """Collects each task's resolved outcome and first error type, keyed by the task's ID"""

    def __init__(self):
        self.results = {}

    def consume(self, task: DeserializeTask) -> None:
        error_type = type(task.errors[0]) if task.errors else None
        self.results[task.id] = (task.outcome, error_type)


class DeserializeFixturesMixin:
    """Builds store records for the `facilitydata` profile and runs them through the transforms"""

    profile = "facilitydata"

    def _store(self, app_model, dirty_bit=True, **kwargs):
        store_kwargs = dict(
            id=app_model.id,
            profile=self.profile,
            model_name=app_model.morango_model_name,
            partition=app_model.id,
            source_id=uuid.uuid4().hex,
            serialized=DjangoJSONEncoder().encode(app_model.serialize()),
            last_saved_instance=uuid.uuid4().hex,
            last_saved_counter=1,
            dirty_bit=dirty_bit,
        )
        store_kwargs.update(kwargs)
        return StoreFactory(**store_kwargs)

    def _facility(self, parent=None, order=None, name="facility", **kwargs):
        facility = Facility(
            id=uuid.uuid4().hex,
            name=name,
            parent_id=parent.id if parent else None,
        )
        if order is None and parent is None:
            order = 0
        return self._store(
            facility, _self_ref_fk=facility.parent_id or "", _self_ref_order=order, **kwargs
        )

    def _user(self, **kwargs):
        user = MyUser(id=uuid.uuid4().hex, username=uuid.uuid4().hex[:20], password="password")
        return self._store(user, **kwargs)

    def _summary_log(self, user_id, **kwargs):
        return self._store(SummaryLog(id=uuid.uuid4().hex, user_id=user_id), **kwargs)

    def _run(self, sync_filter=None):
        sink = CollectingSink()
        (
            StoreModelSource(self.profile, sync_filter=sync_filter)
            .pipe(AppModelDeserialize(sync_filter=sync_filter))
            .pipe(AppModelValidate(sync_filter=sync_filter))
            .end(sink)
        )
        return sink.results

    def _run_both_modes(self):
        """Runs once for each `Facility.clean_dereferences_parent` mode, returning both results"""
        results = {}
        for dereferences_parent in (False, True):
            with mock.patch.object(Facility, "clean_dereferences_parent", dereferences_parent):
                results[dereferences_parent] = self._run()
        return results


class DeserializeTransformsIntegrationTestCase(DeserializeFixturesMixin, TestCase):
    """
    Streams real store records through the source and both transforms, without a writing sink,
    so app rows only exist where a test creates them. This mirrors the sink lag, where records
    accepted earlier in the run have not been written yet.
    """

    def test_self_ref_chain_in_run(self):
        root = self._facility()
        child = self._facility(parent=root, order=1)
        grandchild = self._facility(parent=child, order=2)

        with mock.patch.object(Facility, "clean_dereferences_parent", False):
            results = self._run()

        self.assertEqual(results[root.id], (DeserializeOutcome.SAVE, None))
        self.assertEqual(results[child.id], (DeserializeOutcome.SAVE, None))
        self.assertEqual(results[grandchild.id], (DeserializeOutcome.SAVE, None))
        self.assertEqual(Facility.objects.count(), 0)

    def test_self_ref_chain_in_run__dereferences_parent(self):
        """Loading the parent fails, because the sink has not written it yet"""
        root = self._facility()
        child = self._facility(parent=root, order=1)
        grandchild = self._facility(parent=child, order=2)

        with mock.patch.object(Facility, "clean_dereferences_parent", True):
            results = self._run()

        self.assertEqual(results[root.id], (DeserializeOutcome.SAVE, None))
        self.assertEqual(results[child.id], (DeserializeOutcome.ERROR, MorangoDirtyParent))
        self.assertEqual(results[grandchild.id], (DeserializeOutcome.ERROR, MorangoDirtyParent))

    def test_own_error_with_parent_accepted_in_run(self):
        """The parent's store record is still dirty, but the child fails on its own field"""
        root = self._facility()
        child = self._facility(parent=root, order=1, name="x" * 200)

        with mock.patch.object(Facility, "clean_dereferences_parent", False):
            results = self._run()

        self.assertEqual(results[root.id], (DeserializeOutcome.SAVE, None))
        self.assertEqual(results[child.id], (DeserializeOutcome.ERROR, ValidationError))

    def test_cross_model_chain_in_run(self):
        user = self._user()
        log = self._summary_log(user.id)

        results = self._run()

        self.assertEqual(results[user.id], (DeserializeOutcome.SAVE, None))
        self.assertEqual(results[log.id], (DeserializeOutcome.SAVE, None))

    def test_parent_not_in_run(self):
        """The common case, where the parent is unchanged and found through the app tables"""
        parent = Facility.objects.create(name="parent")
        self._store(parent, dirty_bit=False, _self_ref_order=0)
        child = self._facility(parent=parent, order=1)

        for mode, results in self._run_both_modes().items():
            with self.subTest(dereferences_parent=mode):
                self.assertEqual(results[child.id], (DeserializeOutcome.SAVE, None))

    def test_parent_failed_in_run(self):
        root = self._facility(serialized="{bad")
        child = self._facility(parent=root, order=1)
        grandchild = self._facility(parent=child, order=2)

        for mode, results in self._run_both_modes().items():
            with self.subTest(dereferences_parent=mode):
                self.assertEqual(results[root.id], (DeserializeOutcome.ERROR, json.JSONDecodeError))
                self.assertEqual(results[child.id], (DeserializeOutcome.ERROR, MorangoDirtyParent))
                self.assertEqual(
                    results[grandchild.id], (DeserializeOutcome.ERROR, MorangoDirtyParent)
                )

    def test_parent_missing(self):
        missing_parent = Facility(id=uuid.uuid4().hex, name="missing")
        child = self._facility(parent=missing_parent)

        for mode, results in self._run_both_modes().items():
            with self.subTest(dereferences_parent=mode):
                self.assertEqual(
                    results[child.id], (DeserializeOutcome.ERROR, MorangoMissingParent)
                )

    def test_parent_deleted_in_run(self):
        """
        Models with a self-referential FK don't propagate the deletion. When the parent object is
        loaded, this differs from legacy, which propagates it once the sink has deleted the parent.
        """
        root = self._facility(deleted=True)
        child = self._facility(parent=root, order=1)
        grandchild = self._facility(parent=child, order=2)

        for mode, results in self._run_both_modes().items():
            with self.subTest(dereferences_parent=mode):
                self.assertEqual(results[root.id], (DeserializeOutcome.DELETE, None))
                self.assertEqual(results[child.id], (DeserializeOutcome.ERROR, ValidationError))
                self.assertEqual(
                    results[grandchild.id], (DeserializeOutcome.ERROR, MorangoDirtyParent)
                )

    def test_parent_deleted_earlier(self):
        root = self._facility(dirty_bit=False, deleted=True)
        child = self._facility(parent=root, order=1)

        results = self._run_both_modes()

        self.assertEqual(results[False][child.id], (DeserializeOutcome.ERROR, ValidationError))
        self.assertEqual(results[True][child.id], (DeserializeOutcome.PROPAGATE_DELETE, None))

    def test_parent_hard_deleted_earlier(self):
        root = self._facility(dirty_bit=False, deleted=True, hard_deleted=True)
        child = self._facility(parent=root, order=1)

        results = self._run_both_modes()

        self.assertEqual(results[False][child.id], (DeserializeOutcome.ERROR, ValidationError))
        self.assertEqual(results[True][child.id], (DeserializeOutcome.PROPAGATE_HARD_DELETE, None))

    def test_target_deleted_in_run(self):
        user = self._user(deleted=True)
        log = self._summary_log(user.id)

        results = self._run()

        self.assertEqual(results[user.id], (DeserializeOutcome.DELETE, None))
        self.assertEqual(results[log.id], (DeserializeOutcome.PROPAGATE_DELETE, None))

    def test_target_deleted_earlier(self):
        user = self._user(dirty_bit=False, deleted=True, hard_deleted=True)
        log = self._summary_log(user.id)

        results = self._run()

        self.assertEqual(results[log.id], (DeserializeOutcome.PROPAGATE_HARD_DELETE, None))

    def test_target_missing(self):
        log = self._summary_log(uuid.uuid4().hex)

        results = self._run()

        self.assertEqual(results[log.id], (DeserializeOutcome.ERROR, IntegrityError))

    def test_transforms_only_read(self):
        root = self._facility()
        self._facility(parent=root, order=1)
        self._facility(serialized="{bad")
        self._facility(deleted=True)
        self._summary_log(uuid.uuid4().hex)

        with CaptureQueriesContext(connection) as context:
            self._run()

        statements = [query["sql"].strip().upper() for query in context.captured_queries]
        self.assertTrue(statements)
        for statement in statements:
            # postgres wraps iterated reads in a server-side cursor (`DECLARE ... FOR SELECT`), so
            # check for writes rather than requiring every statement to start with `SELECT`
            self.assertFalse(
                statement.startswith(("INSERT", "UPDATE", "DELETE")),
                statement,
            )


class LegacyParityTestCase(DeserializeFixturesMixin, TestCase):
    """
    Characterizes legacy `_deserialize_from_store` errors and propagated deletions against the
    transforms, on the same store records. The transforms don't write, so both can run against the
    same database state. Runs with the default `Facility.clean_dereferences_parent`, which is the
    legacy test behavior, unless a test patches it.
    """

    def _legacy_result(self, store_id):
        """The legacy result of a store record that did not save, in the transforms' terms"""
        if HardDeletedModels.objects.filter(id=store_id).exists():
            return DeserializeOutcome.PROPAGATE_HARD_DELETE, None
        if DeletedModels.objects.filter(id=store_id).exists():
            return DeserializeOutcome.PROPAGATE_DELETE, None
        exception = Store.objects.get(id=store_id).deserialization_exception
        self.assertIsNotNone(exception)
        return DeserializeOutcome.ERROR, exception

    def _assert_parity(self, *store_ids):
        results = self._run()
        _deserialize_from_store(self.profile)

        for store_id in store_ids:
            with self.subTest(store_id=store_id):
                outcome, error_type = results[store_id]
                error_path = exception_path(error_type) if error_type else None
                self.assertEqual((outcome, error_path), self._legacy_result(store_id))

    def test_parent_failed(self):
        root = self._facility(serialized="{bad")
        child = self._facility(parent=root, order=1)
        grandchild = self._facility(parent=child, order=2)

        self._assert_parity(root.id, child.id, grandchild.id)

    def test_parent_missing(self):
        missing_parent = Facility(id=uuid.uuid4().hex, name="missing")
        child = self._facility(parent=missing_parent)

        self._assert_parity(child.id)

    def test_target_missing(self):
        log = self._summary_log(uuid.uuid4().hex)

        self._assert_parity(log.id)

    @mock.patch.object(Facility, "clean_dereferences_parent", False)
    def test_own_error_with_parent_accepted(self):
        root = self._facility()
        child = self._facility(parent=root, order=1, name="x" * 200)

        self._assert_parity(child.id)

    def test_target_deleted_in_run(self):
        user = self._user(deleted=True)
        log = self._summary_log(user.id)

        self._assert_parity(log.id)

    def test_target_deleted_earlier(self):
        user = self._user(dirty_bit=False, deleted=True)
        log = self._summary_log(user.id)

        self._assert_parity(log.id)

    def test_target_hard_deleted_earlier(self):
        user = self._user(dirty_bit=False, deleted=True, hard_deleted=True)
        log = self._summary_log(user.id)

        self._assert_parity(log.id)

    def test_parent_deleted_earlier(self):
        root = self._facility(dirty_bit=False, deleted=True)
        child = self._facility(parent=root, order=1)

        self._assert_parity(child.id)

    def test_parent_hard_deleted_earlier(self):
        root = self._facility(dirty_bit=False, deleted=True, hard_deleted=True)
        child = self._facility(parent=root, order=1)

        self._assert_parity(child.id)

    @mock.patch.object(Facility, "clean_dereferences_parent", False)
    def test_parent_deleted_earlier__parent_not_loaded(self):
        root = self._facility(dirty_bit=False, deleted=True)
        child = self._facility(parent=root, order=1)

        self._assert_parity(child.id)

    @mock.patch.object(Facility, "clean_dereferences_parent", False)
    def test_parent_deleted_in_run__parent_not_loaded(self):
        root = self._facility(deleted=True)
        child = self._facility(parent=root, order=1)
        grandchild = self._facility(parent=child, order=2)

        self._assert_parity(child.id, grandchild.id)
