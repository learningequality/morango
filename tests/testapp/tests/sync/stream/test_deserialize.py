import uuid

import mock
from django.test import SimpleTestCase
from django.test import TestCase

from morango.models.certificates import Filter
from morango.models.core import Store
from morango.models.core import SyncableModel
from morango.sync.stream.deserialize import DeserializeTask
from morango.sync.stream.deserialize import StoreModelSource

from ...helpers import StoreFactory


class DeserializeTaskTestCase(SimpleTestCase):
    def setUp(self):
        self.store = mock.Mock(spec_set=Store)
        self.store.profile = "test"
        self.store.model_name = "testmodel"
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
