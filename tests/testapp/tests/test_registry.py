import uuid
from collections import defaultdict
from contextlib import contextmanager

import mock
from django.test import SimpleTestCase
from django.test import TestCase

from morango.registry import syncable_models

from .helpers import StoreFactory


class SyncableModelRegistryTestCase(SimpleTestCase):
    def setUp(self):
        self._original_self_referential_fks = syncable_models.self_referential_fks
        syncable_models.self_referential_fks = defaultdict(dict)

    def tearDown(self):
        syncable_models.self_referential_fks = self._original_self_referential_fks

    def _model(self, profile, model_name):
        model = mock.Mock()
        model.morango_profile = profile
        model.morango_model_name = model_name
        return model

    @mock.patch("morango.registry.self_referential_fk", return_value="parent_id")
    def test_get_self_referential_fk_caches_non_none(self, mock_self_referential_fk):
        model = self._model("profile", "facility")

        self.assertEqual(syncable_models.get_self_referential_fk(model), "parent_id")
        self.assertEqual(syncable_models.get_self_referential_fk(model), "parent_id")

        mock_self_referential_fk.assert_called_once_with(model)

    @mock.patch("morango.registry.self_referential_fk", return_value=None)
    def test_get_self_referential_fk_caches_none(self, mock_self_referential_fk):
        model = self._model("profile", "user")

        self.assertIsNone(syncable_models.get_self_referential_fk(model))
        self.assertIsNone(syncable_models.get_self_referential_fk(model))

        mock_self_referential_fk.assert_called_once_with(model)

    @mock.patch(
        "morango.registry.self_referential_fk",
        side_effect=["parent_id", None, "sibling_id"],
    )
    def test_get_self_referential_fk_cache_keys_include_profile_and_model_name(
        self, mock_self_referential_fk
    ):
        model_a = self._model("profile_a", "facility")
        model_b = self._model("profile_a", "user")
        model_c = self._model("profile_b", "facility")

        self.assertEqual(syncable_models.get_self_referential_fk(model_a), "parent_id")
        self.assertIsNone(syncable_models.get_self_referential_fk(model_b))
        self.assertEqual(syncable_models.get_self_referential_fk(model_c), "sibling_id")

        self.assertEqual(syncable_models.get_self_referential_fk(model_a), "parent_id")
        self.assertIsNone(syncable_models.get_self_referential_fk(model_b))
        self.assertEqual(syncable_models.get_self_referential_fk(model_c), "sibling_id")

        self.assertEqual(mock_self_referential_fk.call_count, 3)

    def test_get_model_querysets_without_ordering(self):
        queryset = mock.Mock()
        model = mock.Mock()
        model.syncing_objects.all.return_value = queryset
        model.morango_ordering = ()

        with mock.patch.object(syncable_models, "get_models", return_value=[model]):
            result = list(syncable_models.get_model_querysets("profile"))

        self.assertEqual(result, [queryset])
        queryset.order_by.assert_not_called()

    def test_get_model_querysets_with_ordering(self):
        queryset = mock.Mock()
        ordered_queryset = mock.Mock()
        queryset.order_by.return_value = ordered_queryset
        model = mock.Mock()
        model.syncing_objects.all.return_value = queryset
        model.morango_ordering = ("field_a", "-field_b")

        with mock.patch.object(syncable_models, "get_models", return_value=[model]):
            with mock.patch.object(
                syncable_models,
                "_get_nulls_last_ordering",
                return_value=("normalized_a", "normalized_b"),
            ) as mock_get_ordering:
                result = list(syncable_models.get_model_querysets("profile"))

        self.assertEqual(result, [ordered_queryset])
        mock_get_ordering.assert_called_once_with(("field_a", "-field_b"))
        queryset.order_by.assert_called_once_with("normalized_a", "normalized_b")

    def test_get_model_querysets_applies_ordering_per_model(self):
        queryset_a = mock.Mock()
        ordered_queryset_a = mock.Mock()
        queryset_a.order_by.return_value = ordered_queryset_a
        model_a = mock.Mock()
        model_a.syncing_objects.all.return_value = queryset_a
        model_a.morango_ordering = ("field_a",)

        queryset_b = mock.Mock()
        model_b = mock.Mock()
        model_b.syncing_objects.all.return_value = queryset_b
        model_b.morango_ordering = ()

        with mock.patch.object(syncable_models, "get_models", return_value=[model_a, model_b]):
            with mock.patch.object(
                syncable_models,
                "_get_nulls_last_ordering",
                return_value=("normalized_a",),
            ) as mock_get_ordering:
                result = list(syncable_models.get_model_querysets("profile"))

        self.assertEqual(result, [ordered_queryset_a, queryset_b])
        mock_get_ordering.assert_called_once_with(("field_a",))
        queryset_a.order_by.assert_called_once_with("normalized_a")
        queryset_b.order_by.assert_not_called()


class GetStoreQuerysetsTestCase(TestCase):
    """
    `get_store_querysets` is the deserialization counterpart to `get_model_querysets`, and is
    responsible for the ordering guarantees the deserialization stage depends upon.

    These assert the order of records actually returned, rather than the query used to fetch
    them, since the latter varies by backend.
    """

    def _model(self, model_name, self_ref_fk=None):
        """
        :param self_ref_fk: the attname of the model's self-referential FK, or None if it has none
        """
        return mock.Mock(morango_model_name=model_name, self_ref_fk=self_ref_fk)

    @contextmanager
    def _registry(self, models):
        with mock.patch.object(syncable_models, "get_models", return_value=models) as get_models:
            with mock.patch.object(
                syncable_models,
                "get_self_referential_fk",
                side_effect=lambda model: model.self_ref_fk,
            ):
                yield get_models

    def _store(self, model_name, self_ref_order=None, profile="facilitydata"):
        return StoreFactory(
            id=uuid.uuid4().hex,
            profile=profile,
            model_name=model_name,
            partition="partition",
            serialized="{}",
            last_saved_instance=uuid.uuid4().hex,
            last_saved_counter=1,
            _self_ref_order=self_ref_order,
        )

    def _queryset_for(self, model_name, self_ref_fk="parent_id"):
        with self._registry([self._model(model_name, self_ref_fk=self_ref_fk)]):
            return next(iter(syncable_models.get_store_querysets("facilitydata")))

    def test_orders_parents_before_children(self):
        # inserted out of order, so row order alone would not produce the expected result
        for self_ref_order in (2, 0, 1):
            self._store("abc", self_ref_order=self_ref_order)

        self.assertEqual(
            [store._self_ref_order for store in self._queryset_for("abc")],
            [0, 1, 2],
        )

    def test_orders_unresolved_parents_last(self):
        # nulls are inserted first, so row order alone would place them at the front. A null
        # `_self_ref_order` means the record's parent could not be resolved
        self._store("abc", self_ref_order=None)
        self._store("abc", self_ref_order=None)
        for self_ref_order in (1, 0, 2):
            self._store("abc", self_ref_order=self_ref_order)

        self.assertEqual(
            [store._self_ref_order for store in self._queryset_for("abc")],
            [0, 1, 2, None, None],
        )

    def test_no_ordering_for_models_without_a_self_referential_fk(self):
        """
        `_self_ref_order` is null for every record of a model with no self-referential FK, so
        there is no tree to walk and sorting by it would only cost the database work
        """
        for _ in range(3):
            self._store("abc")

        queryset = self._queryset_for("abc", self_ref_fk=None)

        self.assertFalse(queryset.ordered)
        self.assertEqual(queryset.count(), 3)

    def test_orders_only_the_self_referential_models(self):
        models = [self._model("plain"), self._model("tree", self_ref_fk="parent_id")]

        with self._registry(models):
            plain_qs, tree_qs = list(syncable_models.get_store_querysets("facilitydata"))

        self.assertFalse(plain_qs.ordered)
        self.assertTrue(tree_qs.ordered)

    def test_filters_by_profile_and_model_name(self):
        expected = self._store("abc")
        self._store("other")
        self._store("abc", profile="otherprofile")

        self.assertEqual([store.id for store in self._queryset_for("abc")], [expected.id])

    def test_yields_one_queryset_per_model_in_dependency_order(self):
        self._store("first")
        self._store("second")
        self._store("second")
        self._store("third")

        models = [self._model("first"), self._model("second"), self._model("third")]
        with self._registry(models) as mock_get_models:
            querysets = list(syncable_models.get_store_querysets("facilitydata"))

        mock_get_models.assert_called_once_with("facilitydata")
        # the registry orders models such that a model's foreign key targets precede it, and each
        # queryset must stay paired with its model to preserve that order downstream
        self.assertEqual(
            [[store.model_name for store in queryset] for queryset in querysets],
            [["first"], ["second", "second"], ["third"]],
        )
