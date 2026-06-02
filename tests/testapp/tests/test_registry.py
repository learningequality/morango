from collections import defaultdict

import mock
from django.test import SimpleTestCase

from morango.registry import syncable_models


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
