from typing import Dict
from typing import Generator
from typing import List
from typing import Optional
from typing import Type

from morango.models.certificates import Filter
from morango.models.core import Store
from morango.models.core import SyncableModel
from morango.registry import syncable_models
from morango.sync.stream.source import MorangoSource
from morango.sync.stream.source import SourceTask


class DeserializeTask(SourceTask):
    """Carrier class for providing context through the deserialization pipeline."""

    __slots__ = ("store", "app_model", "fk_cache", "errors")

    def __init__(self, store: Store, fk_cache: Dict):
        self.store = store
        self.fk_cache: Dict = fk_cache
        self.app_model: Optional[SyncableModel] = None
        self.errors: List[Exception] = []

    @property
    def id(self) -> str:
        return self.store.id

    @property
    def model(self) -> Type[SyncableModel]:
        return syncable_models.get_model(self.store.profile, self.store.model_name)

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def set_app_model(self, app_model: Optional[SyncableModel]) -> None:
        self.app_model = app_model

    def add_error(self, error: Exception) -> None:
        self.errors.append(error)


class StoreModelSource(MorangoSource[DeserializeTask]):
    """
    Yields ``DeserializeTask`` objects for dirty store models that match the optional
    *sync_filter*.
    """

    def __init__(
        self,
        profile: str,
        sync_filter: Optional[Filter] = None,
        dirty_only: bool = True,
        partition_order: str = "asc",
        fk_cache: Optional[Dict] = None,
        skip_errored: bool = False,
    ):
        """
        :param profile: The Morango model profile
        :param sync_filter: The Filter object for this sync
        :param dirty_only: Whether to filter on dirty records only
        :param partition_order: Controls how the filter specificity is applied, "asc" or "desc"
        :param fk_cache: Dictionary cache for FK references
        :param skip_errored: Whether to skip Store records with deserialization errors
        """
        super().__init__(profile, sync_filter, dirty_only, partition_order)
        self.fk_cache = fk_cache if fk_cache is not None else {}
        self.skip_errored = skip_errored

    def begin(self) -> None:
        """Reset fk_cache at the beginning of stream"""
        super().begin()
        self.fk_cache.clear()

    def stream_for_filter(
        self, partition_condition: Optional[str]
    ) -> Generator[DeserializeTask, None, None]:
        # the registry yields models in foreign key dependency order, so streaming model by model
        # ensures a record's foreign key targets are deserialized before it is
        for store_qs in syncable_models.get_store_querysets(self.profile):
            qs = store_qs
            if partition_condition is not None:
                qs = qs.filter(partition__startswith=partition_condition)
            if self.dirty_only:
                qs = qs.filter(dirty_bit=True)
            if self.skip_errored:
                qs = qs.exclude_has_deserialization_error()

            for store_model in qs.iterator():
                yield DeserializeTask(store_model, self.fk_cache)
