import abc
from typing import Generator
from typing import Iterator
from typing import Optional
from typing import TypeVar

from morango.models.certificates import Filter
from morango.sync.stream.core import Source


class SourceTask(abc.ABC):
    """Typing for source object passed through streaming pipeline"""

    __slots__ = ()

    @property
    @abc.abstractmethod
    def id(self) -> str:
        pass


T = TypeVar("T", bound=SourceTask)


class MorangoSource(Source[T], abc.ABC):
    """
    Common source functionality for Morango sources, such as SyncableModels and Store records.
    """

    def __init__(
        self,
        profile: str,
        sync_filter: Optional[Filter] = None,
        dirty_only: bool = True,
        partition_order: str = "asc",
    ):
        """
        :param profile: The Morango model profile
        :param sync_filter: The Filter object for this sync
        :param dirty_only: Whether to filter on dirty records only
        :param partition_order: Controls how the filter specificity is applied, "asc" or "desc"
        """
        self.profile = profile
        self.sync_filter = sync_filter
        self.dirty_only = dirty_only
        self.partition_order = partition_order
        self._seen: Optional[set] = None

    def begin(self) -> None:
        """Initialize seen set at the beginning of the stream"""
        self._seen = set()

    def prefix_conditions(self) -> Generator[Optional[str], None, None]:
        """
        Generates partition prefixes for queries based on the sync filter and partition order.

        This method outputs prefixes in sorted order according to the specified partition
        order. If no sync filter is provided, it yields `None` to indicate a query
        without filtering by partition.

        :return: A generator yielding partition prefixes or `None` if no filtering is applied.
        """
        if self.sync_filter is None:
            # yield None once, so we do one query without a partition filter (everything)
            yield None
        else:
            partitions_prefixes = [str(prefix) for prefix in self.sync_filter]
            partition_iterator = sorted(
                partitions_prefixes,
                reverse=self.partition_order == "desc",
            )

            for prefix in partition_iterator:
                yield prefix

    def stream(self) -> Generator[T, None, None]:
        """
        Streams unique objects based on prefix conditions. This generator method iterates over
        partition conditions defined in the sync_filter and passes through to `stream_for_filter`
        to stream back objects, ensuring that only objects with unique `id` values are yielded.

        :return: A generator yielding unique objects.
        """
        for partition_condition in self.prefix_conditions():
            for obj in self.stream_for_filter(partition_condition):
                # partition filtering could result in overlaps, and since we're walking
                # through the partitions one by one, we should avoid duplicates. Morango
                # syncable models and store records have unique IDs across the entire profile
                if obj.id not in self._seen:
                    # without sync filters, we do not need to worry about repeating objects
                    if self.sync_filter is not None:
                        self._seen.add(obj.id)
                    yield obj

    @abc.abstractmethod
    def stream_for_filter(self, partition_condition: Optional[str]) -> Iterator[T]:
        """
        This method is intended to generate an iterator that yields data based on the given
        filtering condition.

        :param partition_condition: A string representing a partition filter prefix condition
        :return: An iterator yielding items
        """
        pass
