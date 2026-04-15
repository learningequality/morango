"""
Foundational classes for streaming ETL-like pipelines.

Provides a modular source > transform > sink pattern where models are streamed one-by-one through
a pipeline of connected modules, reducing memory overhead.
"""

import abc
from typing import Any, Generic, Iterable, Iterator, List, Optional, TypeVar

T = TypeVar("T")


class StreamModule(abc.ABC):
    """
    Abstract base class for all stream modules
    """

    pass


class PipelineModule(StreamModule):
    """
    Abstract base class for all stream modules that can be read,
    or rather can pipe to another module.
    """

    @abc.abstractmethod
    def pipe(self, other: "StreamModule") -> "StreamModule":
        """Connect this module to another module, returning a Pipeline."""
        pass


class OperatorModule(StreamModule):
    """
    Abstract base class for all pipeline transform-like modules.

    Each module receives an iterable of items and yields transformed items. Modules are composable
    using a `Pipeline`, allowing them to be chained together via its pipe method.
    """

    @abc.abstractmethod
    def __call__(self, items: Iterable[Any]) -> Iterator:
        """Process the incoming iterable and yield output items."""
        pass


class Source(PipelineModule, Generic[T]):
    """
    A module that represents a streaming pipeline source.
    """

    def begin(self) -> None:
        """Called once before the stream begins."""
        pass

    @abc.abstractmethod
    def stream(self) -> Iterator[T]:
        """The primary read method that returns the iterator stream of items."""
        pass

    def pipe(self, other: "OperatorModule") -> "Pipeline":
        """
        Start a pipeline, by attaching this as the source of a pipeline,
        and add the first transform module
        """
        return Pipeline(self, [other])


class Sink(StreamModule, Generic[T]):
    """
    A terminal module that consumes items without yielding further output.
    """

    @abc.abstractmethod
    def consume(self, item: T) -> None:
        """Process the incoming item from the stream."""
        pass

    def finalize(self) -> None:
        """Called once after all items have been consumed."""
        pass


class Pipeline(PipelineModule):
    """
    An ordered sequence of `OperatorModule` instances that are executed in series. The output of
    each module feeds into the next.
    """

    def __init__(self, source: Source, modules: Optional[List[OperatorModule]] = None) -> None:
        self._source = source
        self._modules = list(modules) if modules else []

    def pipe(self, other: OperatorModule) -> "Pipeline":
        """Append another pipeline module to this pipeline and return self"""
        if isinstance(other, OperatorModule):
            self._modules.append(other)
        else:
            raise ValueError("Cannot pipe another module that is not an OperatorModule")
        return self

    def end(self, sink: "Sink") -> int:
        """Run the source through each module in order."""
        self._source.begin()
        stream = self._source.stream()

        for module in self._modules:
            stream = module(stream)

        count = 0
        for item in stream:
            sink.consume(item)
            count += 1
        sink.finalize()
        return count


class Transform(OperatorModule, Generic[T]):
    """
    A module that transforms each incoming item one-by-one.
    """

    def __call__(self, items: Iterable[T]) -> Iterator[T]:
        for item in items:
            result = self.transform(item)
            if result is not None:
                yield result

    @abc.abstractmethod
    def transform(self, item: T) -> T:
        """Logic for transforming an item"""
        pass


class FlatMap(OperatorModule, Generic[T]):
    """
    A module that maps each incoming item to zero or more output items, flattening the result into a
    single stream.
    """

    def __call__(self, items: Iterable[T]) -> Iterator[T]:
        for item in items:
            for result in self.flat_map(item):
                yield result

    @abc.abstractmethod
    def flat_map(self, item: T) -> Iterable[T]:
        """Transform a single item, into multiple stream items"""
        pass


class Buffer(OperatorModule, Generic[T]):
    """
    Collects incoming items into fixed-size chunks (lists).

    Inserting a buffer into the pipeline converts a stream of individual items into a stream of
    lists of those items, which is useful for batching database operations such as `bulk_create`.
    """

    def __init__(self, size: int) -> None:
        """
        :param size: Maximum number of items per chunk.
        """
        if size < 1:
            raise ValueError("Buffer size must be >= 1")
        self.size = size

    def __call__(self, items: Iterable[T]) -> Iterator[List[T]]:
        chunk = []
        for item in items:
            chunk.append(item)
            if len(chunk) >= self.size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk


class Unbuffer(OperatorModule, Generic[T]):
    """
    Flattens a stream of iterables (e.g. like chunks from `Buffer`) back into a stream of
    individual items.
    """

    def __call__(self, items: Iterable[Iterable[T]]) -> Iterator[T]:
        for chunk in items:
            for item in chunk:
                yield item
