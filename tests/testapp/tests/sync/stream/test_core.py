from django.test import SimpleTestCase

from morango.sync.stream.core import Buffer
from morango.sync.stream.core import FlatMap
from morango.sync.stream.core import Pipeline
from morango.sync.stream.core import Sink
from morango.sync.stream.core import Source
from morango.sync.stream.core import Transform
from morango.sync.stream.core import Unbuffer


class FakeSource(Source):
    def stream(self):
        yield 1
        yield 2
        yield 3


class FakeTransform(Transform):
    def transform(self, item):
        return item * 2


class FakeFlatMap(FlatMap):
    def flat_map(self, item):
        return [item, item + 0.5]


class FakeSink(Sink):
    def __init__(self):
        self.consumed = []

    def consume(self, item):
        self.consumed.append(item)


class SourceTestCase(SimpleTestCase):
    def test_stream(self):
        source = FakeSource()
        self.assertEqual([1, 2, 3], list(source.stream()))

    def test_pipe(self):
        source = FakeSource()
        transform = FakeTransform()
        pipeline = source.pipe(transform)
        self.assertIsInstance(pipeline, Pipeline)
        self.assertEqual(pipeline._source, source)
        self.assertEqual(pipeline._modules, [transform])


class TransformTestCase(SimpleTestCase):
    def test_transform_call(self):
        transform = FakeTransform()
        result = list(transform([1, 2, 3]))
        self.assertEqual([2, 4, 6], result)

    def test_transform_skips_none(self):
        class SkipTransform(Transform):
            def transform(self, item):
                return item if item > 1 else None

        transform = SkipTransform()
        result = list(transform([1, 2]))
        self.assertEqual([2], result)


class FlatMapTestCase(SimpleTestCase):
    def test_flat_map_call(self):
        flat_map = FakeFlatMap()
        result = list(flat_map([1, 2]))
        self.assertEqual([1, 1.5, 2, 2.5], result)


class BufferTestCase(SimpleTestCase):
    def test_buffer(self):
        buff = Buffer(size=2)
        result = list(buff([1, 2, 3]))
        self.assertEqual([[1, 2], [3]], result)

    def test_buffer_invalid_size(self):
        with self.assertRaises(ValueError):
            Buffer(size=0)


class PartitionedBufferTestCase(SimpleTestCase):
    def setUp(self):
        self.buff = Buffer(size=3, partition_fn=lambda n: n % 2)

    def test_mixed(self):
        result = list(self.buff([0, 2, 3, 4, 6, 7, 8]))
        self.assertEqual([[0, 2], [3], [4, 6], [7], [8]], result)

    def test_uniform(self):
        result = list(self.buff([0, 2, 4, 6, 8]))
        self.assertEqual([[0, 2, 4], [6, 8]], result)

    def test_leading(self):
        result = list(self.buff([0, 3, 5, 7, 9]))
        self.assertEqual([[0], [3, 5, 7], [9]], result)

    def test_trailing(self):
        result = list(self.buff([0, 2, 4, 6, 8, 9]))
        self.assertEqual([[0, 2, 4], [6, 8], [9]], result)


class UnbufferTestCase(SimpleTestCase):
    def test_unbuffer(self):
        unbuff = Unbuffer()
        result = list(unbuff([[1, 2], [3]]))
        self.assertEqual([1, 2, 3], result)


class PipelineTestCase(SimpleTestCase):
    def test_pipeline_execution(self):
        source = FakeSource()
        transform = FakeTransform()
        sink = FakeSink()

        pipeline = source.pipe(transform)
        count = pipeline.end(sink)

        self.assertEqual(count, 3)
        self.assertEqual([2, 4, 6], sink.consumed)

    def test_pipeline_chaining(self):
        source = FakeSource()
        pipeline = source.pipe(FakeTransform()).pipe(FakeTransform())
        sink = FakeSink()

        count = pipeline.end(sink)
        self.assertEqual(count, 3)
        self.assertEqual([4, 8, 12], sink.consumed)
