from django.test import SimpleTestCase

from morango.models.certificates import Filter
from morango.sync.stream.source import MorangoSource
from morango.sync.stream.source import SourceTask


class FakeTask(SourceTask):
    __slots__ = ("_id",)

    def __init__(self, task_id):
        self._id = task_id

    @property
    def id(self):
        return self._id


class FakeSource(MorangoSource[FakeTask]):
    def __init__(self, *args, task_ids=(), **kwargs):
        super().__init__(*args, **kwargs)
        self.task_ids = task_ids

    def stream_for_filter(self, partition_condition):
        for task_id in self.task_ids:
            yield FakeTask(task_id)


class MorangoSourceBeginTestCase(SimpleTestCase):
    def test_seen_is_uninitialized_before_begin(self):
        source = FakeSource("test")
        self.assertIsNone(source._seen)

    def test_begin_initializes_seen(self):
        source = FakeSource("test")
        source.begin()
        self.assertEqual(source._seen, set())

    def test_begin_resets_seen_between_runs(self):
        """A source may be streamed more than once, and must not carry state across runs"""
        source = FakeSource("test", sync_filter=Filter("a"), task_ids=("1", "2"))

        source.begin()
        first = [task.id for task in source.stream()]
        self.assertEqual(first, ["1", "2"])

        # without `begin`, every id is already in `_seen` and nothing would be yielded
        source.begin()
        second = [task.id for task in source.stream()]
        self.assertEqual(second, ["1", "2"])
