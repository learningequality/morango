import mock
from django.test import TestCase

from morango.sync.utils import SyncSignal
from morango.sync.utils import SyncSignalGroup


class SyncSignalTestCase(TestCase):
    def test_defaults(self):
        signaler = SyncSignal(this_is_a_default=True)
        handler = mock.Mock()
        signaler.connect(handler)
        signaler.fire()

        handler.assert_called_once_with(this_is_a_default=True)

    def test_fire_with_kwargs(self):
        signaler = SyncSignal(my_key="abc")
        handler = mock.Mock()
        signaler.connect(handler)
        signaler.fire(my_key="123", not_default=True)

        handler.assert_called_once_with(my_key="123", not_default=True)


class SyncSignalGroupTestCase(TestCase):
    def test_started_defaults(self):
        signaler = SyncSignalGroup(this_is_a_default=True)
        handler = mock.Mock()
        signaler.connect(handler)

        signaler.fire()
        handler.assert_called_with(this_is_a_default=True)

        signaler.started.fire(this_is_a_default=False)
        handler.assert_called_with(this_is_a_default=False)

    def test_in_progress_defaults(self):
        signaler = SyncSignalGroup(this_is_a_default=True)
        handler = mock.Mock()
        signaler.connect(handler)

        signaler.fire()
        handler.assert_called_with(this_is_a_default=True)

        signaler.in_progress.fire(this_is_a_default=False)
        handler.assert_called_with(this_is_a_default=False)

    def test_completed_defaults(self):
        signaler = SyncSignalGroup(this_is_a_default=True)
        handler = mock.Mock()
        signaler.connect(handler)

        signaler.fire()
        handler.assert_called_with(this_is_a_default=True)

        signaler.completed.fire(this_is_a_default=False)
        handler.assert_called_with(this_is_a_default=False)

    def test_send(self):
        signaler = SyncSignalGroup(this_is_a_default=True)

        start_handler = mock.Mock()
        signaler.started.connect(start_handler)
        in_progress_handler = mock.Mock()
        signaler.in_progress.connect(in_progress_handler)
        completed_handler = mock.Mock()
        signaler.completed.connect(completed_handler)

        with signaler.send(other="A") as status:
            start_handler.assert_called_once_with(this_is_a_default=True, other="A")
            status.in_progress.fire(this_is_a_default=False, other="B")
            in_progress_handler.assert_called_once_with(
                this_is_a_default=False, other="B"
            )
            completed_handler.assert_not_called()

        completed_handler.assert_called_once_with(this_is_a_default=True, other="A")
