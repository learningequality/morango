import datetime
import uuid

from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from .helpers import create_buffer_and_store_dummy_data
from morango.models.core import Buffer
from morango.models.core import RecordMaxCounterBuffer
from morango.models.core import SyncSession
from morango.models.core import TransferSession


def _create_sessions(last_activity_offset=0):

    last_activity_timestamp = timezone.now() - datetime.timedelta(
        hours=last_activity_offset
    )

    sync_session = SyncSession.objects.create(
        id=uuid.uuid4().hex,
        profile="facilitydata",
        last_activity_timestamp=last_activity_timestamp,
    )
    transfer_session = TransferSession.objects.create(
        id=uuid.uuid4().hex,
        sync_session=sync_session,
        push=True,
        last_activity_timestamp=last_activity_timestamp,
    )

    return sync_session, transfer_session


def assert_session_is_cleared(transfersession):
    transfersession.refresh_from_db()
    transfersession.sync_session.refresh_from_db()
    assert transfersession.buffer_set.all().count() == 0
    assert transfersession.recordmaxcounterbuffer_set.all().count() == 0
    assert transfersession.active == False
    assert transfersession.sync_session.active == False


def assert_session_is_not_cleared(transfersession):
    transfersession.refresh_from_db()
    transfersession.sync_session.refresh_from_db()
    assert transfersession.buffer_set.all().count() > 0
    assert transfersession.recordmaxcounterbuffer_set.all().count() > 0
    assert transfersession.active == True
    assert transfersession.sync_session.active == True


class CleanupSyncsTestCase(TestCase):
    def setUp(self):
        self.syncsession_old, self.transfersession_old = _create_sessions(24)
        self.syncsession_new, self.transfersession_new = _create_sessions(2)
        self.data_old = create_buffer_and_store_dummy_data(self.transfersession_old.id)
        self.data_new = create_buffer_and_store_dummy_data(self.transfersession_new.id)

    def test_no_sessions_cleared(self):
        call_command("cleanupsyncs", expiration=48)
        assert_session_is_not_cleared(self.transfersession_old)
        assert_session_is_not_cleared(self.transfersession_new)

    def test_some_sessions_cleared(self):
        call_command("cleanupsyncs", expiration=6)
        assert_session_is_cleared(self.transfersession_old)
        assert_session_is_not_cleared(self.transfersession_new)

    def test_all_sessions_cleared(self):
        call_command("cleanupsyncs", expiration=1)
        assert_session_is_cleared(self.transfersession_old)
        assert_session_is_cleared(self.transfersession_new)

    def test_filtering_sessions_cleared(self):
        call_command("cleanupsyncs", ids=self.syncsession_old.id, expiration=0)
        assert_session_is_cleared(self.transfersession_old)
        assert_session_is_not_cleared(self.transfersession_new)
