import datetime
import uuid

from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from .helpers import create_buffer_and_store_dummy_data
from morango.models.core import SyncSession
from morango.models.core import TransferSession


def _create_sessions(last_activity_offset=0, sync_session=None):

    last_activity_timestamp = timezone.now() - datetime.timedelta(
        hours=last_activity_offset
    )

    if sync_session is None:
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


class CleanupSyncsTestCase(TestCase):
    def setUp(self):
        self.syncsession_old, self.transfersession_old = _create_sessions(24)
        self.syncsession_new, self.transfersession_new = _create_sessions(2)
        self.data_old = create_buffer_and_store_dummy_data(self.transfersession_old.id)
        self.data_new = create_buffer_and_store_dummy_data(self.transfersession_new.id)

    def assertTransferSessionState(self, transfer_session, cleared):
        transfer_session.refresh_from_db()
        self.assertNotEqual(cleared, transfer_session.buffer_set.all().exists())
        self.assertNotEqual(cleared, transfer_session.recordmaxcounterbuffer_set.all().exists())
        self.assertNotEqual(cleared, transfer_session.active)

    def assertTransferSessionIsCleared(self, transfer_session):
        self.assertTransferSessionState(transfer_session, True)

    def assertTransferSessionIsNotCleared(self, transfer_session):
        self.assertTransferSessionState(transfer_session, False)

    def assertSyncSessionState(self, sync_session, active):
        sync_session.refresh_from_db()
        self.assertEqual(active, sync_session.active)

    def assertSyncSessionIsActive(self, sync_session):
        self.assertSyncSessionState(sync_session, True)

    def assertSyncSessionIsNotActive(self, sync_session):
        self.assertSyncSessionState(sync_session, False)

    def test_no_sessions_cleared(self):
        call_command("cleanupsyncs", expiration=48)
        self.assertTransferSessionIsNotCleared(self.transfersession_old)
        self.assertSyncSessionIsActive(self.syncsession_old)
        self.assertTransferSessionIsNotCleared(self.transfersession_new)
        self.assertSyncSessionIsActive(self.syncsession_new)

    def test_some_sessions_cleared(self):
        call_command("cleanupsyncs", expiration=6)
        self.assertTransferSessionIsCleared(self.transfersession_old)
        self.assertSyncSessionIsNotActive(self.syncsession_old)
        self.assertTransferSessionIsNotCleared(self.transfersession_new)
        self.assertSyncSessionIsActive(self.syncsession_new)

    def test_sync_session_handling(self):
        _, old_sync_new_transfer = _create_sessions(2, sync_session=self.syncsession_old)
        create_buffer_and_store_dummy_data(old_sync_new_transfer.id)
        call_command("cleanupsyncs", expiration=6)
        self.assertTransferSessionIsCleared(self.transfersession_old)
        self.assertTransferSessionIsNotCleared(old_sync_new_transfer)
        self.assertSyncSessionIsActive(self.syncsession_old)
        self.assertTransferSessionIsNotCleared(self.transfersession_new)
        self.assertSyncSessionIsActive(self.syncsession_new)

    def test_all_sessions_cleared(self):
        call_command("cleanupsyncs", expiration=1)
        self.assertTransferSessionIsCleared(self.transfersession_old)
        self.assertSyncSessionIsNotActive(self.syncsession_old)
        self.assertTransferSessionIsCleared(self.transfersession_new)
        self.assertSyncSessionIsNotActive(self.syncsession_new)

    def test_filtering_sessions_cleared(self):
        call_command("cleanupsyncs", ids=[self.syncsession_old.id], expiration=0)
        self.assertTransferSessionIsCleared(self.transfersession_old)
        self.assertSyncSessionIsNotActive(self.syncsession_old)
        self.assertTransferSessionIsNotCleared(self.transfersession_new)
        self.assertSyncSessionIsActive(self.syncsession_new)

    def test_multiple_ids_as_list(self):
        ids = [self.syncsession_old.id, self.syncsession_new.id]
        call_command("cleanupsyncs", ids=ids, expiration=0)
        self.assertTransferSessionIsCleared(self.transfersession_old)
        self.assertSyncSessionIsNotActive(self.syncsession_old)
        self.assertTransferSessionIsCleared(self.transfersession_new)
        self.assertSyncSessionIsNotActive(self.syncsession_new)
