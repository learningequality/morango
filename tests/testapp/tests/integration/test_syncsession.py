import mock
from facility_profile.models import InteractionLog
from facility_profile.models import SummaryLog
from requests.exceptions import Timeout

from morango.errors import MorangoError
from morango.models.core import Buffer
from morango.models.core import TransferSession

from .helpers import BasePushPullClientTestCase
from .helpers import second_environment


class PushPullClientTestCase(BasePushPullClientTestCase):
    def setUp(self):
        super(PushPullClientTestCase, self).setUp()
        self.last_session_activity = self.session.last_activity_timestamp
        self.last_transfer_activity = None

    def assertLastActivityUpdate(self, transfer_session=None):
        """A signal callable that asserts `last_activity_timestamp`s are updated"""
        if self.last_transfer_activity is not None:
            self.assertLessEqual(
                self.last_transfer_activity, transfer_session.last_activity_timestamp
            )
            self.assertLessEqual(
                self.last_session_activity,
                transfer_session.sync_session.last_activity_timestamp,
            )
        self.last_transfer_activity = transfer_session.last_activity_timestamp
        self.last_session_activity = transfer_session.sync_session.last_activity_timestamp

    def test_push(self):
        for _ in range(5):
            SummaryLog.objects.create(user=self.local_user)
            InteractionLog.objects.create(user=self.local_user)

        with second_environment():
            self.assertEqual(0, SummaryLog.objects.filter(user=self.remote_user).count())
            self.assertEqual(0, InteractionLog.objects.filter(user=self.remote_user).count())

        client = self.client.get_push_client()
        client.signals.queuing.completed.connect(self.assertLastActivityUpdate)
        client.signals.transferring.in_progress.connect(self.assertLastActivityUpdate)
        client.signals.dequeuing.completed.connect(self.assertLastActivityUpdate)

        self.assertEqual(0, TransferSession.objects.filter(active=True).count())
        client.initialize(self.filter)
        self.assertEqual(1, TransferSession.objects.filter(active=True).count())
        transfer_session = client.context.transfer_session
        self.assertNotEqual(0, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        self.assertLessEqual(1, Buffer.objects.filter(transfer_session=transfer_session).count())
        client.run()
        self.assertNotEqual(0, transfer_session.records_transferred)
        client.finalize()
        self.assertEqual(0, Buffer.objects.filter(transfer_session=transfer_session).count())
        self.assertEqual(0, TransferSession.objects.filter(active=True).count())

        with second_environment():
            self.assertEqual(5, SummaryLog.objects.filter(user=self.remote_user).count())
            self.assertEqual(5, InteractionLog.objects.filter(user=self.remote_user).count())

    def test_pull(self):
        with second_environment():
            for _ in range(5):
                SummaryLog.objects.create(user=self.remote_user)
                InteractionLog.objects.create(user=self.remote_user)

        self.assertEqual(0, SummaryLog.objects.filter(user=self.local_user).count())
        self.assertEqual(0, InteractionLog.objects.filter(user=self.local_user).count())

        client = self.client.get_pull_client()
        client.signals.queuing.completed.connect(self.assertLastActivityUpdate)
        client.signals.transferring.in_progress.connect(self.assertLastActivityUpdate)
        client.signals.dequeuing.completed.connect(self.assertLastActivityUpdate)

        self.assertEqual(0, TransferSession.objects.filter(active=True).count())
        client.initialize(self.filter)
        self.assertEqual(1, TransferSession.objects.filter(active=True).count())
        transfer_session = client.context.transfer_session
        self.assertNotEqual(0, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        client.run()
        self.assertNotEqual(0, transfer_session.records_transferred)
        self.assertLessEqual(1, Buffer.objects.filter(transfer_session=transfer_session).count())
        client.finalize()
        self.assertEqual(0, Buffer.objects.filter(transfer_session=transfer_session).count())
        self.assertEqual(0, TransferSession.objects.filter(active=True).count())

        self.assertEqual(5, SummaryLog.objects.filter(user=self.local_user).count())
        self.assertEqual(5, InteractionLog.objects.filter(user=self.local_user).count())

    def test_full_flow_and_repeat(self):
        with second_environment():
            for _ in range(5):
                SummaryLog.objects.create(user=self.remote_user)
                InteractionLog.objects.create(user=self.remote_user)

        self.assertEqual(0, SummaryLog.objects.filter(user=self.local_user).count())
        self.assertEqual(0, InteractionLog.objects.filter(user=self.local_user).count())

        # first pull
        pull_client = self.client.get_pull_client()
        pull_client.initialize(self.filter)
        transfer_session = pull_client.context.transfer_session
        self.assertNotEqual(0, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        pull_client.run()
        self.assertNotEqual(0, transfer_session.records_transferred)
        pull_client.finalize()

        # sanity check pull worked
        self.assertEqual(5, SummaryLog.objects.filter(user=self.local_user).count())
        self.assertEqual(5, InteractionLog.objects.filter(user=self.local_user).count())

        # now do a push after pull, but nothing to actually transfer
        push_client = self.client.get_push_client()
        push_client.initialize(self.filter)
        transfer_session = push_client.context.transfer_session
        self.assertEqual(0, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        push_client.run()
        self.assertEqual(0, transfer_session.records_transferred)
        push_client.finalize()

        # second pass for pull, only do initialize to make sure nothing gets queued for sync
        second_pull_client = self.client.get_pull_client()
        second_pull_client.initialize(self.filter)
        transfer_session = second_pull_client.context.transfer_session
        self.assertEqual(0, transfer_session.records_total)

    def test_second_pull_with_instance_id_no_longer_in_store(self):
        with second_environment():
            SummaryLog.objects.create(user=self.remote_user)
            summ_log_id = SummaryLog.objects.first().id

        self.assertEqual(0, SummaryLog.objects.filter(id=summ_log_id).count())

        # first pull
        pull_client = self.client.get_pull_client()
        pull_client.initialize(self.filter)
        transfer_session = pull_client.context.transfer_session
        self.assertEqual(1, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        pull_client.run()
        self.assertEqual(1, transfer_session.records_transferred)
        pull_client.finalize()

        # sanity check pull worked
        self.assertEqual(1, SummaryLog.objects.filter(id=summ_log_id).count())

        # update the log record locally
        SummaryLog.objects.filter(id=summ_log_id).update(content_id="a" * 32)

        # now start a push, to serialize the local record, but don't actually push
        second_push_client = self.client.get_push_client()
        second_push_client.initialize(self.filter)

        # now do another pull, which shouldn't have anything new to bring down
        second_pull_client = self.client.get_pull_client()
        second_pull_client.initialize(self.filter)
        transfer_session = second_pull_client.context.transfer_session
        self.assertEqual(0, transfer_session.records_total)

    def test_resume(self):
        # create data
        for _ in range(5):
            SummaryLog.objects.create(user=self.local_user)
            InteractionLog.objects.create(user=self.local_user)

        with second_environment():
            self.assertEqual(0, SummaryLog.objects.filter(user=self.remote_user).count())
            self.assertEqual(0, InteractionLog.objects.filter(user=self.remote_user).count())

        # use client to start a sync
        client = self.client.get_push_client()
        self.assertEqual(0, TransferSession.objects.filter(active=True).count())
        client.initialize(self.filter)
        self.assertEqual(1, TransferSession.objects.filter(active=True).count())
        transfer_session = client.context.transfer_session
        self.assertNotEqual(0, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        self.assertLessEqual(1, Buffer.objects.filter(transfer_session=transfer_session).count())

        # simulate timeout
        with mock.patch("morango.sync.operations.NetworkOperation.put_buffers") as mock_put_buffers:
            mock_put_buffers.side_effect = Timeout("Network disconnected")
            with self.assertRaises(MorangoError):
                client.run()

        self.assertEqual(0, transfer_session.records_transferred)

        # get resume client and retry
        resume_client = self.conn.resume_sync_session(client.sync_session.id).get_push_client()
        self.assertEqual(1, TransferSession.objects.filter(active=True).count())
        resume_client.initialize(self.filter)
        self.assertEqual(1, TransferSession.objects.filter(active=True).count())
        transfer_session = resume_client.context.transfer_session
        self.assertNotEqual(0, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        self.assertLessEqual(1, Buffer.objects.filter(transfer_session=transfer_session).count())
        self.assertEqual(0, transfer_session.records_transferred)
        resume_client.run()
        self.assertNotEqual(0, transfer_session.records_transferred)

        resume_client.finalize()
        self.assertEqual(0, Buffer.objects.filter(transfer_session=transfer_session).count())
        self.assertEqual(0, TransferSession.objects.filter(active=True).count())

        with second_environment():
            self.assertEqual(5, SummaryLog.objects.filter(user=self.remote_user).count())
            self.assertEqual(5, InteractionLog.objects.filter(user=self.remote_user).count())

    def test_create_sync_delete_sync_recreate_sync(self):
        with second_environment():
            SummaryLog.objects.create(user=self.remote_user)
            summ_log = SummaryLog.objects.first()
            summ_log_id = summ_log.id
            content_id = summ_log.content_id

        self.assertEqual(0, SummaryLog.objects.filter(id=summ_log_id).count())

        # first pull
        pull_client = self.client.get_pull_client()
        pull_client.initialize(self.filter)
        transfer_session = pull_client.context.transfer_session
        self.assertEqual(1, transfer_session.records_total)
        self.assertEqual(0, transfer_session.records_transferred)
        pull_client.run()
        self.assertEqual(1, transfer_session.records_transferred)
        pull_client.finalize()

        # sanity check pull worked
        self.assertEqual(1, SummaryLog.objects.filter(id=summ_log_id).count())

        with second_environment():
            SummaryLog.objects.get(id=summ_log_id).delete()

        # now do another pull, which should pull in the deletion
        second_pull_client = self.client.get_pull_client()
        second_pull_client.initialize(self.filter)
        second_pull_client.run()
        second_pull_client.finalize()
        self.assertEqual(0, SummaryLog.objects.filter(id=summ_log_id).count())

        with second_environment():
            sum_log = SummaryLog.objects.create(user=self.remote_user, content_id=content_id)
            self.assertEqual(sum_log.id, summ_log_id)

        # now do another pull, which should pull in the recreation
        third_pull_client = self.client.get_pull_client()
        third_pull_client.initialize(self.filter)
        third_pull_client.run()
        third_pull_client.finalize()
        self.assertEqual(1, SummaryLog.objects.filter(id=summ_log_id).count())
