import mock
import pickle
from django.test import SimpleTestCase
from django.test import TestCase

from ..helpers import create_dummy_store_data
from morango.constants import capabilities
from morango.constants import transfer_stage
from morango.constants import transfer_status
from morango.errors import MorangoContextUpdateError
from morango.models.certificates import Filter
from morango.models.core import SyncSession
from morango.models.core import TransferSession
from morango.sync.context import SessionContext
from morango.sync.context import LocalSessionContext
from morango.sync.context import NetworkSessionContext


class SessionContextTestCase(SimpleTestCase):
    def test_init__nothing(self):
        context = SessionContext()
        self.assertEqual(transfer_stage.INITIALIZING, context.stage)
        self.assertEqual(transfer_status.PENDING, context.stage_status)

    def test_init__capabilities__no_match(self):
        context = SessionContext(capabilities=["testing"])
        self.assertNotIn("testing", context.capabilities)

    @mock.patch("morango.sync.context.CAPABILITIES", {"testing"})
    def test_init__capabilities(self):
        context = SessionContext(capabilities=["testing"])
        self.assertIn("testing", context.capabilities)

    def test_init__no_transfer_session(self):
        sync_session = mock.Mock(spec=SyncSession)
        sync_filter = mock.Mock(spec=Filter)

        context = SessionContext(sync_session=sync_session, sync_filter=sync_filter, is_push=True)
        self.assertEqual(sync_session, context.sync_session)
        self.assertEqual(sync_filter, context.filter)
        self.assertTrue(context.is_push)
        self.assertFalse(context.is_pull)

    def test_init__with_transfer_session(self):
        sync_session = mock.Mock(spec=SyncSession)
        sync_filter = mock.Mock(spec=Filter)
        transfer_session = mock.Mock(
            spec=TransferSession,
            sync_session=sync_session,
            push=False,
            transfer_stage=transfer_stage.TRANSFERRING,
            transfer_stage_status=transfer_status.STARTED,
        )
        transfer_session.get_filter.return_value = sync_filter

        context = SessionContext(transfer_session=transfer_session)
        self.assertEqual(transfer_session, context.transfer_session)
        self.assertEqual(sync_session, context.sync_session)
        self.assertEqual(sync_filter, context.filter)
        self.assertFalse(context.is_push)
        self.assertTrue(context.is_pull)
        self.assertEqual(transfer_stage.TRANSFERRING, context.stage)
        self.assertEqual(transfer_status.STARTED, context.stage_status)

    def test_update__no_overwrite__transfer_session(self):
        sync_session = mock.Mock(spec=SyncSession)
        sync_filter = mock.Mock(spec=Filter)
        transfer_session = mock.Mock(
            spec=TransferSession,
            sync_session=sync_session,
            push=False,
            transfer_stage=transfer_stage.TRANSFERRING,
            transfer_stage_status=transfer_status.STARTED,
        )
        transfer_session.get_filter.return_value = sync_filter

        context = SessionContext(transfer_session=transfer_session)
        with self.assertRaises(MorangoContextUpdateError):
            context.update(transfer_session=transfer_session)

    def test_update__no_overwrite__filter(self):
        sync_filter = mock.Mock(spec=Filter)
        context = SessionContext(sync_filter=sync_filter)

        with self.assertRaises(MorangoContextUpdateError):
            context.update(sync_filter=sync_filter)

    def test_update__no_overwrite__push(self):
        context = SessionContext(is_push=True)

        with self.assertRaises(MorangoContextUpdateError):
            context.update(is_push=False)

    @mock.patch("morango.sync.context.CAPABILITIES", {"testing"})
    def test_update__basic(self):
        context = SessionContext()

        sync_filter = mock.Mock(spec=Filter)
        context.update(
            sync_filter=sync_filter,
            is_push=True,
            stage=transfer_stage.TRANSFERRING,
            stage_status=transfer_status.STARTED,
            capabilities={"testing"}
        )
        self.assertEqual(sync_filter, context.filter)
        self.assertTrue(context.is_push)
        self.assertFalse(context.is_pull)
        self.assertEqual(transfer_stage.TRANSFERRING, context.stage)
        self.assertEqual(transfer_status.STARTED, context.stage_status)

    @mock.patch("morango.sync.context.CAPABILITIES", {"testing"})
    def test_update__with_transfer_session(self):
        context = SessionContext(
            capabilities={"testing"}
        )

        sync_session = mock.Mock(spec=SyncSession)
        sync_filter = mock.Mock(spec=Filter)
        transfer_session = mock.Mock(
            spec=TransferSession,
            sync_session=sync_session,
            push=True,
        )
        transfer_session.get_filter.return_value = sync_filter
        context.update(transfer_session=transfer_session)
        self.assertEqual(sync_filter, context.filter)
        self.assertTrue(context.is_push)
        self.assertFalse(context.is_pull)


class LocalSessionContextTestCase(SimpleTestCase):
    @mock.patch("morango.sync.context.CAPABILITIES", {"testing"})
    @mock.patch("morango.sync.context.parse_capabilities_from_server_request")
    def test_init(self, mock_parse_capabilities):
        mock_parse_capabilities.return_value = {"testing"}
        request = mock.Mock(spec="django.http.request.HttpRequest")
        context = LocalSessionContext(request=request)

        self.assertEqual(request, context.request)
        self.assertTrue(context.is_server)
        self.assertIn("testing", context.capabilities)

    def test_update(self):
        transfer_session = mock.Mock(
            spec=TransferSession,
            transfer_stage=transfer_stage.TRANSFERRING,
            transfer_stage_status=transfer_status.STARTED,
        )
        context = LocalSessionContext()
        self.assertNotEqual(transfer_stage.TRANSFERRING, context.stage)
        self.assertNotEqual(transfer_status.STARTED, context.stage_status)

        context.update(transfer_session=transfer_session)
        self.assertEqual(transfer_stage.TRANSFERRING, context.stage)
        self.assertEqual(transfer_status.STARTED, context.stage_status)

        transfer_session.refresh_from_db.assert_called_once()
        transfer_session.update_state.assert_called_once_with(
            stage=transfer_stage.TRANSFERRING, stage_status=transfer_status.STARTED
        )


class NetworkSessionContextTestCase(SimpleTestCase):
    @mock.patch("morango.sync.context.CAPABILITIES", {"testing"})
    def test_init(self):
        conn = mock.Mock(
            spec="morango.sync.syncsession.NetworkSyncConnection",
            server_info=mock.Mock(),
        )
        conn.server_info.get.return_value = {"testing"}
        context = NetworkSessionContext(conn)
        self.assertEqual(conn, context.connection)
        self.assertIn("testing", context.capabilities)


class ContextPicklingTestCase(TestCase):
    def test_basic(self):
        data = create_dummy_store_data()
        transfer_session = data["sc"].current_transfer_session
        transfer_session.filter = "abc123"
        transfer_session.save()

        context = SessionContext(transfer_session=transfer_session)
        pickled_context = pickle.dumps(context)
        unpickled_context = pickle.loads(pickled_context)
        self.assertIsNotNone(context.transfer_session)
        self.assertEqual(context.filter, unpickled_context.filter)
        self.assertEqual(context.is_push, unpickled_context.is_push)
        self.assertEqual(context.stage, unpickled_context.stage)
        self.assertEqual(context.stage_status, unpickled_context.stage_status)
        self.assertEqual(context.capabilities, unpickled_context.capabilities)

    @mock.patch("morango.sync.context.parse_capabilities_from_server_request")
    def test_local(self, mock_parse_capabilities):
        request = mock.Mock(spec="django.http.request.HttpRequest")
        mock_parse_capabilities.return_value = {}

        context = LocalSessionContext(request=request)
        pickled_context = pickle.dumps(context)
        unpickled_context = pickle.loads(pickled_context)
        self.assertEqual(context.is_push, unpickled_context.is_push)
        self.assertEqual(context.stage, unpickled_context.stage)
        self.assertEqual(context.stage_status, unpickled_context.stage_status)
        self.assertEqual(context.capabilities, unpickled_context.capabilities)
        self.assertEqual(context.is_server, unpickled_context.is_server)

