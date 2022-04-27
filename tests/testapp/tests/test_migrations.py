import uuid

import pytest
from django.conf import settings
from django.db import connection
from django.db.utils import IntegrityError
from django.test import TestCase
from django.test import TransactionTestCase
from django.utils import timezone

from .helpers import TestMigrationsMixin


@pytest.mark.skipif(not settings.MORANGO_TEST_POSTGRESQL, reason="Only postgres")
class MorangoNullableMigrationTest(TestMigrationsMixin, TestCase):
    """
    Test migration that applies nullable status to `transfer_stage` and `transfer_stage_status`
    """

    app = "morango"
    migrate_from = "0018_auto_20210714_2216"
    migrate_to = "0020_postgres_fix_nullable"

    def setUpBeforeMigration(self, apps):
        # simulate as if 0018_auto_20210714_2216 hadn't applied Nullablity to the columns,
        # a change which we added after the migration might have run on other
        SyncSession = apps.get_model("morango", "SyncSession")

        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE morango_transfersession ALTER COLUMN transfer_stage SET NOT NULL")
            cursor.execute("ALTER TABLE morango_transfersession ALTER COLUMN transfer_stage_status SET NOT NULL")

        self.sync_session = SyncSession.objects.create(
            id=uuid.uuid4().hex,
            profile="facilitydata",
            last_activity_timestamp=timezone.now(),
        )

    def test_nullable(self):
        TransferSession = self.apps.get_model("morango", "TransferSession")

        try:
            transfer_session = TransferSession.objects.create(
                id=uuid.uuid4().hex,
                sync_session_id=self.sync_session.id,
                push=True,
                last_activity_timestamp=timezone.now(),
                transfer_stage=None,
                transfer_stage_status=None,
            )
        except IntegrityError:
            self.fail("Couldn't create TransferSession with nullable fields")

        self.assertIsNone(transfer_session.transfer_stage)
        self.assertIsNone(transfer_session.transfer_stage_status)


@pytest.mark.skipif(not settings.MORANGO_TEST_POSTGRESQL, reason="Only postgres")
class SkipIfExistsMigrationTest(TestMigrationsMixin, TransactionTestCase):
    """
    Test migration that creates an index on the partition field of the Store model
    """

    app = "morango"
    migrate_from = "0020_postgres_fix_nullable"
    migrate_to = "0021_store_partition_index_create"

    def setUpBeforeMigration(self, apps):
        # simulate as if we already created an index on the partition field of the Store model
        with connection.cursor() as cursor:
            cursor.execute('CREATE INDEX "idx_morango_store_partition" ON "morango_store" ("partition" text_pattern_ops);')

    def test_runs(self):
        """
        A dummy test, as the main point here is to make sure that the migration runs properly
        with the index already existing
        """
        self.assertTrue(True)
