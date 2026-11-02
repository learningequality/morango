import datetime
import logging
import uuid

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from morango.models import SyncSession
from morango.models import TransferSession
from morango.models.certificates import Filter

logger = logging.getLogger(__name__)

# the number of transfer sessions to query at once, which keeps each query below the limit of 999
# bound parameters in older versions of SQLite
TRANSFER_SESSION_BATCH_SIZE = 500


class Command(BaseCommand):
    help = "Closes and cleans up the data for any incomplete sync sessions older than a certain number of hours."

    def add_arguments(self, parser):
        parser.add_argument(
            "--ids",
            type=lambda ids: ids.split(","),
            default=None,
            help="Comma separated list of SyncSession IDs to filter against",
        )
        parser.add_argument(
            "--expiration",
            action="store",
            type=int,
            default=6,
            help="Number of hours of inactivity after which a session should be considered stale",
        )
        parser.add_argument(
            "--client-instance-id",
            type=uuid.UUID,
            default=None,
            help="Filters the SyncSession models to those with matching 'client_instance_id'",
        )
        parser.add_argument(
            "--server-instance-id",
            type=uuid.UUID,
            default=None,
            help="Filters the SyncSession models to those with matching 'server_instance_id'",
        )
        parser.add_argument(
            "--sync-filter",
            type=str,
            default=None,
            help="Filters the TransferSession models to those with 'filters' starting with 'sync_filter', or with the same partitions in any order",
        )
        parser.add_argument(
            "--push",
            type=bool,
            default=None,
            help="Filters the TransferSession models to those with 'push' set to True",
        )
        parser.add_argument(
            "--pull",
            type=bool,
            default=None,
            help="Filters the TransferSession models to those with 'push' set to False",
        )

    @staticmethod
    def _matching_ids(transfer_sessions, sync_filter):
        """
        Returns the IDs of the transfer sessions whose filter starts with `sync_filter`, such as a
        dataset ID, or has the same partitions in any order. The partition order of a filter can
        change between versions, such as when `Scope` began listing read-write partitions last.
        """
        partitions = Filter(sync_filter)
        return [
            transfer_session_id
            for transfer_session_id, transfer_session_filter in transfer_sessions.values_list(
                "id", "filter"
            )
            if transfer_session_filter.startswith(sync_filter)
            or Filter(transfer_session_filter) == partitions
        ]

    def handle(self, *args, **options):

        # establish the cutoff time and date for stale sessions
        cutoff = timezone.now() - datetime.timedelta(hours=options["expiration"])

        sync_sessions = SyncSession.objects.filter(active=True)

        # if ids arg was passed, filter down sessions to only those IDs
        # if included by expiration filter
        if options["ids"]:
            sync_sessions = sync_sessions.filter(id__in=options["ids"])

        if options["client_instance_id"]:
            sync_sessions = sync_sessions.filter(client_instance_id=options["client_instance_id"])

        if options["server_instance_id"]:
            sync_sessions = sync_sessions.filter(server_instance_id=options["server_instance_id"])

        # retrieve all sessions still marked as active but with no activity since the cutoff
        transfer_sessions = TransferSession.objects.filter(
            sync_session_id__in=sync_sessions.values("id"),
            active=True,
            last_activity_timestamp__lt=cutoff,
        )

        if options["push"] and not options["pull"]:
            transfer_sessions = transfer_sessions.filter(push=True)

        if options["pull"] and not options["push"]:
            transfer_sessions = transfer_sessions.filter(push=False)

        # ordering this last to keep manual filtering to a minimum
        if options["sync_filter"]:
            transfer_session_ids = self._matching_ids(transfer_sessions, options["sync_filter"])
        else:
            transfer_session_ids = list(transfer_sessions.values_list("id", flat=True))

        transfer_count = len(transfer_session_ids)

        # loop over the stale sessions one by one to close them out, querying them in batches
        for offset in range(0, transfer_count, TRANSFER_SESSION_BATCH_SIZE):
            batch_ids = transfer_session_ids[offset : offset + TRANSFER_SESSION_BATCH_SIZE]
            batch = TransferSession.objects.filter(id__in=batch_ids)
            for i, transfer_session in enumerate(batch, start=offset):
                logger.info(
                    "TransferSession {} of {}: deleting {} Buffers and {} RMC Buffers...".format(
                        i + 1,
                        transfer_count,
                        transfer_session.buffer_set.all().count(),
                        transfer_session.recordmaxcounterbuffer_set.all().count(),
                    )
                )

                # delete buffer data and mark as inactive
                with transaction.atomic():
                    transfer_session.delete_buffers()
                    transfer_session.active = False
                    transfer_session.save()

        # in order to close a sync session, it must have no active transfer sessions
        # and must have no activity since the cutoff
        sync_sessions = sync_sessions.filter(
            last_activity_timestamp__lt=cutoff,
        ).exclude(
            transfersession__active=True,
        )
        sync_count = sync_sessions.count()

        for i, sync_session in enumerate(sync_sessions):
            logger.info(
                "Closing SyncSession {} of {}".format(
                    i + 1,
                    sync_count,
                )
            )
            sync_session.active = False
            sync_session.save()
