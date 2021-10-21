import datetime
import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from morango.models import SyncSession
from morango.models import TransferSession


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Closes and cleans up the data for any incomplete sync sessions older than a certain number of hours."

    def add_arguments(self, parser):
        parser.add_argument(
            "--ids",
            type=lambda ids: ids.split(","),
            default=None,
            help="Comma separated list of SyncSession IDs to filter against"
        )
        parser.add_argument(
            "--expiration",
            action="store",
            type=int,
            default=6,
            help="Number of hours of inactivity after which a session should be considered stale",
        )

    def handle(self, *args, **options):

        # establish the cutoff time and date for stale sessions
        cutoff = timezone.now() - datetime.timedelta(hours=options["expiration"])

        sync_sessions = SyncSession.objects.filter(active=True)

        # if ids arg was passed, filter down sessions to only those IDs if included by expiration filter
        if options["ids"]:
            sync_sessions = sync_sessions.filter(id__in=options["ids"])

        # retrieve all sessions still marked as active but with no activity since the cutoff
        transfer_sessions = TransferSession.objects.filter(
            sync_session_id__in=sync_sessions.values("id"),
            active=True,
            last_activity_timestamp__lt=cutoff,
        )

        transfer_count = transfer_sessions.count()

        # loop over the stale sessions one by one to close them out
        for i in range(transfer_count):
            transfer_session = transfer_sessions[0]
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

        sync_count = sync_sessions.count()

        # finally loop over sync sessions and close out if there are no other active transfer sessions
        for i in range(sync_count):
            sync_session = sync_sessions[0]
            if not sync_session.transfersession_set.filter(active=True).exists():
                logger.info(
                    "Closing SyncSession {} of {}".format(
                        i + 1,
                        sync_count,
                    )
                )
                sync_session.active = False
                sync_session.save()
