import datetime
import logging
import uuid

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
            help="Filters the TransferSession models to those with 'filters' starting with 'sync_filter'",
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

        if options["sync_filter"]:
            transfer_sessions = transfer_sessions.filter(filter__startswith=options["sync_filter"])

        if options["push"] and not options["pull"]:
            transfer_sessions = transfer_sessions.filter(push=True)

        if options["pull"] and not options["push"]:
            transfer_sessions = transfer_sessions.filter(push=False)

        transfer_count = transfer_sessions.count()

        # loop over the stale sessions one by one to close them out
        for i, transfer_session in enumerate(transfer_sessions):
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
