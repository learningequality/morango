import datetime

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from morango.models import TransferSession


class Command(BaseCommand):
    help = "Closes and cleans up the data for any incomplete sync sessions older than a certain number of hours."

    def add_arguments(self, parser):
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

        # retrieve all sessions still marked as active but with no activity since the cutoff
        oldsessions = TransferSession.objects.filter(
            last_activity_timestamp__lt=cutoff, active=True
        )
        sesscount = oldsessions.count()

        # loop over the stale sessions one by one to close them out
        for i in range(sesscount):
            sess = oldsessions[0]
            print(
                "Session {} of {}: deleting {} Buffers and {} RMC Buffers...".format(
                    i + 1,
                    sesscount,
                    sess.buffer_set.all().count(),
                    sess.recordmaxcounterbuffer_set.all().count(),
                )
            )

            # delete buffer data and mark session as inactive
            with transaction.atomic():
                sess.delete_buffers()
                sess.active = False
                sess.save()
                sess.sync_session.active = False
                sess.sync_session.save()
