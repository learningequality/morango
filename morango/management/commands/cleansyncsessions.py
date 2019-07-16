from django.core.management.base import BaseCommand

from morango.models.core import SyncSession


class Command(BaseCommand):
    help = "Cleans up syncing session data."

    def handle(self, *args, **options):
        self.stdout.write(
            "Cleaning out stale sync session data. This may take a while..."
        )
        SyncSession.objects.filter(active=True).delete(soft=True)
        self.stdout.write("Finished cleaning up sync session data.")
