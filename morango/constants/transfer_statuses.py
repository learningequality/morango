"""
This module contains constants representing the possible statuses of a transfer session stage.
"""
from django.utils.translation import ugettext_lazy as _

PENDING = "pending"
STARTED = "started"
COMPLETED = "completed"
ERRORED = "errored"

CHOICES = (
    (PENDING, _("Pending")),
    (STARTED, _("Started")),
    (COMPLETED, _("Completed")),
    (ERRORED, _("Errored")),
)

ALL = {stage for stage, _ in CHOICES}

IN_PROGRESS_STATES = (
    PENDING,
    STARTED,
)

FINISHED_STATES = (COMPLETED, ERRORED)
