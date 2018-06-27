"""
This module contains constants representing the possible stages of a transfer session.
"""
from django.utils.translation import ugettext_lazy as _

STARTED = "started"
QUEUING = "queuing"
DEQUEUING = "dequeuing"
PUSHING = "pushing"
PULLING = "pulling"
COMPLETED = "completed"

choices = (
    (STARTED, _("Started")),
    (QUEUING, _("Queuing")),
    (DEQUEUING, _("Dequeuing")),
    (PUSHING, _("Pushing")),
    (PULLING, _("Pulling")),
    (COMPLETED, _("Completed")),
)
