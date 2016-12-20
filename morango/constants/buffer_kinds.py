"""
This module contains constants representing the kinds of Buffers.
"""
from django.utils.translation import ugettext_lazy as _

OUTGOING = "outgoing"
INCOMING = "incoming"

# the ordering of kinds in the following tuple corresponds to their level in the hierarchy tree
choices = (
    (OUTGOING, _("Outgoing")),
    (INCOMING, _("Incoming")),
)
