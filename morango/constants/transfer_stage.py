"""
This module contains constants representing the possible stages of a transfer session.
"""
from django.utils.translation import ugettext_lazy as _

INITIALIZING = "initializing"
SERIALIZING = "serializing"
QUEUING = "queuing"
DEQUEUING = "dequeuing"
DESERIALIZING = "deserializing"
TRANSFERRING = "transferring"
CLEANUP = "cleanup"

CHOICES = (
    (INITIALIZING, _("Initializing")),
    (SERIALIZING, _("Serializing")),
    (QUEUING, _("Queuing")),
    (TRANSFERRING, _("Transferring")),
    (DEQUEUING, _("Dequeuing")),
    (DESERIALIZING, _("Deserializing")),
    (CLEANUP, _("Cleanup")),
)

PRECEDENCE = {
    SERIALIZING: 1,
    QUEUING: 2,
    TRANSFERRING: 3,
    DEQUEUING: 4,
    DESERIALIZING: 5,
    CLEANUP: 6,
}


def precedence(stage_key):
    """
    :param stage_key: The stage constant
    """
    try:
        return PRECEDENCE[stage_key]
    except KeyError:
        return None


class stage(str):
    """
    Modeled after celery's status utilities
    """

    def __gt__(self, other):
        return precedence(self) < precedence(other)

    def __ge__(self, other):
        return precedence(self) <= precedence(other)

    def __lt__(self, other):
        return precedence(self) > precedence(other)

    def __le__(self, other):
        return precedence(self) >= precedence(other)
