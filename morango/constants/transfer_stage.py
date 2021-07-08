"""
This module contains constants representing the possible stages of a transfer session.
"""
from django.utils.translation import ugettext_lazy as _

INITIALIZING = "initializing"
SERIALIZING = "serializing"
QUEUING = "queuing"
TRANSFERRING = "transferring"
DEQUEUING = "dequeuing"
DESERIALIZING = "deserializing"
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

ALL = {stage for stage, _ in CHOICES}

PRECEDENCE = {
    INITIALIZING: 10,
    SERIALIZING: 20,
    QUEUING: 30,
    TRANSFERRING: 40,
    DEQUEUING: 50,
    DESERIALIZING: 60,
    CLEANUP: 70,
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
        return precedence(self) > precedence(other)

    def __ge__(self, other):
        return precedence(self) >= precedence(other)

    def __lt__(self, other):
        return precedence(self) < precedence(other)

    def __le__(self, other):
        return precedence(self) <= precedence(other)
