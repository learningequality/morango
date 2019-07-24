"""
This module contains constants representing the possible stages of a transfer session.
"""
STARTED = "started"
QUEUING = "queuing"
DEQUEUING = "dequeuing"
PUSHING = "pushing"
PULLING = "pulling"
COMPLETED = "completed"
ERROR = "error"

choices = (
    (STARTED, "Started"),
    (QUEUING, "Queuing"),
    (DEQUEUING, "Dequeuing"),
    (PUSHING, "Pushing"),
    (PULLING, "Pulling"),
    (COMPLETED, "Completed"),
    (ERROR, "Error"),
)
