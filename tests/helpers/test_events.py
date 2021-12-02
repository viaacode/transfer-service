#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from cloudevents.events import EventOutcome

from app.helpers.events import create_event
from tests.resources import (
    transfer_message,
)


def test_create_event():
    """Check if the correct event is created."""
    event = create_event(transfer_message, "message", EventOutcome.WARNING, "cor_id")
    assert event.get_data() == {
        "message": "message",
        "outcome": EventOutcome.WARNING,
        "source": transfer_message["source"]["url"],
        "destination": transfer_message["destination"]["path"],
        "host": transfer_message["destination"]["host"],
    }
    assert event.correlation_id == "cor_id"
