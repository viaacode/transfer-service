#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from app.helpers.events import create_event
from app.helpers.message_parser import parse_validate_json
from tests.resources import (
    transfer_message,
)


def test_create_event():
    """Check if the correct event is created."""
    transfer_message_json = parse_validate_json(transfer_message)
    event = create_event(transfer_message_json, "message", "outcome")
    assert event == {
        "message": "message",
        "outcome": "outcome",
        "source": transfer_message_json["source"]["url"],
        "destination": transfer_message_json["destination"]["path"],
        "host": transfer_message_json["destination"]["host"],
    }
