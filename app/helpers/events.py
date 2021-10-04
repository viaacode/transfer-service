#!/usr/bin/env python3
# -*- coding: utf-8 -*-


def create_event(
    transfer_message: dict, event_message: str, event_outcome: str
) -> dict:
    """Create an event to produce on a topic

    Args:
        transfer_message: The incoming transfer message.
        event_message: The message of the event.
        event_outcome: The outcome of the event.
    Returns:
        The event
    """
    return {
        "message": event_message,
        "outcome": event_outcome,
        "source": transfer_message["source"]["url"],
        "destination": transfer_message["destination"]["path"],
        "host": transfer_message["destination"]["host"],
    }
