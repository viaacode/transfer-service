#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from cloudevents import (
    Event,
    EventOutcome,
    EventAttributes,
)

from app import APP_NAME


def create_event(
    transfer_message: dict,
    event_message: str,
    event_outcome: EventOutcome,
    correlation_id: str,
) -> Event:
    """Create a cloudevent to produce on a topic.

    Args:
        transfer_message: The incoming transfer message.
        event_message: The message of the event.
        event_outcome: The outcome of the event.
    Returns:
        The event
    """
    attributes = EventAttributes(
        type=transfer_message["outcome"]["pulsar-topic"],
        source=APP_NAME,
        subject=f'{transfer_message["destination"]["host"]}/{transfer_message["destination"]["path"]}',
        outcome=event_outcome,
        correlation_id=correlation_id,
    )
    data = {
        "message": event_message,
        "outcome": event_outcome,
        "source": transfer_message["source"]["url"],
        "destination": transfer_message["destination"]["path"],
        "host": transfer_message["destination"]["host"],
    }
    return Event(attributes, data)
