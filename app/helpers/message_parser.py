#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json

from cloudevents.events import AMQPBinding, Event


class InvalidMessageException(Exception):
    def __init__(self, message):
        self.message = message


def validate_transfer_message(message: dict) -> bool:
    """Validate if the message contains all the needed information.

    Args:
        message: The JSON message.

    Raises:
        InvalidMessageException: If the message misses mandatory key(s)
    """
    try:
        message["source"]["url"]
        message["source"]["headers"]
        message["destination"]["host"]
        message["destination"]["path"]
        message["destination"]["credentials"]
        message["outcome"]["pulsar-topic"]
    except KeyError as ke:
        raise InvalidMessageException(
            f"Invalid transfer message: {ke} is a mandatory key"
        )
    return True


def parse_incoming_message(properties, body: bytes) -> Event:
    """Parse the incoming message as a cloudevent.

    Args:
        properties: The RabbitMQ properties.
        body: The JSON message.

    Returns:
        The incoming message as a cloudevent.

    Raises:
        InvalidMessageException: If the message is not valid JSON.
    """
    try:
        incoming_event = AMQPBinding.from_protocol(properties, body)
    except json.decoder.JSONDecodeError as jde:
        raise InvalidMessageException(f'Not valid JSON: "{jde}"')

    return incoming_event
