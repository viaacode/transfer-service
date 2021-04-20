#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json


class InvalidMessageException(Exception):
    def __init__(self, message):
        self.message = message


def _validate_json(message: dict):
    """Validate if the message contains all the needed information.

    Args:
        message: The JSON message.

    Raises:
        InvalidMessageException: If the message misses mandatory key(s)
    """
    try:
        message["source"]["domain"]["name"]
        message["source"]["object"]["key"]
        message["source"]["bucket"]["name"]
        message["destination"]["path"]
    except KeyError as ke:
        raise InvalidMessageException(
            f"Invalid transfer message: {ke} is a mandatory key"
        )


def parse_validate_json(message: bytes) -> dict:
    """Parse and validate the JSON message.

    Args:
        message: The JSON message.

    Returns:
        The parsed message as a dict.

    Raises:
        InvalidMessageException: If the message is not valid JSON
            or if the JSON misses mandatory keys.
    """
    try:
        json_data = json.loads(message)
    except json.decoder.JSONDecodeError as jde:
        raise InvalidMessageException(f'Not valid JSON: "{jde}"')

    _validate_json(json_data)

    return json_data
