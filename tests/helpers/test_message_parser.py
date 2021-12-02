#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from unittest.mock import patch, MagicMock
import pytest

from app.helpers.message_parser import (
    parse_incoming_message,
    validate_transfer_message,
    InvalidMessageException,
)
from tests.resources import (
    transfer_message,
    transfer_message_empty,
    transfer_message_no_source,
    transfer_message_no_destination,
    transfer_message_no_url,
    transfer_message_no_headers,
    transfer_message_no_host,
    transfer_message_no_path,
    transfer_message_no_credentials,
    transfer_message_no_outcome,
)


INVALID_JSON_MESSAGES = [
    (transfer_message_empty, "source"),
    (transfer_message_no_source, "source"),
    (transfer_message_no_destination, "destination"),
    (transfer_message_no_url, "url"),
    (transfer_message_no_headers, "headers"),
    (transfer_message_no_host, "host"),
    (transfer_message_no_path, "path"),
    (transfer_message_no_credentials, "credentials"),
    (transfer_message_no_outcome, "outcome"),
]


@patch("app.helpers.message_parser.AMQPBinding")
def test_parse_incoming_message(amqp_binding_mock):
    event_mock = MagicMock()
    amqp_binding_mock.from_protocol.return_value = event_mock
    properties_mock = MagicMock()

    returned_event = parse_incoming_message(properties_mock, b"body")
    assert returned_event == event_mock
    amqp_binding_mock.from_protocol.assert_called_once_with(properties_mock, b"body")


def test_validate_transfer_message():
    assert validate_transfer_message(transfer_message)


@pytest.mark.parametrize("json, missing_key", INVALID_JSON_MESSAGES)
def test_validate_transfer_message_invalid(json, missing_key):
    with pytest.raises(InvalidMessageException) as ime:
        validate_transfer_message(json)
    error_str = f"Invalid transfer message: '{missing_key}' is a mandatory key"
    assert ime.value.message == error_str
