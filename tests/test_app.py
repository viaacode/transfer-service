#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
from unittest.mock import patch, MagicMock

import pytest

from app.helpers.message_parser import InvalidMessageException
from app.app import EventListener


@pytest.fixture
@patch("app.app.PulsarClient")
@patch("app.app.VaultClient")
@patch("app.app.RabbitClient")
def event_listener(rabbit_client_mock, vault_client_mock, pulsar_client_mock):
    return EventListener()


@patch("app.app.parse_incoming_message", side_effect=InvalidMessageException("invalid"))
@patch("app.app.Transfer")
def test_do_work_invalid_message(transfer_mock, parse_mock, event_listener, caplog):
    event_listener.do_work(None, None, None, None)
    assert "invalid" in caplog.messages
    rabbit_client_mock = event_listener.rabbit_client
    assert rabbit_client_mock.connection.add_callback_threadsafe.call_count == 1
    assert not transfer_mock.call_count


@patch("app.app.Transfer")
@patch("app.app.validate_transfer_message")
@patch("app.app.parse_incoming_message")
@patch("app.app.create_event")
def test_do_work(
    create_event_mock,
    parse_incoming_message_mock,
    validate_transfer_message_mock,
    transfer_mock,
    event_listener,
    caplog,
):
    """Successfully finish do_work:

    - Parse and validate incoming message
    - Start a successful transfer
    - Send an ack back to RabbitMQ broker
    - Create a "successful transfer" event
    - Send that event on a Pulsar topic
    """
    # Mock parsed incoming message
    transfer_message = {"outcome": {"pulsar-topic": "topic"}}
    transfer_message_bytes = json.dumps(transfer_message)
    incoming_event = MagicMock()
    incoming_event.get_data.return_value = transfer_message
    parse_incoming_message_mock.return_value = incoming_event
    properties = MagicMock()
    # Mock returned event
    outgoing_event = MagicMock()
    create_event_mock.return_value = outgoing_event
    event_listener.do_work(None, None, properties, transfer_message_bytes)
    # Parse message
    parse_incoming_message_mock.assert_called_once_with(
        properties, transfer_message_bytes
    )
    # Validate message
    validate_transfer_message_mock.assert_called_once_with(transfer_message)

    # Transfer
    transfer_mock.assert_called_once_with(transfer_message, event_listener.vault_client)
    transfer_mock().transfer.assert_called_once_with()

    # Rabbit
    rabbit_client_mock = event_listener.rabbit_client
    assert rabbit_client_mock.connection.add_callback_threadsafe.call_count == 1

    # Pulsar
    pulsar_client_mock = event_listener.pulsar_client

    # Check the Pulsar event
    create_event_mock.assert_called_once_with(
        transfer_message, "Transfer successful", "success"
    )

    # Check is message is send
    pulsar_client_mock.produce_event.assert_called_once_with("topic", outgoing_event)
