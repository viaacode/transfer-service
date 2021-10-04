#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from unittest.mock import patch

import pytest

from app.helpers.message_parser import InvalidMessageException
from app.app import EventListener


@pytest.fixture
@patch("app.app.PulsarClient")
@patch("app.app.VaultClient")
@patch("app.app.RabbitClient")
def event_listener(rabbit_client_mock, vault_client_mock, pulsar_client_mock):
    return EventListener()


@patch("app.app.parse_validate_json", side_effect=InvalidMessageException("invalid"))
@patch("app.app.Transfer")
def test_do_work_invalid_message(transfer_mock, parse_mock, event_listener, caplog):
    event_listener.do_work(None, None, None)
    assert "invalid" in caplog.messages
    rabbit_client_mock = event_listener.rabbit_client
    assert rabbit_client_mock.connection.add_callback_threadsafe.call_count == 1
    assert not transfer_mock.call_count


@patch("app.app.Transfer")
@patch("app.app.parse_validate_json")
@patch("app.app.create_event")
def test_do_work(
    create_event_mock, parse_validate_json_mock, transfer_mock, event_listener, caplog
):
    """Successfully finish do_work:

    - Parse and validate incoming message
    - Start a successful transfer
    - Send an ack back to RabbitMQ broker
    - Create a "successful transfer" event
    - Send that event on a Pulsar topic
    """
    message = {}
    # Mock parsed incoming message
    parse_validate_json_mock.return_value = {"outcome": {"pulsar-topic": "topic"}}
    # Mock returned event
    create_event_mock.return_value = {}
    event_listener.do_work(None, None, message)
    # Parse message
    parse_validate_json_mock.assert_called_once_with(message)

    # Transfer
    transfer_mock.assert_called_once_with(
        parse_validate_json_mock(), event_listener.vault_client
    )
    transfer_mock().transfer.assert_called_once_with()

    # Rabbit
    rabbit_client_mock = event_listener.rabbit_client
    assert rabbit_client_mock.connection.add_callback_threadsafe.call_count == 1

    # Pulsar
    pulsar_client_mock = event_listener.pulsar_client

    # Check the Pulsar event
    create_event_mock.assert_called_once_with(
        parse_validate_json_mock(), "Transfer successful", "Success"
    )

    # Check is message is send
    pulsar_client_mock.produce_event.assert_called_once_with("topic", "{}")
