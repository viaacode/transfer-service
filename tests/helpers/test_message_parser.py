#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest

from app.helpers.message_parser import parse_validate_json, InvalidMessageException
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


def test_parse_validate_json():
    msg_json = parse_validate_json(transfer_message)
    assert msg_json["source"]["url"] == "http://host:port/path/"
    assert msg_json["source"]["headers"] == {"host": "domain"}
    assert msg_json["destination"]["host"] == "tst-server"
    assert msg_json["destination"]["path"] == "/path/to/folder/pid.mxf"
    assert msg_json["destination"]["credentials"] == "vault-secret"
    assert msg_json["outcome"]["pulsar-topic"] == "topic"


def test_parse_validate_json_decode_error():
    with pytest.raises(InvalidMessageException) as ime:
        parse_validate_json("nojson")
    error_str = 'Not valid JSON: "Expecting value: line 1 column 1 (char 0)"'
    assert ime.value.message == error_str


@pytest.mark.parametrize("json, missing_key", INVALID_JSON_MESSAGES)
def test_parse_validate_json_invalid(json, missing_key):
    with pytest.raises(InvalidMessageException) as ime:
        parse_validate_json(json)
    error_str = f"Invalid transfer message: '{missing_key}' is a mandatory key"
    assert ime.value.message == error_str
