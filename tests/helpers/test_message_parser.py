#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest

from app.helpers.message_parser import parse_validate_json, InvalidMessageException
from tests.resources import (
    transfer_message,
    transfer_message_empty,
    transfer_message_no_bucket,
    transfer_message_no_destination,
    transfer_message_no_domain,
    transfer_message_no_object,
)


INVALID_JSON_MESSAGES = [
    (transfer_message_empty, "source"),
    (transfer_message_no_bucket, "bucket"),
    (transfer_message_no_destination, "destination"),
    (transfer_message_no_domain, "domain"),
    (transfer_message_no_object, "object"),
]


def test_parse_validate_json():
    msg_json = parse_validate_json(transfer_message)
    assert msg_json["source"]["domain"]["name"] == "prefix.domain.be"
    assert msg_json["source"]["object"]["key"] == "key.mxf"
    assert msg_json["source"]["bucket"]["name"] == "bucket-highres"
    assert msg_json["destination"]["path"] == "/path/to/folder/pid.mxf"


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
