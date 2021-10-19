#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os

folder = os.path.join(os.getcwd(), "tests", "resources", "messages")


def _load_resource(filename):
    with open(os.path.join(folder, filename), "rb") as f:
        contents = json.load(f)
    return contents


transfer_message: dict = _load_resource("transfer_message.json")
transfer_message_empty: dict = _load_resource("transfer_message_empty.json")
transfer_message_no_source: dict = _load_resource("transfer_message_no_source.json")
transfer_message_no_destination: dict = _load_resource(
    "transfer_message_no_destination.json"
)
transfer_message_no_url: dict = _load_resource("transfer_message_no_url.json")
transfer_message_no_headers: dict = _load_resource("transfer_message_no_headers.json")
transfer_message_no_host: dict = _load_resource("transfer_message_no_host.json")
transfer_message_no_path: dict = _load_resource("transfer_message_no_path.json")
transfer_message_no_credentials: dict = _load_resource(
    "transfer_message_no_credentials.json"
)
transfer_message_no_outcome: dict = _load_resource("transfer_message_no_outcome.json")