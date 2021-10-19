#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

folder = os.path.join(os.getcwd(), "tests", "resources", "messages")


def _load_resource(filename):
    with open(os.path.join(folder, filename), "rb") as f:
        contents = f.read()
    return contents


transfer_message = _load_resource("transfer_message.json")
transfer_message_empty = _load_resource("transfer_message_empty.json")
transfer_message_no_source = _load_resource("transfer_message_no_source.json")
transfer_message_no_destination = _load_resource("transfer_message_no_destination.json")
transfer_message_no_url = _load_resource("transfer_message_no_url.json")
transfer_message_no_headers = _load_resource("transfer_message_no_headers.json")
transfer_message_no_host = _load_resource("transfer_message_no_host.json")
transfer_message_no_path = _load_resource("transfer_message_no_path.json")
transfer_message_no_credentials = _load_resource("transfer_message_no_credentials.json")
transfer_message_no_outcome = _load_resource("transfer_message_no_outcome.json")