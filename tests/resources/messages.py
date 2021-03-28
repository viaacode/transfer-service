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
transfer_message_no_bucket = _load_resource("transfer_message_no_bucket.json")
transfer_message_no_destination = _load_resource("transfer_message_no_destination.json")
transfer_message_no_domain = _load_resource("transfer_message_no_domain.json")
transfer_message_no_object = _load_resource("transfer_message_no_object.json")
