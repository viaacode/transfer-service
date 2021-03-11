#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json

from pika.exceptions import AMQPConnectionError
from viaa.configuration import ConfigParser
from viaa.observability import logging

from app.helpers.transfer import TransferPartException, TransferException, transfer
from app.services.rabbit import RabbitClient


class EventListener:
    def __init__(self):
        config_parser = ConfigParser()
        self.config = config_parser.app_cfg
        self.log = logging.get_logger(__name__, config=config_parser)
        try:
            self.rabbit_client = RabbitClient()
        except AMQPConnectionError as error:
            self.log.error("Connection to RabbitMQ failed.")
            raise error

    def handle_message(self, channel, method, properties, body):
        """Main method that will handle the incoming messages."""
        self.log.debug(f"Incoming message: {body}")
        try:
            transfer(json.loads(body))
        except (TransferPartException, TransferException, OSError):
            self.log.error("Transfer failed")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        # Start listening for incoming messages
        self.log.info("Start to listen for incoming transfer messages...")
        self.rabbit_client.listen(self.handle_message)
