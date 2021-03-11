#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time

from viaa.configuration import ConfigParser
from viaa.observability import logging

import pika


class RabbitClient:
    def __init__(self):
        config_parser = ConfigParser()
        self.logger = logging.get_logger(__name__, config=config_parser)
        self.rabbit_config = config_parser.app_cfg["rabbitmq"]

        self.credentials = pika.PlainCredentials(
            self.rabbit_config["username"], self.rabbit_config["password"]
        )

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.rabbit_config["host"],
                port=self.rabbit_config["port"],
                credentials=self.credentials,
            )
        )

        self.prefetch_count = int(self.rabbit_config["prefetch_count"])

    def listen(self, on_message_callback, queue=None):
        if queue is None:
            queue = self.rabbit_config["queue"]

        try:
            while True:
                try:
                    channel = self.connection.channel()

                    channel.basic_qos(
                        prefetch_count=self.prefetch_count, global_qos=False
                    )
                    channel.basic_consume(
                        queue=queue, on_message_callback=on_message_callback
                    )

                    channel.start_consuming()
                except pika.exceptions.StreamLostError:
                    self.logger.warning("RMQBridge lost connection, reconnecting...")
                    time.sleep(3)
                except pika.exceptions.ChannelWrongStateError:
                    self.logger.warning(
                        "RMQBridge wrong state in channel, reconnecting..."
                    )
                    time.sleep(3)
                except pika.exceptions.AMQPHeartbeatTimeout:
                    self.logger.warning(
                        "RMQBridge heartbeat timed out, reconnecting..."
                    )
                    time.sleep(3)

        except KeyboardInterrupt:
            channel.stop_consuming()

        self.connection.close()
