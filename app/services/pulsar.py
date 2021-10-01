#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pulsar

from viaa.configuration import ConfigParser


class PulsarClient:
    def __init__(self):
        config_parser = ConfigParser()
        self.pulsar_config = config_parser.app_cfg["pulsar"]
        self.client = pulsar.Client(
            f'pulsar://{self.pulsar_config["host"]}:{self.pulsar_config["port"]}'
        )
        self.producers = {}

    def produce_event(self, topic: str, event: str):
        """Produce an event on a topic

        If there is no producer yet for the given topic, a new one will be created
        """
        if topic not in self.producers:
            self.producers[topic] = self.client.create_producer(topic)
        self.producers[topic].send(event.encode("utf8"))

    def close(self):
        """Close all the open producers"""
        for producer in self.producers.values():
            producer.close()
