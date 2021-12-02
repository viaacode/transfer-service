#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pulsar

from viaa.configuration import ConfigParser
from cloudevents.events import Event, CEMessageMode, PulsarBinding


class PulsarClient:
    def __init__(self):
        config_parser = ConfigParser()
        self.pulsar_config = config_parser.app_cfg["pulsar"]
        self.client = pulsar.Client(
            f'pulsar://{self.pulsar_config["host"]}:{self.pulsar_config["port"]}'
        )
        self.producers = {}

    def produce_event(self, topic: str, event: Event):
        """Produce a cloudevent on a topic

        If there is no producer yet for the given topic, a new one will be created.

        Args:
            topic: The topic to send the cloudevent to.
            event: The cloudevent to send to the topic.
        """
        if topic not in self.producers:
            self.producers[topic] = self.client.create_producer(topic)

        msg = PulsarBinding.to_protocol(event, CEMessageMode.STRUCTURED)
        self.producers[topic].send(
            msg.data,
            properties=msg.attributes,
            event_timestamp=event.get_event_time_as_int(),
        )

    def close(self):
        """Close all the open producers"""
        for producer in self.producers.values():
            producer.close()
