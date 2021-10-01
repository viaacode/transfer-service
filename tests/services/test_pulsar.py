#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest.mock import patch, MagicMock

import pytest

from app.services.pulsar import PulsarClient


class TestPulsarClient:
    @pytest.fixture
    @patch("pulsar.Client")
    def pulsar_client(self, client) -> PulsarClient:
        pulsar_client = PulsarClient()
        return pulsar_client

    @patch("pulsar.Client")
    def test_init(self, client):
        """Check if the pulsar client got instantiated correctly."""
        PulsarClient()
        client.assert_called_once_with("pulsar://pulsar_host:6650")

    def test_produce_event(self, pulsar_client):
        """Produce an event.

        Producer for the topic doesn't exist yet.
        """
        assert len(pulsar_client.producers) == 0

        topic = "tst-topic"
        pulsar_client.produce_event(topic, "event")
        assert topic in pulsar_client.producers
        pulsar_client.producers[topic].send.assert_called_once_with(
            "event".encode("utf8")
        )

    def test_close(self, pulsar_client):
        """Test that all producers were closed."""
        producer_1 = MagicMock()
        producer_2 = MagicMock()
        pulsar_client.producers["producer1"] = producer_1
        pulsar_client.producers["producer2"] = producer_2

        pulsar_client.close()

        producer_1.close.assert_called_once()
        producer_2.close.assert_called_once()
