#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest.mock import patch

import pytest

from app.services.vault import VaultClient


class TestPulsarClient:
    @pytest.fixture
    @patch("hvac.Client")
    def vault_client(self, client) -> VaultClient:
        vault_client = VaultClient()
        return vault_client

    @patch("hvac.Client")
    def test_init(self, client):
        """Check if the vault client got instantiated correctly."""
        vault_client = VaultClient()
        client.assert_called_once_with(
            **{
                "url": "https://vault/",
                "token": "vault_token",
                "namespace": "namespace",
                "verify": False,
            }
        )
        assert len(vault_client.secrets) == 0

    def test_fetch_secret(self, vault_client: VaultClient):
        path = "engine/name"
        assert path not in vault_client.secrets
        vault_client.fetch_secret(path)
        assert path in vault_client.secrets
        vault_client.client.secrets.kv.v2.read_secret.assert_called_once_with(
            **{"path": "name", "mount_point": "engine"}
        )

    def test_get_username(self, vault_client: VaultClient):
        path = "path"
        vault_client.secrets["path"] = {"data": {"username": "user"}, "metadata": {}}

        assert vault_client.get_username(path) == "user"

    def test_get_password(self, vault_client: VaultClient):
        path = "path"
        vault_client.secrets["path"] = {"data": {"password": "pass"}, "metadata": {}}

        assert vault_client.get_password(path) == "pass"

    def test_get_username_key_error(self, vault_client: VaultClient):
        path = "path"
        with pytest.raises(KeyError):
            vault_client.get_username(path)

    def test_get_password_key_error(self, vault_client: VaultClient):
        path = "path"
        with pytest.raises(KeyError):
            vault_client.get_password(path)
