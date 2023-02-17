#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hvac
from viaa.configuration import ConfigParser


config_parser = ConfigParser()
config = config_parser.app_cfg


class VaultClient:
    def __init__(self):
        self.client = hvac.Client(
            url=config["vault"]["url"],
            token=config["vault"]["token"],
            namespace=config["vault"]["namespace"],
            verify=False,
        )

        self.secrets = {}

    def fetch_secret(self, path: str):
        """Fetch the secret from Vault for a given path.

        After fetching the secret, it will be cached.

        Args:
            path: The path of the secret in format "{secret_engine}/{secret_name}".
        """
        self.mount_point = path.split("/")[0]
        self.path = path.split("/")[1]

        if path not in self.secrets:
            self.secrets[path] = self.client.secrets.kv.v2.read_secret(
                path=self.path, mount_point=self.mount_point
            )["data"]

    def get_username(self, path: str) -> str:
        try:
            return self.secrets[path]["data"]["username"]
        except KeyError:
            raise

    def get_password(self, path: str) -> str:
        try:
            return self.secrets[path]["data"]["password"]
        except KeyError:
            raise
