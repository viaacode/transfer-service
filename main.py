#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from signal import signal, SIGTERM

from app.app import EventListener


if __name__ == "__main__":
    event_listener = EventListener()
    signal(SIGTERM, event_listener.exit_gracefully)
    event_listener.start()
