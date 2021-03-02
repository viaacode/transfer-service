#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from app.app import EventListener


if __name__ == "__main__":
    event_listener = EventListener()
    event_listener.start()
