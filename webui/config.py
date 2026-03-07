# Web UI paths and settings. Override with env vars.
from __future__ import annotations

import os

# Bot paths (read/write)
LEOBOT_CONFIG = os.environ.get("LEOBOT_CONFIG", "/opt/leobot/config/config.json")
LEOBOT_DB = os.environ.get("LEOBOT_DB", "/opt/leobot/data/leonidas.db")
LEOBOT_LOG = os.environ.get("LEOBOT_LOG", "/opt/leobot/bot.log")

# Auth: JSON file with {"users": {"username": "bcrypt_hash"}}
WEBUI_USERS = os.environ.get("WEBUI_USERS", os.path.join(os.path.dirname(__file__), "users.json"))
# Flask secret for session signing (set in production)
SECRET_KEY = os.environ.get("WEBUI_SECRET_KEY", "change-me-in-production")
