#!/usr/bin/env python3
"""Add or update a web UI user. Usage: python add_user.py <username> [password]
If password omitted, prompt securely."""
from __future__ import annotations

import getpass
import json
import os
import sys

import bcrypt

from webui import config as ui_config


def main():
    username = (sys.argv[1] or "").strip()
    if not username:
        print("Usage: python add_user.py <username> [password]", file=sys.stderr)
        sys.exit(1)
    password = sys.argv[2] if len(sys.argv) > 2 else None
    if not password:
        password = getpass.getpass("Password: ")
    if not password:
        print("Password required.", file=sys.stderr)
        sys.exit(1)

    path = ui_config.WEBUI_USERS
    data = {"users": {}}
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        data.setdefault("users", {})

    pw = password.encode("utf-8")[:72]
    data["users"][username] = bcrypt.hashpw(pw, bcrypt.gensalt()).decode("ascii")
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"User {username} saved to {path}")

if __name__ == "__main__":
    main()
