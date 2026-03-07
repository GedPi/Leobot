from __future__ import annotations

import json
import os

from passlib.hash import bcrypt


def load_users(path: str) -> dict[str, str]:
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return dict(data.get("users") or {})
    except Exception:
        return {}


def verify_password(path: str, username: str, password: str) -> bool:
    users = load_users(path)
    if not username or username not in users:
        return False
    return bcrypt.verify(password, users[username])


def hash_password(password: str) -> str:
    return bcrypt.hash(password)
