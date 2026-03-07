from __future__ import annotations

import json
import os

import bcrypt


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
    pw = password.encode("utf-8")[:72]
    try:
        return bcrypt.checkpw(pw, users[username].encode("ascii"))
    except Exception:
        return False


def hash_password(password: str) -> str:
    pw = password.encode("utf-8")[:72]
    return bcrypt.hashpw(pw, bcrypt.gensalt()).decode("ascii")
