from __future__ import annotations

import json
from pathlib import Path


DEFAULT_CONFIG_PATH = Path("./config/config.json")
EXAMPLE_CONFIG_PATH = Path("./config/config.example.json")


class ConfigError(RuntimeError):
    pass


def load_config(path: Path | None = None) -> dict:
    p = path or DEFAULT_CONFIG_PATH
    if not p.exists():
        raise ConfigError(
            f"Missing config at {p}. Copy {EXAMPLE_CONFIG_PATH} to {p} and edit it."
        )

    cfg = json.loads(p.read_text(encoding="utf-8"))
    validate_config(cfg)
    apply_defaults(cfg)
    return cfg


def validate_config(cfg: dict) -> None:
    required = ["server", "port", "nick", "user", "realname", "channels", "services"]
    for k in required:
        if k not in cfg:
            raise ConfigError(f"Config missing required key: {k}")

    if not isinstance(cfg["channels"], list) or not cfg["channels"]:
        raise ConfigError("Config 'channels' must be a non-empty list")

    if not isinstance(cfg["services"], list):
        raise ConfigError("Config 'services' must be a list")

    acl = cfg.get("acl", {})
    if acl and not isinstance(acl, dict):
        raise ConfigError("Config 'acl' must be an object")


def apply_defaults(cfg: dict) -> None:
    cfg.setdefault("use_tls", True)
    cfg.setdefault("verify_tls", True)
    cfg.setdefault("password", None)
    cfg.setdefault("nickserv_password", None)
    cfg.setdefault("command_prefix", "!")
    cfg.setdefault("reconnect_min_seconds", 2)
    cfg.setdefault("reconnect_max_seconds", 60)
    cfg.setdefault("log_path", "./bot.log")

    cfg.setdefault("db_path", "./data/leonidas.db")

    cfg.setdefault("acl", {})
    cfg["acl"].setdefault("admins", [])
    cfg["acl"].setdefault("contributors", [])
    cfg["acl"].setdefault("users", [])
    cfg["acl"].setdefault("guest_allowed", {"commands": ["help", "commands"]})
