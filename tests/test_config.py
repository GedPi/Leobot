# Tests for system.config: validate_config and apply_defaults.
from __future__ import annotations

import pytest

from system.config import ConfigError, apply_defaults, validate_config


def test_validate_config_requires_keys():
    with pytest.raises(ConfigError, match="missing required key"):
        validate_config({})
    with pytest.raises(ConfigError, match="server"):
        validate_config({"port": 6697, "nick": "x", "user": "x", "realname": "x", "channels": ["#x"], "services": []})
    cfg = {
        "server": "irc.example.net",
        "port": 6697,
        "nick": "Bot",
        "user": "bot",
        "realname": "Bot",
        "channels": ["#test"],
        "services": [],
    }
    validate_config(cfg)


def test_validate_config_channels_non_empty_list():
    cfg = {"server": "x", "port": 6697, "nick": "x", "user": "x", "realname": "x", "channels": [], "services": []}
    with pytest.raises(ConfigError, match="channels"):
        validate_config(cfg)
    cfg["channels"] = "not-a-list"
    with pytest.raises(ConfigError, match="channels"):
        validate_config(cfg)
    cfg["channels"] = ["#ok"]
    validate_config(cfg)


def test_validate_config_services_list():
    cfg = {"server": "x", "port": 6697, "nick": "x", "user": "x", "realname": "x", "channels": ["#x"], "services": "x"}
    with pytest.raises(ConfigError, match="services"):
        validate_config(cfg)
    cfg["services"] = []
    validate_config(cfg)


def test_validate_config_acl_object():
    cfg = {"server": "x", "port": 6697, "nick": "x", "user": "x", "realname": "x", "channels": ["#x"], "services": [], "acl": "string"}
    with pytest.raises(ConfigError, match="acl"):
        validate_config(cfg)
    cfg["acl"] = {}
    validate_config(cfg)


def test_apply_defaults():
    cfg = {"server": "x", "port": 6697, "nick": "x", "user": "x", "realname": "x", "channels": ["#x"], "services": []}
    apply_defaults(cfg)
    assert cfg["use_tls"] is True
    assert cfg["command_prefix"] == "!"
    assert cfg["db_path"] == "./data/leonidas.db"
    assert "acl" in cfg
    assert cfg["acl"].get("guest_allowed", {}).get("commands") == ["help", "commands"]
