from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass(slots=True)
class Event:
    nick: str
    user: str | None
    host: str | None
    target: str            # where bot should reply (channel or PM nick)
    channel: str | None    # channel if applicable
    text: str | None
    is_private: bool
    raw: str
    cmd: str
    params: list[str]
    old_nick: str | None = None
    new_nick: str | None = None
    victim: str | None = None
    kicker: str | None = None


@dataclass(slots=True)
class CommandInfo:
    name: str
    min_role: str
    mutating: bool
    help: str
    category: str


Role = str  # 'guest'|'user'|'contributor'|'admin'


CoreHandler = Callable[[Any, Event], "Optional[bool]"]
