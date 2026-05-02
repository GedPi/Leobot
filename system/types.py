from __future__ import annotations

# Shared types: Event (IRC event payload), CommandInfo, Role and CoreHandler for dispatch.

from dataclasses import dataclass
from typing import Any, Callable, Optional, Protocol, runtime_checkable


# Normalized IRC event: nick/user/host, target (reply-to), channel (if applicable), text, raw line, cmd/params and optional extra (old_nick, victim, etc.).
@dataclass(slots=True)
class Event:
    nick: str
    user: str | None
    host: str | None
    target: str
    channel: str | None
    text: str | None
    is_private: bool
    raw: str
    cmd: str
    params: list[str]
    old_nick: str | None = None
    new_nick: str | None = None
    victim: str | None = None
    kicker: str | None = None


# Metadata for a registered command (name, min_role, mutating, help, category).
@dataclass(slots=True)
class CommandInfo:
    name: str
    min_role: str
    mutating: bool
    help: str
    category: str


Role = str


CoreHandler = Callable[[Any, Event], "Optional[bool]"]


@runtime_checkable
class Service(Protocol):
    """Canonical addon contract used by Bot and Dispatcher.

    Required:
      - service_id: stable lowercase identity key used for enablement/ACL policy lookup.

    Optional hooks:
      - setup(bot) at module level creates a service instance.
      - on_privmsg/on_notice/on_join/on_part/on_quit/on_kick/on_nick/on_mode/on_topic async handlers.
    """

    service_id: str
