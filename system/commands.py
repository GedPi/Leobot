from __future__ import annotations

from typing import TypedDict


class ParsedCommand(TypedDict):
    name: str
    args: list[str]
    raw: str
    is_command: bool


def parse_command(text: str | None, prefix: str | None) -> ParsedCommand | None:
    txt = (text or "").strip()
    pfx = str(prefix or "!")
    if not txt.startswith(pfx):
        return None

    raw = txt[len(pfx) :].strip()
    if not raw:
        return None

    parts = raw.split()
    if not parts:
        return None

    return {
        "name": parts[0].lower(),
        "args": parts[1:],
        "raw": raw,
        "is_command": True,
    }


def match_any(command_name: str, aliases: set[str] | tuple[str, ...] | list[str]) -> bool:
    cmd = (command_name or "").strip().lower()
    if not cmd:
        return False
    return cmd in {str(a).strip().lower() for a in aliases if str(a).strip()}
