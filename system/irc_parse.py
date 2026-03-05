from __future__ import annotations

# IRC line parsing: prefix/cmd/params and nick!user@host; message chunking for PRIVMSG length limits.

from dataclasses import dataclass
from typing import Optional


@dataclass(slots=True)
class ParsedLine:
    prefix: str
    cmd: str
    params: list[str]


# Splits IRC prefix into (nick, user, host); if no ! or @ returns (prefix, None, None).
def parse_prefix(prefix: str) -> tuple[str, Optional[str], Optional[str]]:
    if "!" in prefix and "@" in prefix:
        nick, rest = prefix.split("!", 1)
        user, host = rest.split("@", 1)
        return nick, user, host
    return prefix, None, None


# Parses raw IRC line into prefix, cmd and params; trailing after : is last param; returns None if empty or invalid.
def parse_line(line: str) -> ParsedLine | None:
    if not line:
        return None

    prefix = ""
    rest = line
    if rest.startswith(":"):
        try:
            prefix, rest = rest[1:].split(" ", 1)
        except ValueError:
            return None

    trailing = None
    if " :" in rest:
        head, trailing = rest.split(" :", 1)
        parts = head.split()
    else:
        parts = rest.split()

    if trailing is not None:
        parts.append(trailing)

    if not parts:
        return None

    cmd = parts[0]
    params = parts[1:]
    return ParsedLine(prefix=prefix, cmd=cmd, params=params)


# Yields message in chunks of at most limit chars (replacing CR/LF with space) for IRC message limits.
def chunk_message(msg: str, limit: int = 380):
    msg = (msg or "").replace("\r", " ").replace("\n", " ")
    while len(msg) > limit:
        yield msg[:limit]
        msg = msg[limit:]
    yield msg
