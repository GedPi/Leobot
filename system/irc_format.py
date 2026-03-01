from __future__ import annotations

from system.types import Event


def _userhost(ev: Event) -> str:
    if ev.user and ev.host:
        return f"{ev.user}@{ev.host}"
    return ""


def render_event(ev: Event, event_name: str) -> str:
    """
    Stable "house format" render string.

    This is NOT meant to mimic every client perfectly.
    It's meant to be:
      - readable
      - consistent
      - information-complete (includes reasons, new nick, victim, modes)
    """
    nick = ev.nick or ""
    uh = _userhost(ev)
    who = f"{nick} ({uh})" if uh else nick

    channel = ev.channel or ""
    msg = (ev.text or "").strip()

    e = (event_name or "").upper()

    if e == "PRIVMSG":
        # Plain message
        return f"<{nick}> {msg}"

    if e == "ACTION":
        # CTCP ACTION
        return f"* {nick} {msg}"

    if e == "NOTICE":
        # Notice (channel or PM)
        if channel:
            return f"-{nick}- {msg}"
        return f"-{nick}- {msg}"

    if e == "JOIN":
        return f"--> {who} has joined {channel}"

    if e == "PART":
        if msg:
            return f"<-- {who} has left {channel} (Part: {msg})"
        return f"<-- {who} has left {channel}"

    if e == "QUIT":
        if msg:
            return f"<-- {who} has quit (Quit: {msg})"
        return f"<-- {who} has quit"

    if e == "NICK":
        old = ev.old_nick or ""
        new = ev.new_nick or nick
        if old and new:
            return f"-- {old} is now known as {new}"
        return f"-- nick change: {old} -> {new}"

    if e == "KICK":
        victim = ev.victim or ""
        kicker = ev.kicker or nick
        if msg:
            return f"<-- {victim} was kicked from {channel} by {kicker} (Kick: {msg})"
        return f"<-- {victim} was kicked from {channel} by {kicker}"

    if e == "MODE":
        # ev.target carries channel or nick being mode-changed; ev.text carries "+b mask" etc
        tgt = ev.target or channel
        if msg:
            return f"-- {nick} sets mode on {tgt}: {msg}"
        return f"-- {nick} sets mode on {tgt}"

    if e == "TOPIC":
        if msg:
            return f"-- {nick} changed topic on {channel} to: {msg}"
        return f"-- {nick} changed topic on {channel}"

    # Fallback
    if channel:
        return f"-- {e} {channel} {who}: {msg}".strip()
    return f"-- {e} {who}: {msg}".strip()