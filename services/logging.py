from __future__ import annotations

import json
import logging
import re
import time

from system.irc_format import render_event

log = logging.getLogger("leobot.logging")

LINK_RE = re.compile(r"https?://\S+", re.I)


def setup(bot):
    return LoggingService(bot)


def _has_link(text: str) -> bool:
    return bool(LINK_RE.search(text or ""))


def _userhost(user: str | None, host: str | None) -> str | None:
    if user and host:
        return f"{user}@{host}"
    return None


class LoggingService:
    """
    Canonical IRC event journal.

    - Writes ALL relevant events into irc_log (posterity).
    - Does NOT write seen/stats/nick_changes/messages.
    - lastseen/stats will derive from irc_log later.

    Enable per channel:
      !service enable logging #Channel
    """

    service_id = "logging"

    def __init__(self, bot):
        self.bot = bot

    async def _insert(self, ev, event_name: str, *, channel: str | None, target: str | None, message: str | None) -> None:
        ts = int(time.time())
        actor_uh = _userhost(ev.user, ev.host)

        rendered = render_event(ev, event_name)

        # For PRIVMSG-like, store link bit in message? keep raw message; link can be derived later.
        params_json = json.dumps(ev.params or [], ensure_ascii=False)

        await self.bot.store.execute(
            """
            INSERT INTO irc_log(
              ts, channel, event,
              actor_nick, actor_user, actor_host, actor_userhost,
              target, message,
              rendered, raw, cmd, params_json
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                ts,
                channel,
                event_name.upper(),
                ev.nick,
                ev.user,
                ev.host,
                actor_uh,
                target,
                message,
                rendered,
                ev.raw,
                ev.cmd,
                params_json,
            ),
        )

    async def on_privmsg(self, bot, ev):
        # Dispatcher already gated by channel enablement for channel messages.
        # For PMs: ev.channel is None => no gating. We DO NOT log PMs by default.
        if not ev.channel:
            return

        text = ev.text or ""
        if text.startswith("\x01ACTION "):
            clean = text[len("\x01ACTION ") :].rstrip("\x01").strip()
            ev2 = ev
            ev2.text = clean
            await self._insert(ev2, "ACTION", channel=ev.channel, target=None, message=clean)
            return

        clean = text
        await self._insert(ev, "PRIVMSG", channel=ev.channel, target=None, message=clean)

    async def on_notice(self, bot, ev):
        # Same policy as PRIVMSG: only channel notices (not PM notices)
        if not ev.channel:
            return
        await self._insert(ev, "NOTICE", channel=ev.channel, target=None, message=(ev.text or ""))

    async def on_join(self, bot, ev):
        # Channel gated by dispatcher
        await self._insert(ev, "JOIN", channel=ev.channel, target=None, message=None)

    async def on_part(self, bot, ev):
        # Channel gated by dispatcher
        await self._insert(ev, "PART", channel=ev.channel, target=None, message=(ev.text or None))

    async def on_kick(self, bot, ev):
        # Channel gated by dispatcher
        await self._insert(ev, "KICK", channel=ev.channel, target=(ev.victim or None), message=(ev.text or None))

    async def on_mode(self, bot, ev):
        # Channel gated when channel mode; for user modes (channel None) we ignore by default
        if not ev.channel:
            return
        # target is ev.target (channel name), message is the full mode string
        await self._insert(ev, "MODE", channel=ev.channel, target=(ev.target or ev.channel), message=(ev.text or None))

    async def on_topic(self, bot, ev):
        # Channel gated by dispatcher
        await self._insert(ev, "TOPIC", channel=ev.channel, target=None, message=(ev.text or None))

    async def on_nick(self, bot, ev):
        # No channel; this affects multiple channels. We journal it globally (channel NULL).
        # target := new nick, message := old nick (useful for lookups)
        msg = ev.old_nick or None
        await self._insert(ev, "NICK", channel=None, target=(ev.new_nick or ev.nick or None), message=msg)

    async def on_quit(self, bot, ev):
        # No channel; journal globally (channel NULL). Includes quit reason in message.
        await self._insert(ev, "QUIT", channel=None, target=None, message=(ev.text or None))