from __future__ import annotations

import logging
import re
import time

log = logging.getLogger("leobot.logging")

LINK_RE = re.compile(r"https?://\S+", re.I)


def setup(bot):
    return LoggingService(bot)


def _utc_day(ts: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ts))


def _word_count(text: str) -> int:
    return len([w for w in (text or "").split() if w])


def _has_link(text: str) -> bool:
    return bool(LINK_RE.search(text or ""))


class LoggingService:
    """Channel logging into SQLite.

    Disabled by default per channel.
      !service enable logging #Channel

    This service is intentionally lean: it stores messages and updates basic daily counters.
    """

    def __init__(self, bot):
        self.bot = bot

    async def on_privmsg(self, bot, ev):
        if not ev.channel:
            return
        if not await bot.store.is_service_enabled(ev.channel, "logging"):
            return

        ts = int(time.time())
        text = ev.text or ""
        is_action = 1 if text.startswith("\x01ACTION ") else 0
        has_link = 1 if _has_link(text) else 0
        clean = text
        if is_action:
            clean = clean[len("\x01ACTION ") :].rstrip("\x01").strip()

        await bot.store.execute(
            "INSERT INTO messages(ts,channel,nick,is_action,has_link,text) VALUES(?,?,?,?,?,?)",
            (ts, ev.channel, ev.nick, is_action, has_link, clean),
        )

        await bot.store.execute(
            "INSERT INTO seen(nick,ts,event,channel,last_msg) VALUES(?,?,?,?,?) "
            "ON CONFLICT(nick) DO UPDATE SET ts=excluded.ts, event=excluded.event, channel=excluded.channel, last_msg=excluded.last_msg",
            (ev.nick, ts, "msg", ev.channel, clean[:240]),
        )

        day = _utc_day(ts)
        words = _word_count(clean)
        await bot.store.execute(
            "INSERT INTO stats_daily(day,channel,nick,msgs,words,links,actions,joins,parts,quits,kicks,nickchanges) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(day,channel,nick) DO UPDATE SET "
            "msgs=msgs+1, words=words+excluded.words, links=links+excluded.links, actions=actions+excluded.actions",
            (day, ev.channel, ev.nick, 1, words, has_link, is_action, 0, 0, 0, 0, 0),
        )

    async def on_join(self, bot, ev):
        if not ev.channel:
            return
        if not await bot.store.is_service_enabled(ev.channel, "logging"):
            return
        ts = int(time.time())
        day = _utc_day(ts)
        await bot.store.execute(
            "INSERT INTO seen(nick,ts,event,channel,last_msg) VALUES(?,?,?,?,?) "
            "ON CONFLICT(nick) DO UPDATE SET ts=excluded.ts, event=excluded.event, channel=excluded.channel, last_msg=excluded.last_msg",
            (ev.nick, ts, "join", ev.channel, None),
        )
        await bot.store.execute(
            "INSERT INTO stats_daily(day,channel,nick,joins) VALUES(?,?,?,1) "
            "ON CONFLICT(day,channel,nick) DO UPDATE SET joins=joins+1",
            (day, ev.channel, ev.nick),
        )

    async def on_part(self, bot, ev):
        if not ev.channel:
            return
        if not await bot.store.is_service_enabled(ev.channel, "logging"):
            return
        ts = int(time.time())
        day = _utc_day(ts)
        await bot.store.execute(
            "INSERT INTO seen(nick,ts,event,channel,last_msg) VALUES(?,?,?,?,?) "
            "ON CONFLICT(nick) DO UPDATE SET ts=excluded.ts, event=excluded.event, channel=excluded.channel, last_msg=excluded.last_msg",
            (ev.nick, ts, "part", ev.channel, (ev.text or "")[:240] if ev.text else None),
        )
        await bot.store.execute(
            "INSERT INTO stats_daily(day,channel,nick,parts) VALUES(?,?,?,1) "
            "ON CONFLICT(day,channel,nick) DO UPDATE SET parts=parts+1",
            (day, ev.channel, ev.nick),
        )

    async def on_quit(self, bot, ev):
        ts = int(time.time())
        await bot.store.execute(
            "INSERT INTO seen(nick,ts,event,channel,last_msg) VALUES(?,?,?,?,?) "
            "ON CONFLICT(nick) DO UPDATE SET ts=excluded.ts, event=excluded.event, channel=excluded.channel, last_msg=excluded.last_msg",
            (ev.nick, ts, "quit", None, (ev.text or "")[:240] if ev.text else None),
        )

    async def on_nick(self, bot, ev):
        ts = int(time.time())
        # record nick change mapping (best-effort)
        if ev.old_nick and ev.new_nick:
            await bot.store.execute(
                "INSERT INTO nick_changes(ts,channel,old_nick,new_nick) VALUES(?,?,?,?)",
                (ts, ev.channel, ev.old_nick, ev.new_nick),
            )
            day = _utc_day(ts)
            if ev.channel and await bot.store.is_service_enabled(ev.channel, "logging"):
                await bot.store.execute(
                    "INSERT INTO stats_daily(day,channel,nick,nickchanges) VALUES(?,?,?,1) "
                    "ON CONFLICT(day,channel,nick) DO UPDATE SET nickchanges=nickchanges+1",
                    (day, ev.channel, ev.old_nick),
                )
