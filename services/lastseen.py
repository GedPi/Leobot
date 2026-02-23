import time
from dataclasses import dataclass
from typing import Optional

from services.chatdb import ChatDB, DBConfig


def _now() -> int:
    return int(time.time())


def _fmt_ago(seconds: int) -> str:
    if seconds < 0:
        seconds = 0
    mins, sec = divmod(seconds, 60)
    hrs, mins = divmod(mins, 60)
    days, hrs = divmod(hrs, 24)
    if days >= 14:
        weeks = days // 7
        return f"{weeks}w ago"
    if days >= 2:
        return f"{days}d ago"
    if days == 1:
        return "yesterday"
    if hrs:
        return f"{hrs}h {mins}m ago"
    if mins:
        return f"{mins}m ago"
    return f"{sec}s ago"


@dataclass
class LastSeenConfig:
    db_path: str = "/var/lib/leobot/db/leobot.db"


class LastSeenService:
    def __init__(self, bot, cfg: Optional[object] = None):
        # cfg can be dict or LastSeenConfig
        if isinstance(cfg, dict) or cfg is None:
            cfg = LastSeenConfig(db_path=str((cfg or {}).get("db_path", LastSeenConfig.db_path)))
        self.cfg: LastSeenConfig = cfg  # type: ignore[assignment]
        self.db = ChatDB(DBConfig(self.cfg.db_path))

    async def on_privmsg(self, bot, ev) -> None:
        # Store channel messages only (no PMs)
        if ev.is_private:
            return

        text = (ev.text or "").strip()
        if not text:
            return

        # Track normal messages in DB
        ts = _now()
        is_action = 1 if getattr(ev, "is_action", False) else 0
        has_link = 1 if ("http://" in text or "https://" in text) else 0

        await self.db.execute(
            "INSERT INTO messages(ts, channel, nick, is_action, has_link, text) VALUES(?,?,?,?,?,?)",
            (ts, ev.channel, ev.nick, is_action, has_link, text),
        )

        # Update "seen" with last message
        await self.db.execute(
            "INSERT INTO seen(nick, ts, event, channel, last_msg) VALUES(?,?,?,?,?) "
            "ON CONFLICT(nick) DO UPDATE SET ts=excluded.ts, event=excluded.event, channel=excluded.channel, last_msg=excluded.last_msg",
            (ev.nick, ts, "msg", ev.channel, text[:300]),
        )

        # Command: !lastseen <nick>
        prefix = bot.cfg.get("command_prefix", "!")
        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return
        cmd, *rest = cmdline.split(maxsplit=1)
        if cmd.lower() not in ("lastseen", "seen"):
            return

        if not rest or not rest[0].strip():
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !lastseen <nick>")
            return

        query_nick = rest[0].strip()
        row = await self.db.fetchone("SELECT ts, event, channel, last_msg FROM seen WHERE lower(nick)=lower(?)", (query_nick,))
        if not row:
            await bot.privmsg(ev.target, f"I haven't seen {query_nick} yet.")
            return

        seen_ts, event, channel, last_msg = row
        ago = _fmt_ago(_now() - int(seen_ts))

        # Nick change context (last change involving this nick)
        chg = await self.db.fetchone(
            "SELECT ts, old_nick, new_nick FROM nick_changes WHERE lower(old_nick)=lower(?) OR lower(new_nick)=lower(?) ORDER BY ts DESC LIMIT 1",
            (query_nick, query_nick),
        )

        parts = []
        parts.append(f"I last saw {query_nick} {ago}.")
        if event == "quit":
            parts.append("They quit.")
        elif event == "part":
            parts.append(f"They left {channel}.")
        elif event == "join":
            parts.append(f"They joined {channel}.")
        elif event == "kick":
            parts.append(f"They were kicked from {channel}.")
        elif event == "nick":
            parts.append("They changed their nick.")
        else:
            parts.append(f"They were chatting in {channel}.")

        if chg:
            _, old_n, new_n = chg
            if old_n.lower() == query_nick.lower() and new_n.lower() != query_nick.lower():
                parts.append(f"They changed their name to {new_n}.")
            elif new_n.lower() == query_nick.lower() and old_n.lower() != query_nick.lower():
                parts.append(f"They used to be known as {old_n}.")

        if last_msg:
            parts.append(f'Last thing I saw them say: "{last_msg}"')

        await bot.privmsg(ev.target, " ".join(parts))

    async def _mark_seen(self, nick: str, ts: int, event: str, channel: Optional[str] = None, last_msg: Optional[str] = None) -> None:
        await self.db.execute(
            "INSERT INTO seen(nick, ts, event, channel, last_msg) VALUES(?,?,?,?,?) "
            "ON CONFLICT(nick) DO UPDATE SET ts=excluded.ts, event=excluded.event, channel=excluded.channel, last_msg=excluded.last_msg",
            (nick, ts, event, channel, (last_msg or None)),
        )

    async def on_join(self, bot, ev) -> None:
        if ev.is_private:
            return
        await self._mark_seen(ev.nick, _now(), "join", ev.channel, None)
        await self._bump_daily(ev, joins=1)

    async def on_part(self, bot, ev) -> None:
        if ev.is_private:
            return
        await self._mark_seen(ev.nick, _now(), "part", ev.channel, None)
        await self._bump_daily(ev, parts=1)

    async def on_quit(self, bot, ev) -> None:
        await self._mark_seen(ev.nick, _now(), "quit", None, None)
        await self._bump_daily(ev, quits=1)

    async def on_kick(self, bot, ev) -> None:
        if ev.is_private:
            return
        victim = getattr(ev, "victim", None)
        if victim:
            await self._mark_seen(victim, _now(), "kick", ev.channel, None)
            await self._bump_daily(ev, kicks=1, victim=victim)

    async def on_nick(self, bot, ev) -> None:
        # record nick changes, but don't treat as disappearance
        if ev.old_nick and ev.new_nick:
            await self.db.execute(
                "INSERT INTO nick_changes(ts, channel, old_nick, new_nick) VALUES(?,?,?,?)",
                (_now(), ev.channel, ev.old_nick, ev.new_nick),
            )
            await self._mark_seen(ev.old_nick, _now(), "nick", ev.channel, None)
            await self._mark_seen(ev.new_nick, _now(), "nick", ev.channel, None)
            await self._bump_daily(ev, nickchanges=1)

    async def _bump_daily(self, ev, **incs) -> None:
        # in ev, channel may be None for quit; skip then
        if not ev.channel:
            return
        # day is UTC YYYY-MM-DD
        day = time.strftime("%Y-%m-%d", time.gmtime(_now()))
        nick = ev.nick
        cols = ["msgs", "words", "links", "actions", "joins", "parts", "quits", "kicks", "nickchanges"]
        row = [incs.get(c, 0) for c in cols]
        await self.db.execute(
            "INSERT INTO stats_daily(day, channel, nick, msgs, words, links, actions, joins, parts, quits, kicks, nickchanges) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(day, channel, nick) DO UPDATE SET "
            "msgs=msgs+excluded.msgs, words=words+excluded.words, links=links+excluded.links, actions=actions+excluded.actions, "
            "joins=joins+excluded.joins, parts=parts+excluded.parts, quits=quits+excluded.quits, kicks=kicks+excluded.kicks, "
            "nickchanges=nickchanges+excluded.nickchanges",
            (day, ev.channel, nick, *row),
        )


def setup(bot):
    cfg = {}
    if isinstance(getattr(bot, "cfg", None), dict):
        cfg = bot.cfg.get("lastseen", {}) if isinstance(bot.cfg.get("lastseen", {}), dict) else {}

    if hasattr(bot, "register_command"):
        bot.register_command("lastseen", min_role="user", mutating=False, help="When was a nick last seen? Usage: !lastseen <nick>", category="Fun")
        bot.register_command("seen", min_role="user", mutating=False, help="Alias for !lastseen", category="Fun")

    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register("lastseen", min_role="user", mutating=False, help="When was a nick last seen? Usage: !lastseen <nick>", category="Fun")
        bot.acl.register("seen", min_role="user", mutating=False, help="Alias for !lastseen", category="Fun")

    return LastSeenService(bot, cfg)
