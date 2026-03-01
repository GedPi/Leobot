from __future__ import annotations

import calendar
import re
import time
from dataclasses import dataclass
from typing import Optional, Tuple


def setup(bot):
    return StatsService(bot)


DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass
class Window:
    label: str
    start_ts: Optional[int]  # None => no lower bound
    end_ts: Optional[int]    # None => no upper bound


def _count_words(s: str) -> int:
    return len([w for w in (s or "").strip().split() if w])


def _utc_day_bounds_for_date(date_str: str) -> Tuple[int, int]:
    y, m, d = (int(x) for x in date_str.split("-"))
    start = calendar.timegm((y, m, d, 0, 0, 0))
    return start, start + 86400


def _utc_today_bounds(now_ts: int) -> Tuple[int, int, str]:
    g = time.gmtime(now_ts)
    date_str = f"{g.tm_year:04d}-{g.tm_mon:02d}-{g.tm_mday:02d}"
    start, end = _utc_day_bounds_for_date(date_str)
    return start, end, date_str


def _parse_window(args: list[str], now_ts: int) -> Tuple[Window, list[str]]:
    """
    Returns (Window, remaining_args).
    Default = last 24h.
    """
    if not args:
        return Window("last 24h", now_ts - 86400, now_ts + 1), []

    tok = args[0].lower()

    if tok in ("24h", "1d"):
        return Window("last 24h", now_ts - 86400, now_ts + 1), args[1:]

    if tok in ("7d", "week"):
        return Window("last 7d", now_ts - 7 * 86400, now_ts + 1), args[1:]

    if tok in ("all", "ever"):
        return Window("all time", None, None), args[1:]

    if tok in ("today",):
        start, end, ds = _utc_today_bounds(now_ts)
        return Window(f"today ({ds} UTC)", start, end), args[1:]

    if DATE_RE.match(tok):
        start, end = _utc_day_bounds_for_date(tok)
        return Window(f"{tok} UTC", start, end), args[1:]

    return Window("last 24h", now_ts - 86400, now_ts + 1), args


def _parse_channel_override(args: list[str]) -> Tuple[Optional[str], list[str]]:
    if args and args[0].startswith("#"):
        return args[0], args[1:]
    return None, args


class StatsService:
    """
    !stats [me|nick|top] [N] [#channel] [window]

    window:
      - (default) last 24h
      - today
      - 24h
      - 7d
      - all
      - YYYY-MM-DD
    """

    service_id = "stats"

    def __init__(self, bot):
        self.bot = bot
        bot.register_command(
            "stats",
            min_role="user",
            mutating=False,
            help="Usage: !stats [me|nick|top] [N] [#channel] [today|24h|7d|all|YYYY-MM-DD]. Default=24h.",
            category="Utility",
        )

    async def on_privmsg(self, bot, ev):
        prefix = bot.cfg.get("command_prefix", "!")
        txt = (ev.text or "").strip()
        if not txt.startswith(prefix):
            return

        parts = txt[len(prefix):].strip().split()
        if not parts or parts[0].lower() != "stats":
            return

        if not ev.channel:
            await bot.privmsg(ev.target, f"{ev.nick}: stats are channel-based; run in a channel.")
            return

        args = parts[1:]
        now = int(time.time())

        # Subcommands
        if args and args[0].lower() == "top":
            await self._cmd_top(bot, ev, args[1:], now)
            return

        # "me" explicit
        if args and args[0].lower() == "me":
            args = args[1:]
            chan_override, args = _parse_channel_override(args)
            window, _ = _parse_window(args, now)
            await self._report_nick(bot, ev, ev.nick, chan_override or ev.channel, window, now)
            return

        # Otherwise: maybe channel override, maybe nick, maybe window
        chan_override, args2 = _parse_channel_override(args)

        # If first token is a window, it applies to "self"
        window, rem = _parse_window(args2, now)
        if rem != args2:
            await self._report_nick(bot, ev, ev.nick, chan_override or ev.channel, window, now)
            return

        # Else if token exists, treat it as nick then parse window
        nick = ev.nick
        rest = args2
        if rest:
            nick = rest[0]
            rest = rest[1:]

        window2, _ = _parse_window(rest, now)
        await self._report_nick(bot, ev, nick, chan_override or ev.channel, window2, now)

    async def _cmd_top(self, bot, ev, args: list[str], now: int) -> None:
        # Parse N
        n = 10
        if args and args[0].isdigit():
            n = max(1, min(50, int(args[0])))
            args = args[1:]

        chan_override, args = _parse_channel_override(args)
        channel = chan_override or ev.channel

        window, _ = _parse_window(args, now)

        where_time = ""
        params_time = []
        if window.start_ts is not None:
            where_time += " AND ts >= ?"
            params_time.append(int(window.start_ts))
        if window.end_ts is not None:
            where_time += " AND ts < ?"
            params_time.append(int(window.end_ts))

        rows = await bot.store.fetchall(
            f"""
            SELECT actor_nick,
                   SUM(CASE WHEN event='PRIVMSG' THEN 1 ELSE 0 END) AS msgs,
                   SUM(CASE WHEN event='ACTION' THEN 1 ELSE 0 END) AS actions
            FROM irc_log
            WHERE channel IS NOT NULL AND lower(channel)=lower(?)
              AND actor_nick IS NOT NULL
              AND event IN ('PRIVMSG','ACTION')
              {where_time}
            GROUP BY lower(actor_nick)
            ORDER BY msgs DESC, actions DESC
            LIMIT ?
            """,
            (channel, *params_time, int(n)),
        )

        if not rows:
            await bot.privmsg(ev.target, f"{ev.nick}: no data for {channel} ({window.label}).")
            return

        # Build compact leaderboard
        parts = []
        rank = 1
        for r in rows:
            nick = r[0]
            msgs = int(r[1] or 0)
            actions = int(r[2] or 0)
            if actions:
                parts.append(f"{rank}) {nick} {msgs} msgs (+{actions} act)")
            else:
                parts.append(f"{rank}) {nick} {msgs} msgs")
            rank += 1

        await bot.privmsg(ev.target, f"{ev.nick}: top {len(rows)} in {channel} ({window.label}): " + " | ".join(parts))

    async def _report_nick(self, bot, ev, nick: str, channel: str, window: Window, now: int) -> None:
        where_time = ""
        params_time = []
        if window.start_ts is not None:
            where_time += " AND ts >= ?"
            params_time.append(int(window.start_ts))
        if window.end_ts is not None:
            where_time += " AND ts < ?"
            params_time.append(int(window.end_ts))

        row = await bot.store.fetchone(
            f"""
            SELECT
              SUM(CASE WHEN event='PRIVMSG' THEN 1 ELSE 0 END) AS msgs,
              SUM(CASE WHEN event='ACTION' THEN 1 ELSE 0 END) AS actions,
              SUM(CASE WHEN event='NOTICE' THEN 1 ELSE 0 END) AS notices,
              SUM(CASE WHEN event='JOIN' THEN 1 ELSE 0 END) AS joins,
              SUM(CASE WHEN event='PART' THEN 1 ELSE 0 END) AS parts,
              SUM(CASE WHEN event='KICK' THEN 1 ELSE 0 END) AS kicks,
              SUM(CASE WHEN event='MODE' THEN 1 ELSE 0 END) AS modes,
              SUM(CASE WHEN event='TOPIC' THEN 1 ELSE 0 END) AS topics,
              SUM(CASE WHEN (event IN ('PRIVMSG','ACTION','NOTICE')) AND message LIKE '%http%' THEN 1 ELSE 0 END) AS links
            FROM irc_log
            WHERE channel IS NOT NULL AND lower(channel)=lower(?)
              AND actor_nick IS NOT NULL AND lower(actor_nick)=lower(?)
              {where_time}
            """,
            (channel, nick, *params_time),
        )

        msgs = int(row[0] or 0)
        actions = int(row[1] or 0)
        notices = int(row[2] or 0)
        joins = int(row[3] or 0)
        parts_n = int(row[4] or 0)
        kicks = int(row[5] or 0)
        modes = int(row[6] or 0)
        topics = int(row[7] or 0)
        links = int(row[8] or 0)

        rows = await bot.store.fetchall(
            f"""
            SELECT message
            FROM irc_log
            WHERE channel IS NOT NULL AND lower(channel)=lower(?)
              AND actor_nick IS NOT NULL AND lower(actor_nick)=lower(?)
              AND event IN ('PRIVMSG','ACTION')
              {where_time}
            ORDER BY ts ASC
            """,
            (channel, nick, *params_time),
        )
        words = 0
        for r in rows:
            words += _count_words(r[0] or "")

        g = await bot.store.fetchone(
            f"""
            SELECT
              SUM(CASE WHEN event='QUIT' THEN 1 ELSE 0 END) AS quits,
              SUM(CASE WHEN event='NICK' THEN 1 ELSE 0 END) AS nickchanges
            FROM irc_log
            WHERE channel IS NULL
              AND actor_nick IS NOT NULL AND lower(actor_nick)=lower(?)
              {where_time}
            """,
            (nick, *params_time),
        )
        quits = int(g[0] or 0)
        nickchanges = int(g[1] or 0)

        await bot.privmsg(
            ev.target,
            (
                f"{ev.nick}: {nick} in {channel} ({window.label}): "
                f"msgs={msgs} words={words} links={links} actions={actions} notices={notices} "
                f"joins={joins} parts={parts_n} kicks={kicks} modes={modes} topics={topics} "
                f"(global: quits={quits} nickchanges={nickchanges})"
            ),
        )