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

    # not a window token
    return Window("last 24h", now_ts - 86400, now_ts + 1), args


class StatsService:
    """
    !stats [nick] [window]

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
            help="Usage: !stats [nick] [today|24h|7d|all|YYYY-MM-DD]. Default=24h.",
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

        # Decide if first arg is a window token/date or a nick
        nick = ev.nick
        window = None

        if args:
            # Try parse window first
            w, rem = _parse_window(args, now)
            if rem != args:
                # first token was window
                window = w
                args = rem
            else:
                # first token wasn't a window; treat as nick, then parse window from the remainder
                nick = args[0]
                args = args[1:]
                w2, rem2 = _parse_window(args, now)
                window = w2
                args = rem2
        else:
            window = Window("last 24h", now - 86400, now + 1)

        # Build WHERE fragments
        where_time = ""
        params_time = []
        if window.start_ts is not None:
            where_time += " AND ts >= ?"
            params_time.append(int(window.start_ts))
        if window.end_ts is not None:
            where_time += " AND ts < ?"
            params_time.append(int(window.end_ts))

        chan_param = ev.channel

        # Channel-scoped aggregates
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
            (chan_param, nick, *params_time),
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

        # Word count (only PRIVMSG/ACTION)
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
            (chan_param, nick, *params_time),
        )
        words = 0
        for r in rows:
            words += _count_words(r[0] or "")

        # Global events in same window
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
                f"{ev.nick}: {nick} in {ev.channel} ({window.label}): "
                f"msgs={msgs} words={words} links={links} actions={actions} notices={notices} "
                f"joins={joins} parts={parts_n} kicks={kicks} modes={modes} topics={topics} "
                f"(global: quits={quits} nickchanges={nickchanges})"
            ),
        )