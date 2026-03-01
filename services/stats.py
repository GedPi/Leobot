from __future__ import annotations

import time


def setup(bot):
    return StatsService(bot)


def _utc_day_bounds(ts: int) -> tuple[int, int, str]:
    # returns (start_ts, end_ts, day_str) in UTC
    day_str = time.strftime("%Y-%m-%d", time.gmtime(ts))
    start = int(time.mktime(time.strptime(day_str, "%Y-%m-%d")))  # local-based; don't use
    # Do it properly: build via gmtime components
    y, m, d = time.gmtime(ts).tm_year, time.gmtime(ts).tm_mon, time.gmtime(ts).tm_mday
    start_utc = int(time.mktime((y, m, d, 0, 0, 0, 0, 0, 0)))  # still local tz influenced in some envs

    # Avoid timezone traps: compute with gmtime -> seconds via calendar.timegm
    import calendar
    start_ts = calendar.timegm((y, m, d, 0, 0, 0))
    end_ts = start_ts + 86400
    return start_ts, end_ts, day_str


def _count_words(s: str) -> int:
    # cheap + stable for IRC text
    return len([w for w in (s or "").strip().split() if w])


class StatsService:
    """!stats [nick] (reads irc_log; no duplicate logging)."""

    service_id = "stats"

    def __init__(self, bot):
        self.bot = bot
        bot.register_command("stats", min_role="user", mutating=False, help="Show today's stats. Usage: !stats [nick]", category="Utility")

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

        nick = parts[1] if len(parts) >= 2 else ev.nick
        now = int(time.time())
        start_ts, end_ts, day_str = _utc_day_bounds(now)

        # Channel-scoped aggregates
        row = await bot.store.fetchone(
            """
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
            WHERE channel=?
              AND actor_nick IS NOT NULL AND lower(actor_nick)=lower(?)
              AND ts >= ? AND ts < ?
            """,
            (ev.channel, nick, start_ts, end_ts),
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

        # Word count (only PRIVMSG/ACTION in this channel/day/user)
        rows = await bot.store.fetchall(
            """
            SELECT message
            FROM irc_log
            WHERE channel=?
              AND actor_nick IS NOT NULL AND lower(actor_nick)=lower(?)
              AND event IN ('PRIVMSG','ACTION')
              AND ts >= ? AND ts < ?
            ORDER BY ts ASC
            """,
            (ev.channel, nick, start_ts, end_ts),
        )
        words = 0
        for r in rows:
            words += _count_words(r[0] or "")

        # Global events: quit/nickchange today (not tied to channel)
        g = await bot.store.fetchone(
            """
            SELECT
              SUM(CASE WHEN event='QUIT' THEN 1 ELSE 0 END) AS quits,
              SUM(CASE WHEN event='NICK' THEN 1 ELSE 0 END) AS nickchanges
            FROM irc_log
            WHERE channel IS NULL
              AND actor_nick IS NOT NULL AND lower(actor_nick)=lower(?)
              AND ts >= ? AND ts < ?
            """,
            (nick, start_ts, end_ts),
        )
        quits = int(g[0] or 0)
        nickchanges = int(g[1] or 0)

        await bot.privmsg(
            ev.target,
            (
                f"{ev.nick}: {nick} on {day_str} in {ev.channel}: "
                f"msgs={msgs} words={words} links={links} actions={actions} notices={notices} "
                f"joins={joins} parts={parts_n} kicks={kicks} modes={modes} topics={topics} "
                f"(global: quits={quits} nickchanges={nickchanges})"
            ),
        )