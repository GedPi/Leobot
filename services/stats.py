import time

from services.chatdb import ChatDB, DBConfig, utc_day, word_count, has_link


def _window_to_since(window: str) -> int | None:
    now = int(time.time())
    w = (window or "").lower()
    if w == "today":
        # since UTC midnight
        day = time.strftime("%Y-%m-%d", time.gmtime(now))
        midnight = int(time.mktime(time.strptime(day, "%Y-%m-%d")))  # local mktime; acceptable for roughness
        # Safer: derive by truncation in UTC:
        midnight = now - (now % 86400)
        return midnight
    if w.endswith("d") and w[:-1].isdigit():
        days = int(w[:-1])
        return now - days * 86400
    if w in ("7d", "30d"):
        days = int(w[:-1])
        return now - days * 86400
    if w == "all":
        return None
    # default 7d
    return now - 7 * 86400


def _fmt_list(rows, fmt, limit=5):
    out = []
    for r in rows[:limit]:
        out.append(fmt(r))
    return ", ".join(out) if out else "(none)"


def _safe_int(x):
    try:
        return int(x)
    except Exception:
        return 0


class StatsService:
    def __init__(self, bot, db_path: str):
        self.bot = bot
        self.db = ChatDB(DBConfig(db_path))

    async def on_privmsg(self, bot, ev) -> None:
        # only count channel messages (not PM)
        target = getattr(ev, "target", "")
        is_chan = bool(target.startswith("#"))
        nick = getattr(ev, "nick", "") or ""
        text = getattr(ev, "text", "") or ""

        if is_chan and nick and text:
            ts = int(time.time())
            # only store stats counters here (messages already stored by lastseen if enabled)
            day = utc_day(ts)
            await self.db.execute(
                "INSERT INTO stats_daily(day, channel, nick) VALUES(?,?,?) ON CONFLICT(day, channel, nick) DO NOTHING",
                (day, target, nick),
            )
            await self.db.execute(
                """
                UPDATE stats_daily
                SET msgs = msgs + 1,
                    words = words + ?,
                    links = links + ?,
                    actions = actions + ?
                WHERE day=? AND channel=? AND nick=?
                """,
                (
                    word_count(text),
                    has_link(text),
                    1 if text.startswith("\x01ACTION ") else 0,
                    day,
                    target,
                    nick,
                ),
            )

        # commands
        prefix = bot.cfg.get("command_prefix", "!")
        raw = (text or "").strip()
        if not raw.startswith(prefix):
            return

        cmdline = raw[len(prefix):].strip()
        if not cmdline:
            return

        parts = cmdline.split()
        if parts[0].lower() != "stats":
            return

        window = "7d"
        top_n = 5
        # usage:
        # !stats
        # !stats today
        # !stats 30d
        # !stats all
        # !stats 7d 10
        if len(parts) >= 2:
            if parts[1].lower() in ("today", "7d", "30d", "all"):
                window = parts[1].lower()
            elif parts[1].lower().endswith("d") and parts[1][:-1].isdigit():
                window = parts[1].lower()

        if len(parts) >= 3 and parts[2].isdigit():
            top_n = max(3, min(15, int(parts[2])))

        channel = target if is_chan else (bot.cfg.get("channels", ["#"])[0])

        await self._cmd_stats(bot, ev, channel, window, top_n)

    async def on_join(self, bot, ev) -> None:
        channel = getattr(ev, "channel", None) or getattr(ev, "target", None)
        nick = getattr(ev, "nick", "") or ""
        if channel and isinstance(channel, str) and channel.startswith("#") and nick:
            day = utc_day()
            await self.db.execute(
                "INSERT INTO stats_daily(day, channel, nick) VALUES(?,?,?) ON CONFLICT(day, channel, nick) DO NOTHING",
                (day, channel, nick),
            )
            await self.db.execute(
                "UPDATE stats_daily SET joins=joins+1 WHERE day=? AND channel=? AND nick=?",
                (day, channel, nick),
            )

    async def on_part(self, bot, ev) -> None:
        channel = getattr(ev, "channel", None) or getattr(ev, "target", None)
        nick = getattr(ev, "nick", "") or ""
        if channel and isinstance(channel, str) and channel.startswith("#") and nick:
            day = utc_day()
            await self.db.execute(
                "INSERT INTO stats_daily(day, channel, nick) VALUES(?,?,?) ON CONFLICT(day, channel, nick) DO NOTHING",
                (day, channel, nick),
            )
            await self.db.execute(
                "UPDATE stats_daily SET parts=parts+1 WHERE day=? AND channel=? AND nick=?",
                (day, channel, nick),
            )

    async def on_quit(self, bot, ev) -> None:
        # attribute quit to the nick without a channel (still useful)
        nick = getattr(ev, "nick", "") or ""
        if not nick:
            return
        day = utc_day()
        channel = "(network)"
        await self.db.execute(
            "INSERT INTO stats_daily(day, channel, nick) VALUES(?,?,?) ON CONFLICT(day, channel, nick) DO NOTHING",
            (day, channel, nick),
        )
        await self.db.execute(
            "UPDATE stats_daily SET quits=quits+1 WHERE day=? AND channel=? AND nick=?",
            (day, channel, nick),
        )

    async def on_nick(self, bot, ev) -> None:
        channel = getattr(ev, "channel", None) or getattr(ev, "target", None)
        old = getattr(ev, "old_nick", "") or ""
        if channel and isinstance(channel, str) and channel.startswith("#") and old:
            day = utc_day()
            await self.db.execute(
                "INSERT INTO stats_daily(day, channel, nick) VALUES(?,?,?) ON CONFLICT(day, channel, nick) DO NOTHING",
                (day, channel, old),
            )
            await self.db.execute(
                "UPDATE stats_daily SET nickchanges=nickchanges+1 WHERE day=? AND channel=? AND nick=?",
                (day, channel, old),
            )

    async def _cmd_stats(self, bot, ev, channel: str, window: str, top_n: int) -> None:
        since = _window_to_since(window)

        # Top chatters
        if since is None:
            top_chat = await self.db.fetchall(
                """
                SELECT nick, SUM(msgs) AS m
                FROM stats_daily
                WHERE channel=?
                GROUP BY nick
                ORDER BY m DESC
                LIMIT ?
                """,
                (channel, top_n),
            )
        else:
            day_since = time.strftime("%Y-%m-%d", time.gmtime(since))
            top_chat = await self.db.fetchall(
                """
                SELECT nick, SUM(msgs) AS m
                FROM stats_daily
                WHERE channel=? AND day >= ?
                GROUP BY nick
                ORDER BY m DESC
                LIMIT ?
                """,
                (channel, day_since, top_n),
            )

        # Nick changes top
        if since is None:
            top_nickchg = await self.db.fetchall(
                """
                SELECT nick, SUM(nickchanges) AS n
                FROM stats_daily
                WHERE channel=?
                GROUP BY nick
                ORDER BY n DESC
                LIMIT ?
                """,
                (channel, top_n),
            )
        else:
            day_since = time.strftime("%Y-%m-%d", time.gmtime(since))
            top_nickchg = await self.db.fetchall(
                """
                SELECT nick, SUM(nickchanges) AS n
                FROM stats_daily
                WHERE channel=? AND day >= ?
                GROUP BY nick
                ORDER BY n DESC
                LIMIT ?
                """,
                (channel, day_since, top_n),
            )

        # Links top
        if since is None:
            top_links = await self.db.fetchall(
                """
                SELECT nick, SUM(links) AS l
                FROM stats_daily
                WHERE channel=?
                GROUP BY nick
                ORDER BY l DESC
                LIMIT ?
                """,
                (channel, top_n),
            )
        else:
            day_since = time.strftime("%Y-%m-%d", time.gmtime(since))
            top_links = await self.db.fetchall(
                """
                SELECT nick, SUM(links) AS l
                FROM stats_daily
                WHERE channel=? AND day >= ?
                GROUP BY nick
                ORDER BY l DESC
                LIMIT ?
                """,
                (channel, day_since, top_n),
            )

        # Joins/Parts/Quits totals
        if since is None:
            totals = await self.db.fetchone(
                """
                SELECT
                  COALESCE(SUM(joins),0),
                  COALESCE(SUM(parts),0),
                  COALESCE(SUM(quits),0)
                FROM stats_daily
                WHERE channel=?
                """,
                (channel,),
            )
        else:
            day_since = time.strftime("%Y-%m-%d", time.gmtime(since))
            totals = await self.db.fetchone(
                """
                SELECT
                  COALESCE(SUM(joins),0),
                  COALESCE(SUM(parts),0),
                  COALESCE(SUM(quits),0)
                FROM stats_daily
                WHERE channel=? AND day >= ?
                """,
                (channel, day_since),
            )

        joins, parts, quits = totals or (0, 0, 0)

        # Peak hour from messages (since daily aggregates don't store hour)
        # If you keep full logs, this is accurate.
        if since is None:
            peak = await self.db.fetchone(
                """
                SELECT CAST((ts % 86400) / 3600 AS INT) AS hr, COUNT(*) AS c
                FROM messages
                WHERE channel=?
                GROUP BY hr
                ORDER BY c DESC
                LIMIT 1
                """,
                (channel,),
            )
        else:
            peak = await self.db.fetchone(
                """
                SELECT CAST((ts % 86400) / 3600 AS INT) AS hr, COUNT(*) AS c
                FROM messages
                WHERE channel=? AND ts >= ?
                GROUP BY hr
                ORDER BY c DESC
                LIMIT 1
                """,
                (channel, since),
            )

        peak_hour = "?"
        if peak:
            peak_hour = f"{int(peak[0]):02d}:00"

        def fmt_pair(row):
            return f"{row[0]}({_safe_int(row[1])})"

        out = (
            f"STATS ({channel}, {window}): "
            f"Top chatters: {_fmt_list(top_chat, fmt_pair, top_n)}. "
            f"Nick changes: {_fmt_list(top_nickchg, fmt_pair, top_n)}. "
            f"Links: {_fmt_list(top_links, fmt_pair, top_n)}. "
            f"Joins/Parts/Quits: {joins}/{parts}/{quits}. "
            f"Peak hour: {peak_hour}."
        )
        await bot.privmsg(ev.target, out)


def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command("stats", min_role="user", mutating=False, help="Channel stats. Usage: !stats [today|7d|30d|all] [topN]", category="Fun")
    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register("stats", min_role="user", mutating=False, help="Channel stats. Usage: !stats [today|7d|30d|all] [topN]", category="Fun")

    return StatsService(bot, str((bot.cfg.get('chatdb', {}) if isinstance(bot.cfg, dict) else {}).get('db_path', '/var/lib/leobot/db/leobot.db')))
