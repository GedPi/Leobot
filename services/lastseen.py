from __future__ import annotations

import time


def setup(bot):
    return LastSeenService(bot)


def _now() -> int:
    return int(time.time())


def _fmt_age(seconds: int) -> str:
    if seconds < 0:
        seconds = 0
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 48:
        return f"{hours}h"
    days = hours // 24
    if days < 14:
        return f"{days}d"
    weeks = days // 7
    if weeks < 8:
        return f"{weeks}w"
    months = days // 30
    return f"{months}mo"


def _clip(s: str, n: int = 120) -> str:
    s = (s or "").replace("\n", " ").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


class LastSeenService:
    """
    !seen <nick> / !lastseen <nick>

    Reads from irc_log only (posterity journal).
    If last event is QUIT, attempts to include the last PRIVMSG/ACTION within 15 minutes before quitting.
    """

    service_id = "lastseen"

    def __init__(self, bot):
        self.bot = bot
        bot.register_command(
            "seen",
            min_role="user",
            mutating=False,
            help="Show last seen info. Usage: !seen <nick>",
            category="Utility",
        )
        bot.register_command(
            "lastseen",
            min_role="user",
            mutating=False,
            help="Alias for !seen <nick>",
            category="Utility",
        )

    async def on_privmsg(self, bot, ev):
        prefix = bot.cfg.get("command_prefix", "!")
        txt = (ev.text or "").strip()
        if not txt.startswith(prefix):
            return

        parts = txt[len(prefix):].strip().split()
        if not parts:
            return

        cmd = parts[0].lower()
        if cmd not in ("seen", "lastseen"):
            return

        if len(parts) < 2:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !{cmd} <nick>")
            return

        q = parts[1].strip()
        if not q:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !{cmd} <nick>")
            return

        # 1) Direct lookup by actor_nick
        row = await bot.store.fetchone(
            """
            SELECT id, ts, channel, event, rendered
            FROM irc_log
            WHERE actor_nick IS NOT NULL AND lower(actor_nick)=lower(?)
            ORDER BY ts DESC, id DESC
            LIMIT 1
            """,
            (q,),
        )

        # 2) If not found, attempt to resolve as "old nick" -> "new nick"
        resolved = q
        if not row:
            nick_row = await bot.store.fetchone(
                """
                SELECT id, ts, target
                FROM irc_log
                WHERE event='NICK' AND message IS NOT NULL AND lower(message)=lower(?)
                ORDER BY ts DESC, id DESC
                LIMIT 1
                """,
                (q,),
            )
            if nick_row and nick_row[2]:
                resolved = str(nick_row[2])
                row = await bot.store.fetchone(
                    """
                    SELECT id, ts, channel, event, rendered
                    FROM irc_log
                    WHERE actor_nick IS NOT NULL AND lower(actor_nick)=lower(?)
                    ORDER BY ts DESC, id DESC
                    LIMIT 1
                    """,
                    (resolved,),
                )

        if not row:
            await bot.privmsg(ev.target, f"{ev.nick}: no record for {q}")
            return

        last_ts = int(row[1])
        last_chan = row[2] or "(global)"
        last_event = (row[3] or "?").upper()
        last_rendered = row[4] or ""
        age = _fmt_age(_now() - last_ts)

        display_nick = resolved

        # Enrich QUIT with last message within 15 minutes
        if last_event == "QUIT":
            window_start = last_ts - 900  # 15 minutes before quit
            msg_row = await bot.store.fetchone(
                """
                SELECT ts, channel, event, rendered, message
                FROM irc_log
                WHERE actor_nick IS NOT NULL AND lower(actor_nick)=lower(?)
                  AND event IN ('PRIVMSG','ACTION')
                  AND ts >= ? AND ts <= ?
                ORDER BY ts DESC, id DESC
                LIMIT 1
                """,
                (resolved, window_start, last_ts),
            )

            if msg_row:
                msg_rendered = msg_row[3] or ""
                # Prefer a cleaner quote from message column if present
                msg_text = msg_row[4] or msg_rendered
                msg_text = _clip(msg_text, 140)

                quit_text = _clip(last_rendered, 200)

                if display_nick.lower() != q.lower():
                    await bot.privmsg(
                        ev.target,
                        f"{ev.nick}: {q} is now {display_nick}. I last saw {display_nick} {age} ago; "
                        f"last message was \"{msg_text}\" before quitting \"{quit_text}\""
                    )
                else:
                    await bot.privmsg(
                        ev.target,
                        f"{ev.nick}: I last saw {display_nick} {age} ago; "
                        f"last message was \"{msg_text}\" before quitting \"{quit_text}\""
                    )
                return

            # QUIT but no recent message found
            quit_text = _clip(last_rendered, 220)
            if display_nick.lower() != q.lower():
                await bot.privmsg(
                    ev.target,
                    f"{ev.nick}: {q} is now {display_nick}. I last saw {display_nick} {age} ago; quitting \"{quit_text}\""
                )
            else:
                await bot.privmsg(ev.target, f"{ev.nick}: I last saw {display_nick} {age} ago; quitting \"{quit_text}\"")
            return

        # Default output for non-QUIT events
        if display_nick.lower() != q.lower():
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: {q} is now {display_nick}. Last seen {age} ago in {last_chan} [{last_event}] {last_rendered}"
            )
        else:
            await bot.privmsg(ev.target, f"{ev.nick}: {display_nick} last seen {age} ago in {last_chan} [{last_event}] {last_rendered}")