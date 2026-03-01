from __future__ import annotations

import time


def setup(bot):
    return LastSeenService(bot)


def _now() -> int:
    return int(time.time())


class LastSeenService:
    """!seen <nick> / !lastseen <nick> (reads irc_log; no duplicate logging)."""

    service_id = "lastseen"

    def __init__(self, bot):
        self.bot = bot
        bot.register_command("seen", min_role="user", mutating=False, help="Show last seen info. Usage: !seen <nick>", category="Utility")
        bot.register_command("lastseen", min_role="user", mutating=False, help="Alias for !seen <nick>", category="Utility")

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

        ts = int(row[1])
        ago = _now() - ts
        chan = row[2] or "(global)"
        event = row[3] or "?"
        rendered = row[4] or ""

        if resolved.lower() != q.lower():
            await bot.privmsg(ev.target, f"{ev.nick}: {q} is now {resolved}. Last seen {ago}s ago in {chan} [{event}] {rendered}")
        else:
            await bot.privmsg(ev.target, f"{ev.nick}: {q} last seen {ago}s ago in {chan} [{event}] {rendered}")