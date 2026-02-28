from __future__ import annotations

import time


def setup(bot):
    return LastSeenService(bot)


class LastSeenService:
    """!seen <nick> (reads the shared 'seen' table populated by logging service)."""

    def __init__(self, bot):
        self.bot = bot
        bot.register_command("seen", min_role="user", mutating=False, help="Show last seen info. Usage: !seen <nick>", category="Utility")

    async def on_privmsg(self, bot, ev):
        prefix = bot.cfg.get("command_prefix", "!")
        txt = (ev.text or "").strip()
        if not txt.startswith(prefix):
            return
        parts = txt[len(prefix):].strip().split()
        if not parts or parts[0].lower() != "seen":
            return
        if len(parts) < 2:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !seen <nick>")
            return
        q = parts[1]
        row = await bot.store.fetchone("SELECT ts,event,channel,last_msg FROM seen WHERE nick=?", (q,))
        if not row:
            await bot.privmsg(ev.target, f"{ev.nick}: no record for {q}")
            return
        ts = int(row[0])
        ago = int(time.time()) - ts
        chan = row[2] or ""
        extra = row[3] or ""
        await bot.privmsg(ev.target, f"{ev.nick}: {q} last seen {ago}s ago ({row[1]}) {chan} {extra}")
