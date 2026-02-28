from __future__ import annotations

import time


def setup(bot):
    return MaintenanceService(bot)


class MaintenanceService:
    """Maintenance commands. Currently: !prune <days> (requires admin)."""

    def __init__(self, bot):
        self.bot = bot
        bot.register_command("prune", min_role="admin", mutating=True, help="Delete logged messages older than N days. Usage: !prune <days>", category="System")

    async def on_privmsg(self, bot, ev):
        prefix = bot.cfg.get("command_prefix", "!")
        txt = (ev.text or "").strip()
        if not txt.startswith(prefix):
            return
        parts = txt[len(prefix):].strip().split()
        if not parts or parts[0].lower() != "prune":
            return
        if len(parts) < 2 or not parts[1].isdigit():
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !prune <days>")
            return
        days = int(parts[1])
        cutoff = int(time.time()) - days * 86400
        # delete
        row = await bot.store.fetchone("SELECT COUNT(*) FROM messages WHERE ts < ?", (cutoff,))
        n = int(row[0]) if row else 0
        await bot.store.execute("DELETE FROM messages WHERE ts < ?", (cutoff,))
        await bot.privmsg(ev.target, f"{ev.nick}: pruned {n} messages older than {days}d")
