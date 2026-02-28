from __future__ import annotations

import time


def setup(bot):
    return StatsService(bot)


def _utc_day(ts: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ts))


class StatsService:
    """Basic daily stats from stats_daily table (populated by logging service)."""

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

        day = _utc_day(int(time.time()))
        nick = parts[1] if len(parts) >= 2 else ev.nick
        chan = ev.channel or "(pm)"

        if not ev.channel:
            await bot.privmsg(ev.target, f"{ev.nick}: stats are channel-based; run in a channel.")
            return

        row = await bot.store.fetchone(
            "SELECT msgs,words,links,actions,joins,parts,quits,kicks,nickchanges FROM stats_daily WHERE day=? AND channel=? AND nick=?",
            (day, ev.channel, nick),
        )
        if not row:
            await bot.privmsg(ev.target, f"{ev.nick}: no stats for {nick} today in {ev.channel}")
            return

        await bot.privmsg(
            ev.target,
            f"{ev.nick}: {nick} today in {ev.channel}: msgs={row[0]} words={row[1]} links={row[2]} actions={row[3]} joins={row[4]} parts={row[5]} kicks={row[7]} nickchanges={row[8]}",
        )
