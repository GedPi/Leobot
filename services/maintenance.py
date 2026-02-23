import time

from services.chatdb import ChatDB, DBConfig


def _parse_age(token: str) -> int | None:
    """
    Returns seconds for "<Nd>" / "<Nh>" / "<Nw>" or None for "all".
    """
    t = (token or "").strip().lower()
    if t == "all":
        return None

    if len(t) < 2:
        return -1

    n, unit = t[:-1], t[-1]
    if not n.isdigit():
        return -1
    v = int(n)

    if v <= 0:
        return -1

    if unit == "h":
        return v * 3600
    if unit == "d":
        return v * 86400
    if unit == "w":
        return v * 7 * 86400
    return -1


class MaintenanceService:
    def __init__(self, bot, db_path: str):
        self.bot = bot
        self.db = ChatDB(DBConfig(db_path))

    async def on_privmsg(self, bot, ev) -> None:
        prefix = bot.cfg.get("command_prefix", "!")
        text = (getattr(ev, "text", "") or "").strip()
        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return

        parts = cmdline.split()
        if parts[0].lower() != "prune":
            return

        # !prune vacuum
        if len(parts) == 2 and parts[1].lower() == "vacuum":
            await bot.privmsg(ev.target, "PRUNE: running VACUUM (may take a moment).")
            await self.db.execute("VACUUM;")
            await bot.privmsg(ev.target, "PRUNE: VACUUM complete.")
            return

        # !prune messages 30d|12h|4w|all
        if len(parts) != 3:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !prune messages <Nd|Nh|Nw|all>  OR  !prune vacuum")
            return

        scope = parts[1].lower()
        age = parts[2].lower()

        if scope not in ("messages", "stats"):
            await bot.privmsg(ev.target, f"{ev.nick}: scope must be 'messages' or 'stats'")
            return

        secs = _parse_age(age)
        if secs == -1:
            await bot.privmsg(ev.target, f"{ev.nick}: age must look like 30d, 12h, 4w, or 'all'")
            return

        if scope == "messages":
            if secs is None:
                await self.db.execute("DELETE FROM messages;")
                await bot.privmsg(ev.target, "PRUNE: deleted ALL messages. Run !prune vacuum to reclaim space.")
                return
            cutoff = int(time.time()) - secs
            await self.db.execute("DELETE FROM messages WHERE ts < ?", (cutoff,))
            await bot.privmsg(ev.target, f"PRUNE: deleted messages older than {age}. Run !prune vacuum to reclaim space.")
            return

        # scope == stats (aggregates)
        if secs is None:
            await self.db.execute("DELETE FROM stats_daily;")
            await bot.privmsg(ev.target, "PRUNE: deleted ALL stats_daily rows. Run !prune vacuum to reclaim space.")
            return

        # stats_daily is keyed by day string; we prune by computing cutoff day in UTC.
        cutoff_ts = int(time.time()) - secs
        cutoff_day = time.strftime("%Y-%m-%d", time.gmtime(cutoff_ts))
        await self.db.execute("DELETE FROM stats_daily WHERE day < ?", (cutoff_day,))
        await bot.privmsg(ev.target, f"PRUNE: deleted stats_daily older than {age}. Run !prune vacuum to reclaim space.")


def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command(
            "prune",
            min_role="admin",
            mutating=True,
            help="Prune stored chat data. Usage: !prune <messages|stats> <Nd|Nh|Nw|all> | !prune vacuum",
            category="Maintenance",
        )
        bot.register_command(
            "prune messages",
            min_role="admin",
            mutating=True,
            help="Delete stored raw message logs. Usage: !prune messages <Nd|Nh|Nw|all>",
            category="Maintenance",
        )
        bot.register_command(
            "prune stats",
            min_role="admin",
            mutating=True,
            help="Delete aggregated stats. Usage: !prune stats <Nd|Nh|Nw|all>",
            category="Maintenance",
        )
        bot.register_command(
            "prune vacuum",
            min_role="admin",
            mutating=True,
            help="Reclaim SQLite space after pruning. Usage: !prune vacuum",
            category="Maintenance",
        )
    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register(
            "prune",
            min_role="admin",
            mutating=True,
            help="Prune stored chat data. Usage: !prune <messages|stats> <Nd|Nh|Nw|all> | !prune vacuum",
            category="Maintenance",
        )
        bot.acl.register(
            "prune messages",
            min_role="admin",
            mutating=True,
            help="Delete stored raw message logs. Usage: !prune messages <Nd|Nh|Nw|all>",
            category="Maintenance",
        )
        bot.acl.register(
            "prune stats",
            min_role="admin",
            mutating=True,
            help="Delete aggregated stats. Usage: !prune stats <Nd|Nh|Nw|all>",
            category="Maintenance",
        )
        bot.acl.register(
            "prune vacuum",
            min_role="admin",
            mutating=True,
            help="Reclaim SQLite space after pruning. Usage: !prune vacuum",
            category="Maintenance",
        )

    return MaintenanceService(bot, str((bot.cfg.get('chatdb', {}) if isinstance(bot.cfg, dict) else {}).get('db_path', '/var/lib/leobot/db/leobot.db')))
