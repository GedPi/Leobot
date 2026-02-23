from collections import defaultdict

ROLE_ORDER = {"guest": 0, "user": 1, "contributor": 2, "admin": 3}

def _role_rank(role: str) -> int:
    return ROLE_ORDER.get((role or "guest").lower(), 0)

def _can_see(user_role: str, cmd_min_role: str) -> bool:
    return _role_rank(user_role) >= _role_rank(cmd_min_role)

class HelpService:
    def __init__(self, bot):
        self.bot = bot

    async def on_privmsg(self, bot, ev):
        prefix = bot.cfg.get("command_prefix", "!")
        text = (getattr(ev, "text", "") or "").strip()
        if not text.startswith(prefix):
            return
        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return

        parts = cmdline.split()
        cmd = parts[0].lower()
        if cmd not in ("help", "commands"):
            return

        # Determine role
        role = "guest"
        if getattr(bot, "acl", None) is not None:
            try:
                role = bot.acl.role_for_event(ev)
            except Exception:
                role = "guest"

        registry = getattr(bot, "commands", {}) or {}
        if not registry:
            await bot.privmsg(ev.target, f"{ev.nick}: no commands registered yet.")
            return

        # Subcommands
        if len(parts) >= 2 and parts[1].lower() == "categories":
            cats = sorted({(m.get("category") or "General") for m in registry.values()}, key=lambda s: s.lower())
            await bot.privmsg(ev.target, f"{ev.nick}: categories: " + ", ".join(cats))
            return

        if len(parts) >= 2 and parts[1].lower() not in ("all",):
            key = " ".join(parts[1:]).lower().lstrip("!")
            meta = registry.get(key)
            if not meta:
                await bot.privmsg(ev.target, f"{ev.nick}: unknown command '{parts[1]}'. Try !help")
                return
            if not _can_see(role, meta.get("min_role", "guest")):
                await bot.privmsg(ev.target, f"{ev.nick}: you don't have access to '{parts[1]}'.")
                return
            h = meta.get("help") or "(no help text yet)"
            await bot.privmsg(ev.target, f"{prefix}{key} — {h} (min role: {meta.get('min_role','guest')})")
            return

        # List all visible commands grouped by category
        grouped = defaultdict(list)
        for c, meta in registry.items():
            min_role = meta.get("min_role", "guest")
            if not _can_see(role, min_role):
                continue
            grouped[meta.get("category", "General")].append(c)

        if not grouped:
            await bot.privmsg(ev.target, f"{ev.nick}: no commands available for your role.")
            return

        # PM if large
        out_target = ev.target
        if sum(len(v) for v in grouped.values()) > 12:
            out_target = ev.nick

        await bot.privmsg(out_target, f"{ev.nick}: commands (role: {role}). Use !help <command> for details. Also: !help categories")
        for cat in sorted(grouped.keys(), key=lambda s: s.lower()):
            cmds = " ".join(f"{prefix}{c}" for c in sorted(grouped[cat]))
            await bot.privmsg(out_target, f"{cat}: {cmds}")

def setup(bot):
    # Ensure command registry exists
    if not hasattr(bot, "commands") or not isinstance(getattr(bot, "commands"), dict):
        bot.commands = {}

    if hasattr(bot, "register_command"):
        bot.register_command(
            "help",
            min_role="guest",
            mutating=False,
            help="Show available commands. Usage: !help [command] | !help categories",
            category="General",
        )
        bot.register_command(
            "help categories",
            min_role="guest",
            mutating=False,
            help="List command categories.",
            category="General",
        )

    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register(
            "help",
            min_role="guest",
            mutating=False,
            help="Show available commands. Usage: !help [command] | !help categories",
            category="General",
        )
        bot.acl.register(
            "help categories",
            min_role="guest",
            mutating=False,
            help="List command categories.",
            category="General",
        )

    return HelpService(bot)
