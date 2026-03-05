from __future__ import annotations

# Per-channel service enablement: !service list|enable|disable; registers commands and handles them in handle_core.

from system.types import Event


# Normalizes service name to lowercase id (e.g. "services.weather" -> "weather").
def _canon_service_id(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return ""
    if "." in n:
        n = n.split(".")[-1]
    return n.lower()


# Returns (comma-joined first limit items, number of remaining) for display.
def _compact(items: list[str], *, limit: int = 12) -> tuple[str, int]:
    items = [x for x in items if x]
    if len(items) <= limit:
        return (", ".join(items) if items else "none", 0)
    return (", ".join(items[:limit]), len(items) - limit)


# Registers !service and !services and handles list/enable/disable using Store service_enablement.
class ServiceCtl:
    def register_commands(self, bot) -> None:
        bot.register_command(
            "service",
            min_role="contributor",
            mutating=False,
            help="Manage per-channel services. Usage: !service <list|enable|disable> [service] [#channel]",
            category="System",
        )
        bot.register_command(
            "service enable",
            min_role="contributor",
            mutating=True,
            help="Enable a service in a channel. Usage: !service enable <service> [#channel]",
            category="System",
        )
        bot.register_command(
            "service disable",
            min_role="contributor",
            mutating=True,
            help="Disable a service in a channel. Usage: !service disable <service> [#channel]",
            category="System",
        )
        bot.register_command(
            "services",
            min_role="contributor",
            mutating=False,
            help="Alias for !service list",
            category="System",
        )

    # Handles !service list (show on/off per channel) and !service enable|disable <service> [#channel]; uses config services as valid set.
    async def handle_core(self, bot, ev: Event) -> bool:
        prefix = bot.cfg.get("command_prefix", "!")
        txt = (ev.text or "").strip()
        if not txt.startswith(prefix):
            return False

        cmdline = txt[len(prefix) :].strip()
        if not cmdline:
            return False

        parts = cmdline.split()
        cmd = parts[0].lower()
        if cmd not in ("service", "services"):
            return False

        sub = "list"
        if cmd == "service" and len(parts) >= 2:
            sub = parts[1].lower()
        if cmd == "services":
            sub = "list"

        if sub == "list":
            chan = ev.channel or ev.target
            cfg_services = sorted(
                {_canon_service_id(str(x)) for x in (bot.cfg.get("services", []) or []) if str(x).strip()}
            )
            rows = await bot.store.list_service_enablement(chan)
            enabled_map = {str(s).lower(): bool(en) for s, en in rows}
            all_services = sorted(set(cfg_services) | set(enabled_map.keys()))

            on = [s for s in all_services if enabled_map.get(s, False)]
            off = [s for s in all_services if not enabled_map.get(s, False)]

            on_s, on_more = _compact(on)
            off_s, off_more = _compact(off)

            msg = f"{ev.nick}: services in {chan} — ON: {on_s}"
            if on_more:
                msg += f" (+{on_more} more)"
            msg += f" | OFF: {off_s}"
            if off_more:
                msg += f" (+{off_more} more)"

            await bot.privmsg(ev.target, msg)
            return True

        if sub in ("enable", "disable"):
            if len(parts) < 3:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !service {sub} <service> [#channel]")
                return True

            svc_in = parts[2]
            svc = _canon_service_id(svc_in)
            chan = parts[3] if len(parts) >= 4 else (ev.channel or ev.target)
            enabled = sub == "enable"
            cfg_services = {_canon_service_id(str(x)) for x in (bot.cfg.get("services", []) or []) if str(x).strip()}
            if enabled and svc not in cfg_services:
                avail = ", ".join(sorted(cfg_services)) if cfg_services else "(none configured)"
                await bot.privmsg(ev.target, f"{ev.nick}: unknown service '{svc_in}'. Available: {avail}")
                return True

            await bot.store.set_service_enabled(chan, svc, enabled, updated_by=ev.nick)
            await bot.privmsg(ev.target, f"{ev.nick}: {svc} {'ENABLED' if enabled else 'DISABLED'} in {chan}")
            return True

        await bot.privmsg(ev.target, f"{ev.nick}: unknown subcommand. Use !service list")
        return True