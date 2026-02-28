from __future__ import annotations

from system.types import Event


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

    async def handle_core(self, bot, ev: Event) -> bool:
        prefix = bot.cfg.get("command_prefix", "!")
        txt = (ev.text or "").strip()
        if not txt.startswith(prefix):
            return False

        cmdline = txt[len(prefix):].strip()
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
            rows = await bot.store.list_service_enablement(chan)
            if not rows:
                await bot.privmsg(ev.target, f"{ev.nick}: services in {chan} — none enabled (default disabled).")
                return True
            on = [s for s, en in rows if en]
            off = [s for s, en in rows if not en]
            msg = f"{ev.nick}: services in {chan} — ON: {', '.join(on) if on else 'none'} | OFF: {', '.join(off) if off else 'none'}"
            await bot.privmsg(ev.target, msg)
            return True

        if sub in ("enable", "disable"):
            if len(parts) < 3:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !service {sub} <service> [#channel]")
                return True
            svc = parts[2]
            chan = parts[3] if len(parts) >= 4 else (ev.channel or ev.target)
            enabled = sub == "enable"
            await bot.store.set_service_enabled(chan, svc, enabled, updated_by=ev.nick)
            await bot.privmsg(ev.target, f"{ev.nick}: {svc} {'ENABLED' if enabled else 'DISABLED'} in {chan}")
            return True

        await bot.privmsg(ev.target, f"{ev.nick}: unknown subcommand. Use !service list")
        return True
