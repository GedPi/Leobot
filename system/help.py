from __future__ import annotations

from collections import defaultdict

from system.types import Event
from system.acl import ROLE_ORDER


class Help:
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

        if cmd not in ("help", "commands"):
            return False

        role = await bot.acl.effective_role(ev)

        if cmd == "help" and len(parts) >= 2:
            q = " ".join([p.lower() for p in parts[1:]])
            info = bot.commands.get(q)
            if not info:
                await bot.privmsg(ev.target, f"{ev.nick}: unknown command '{q}'. Try !commands")
                return True
            await bot.privmsg(
                ev.target,
                f"{q} — role>={info['min_role']} | mutating={info['mutating']} | {info['help'] or 'no help text'}",
            )
            return True

        cats = defaultdict(list)
        for name, info in bot.commands.items():
            min_role = info["min_role"]
            if ROLE_ORDER.get(role, 0) < ROLE_ORDER.get(min_role, 0):
                continue
            cats[info["category"]].append(name)

        out = []
        for cat in sorted(cats.keys()):
            cmds = ", ".join(sorted(cats[cat]))
            out.append(f"{cat}: {cmds}")

        if not out:
            await bot.privmsg(ev.target, f"{ev.nick}: no commands available")
        else:
            await bot.privmsg(ev.target, f"Commands for role={role}: " + " | ".join(out))
        return True
