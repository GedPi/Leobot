from __future__ import annotations

# Handles !help and !commands: shows command help or lists commands by category filtered by caller role.

from collections import defaultdict
import re

from system.acl import ROLE_ORDER
from system.commands import parse_command
from system.types import Event
from system.messaging import send_user_message


_USAGE_RE = re.compile(r"(Usage:\s*.+)", re.IGNORECASE)


def get_command_help(bot, command_key: str) -> str | None:
    key = (command_key or "").strip().lower().lstrip("!")
    if not key:
        return None
    info = bot.commands.get(key) if hasattr(bot, "commands") else None
    if not info:
        return None
    return (info.get("help") or "").strip() or None


async def send_command_usage(bot, ev: Event, command_key: str, fallback: str | None = None) -> None:
    help_text = get_command_help(bot, command_key)
    usage = None
    if help_text:
        m = _USAGE_RE.search(help_text)
        usage = m.group(1).strip() if m else f"Usage: !{(command_key or '').strip().lstrip('!')}"
    msg = usage or (fallback.strip() if fallback else "").strip()
    if not msg:
        return
    await bot.privmsg(ev.target, f"{ev.nick}: {msg}")


# Core handler for !help and !commands; filters visible commands by effective_role and shows category list or single-command help.
# All replies are sent as a private message to the user (ev.nick) to avoid flooding the channel.
class Help:
    async def handle_core(self, bot, ev: Event) -> bool:
        parsed = parse_command(ev.text, bot.cfg.get("command_prefix", "!"))
        if not parsed:
            return False

        cmd = parsed["name"]
        parts = [cmd] + parsed["args"]

        if cmd not in ("help", "commands"):
            return False

        role = await bot.acl.effective_role(ev)
        reply_to = ev.nick

        if cmd == "help" and len(parts) >= 2:
            q_raw = " ".join(parts[1:]).strip()
            q = q_raw.lower()

            info = bot.commands.get(q)
            if info:
                await send_user_message(bot, ev, f"{q} — category={info['category']} | role>={info['min_role']} | mutating={info['mutating']} | {info['help'] or 'no help text'}", scope="pm", mention=False)
                return True

            cats = defaultdict(list)
            for name, info2 in bot.commands.items():
                min_role = info2["min_role"]
                if ROLE_ORDER.get(role, 0) < ROLE_ORDER.get(min_role, 0):
                    continue
                cats[str(info2["category"]).lower()].append(name)

            if q in cats:
                cmds = ", ".join(sorted(cats[q]))
                await send_user_message(bot, ev, f"{q_raw} commands: {cmds}", scope="pm", mention=False)
                return True

            await send_user_message(bot, ev, f"Unknown command/category '{q_raw}'. Try !commands", scope="pm", mention=False)
            return True

        cats = defaultdict(list)
        for name, info in bot.commands.items():
            min_role = info["min_role"]
            if ROLE_ORDER.get(role, 0) < ROLE_ORDER.get(min_role, 0):
                continue
            cats[info["category"]].append(name)

        if not cats:
            await send_user_message(bot, ev, "No commands available", scope="pm", mention=False)
            return True

        await send_user_message(bot, ev, f"Commands for role={role}: use !help <command> or !help <category>", scope="pm", mention=False)
        for cat in sorted(cats.keys()):
            cmds = ", ".join(sorted(cats[cat]))
            await send_user_message(bot, ev, f"{cat}: {cmds}", scope="pm", mention=False)

        return True
