from __future__ import annotations

# Handles !help and !commands: shows command help or lists commands by category filtered by caller role.

from collections import defaultdict

from system.acl import ROLE_ORDER
from system.types import Event


# Splits string into chunks of at most maxlen characters on word boundaries for IRC.
def _split_message(s: str, *, maxlen: int = 380) -> list[str]:
    s = (s or "").strip()
    if not s:
        return []
    if len(s) <= maxlen:
        return [s]

    parts = s.split(" ")
    out: list[str] = []
    cur = ""
    for p in parts:
        if not cur:
            cur = p
            continue
        if len(cur) + 1 + len(p) > maxlen:
            out.append(cur)
            cur = p
        else:
            cur += " " + p
    if cur:
        out.append(cur)
    return out


# Sends message to target in chunks via _split_message to avoid truncation.
async def _privmsg_split(bot, target: str, s: str, *, maxlen: int = 380) -> None:
    for line in _split_message(s, maxlen=maxlen):
        await bot.privmsg(target, line)


# Core handler for !help and !commands; filters visible commands by effective_role and shows category list or single-command help.
# All replies are sent as a private message to the user (ev.nick) to avoid flooding the channel.
class Help:
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

        if cmd not in ("help", "commands"):
            return False

        role = await bot.acl.effective_role(ev)
        reply_to = ev.nick

        if cmd == "help" and len(parts) >= 2:
            q_raw = " ".join(parts[1:]).strip()
            q = q_raw.lower()

            info = bot.commands.get(q)
            if info:
                await _privmsg_split(
                    bot,
                    reply_to,
                    f"{q} — category={info['category']} | role>={info['min_role']} | mutating={info['mutating']} | {info['help'] or 'no help text'}",
                )
                return True

            cats = defaultdict(list)
            for name, info2 in bot.commands.items():
                min_role = info2["min_role"]
                if ROLE_ORDER.get(role, 0) < ROLE_ORDER.get(min_role, 0):
                    continue
                cats[str(info2["category"]).lower()].append(name)

            if q in cats:
                cmds = ", ".join(sorted(cats[q]))
                await _privmsg_split(bot, reply_to, f"{q_raw} commands: {cmds}")
                return True

            await bot.privmsg(reply_to, f"Unknown command/category '{q_raw}'. Try !commands")
            return True

        cats = defaultdict(list)
        for name, info in bot.commands.items():
            min_role = info["min_role"]
            if ROLE_ORDER.get(role, 0) < ROLE_ORDER.get(min_role, 0):
                continue
            cats[info["category"]].append(name)

        if not cats:
            await bot.privmsg(reply_to, "No commands available")
            return True

        await bot.privmsg(reply_to, f"Commands for role={role}: use !help <command> or !help <category>")
        for cat in sorted(cats.keys()):
            cmds = ", ".join(sorted(cats[cat]))
            await _privmsg_split(bot, reply_to, f"{cat}: {cmds}")

        return True