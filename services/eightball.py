from __future__ import annotations

import random
from typing import List


RESPONSES: List[str] = [
    "It is certain.",
    "It is decidedly so.",
    "Without a doubt.",
    "Yes — definitely.",
    "You may rely on it.",
    "As I see it, yes.",
    "Most likely.",
    "Outlook good.",
    "Yes.",
    "Signs point to yes.",
    "Reply hazy, try again.",
    "Ask again later.",
    "Better not tell you now.",
    "Cannot predict now.",
    "Concentrate and ask again.",
    "Don’t count on it.",
    "My reply is no.",
    "My sources say no.",
    "Outlook not so good.",
    "Very doubtful.",
]


class EightBallService:
    """
    Magic 8-ball.

    - Stateless.
    - Respects per-channel enablement via Dispatcher (service_id).
    - Commands: !8ball, !eightball
    """

    service_id = "eightball"

    async def on_privmsg(self, bot, ev) -> None:
        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()

        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return

        cmd, *rest = cmdline.split(maxsplit=1)
        cmd = cmd.lower()
        if cmd not in ("8ball", "eightball"):
            return

        question = rest[0].strip() if rest else ""
        if not question:
            await bot.privmsg(ev.target, f"{ev.nick}: Usage: !8ball <question>")
            return

        await bot.privmsg(ev.target, f"🎱 {random.choice(RESPONSES)}")


def setup(bot):
    # New-world command registry (preferred)
    if hasattr(bot, "register_command"):
        bot.register_command(
            "8ball",
            min_role="guest",
            mutating=False,
            help="Magic 8-ball. Usage: !8ball <question>",
            category="Fun",
        )
        bot.register_command(
            "eightball",
            min_role="guest",
            mutating=False,
            help="Alias for !8ball",
            category="Fun",
        )

    # Legacy ACL registry (harmless if still present)
    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register("8ball", min_role="guest", mutating=False, help="Magic 8-ball. Usage: !8ball <question>", category="Fun")
        bot.acl.register("eightball", min_role="guest", mutating=False, help="Alias for !8ball", category="Fun")

    return EightBallService()