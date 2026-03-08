"""
Insult service: Evil Insult Generator API.
Commands: !insulten {nick} (English), !insultel {nick} (Greek).
Requires at least user role (admins, contributors, users); guests are denied.
"""
from __future__ import annotations

import asyncio
import json
import urllib.request

API_BASE = "https://evilinsult.com/generate_insult.php"
UA = "LeonidasIRCbot/2.0 (https://hairyoctopus.net; admin: Ged)"


def _fetch_insult(lang: str, timeout: int = 10) -> str | None:
    url = f"{API_BASE}?lang={lang}&type=json"
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = json.loads(r.read().decode("utf-8", errors="replace"))
    return (data.get("insult") or "").strip() or None


async def _get_insult(lang: str) -> str | None:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _fetch_insult, lang)


class InsultService:
    """
    Evil Insult Generator: !insulten {nick} (English), !insultel {nick} (Greek).
    min_role=user so guests cannot use it.
    """

    service_id = "insult"

    async def on_privmsg(self, bot, ev) -> None:
        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()

        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix) :].strip()
        if not cmdline:
            return

        parts = cmdline.split(maxsplit=1)
        cmd = (parts[0] or "").lower()
        if cmd not in ("insulten", "insultel"):
            return

        nick = (parts[1].strip() if len(parts) > 1 else "").strip()
        if not nick:
            await bot.privmsg(ev.target, f"{ev.nick}: Usage: !{cmd} <nick>")
            return

        lang = "en" if cmd == "insulten" else "el"
        try:
            insult = await _get_insult(lang)
        except Exception:
            await bot.privmsg(ev.target, f"{ev.nick}: Insult API error.")
            return

        if not insult:
            await bot.privmsg(ev.target, f"{ev.nick}: No insult returned.")
            return

        await bot.privmsg(ev.target, f"{nick}: {insult}")


def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command(
            "insulten",
            min_role="user",
            mutating=False,
            help="Insult someone in English. Usage: !insulten <nick>",
            category="Fun",
            service_id="insult",
        )
        bot.register_command(
            "insultel",
            min_role="user",
            mutating=False,
            help="Insult someone in Greek. Usage: !insultel <nick>",
            category="Fun",
            service_id="insult",
        )
    return InsultService()
