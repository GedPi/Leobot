"""
Joke service: JokeAPI (https://v2.jokeapi.dev).
Commands: !joke [category] [lang] [safe|1|2]
- Default: any joke, English, not safe, any type
- Add "safe" for safe-mode jokes
- Add "1" or "2" for single-part or two-part jokes
- Categories: Any, Programming, Misc, Dark, Pun, Spooky, Christmas
- Languages: en, de, es, fr, pt, cs (Greek not supported by JokeAPI)
"""
from __future__ import annotations

import asyncio
import json
import urllib.parse
import urllib.request

API_BASE = "https://v2.jokeapi.dev/joke"
UA = "LeonidasIRCbot/2.0 (https://hairyoctopus.net; admin: Ged)"

CATEGORIES = frozenset(
    {"any", "programming", "misc", "dark", "pun", "spooky", "christmas"}
)
LANGUAGES = frozenset({"en", "de", "es", "fr", "pt", "cs"})


def _fetch_joke(category: str, lang: str, joke_type: str | None, safe: bool, timeout: int = 12) -> dict | None:
    path = category if category != "any" else "Any"
    params = {"lang": lang}
    if joke_type:
        params["type"] = joke_type
    query = urllib.parse.urlencode(params)
    if safe:
        query += "&safe-mode" if query else "safe-mode"
    url = f"{API_BASE}/{path}?{query}"
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = json.loads(r.read().decode("utf-8", errors="replace"))
    if data.get("error"):
        return None
    return data


async def _get_joke(category: str, lang: str, joke_type: str | None, safe: bool) -> dict | None:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _fetch_joke, category, lang, joke_type, safe)


def _parse_args(args: list[str]) -> tuple[str, str, str | None, bool]:
    category = "any"
    lang = "en"
    joke_type = None
    safe = False

    for part in args:
        p = part.strip().lower()
        if not p:
            continue
        if p == "safe":
            safe = True
        elif p == "1":
            joke_type = "single"
        elif p == "2":
            joke_type = "twopart"
        elif p in LANGUAGES:
            lang = p
        elif p in CATEGORIES:
            category = p

    return category, lang, joke_type, safe


def _format_joke(data: dict) -> str:
    jtype = (data.get("type") or "single").lower()
    if jtype == "twopart":
        setup = (data.get("setup") or "").strip()
        delivery = (data.get("delivery") or "").strip()
        if setup and delivery:
            return f"{setup} ... {delivery}"
        return setup or delivery
    return (data.get("joke") or "").strip()


class JokeService:
    """
    JokeAPI: !joke [category] [lang] [safe|1|2]
    Default: any joke, English, not safe. Add "safe" for safe-mode, "1"/"2" for joke type.
    """

    service_id = "joke"

    async def on_privmsg(self, bot, ev) -> None:
        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()

        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix) :].strip()
        if not cmdline:
            return

        parts = cmdline.split()
        cmd = (parts[0] or "").lower()
        if cmd != "joke":
            return

        args = [p for p in parts[1:]] if len(parts) > 1 else []
        category, lang, joke_type, safe = _parse_args(args)

        try:
            data = await _get_joke(category, lang, joke_type, safe)
        except Exception:
            await bot.privmsg(ev.target, f"{ev.nick}: Joke API error.")
            return

        if not data:
            await bot.privmsg(ev.target, f"{ev.nick}: No matching joke found.")
            return

        formatted = _format_joke(data)
        if not formatted:
            await bot.privmsg(ev.target, f"{ev.nick}: No matching joke found.")
            return

        await bot.privmsg(ev.target, formatted)


def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command(
            "joke",
            min_role="user",
            mutating=False,
            help="Random joke from JokeAPI. Usage: !joke [category] [lang] [safe|1|2] — categories: Any, Programming, Misc, Dark, Pun, Spooky, Christmas; langs: en, de, es, fr, pt, cs",
            category="Fun",
            service_id="joke",
        )
    return JokeService()
