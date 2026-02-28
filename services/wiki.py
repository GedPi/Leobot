from __future__ import annotations

import asyncio
import json
import urllib.parse
import urllib.request


def setup(bot):
    return WikiService(bot)


def _fetch_json(url: str, timeout: int = 12) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "Leonidas/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", errors="ignore"))


class WikiService:
    """Wikipedia lookup via REST API.

    Disabled by default per channel.
      !service enable wiki #Channel

    Usage:
      !wiki <query>
    """

    def __init__(self, bot):
        self.bot = bot
        bot.register_command("wiki", min_role="guest", mutating=False, help="Wikipedia lookup. Usage: !wiki <query>", category="Wiki")

    async def on_privmsg(self, bot, ev):
        prefix = bot.cfg.get("command_prefix", "!")
        txt = (ev.text or "").strip()
        if not txt.startswith(prefix):
            return
        cmdline = txt[len(prefix):].strip()
        if not cmdline:
            return
        parts = cmdline.split(maxsplit=1)
        if parts[0].lower() != "wiki":
            return

        if ev.channel and not await bot.store.is_service_enabled(ev.channel, "wiki"):
            return

        if len(parts) < 2:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !wiki <query>")
            return

        q = parts[1].strip()

        # Search first
        surl = "https://en.wikipedia.org/w/rest.php/v1/search/title?" + urllib.parse.urlencode({"q": q, "limit": 1})
        res = await asyncio.to_thread(_fetch_json, surl)
        pages = res.get("pages") or []
        if not pages:
            await bot.privmsg(ev.target, f"{ev.nick}: no results")
            return

        title = pages[0].get("title")
        key = pages[0].get("key") or title

        # Summary
        sumurl = f"https://en.wikipedia.org/api/rest_v1/page/summary/{urllib.parse.quote(key)}"
        summ = await asyncio.to_thread(_fetch_json, sumurl)
        extract = (summ.get("extract") or "").strip()
        link = ((summ.get("content_urls") or {}).get("desktop") or {}).get("page")
        if extract:
            # first sentence-ish
            out = extract.split(". ")[0].strip()
            if not out.endswith("."):
                out += "."
        else:
            out = "(no summary)"
        await bot.privmsg(ev.target, f"{ev.nick}: {title} — {out} {link or ''}".strip())
