"""
Wolfram|Alpha service: Full Results API.
Command: !Wolf {question}
Returns plaintext only (no images). Truncates to IRC-friendly length.
Requires config: wolfram.appid
"""
from __future__ import annotations

import asyncio
import json
import urllib.parse
import urllib.request

API_BASE = "https://api.wolframalpha.com/v2/query"
UA = "LeonidasIRCbot/2.0 (https://hairyoctopus.net; admin: Ged)"
MAX_CHARS_DEFAULT = 330
PREFIX = "[Wolf] "


def _fetch_wolfram(appid: str, query: str, timeout: int = 15) -> dict | None:
    params = {
        "appid": appid,
        "input": query,
        "output": "json",
        "format": "plaintext",
    }
    url = f"{API_BASE}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = json.loads(r.read().decode("utf-8", errors="replace"))
    return data


async def _get_wolfram(appid: str, query: str, timeout: int) -> dict | None:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _fetch_wolfram, appid, query, timeout)


def _extract_plaintext(data: dict) -> str | None:
    """Extract best plaintext from Full Results API response. Prefer Result pod."""
    qr = data.get("queryresult") if isinstance(data, dict) else None
    if not qr or not qr.get("success", False) or qr.get("error", False):
        return None

    pods = qr.get("pods") or []
    result_text: str | None = None
    fallback_text: str | None = None

    for pod in pods:
        if not isinstance(pod, dict):
            continue
        subpods = pod.get("subpods") or []
        for sp in subpods:
            if not isinstance(sp, dict):
                continue
            pt = (sp.get("plaintext") or "").strip()
            if not pt:
                continue
            pod_id = (pod.get("id") or "").lower()
            if pod_id == "result" or pod.get("primary"):
                result_text = pt
                break
            if fallback_text is None:
                fallback_text = pt
        if result_text is not None:
            break

    return result_text or fallback_text


def _truncate(text: str, max_chars: int) -> str:
    text = " ".join(text.split())
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


class WolframService:
    """
    Wolfram|Alpha Full Results API: !Wolf {question}
    Plaintext only, no images. Truncates for IRC.
    """

    service_id = "wolfram"

    def __init__(self, cfg: dict):
        wcfg = cfg.get("wolfram", {}) if isinstance(cfg, dict) else {}
        self.appid = str(wcfg.get("appid") or "").strip()
        self.max_chars = int(wcfg.get("max_chars", MAX_CHARS_DEFAULT))
        self.timeout = int(wcfg.get("timeout_seconds", 15))

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
        if cmd != "wolf":
            return

        query = (parts[1].strip() if len(parts) > 1 else "").strip()
        if not query:
            await bot.privmsg(ev.target, f"{ev.nick}: Usage: !Wolf <question>")
            return

        if not self.appid:
            await bot.privmsg(ev.target, f"{ev.nick}: Wolfram|Alpha not configured (missing appid).")
            return

        try:
            data = await _get_wolfram(self.appid, query, self.timeout)
        except Exception:
            await bot.privmsg(ev.target, f"{ev.nick}: Wolfram|Alpha API error.")
            return

        if not data:
            await bot.privmsg(ev.target, f"{ev.nick}: Wolfram|Alpha API error.")
            return

        plaintext = _extract_plaintext(data)
        if not plaintext:
            err_msg = None
            qr = (data or {}).get("queryresult") or {}
            if isinstance(qr, dict):
                err = qr.get("error", {})
                if isinstance(err, dict) and err.get("msg"):
                    err_msg = err["msg"]
                elif isinstance(err, dict) and err.get("code") == 1:
                    err_msg = "Wolfram|Alpha did not understand the query."
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: {err_msg or 'No result from Wolfram|Alpha.'}",
            )
            return

        out = PREFIX + _truncate(plaintext, self.max_chars - len(PREFIX))
        await bot.privmsg(ev.target, out)


def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command(
            "wolf",
            min_role="user",
            mutating=False,
            help="Query Wolfram|Alpha. Usage: !Wolf <question>",
            category="Info",
            service_id="wolfram",
        )
    return WolframService(bot.cfg)
