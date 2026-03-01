from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger(__name__)

UA = "LeonidasIRCbot/1.0"


def _http_post_json(url: str, payload: dict, timeout: int) -> dict:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "User-Agent": UA,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode("utf-8", errors="replace")
        return json.loads(body)


async def _post_json(url: str, payload: dict, timeout: int) -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _http_post_json, url, payload, timeout)


def _clean_one_line(s: str) -> str:
    s = (s or "").replace("\r", " ").replace("\n", " ").strip()
    while "  " in s:
        s = s.replace("  ", " ")
    return s


class GeminiService:
    """
    Google Gemini client (stateless; cooldowns in-memory).

    Commands:
      !gemini <question>
      !g <question>

    Enable per channel:
      !service enable gemini #General
    """

    service_id = "gemini"

    def __init__(self):
        # cooldown maps
        #   ("chan", "#General") -> until_ts
        #   ("user", "#General", "Ged") -> until_ts
        self._cooldowns: dict[Tuple[str, ...], float] = {}

    def _cfg(self, bot) -> dict:
        if isinstance(getattr(bot, "cfg", None), dict):
            return bot.cfg.get("gemini", {}) or {}
        return {}

    def _cooldown_ok(self, key: Tuple[str, ...], seconds: int) -> bool:
        if seconds <= 0:
            return True
        now = time.time()
        until = float(self._cooldowns.get(key, 0.0))
        if now < until:
            return False
        self._cooldowns[key] = now + float(seconds)
        return True

    def _extract_text(self, data: dict) -> str:
        """
        Gemini v1beta response:
          { "candidates": [ { "content": { "parts": [ {"text": "..."} ] } } ] }
        """
        try:
            cands = data.get("candidates", [])
            if not cands:
                return ""
            content = (cands[0] or {}).get("content", {}) or {}
            parts = content.get("parts", []) or []
            texts = []
            for p in parts:
                if isinstance(p, dict) and "text" in p and p["text"]:
                    texts.append(str(p["text"]).strip())
            return " ".join([t for t in texts if t])
        except Exception:
            return ""

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

        if cmd not in ("gemini", "g"):
            return

        question = rest[0].strip() if rest else ""
        if not question:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !{cmd} <question>")
            return

        gcfg = self._cfg(bot)
        api_key = str(gcfg.get("api_key") or "").strip()
        model = str(gcfg.get("model") or "gemini-2.5-flash").strip()

        # Transport + generation
        timeout = int(gcfg.get("timeout_seconds", 12))
        temperature = float(gcfg.get("temperature", 0.2))
        max_tokens = int(gcfg.get("max_output_tokens", 380))

        # Output shaping
        max_chars = int(gcfg.get("max_reply_chars", 360))
        system_prefix = str(
            gcfg.get("system_prefix")
            or "Give a short, direct answer in no more than two sentences."
        ).strip()

        # Cooldowns
        # channel cooldown is only relevant for channel usage (not PM)
        chan_cd = int(gcfg.get("cooldown_seconds_channel", gcfg.get("cooldown_seconds", 6)))
        user_cd = int(gcfg.get("cooldown_seconds_user", 6))

        if not api_key:
            await bot.privmsg(ev.target, f"{ev.nick}: Gemini not configured.")
            return

        # Cooldown policy:
        # - For PMs, only per-user cooldown
        # - For channels, per-channel + per-user cooldown
        if ev.channel:
            if not self._cooldown_ok(("chan", ev.channel.lower()), chan_cd):
                await bot.privmsg(ev.target, f"{ev.nick}: slow down.")
                return
            if not self._cooldown_ok(("user", ev.channel.lower(), (ev.nick or "").lower()), user_cd):
                await bot.privmsg(ev.target, f"{ev.nick}: slow down.")
                return
        else:
            if not self._cooldown_ok(("pm", (ev.nick or "").lower()), user_cd):
                await bot.privmsg(ev.target, f"{ev.nick}: slow down.")
                return

        url = (
            "https://generativelanguage.googleapis.com/v1beta/"
            f"models/{model}:generateContent?key={api_key}"
        )

        payload: Dict[str, Any] = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": f"{system_prefix}\n\n{question}"}],
                }
            ],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_tokens,
            },
        }

        # Log minimal metadata only
        log.info("Gemini request model=%s target=%s nick=%s", model, ev.target, ev.nick)

        try:
            data = await _post_json(url, payload, timeout)

        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                pass

            # Never echo key or url; body is clipped
            log.error("Gemini HTTP %s body=%s", getattr(e, "code", "?"), body[:800])

            code = getattr(e, "code", None)
            if code == 400:
                await bot.privmsg(ev.target, f"{ev.nick}: Gemini bad request (model/key/payload).")
            elif code in (401, 403):
                await bot.privmsg(ev.target, f"{ev.nick}: Gemini authentication error.")
            elif code == 404:
                await bot.privmsg(ev.target, f"{ev.nick}: Gemini model not found.")
            elif code == 429:
                await bot.privmsg(ev.target, f"{ev.nick}: Gemini rate limited.")
            else:
                await bot.privmsg(ev.target, f"{ev.nick}: Gemini HTTP error.")
            return

        except urllib.error.URLError as e:
            log.error("Gemini network error: %r", getattr(e, "reason", e))
            await bot.privmsg(ev.target, f"{ev.nick}: Gemini network error.")
            return

        except ssl.SSLError as e:
            log.error("Gemini TLS error: %r", e)
            await bot.privmsg(ev.target, f"{ev.nick}: Gemini TLS error.")
            return

        except Exception:
            log.exception("Gemini unexpected error")
            await bot.privmsg(ev.target, f"{ev.nick}: Gemini internal error.")
            return

        answer = self._extract_text(data)
        answer = _clean_one_line(answer)

        if not answer:
            log.warning("Gemini returned empty text. keys=%s", list(data.keys()))
            await bot.privmsg(ev.target, f"{ev.nick}: Gemini returned no usable text.")
            return

        if max_chars > 0 and len(answer) > max_chars:
            answer = answer[: max_chars - 1].rstrip() + "…"

        await bot.privmsg(ev.target, answer)


def setup(bot):
    if hasattr(bot, "register_command"):
        # You can tighten this to "admin" if you want it restricted.
        bot.register_command(
            "gemini",
            min_role="user",
            mutating=False,
            help="Ask Gemini. Usage: !gemini <question>",
            category="Info",
        )
        bot.register_command(
            "g",
            min_role="user",
            mutating=False,
            help="Alias for !gemini",
            category="Info",
        )

    # If legacy ACL registry still exists, register there too (harmless).
    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register("gemini", min_role="user", mutating=False, help="Ask Gemini. Usage: !gemini <question>", category="Info")
        bot.acl.register("g", min_role="user", mutating=False, help="Alias for !gemini", category="Info")

    return GeminiService()