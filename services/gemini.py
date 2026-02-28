import asyncio
import json
import time
import logging
import urllib.request
import urllib.error
import ssl

log = logging.getLogger(__name__)

UA = "LeonidasIRCbot/1.0"


def _http_post_json(url: str, payload: dict, timeout: int) -> dict:
    data = json.dumps(payload).encode("utf-8")

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


class GeminiService:
    def __init__(self, cfg: dict):
        self.cfg = cfg or {}
        self.cooldowns = {}  # (target) -> timestamp

    def _cfg(self, bot):
        return bot.cfg.get("gemini", {}) if isinstance(bot.cfg, dict) else {}

    def _cooldown_ok(self, target: str, seconds: int) -> bool:
        now = time.time()
        until = self.cooldowns.get(target, 0)
        if now < until:
            return False
        self.cooldowns[target] = now + seconds
        return True

    def _extract_text(self, data: dict) -> str:
        try:
            candidates = data.get("candidates", [])
            if not candidates:
                return ""

            content = candidates[0].get("content", {})
            parts = content.get("parts", [])

            texts = []
            for p in parts:
                if isinstance(p, dict) and "text" in p:
                    texts.append(p["text"].strip())

            return " ".join(t for t in texts if t)
        except Exception:
            return ""

    async def on_privmsg(self, bot, ev):
        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()

        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return

        cmd, *rest = cmdline.split(maxsplit=1)
        cmd = cmd.lower()

        if cmd not in ("gemini", "g", "gamini"):
            return

        question = rest[0].strip() if rest else ""
        if not question:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !gemini <question>")
            return

        gcfg = self._cfg(bot)
        api_key = (gcfg.get("api_key") or "").strip()
        model = (gcfg.get("model") or "gemini-2.5-flash").strip()
        timeout = int(gcfg.get("timeout_seconds", 12))
        cooldown = int(gcfg.get("cooldown_seconds", 6))
        max_chars = int(gcfg.get("max_reply_chars", 360))
        max_tokens = int(gcfg.get("max_output_tokens", 380))
        temperature = float(gcfg.get("temperature", 0.2))

        if not api_key:
            await bot.privmsg(ev.target, f"{ev.nick}: Gemini not configured.")
            return

        if not ev.is_private:
            if not self._cooldown_ok(ev.target, cooldown):
                await bot.privmsg(ev.target, f"{ev.nick}: slow down.")
                return

        url = (
            f"https://generativelanguage.googleapis.com/v1beta/"
            f"models/{model}:generateContent?key={api_key}"
        )

        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": "Give a short, direct answer in no more than two sentences.\n\n"
                                    f"{question}"
                        }
                    ],
                }
            ],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_tokens,
            },
        }

        log.info("Gemini request model=%s target=%s", model, ev.target)

        try:
            data = await _post_json(url, payload, timeout)

        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                pass

            log.error("Gemini HTTP %s body=%s", e.code, body[:800])

            if e.code == 400:
                await bot.privmsg(ev.target, f"{ev.nick}: Gemini bad request (model/key issue).")
            elif e.code == 401 or e.code == 403:
                await bot.privmsg(ev.target, f"{ev.nick}: Gemini authentication error.")
            elif e.code == 404:
                await bot.privmsg(ev.target, f"{ev.nick}: Gemini model not found.")
            elif e.code == 429:
                await bot.privmsg(ev.target, f"{ev.nick}: Gemini rate limited.")
            else:
                await bot.privmsg(ev.target, f"{ev.nick}: Gemini HTTP error {e.code}.")
            return

        except urllib.error.URLError as e:
            log.error("Gemini network error: %r", e.reason)
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

        # Parse response
        answer = self._extract_text(data)

        if not answer:
            log.warning("Gemini returned empty text. Raw keys=%s", list(data.keys()))
            await bot.privmsg(ev.target, f"{ev.nick}: Gemini returned no usable text.")
            return

        # Clean + truncate
        answer = answer.replace("\n", " ").replace("\r", " ").strip()
        while "  " in answer:
            answer = answer.replace("  ", " ")

        if len(answer) > max_chars:
            answer = answer[: max_chars - 1].rstrip() + "…"

        await bot.privmsg(ev.target, f"{answer}")


def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command(
            "gemini",
            min_role="admin",
            mutating=False,
            help="Ask Gemini. Usage: !gemini <question>",
            category="Info",
        )
        bot.register_command(
            "g",
            min_role="admin",
            mutating=False,
            help="Alias for !gemini",
            category="Info",
        )

    return GeminiService(bot.cfg.get("gemini", {}))