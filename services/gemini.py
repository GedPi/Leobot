import asyncio
import json
import time
import urllib.request
from typing import Optional

UA = "LeonidasIRCbot/1.0 (https://hairyoctopus.net; admin: Ged)"


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
        return json.loads(r.read().decode("utf-8", errors="replace"))


async def _post_json(url: str, payload: dict, timeout: int) -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _http_post_json, url, payload, timeout)


def _clean_one_paragraph(s: str) -> str:
    s = (s or "").replace("\r", " ").replace("\n", " ").strip()
    # collapse whitespace
    while "  " in s:
        s = s.replace("  ", " ")
    return s


class GeminiService:
    def __init__(self, cfg: dict):
        self.cfg = cfg or {}
        self.cooldown = {}  # (target, cmd) -> until_epoch

    def _cooldown_ok(self, target: str, cmd: str, seconds: int) -> bool:
        now = time.time()
        k = (target, cmd)
        until = self.cooldown.get(k, 0)
        if now < until:
            return False
        self.cooldown[k] = now + seconds
        return True

    def _cfg(self, bot) -> dict:
        return bot.cfg.get("gemini", {}) if isinstance(bot.cfg, dict) else {}

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

        if cmd not in ("gemini", "gamini", "g"):  # support your typo + short alias
            return

        q = rest[0].strip() if rest else ""
        if not q:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !gemini <question>")
            return

        gcfg = self._cfg(bot)
        api_key = (gcfg.get("api_key") or "").strip()
        model = (gcfg.get("model") or "gemini-1.5-flash").strip()
        timeout = int(gcfg.get("timeout_seconds", 12))
        cd = int(gcfg.get("cooldown_seconds", 6))
        max_chars = int(gcfg.get("max_reply_chars", 360))
        max_tokens = int(gcfg.get("max_output_tokens", 120))
        temperature = float(gcfg.get("temperature", 0.3))

        if not api_key:
            await bot.privmsg(ev.target, f"{ev.nick}: Gemini not configured (missing gemini.api_key).")
            return

        # mild flood control in channels
        if not ev.is_private:
            if not self._cooldown_ok(ev.target, "gemini", cd):
                await bot.privmsg(ev.target, f"{ev.nick}: slow down.")
                return

        # Gemini API endpoint (Generative Language API)
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"

        # Keep the model on rails: short, direct, one paragraph.
        system_hint = (
            "Answer in ONE short paragraph. Be direct. No lists unless unavoidable. "
            "No preamble. No disclaimers. Max ~3 sentences."
        )

        payload = {
            "contents": [
                {"role": "user", "parts": [{"text": f"{system_hint}\n\nQuestion: {q}"}]}
            ],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_tokens,
            },
        }

        try:
            data = await _post_json(url, payload, timeout=timeout)
        except Exception:
            await bot.privmsg(ev.target, f"{ev.nick}: Gemini request failed.")
            return

        # Extract text (best-effort across response shapes)
        answer: Optional[str] = None
        try:
            cands = data.get("candidates") or []
            if cands:
                content = (cands[0].get("content") or {})
                parts = content.get("parts") or []
                if parts and isinstance(parts[0], dict):
                    answer = parts[0].get("text")
        except Exception:
            answer = None

        answer = _clean_one_paragraph(answer or "")
        if not answer:
            await bot.privmsg(ev.target, f"{ev.nick}: Gemini returned no answer.")
            return

        if len(answer) > max_chars:
            answer = answer[: max_chars - 1].rstrip() + "…"

        await bot.privmsg(ev.target, f"GEMINI: {answer}")


def setup(bot):
    # Register for help + ACL like your other modules
    if hasattr(bot, "register_command"):
        bot.register_command("gemini", min_role="user", mutating=False, help="Ask Gemini. Usage: !gemini <question>", category="Info")
        bot.register_command("g", min_role="user", mutating=False, help="Alias for !gemini", category="Info")
    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register("gemini", min_role="user", mutating=False, help="Ask Gemini. Usage: !gemini <question>", category="Info")
        bot.acl.register("g", min_role="user", mutating=False, help="Alias for !gemini", category="Info")

    return GeminiService(bot.cfg.get("gemini", {}) if isinstance(bot.cfg, dict) else {})