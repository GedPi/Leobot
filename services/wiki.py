import asyncio
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

WATCHLIST_PATH = Path("/var/lib/leobot/wiki_watch.json")

UA = "LeonidasIRCbot/1.0 (https://hairyoctopus.net; admin: George)"

def _http_get_json(url: str, timeout: int = 10) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))

async def _get_json(url: str, timeout: int = 10) -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _http_get_json, url, timeout)

def _load_watchlist() -> dict:
    if not WATCHLIST_PATH.exists():
        return {"lang": "en", "pages": []}
    try:
        return json.loads(WATCHLIST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"lang": "en", "pages": []}

def _save_watchlist(data: dict) -> None:
    WATCHLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = WATCHLIST_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(WATCHLIST_PATH)

def _norm_title(s: str) -> str:
    s = (s or "").strip()
    # Wikipedia titles are case-sensitive-ish, but first char is usually uppercase.
    if not s:
        return s
    return s[0].upper() + s[1:]

class WikiService:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.cache = {}  # key -> (expires_epoch, payload)
        self.cooldown = {}  # (target, cmd) -> until_epoch

    def _cooldown_ok(self, target: str, cmd: str, seconds: int) -> bool:
        now = time.time()
        k = (target, cmd)
        until = self.cooldown.get(k, 0)
        if now < until:
            return False
        self.cooldown[k] = now + seconds
        return True

    def _cache_get(self, key):
        item = self.cache.get(key)
        if not item:
            return None
        exp, val = item
        if time.time() >= exp:
            self.cache.pop(key, None)
            return None
        return val

    def _cache_set(self, key, val, ttl: int):
        self.cache[key] = (time.time() + ttl, val)

    async def _summary(self, lang: str, title: str) -> dict:
        ttl = int(self.cfg.get("cache_ttl_seconds", 3600))
        key = ("summary", lang, title)
        cached = self._cache_get(key)
        if cached:
            return cached

        enc = urllib.parse.quote(title.replace(" ", "_"), safe="")
        url = f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/{enc}"
        data = await _get_json(url, timeout=10)
        self._cache_set(key, data, ttl)
        return data

    async def _random_summary(self, lang: str) -> dict:
        # Random pages are, by definition, not cache-friendly across long TTLs.
        url = f"https://{lang}.wikipedia.org/api/rest_v1/page/random/summary"
        return await _get_json(url, timeout=10)

    async def _opensearch(self, lang: str, query: str, limit: int = 5) -> list[str]:
        ttl = int(self.cfg.get("cache_ttl_seconds", 3600))
        key = ("opensearch", lang, query, limit)
        cached = self._cache_get(key)
        if cached:
            return cached

        q = urllib.parse.quote(query)
        url = f"https://{lang}.wikipedia.org/w/api.php?action=opensearch&search={q}&limit={limit}&namespace=0&format=json"
        data = await _get_json(url, timeout=10)
        titles = data[1] if isinstance(data, list) and len(data) > 1 else []
        self._cache_set(key, titles, ttl)
        return titles

    async def on_privmsg(self, bot, ev) -> None:
        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()
        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return

        parts = cmdline.split()
        cmd = parts[0].lower()

        if cmd not in ("wiki", "wikicheck", "wikimon"):
            return

        # mild flood protection in channels
        if not ev.is_private and cmd in ("wiki", "wikicheck"):
            if not self._cooldown_ok(ev.target, cmd, seconds=int(self.cfg.get("cooldown_seconds", 5))):
                await bot.privmsg(ev.target, f"{ev.nick}: slow down.")
                return

        lang = self.cfg.get("lang", "en")

        # ------------------- !wiki -------------------
        if cmd == "wiki":
            query = cmdline[len("wiki"):].strip()
            if not query:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !wiki <term>")
                return

            # !wiki random
            if query.strip().lower() in ("random", "rand"):
                try:
                    data = await self._random_summary(lang)
                except Exception:
                    await bot.privmsg(ev.target, f"{ev.nick}: Wikipedia lookup failed.")
                    return

                actual_title = (data.get("title") or "Random").strip() or "Random"
                extract = (data.get("extract") or "").strip()
                url = (((data.get("content_urls") or {}).get("desktop") or {}).get("page")) or ""

                if extract:
                    max_chars = int(self.cfg.get("max_summary_chars", 320))
                    if len(extract) > max_chars:
                        extract = extract[: max_chars - 1].rstrip() + "…"
                    await bot.privmsg(ev.target, f"WIKI: {actual_title} — {extract}" + (f" ({url})" if url else ""))
                else:
                    await bot.privmsg(ev.target, f"WIKI: {actual_title}" + (f" ({url})" if url else ""))
                return

            title = _norm_title(query)
            try:
                data = await self._summary(lang, title)
            except Exception as e:
                await bot.privmsg(ev.target, f"{ev.nick}: Wikipedia lookup failed.")
                return

            # REST summary returns 'type': 'standard'|'disambiguation'|'https://...'
            page_type = (data.get("type") or "").lower()
            actual_title = data.get("title") or title
            extract = (data.get("extract") or "").strip()
            url = (((data.get("content_urls") or {}).get("desktop") or {}).get("page")) or ""

            if page_type == "disambiguation":
                suggestions = await self._opensearch(lang, query, limit=5)
                s = ", ".join(suggestions[:5]) if suggestions else "no suggestions"
                await bot.privmsg(ev.target, f"WIKI: {actual_title} is a disambiguation page. Try: {s}")
                return

            if not extract:
                suggestions = await self._opensearch(lang, query, limit=5)
                s = ", ".join(suggestions[:5]) if suggestions else "no suggestions"
                await bot.privmsg(ev.target, f"WIKI: no summary found. Suggestions: {s}")
                return

            # keep IRC-friendly length
            max_chars = int(self.cfg.get("max_summary_chars", 320))
            if len(extract) > max_chars:
                extract = extract[: max_chars - 1].rstrip() + "…"

            if url:
                await bot.privmsg(ev.target, f"WIKI: {actual_title} — {extract} ({url})")
            else:
                await bot.privmsg(ev.target, f"WIKI: {actual_title} — {extract}")
            return

        # ------------------- !wikicheck -------------------
        if cmd == "wikicheck":
            query = cmdline[len("wikicheck"):].strip()
            if not query:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !wikicheck <term>")
                return

            # Use summary endpoint as a cheap existence check too
            title = _norm_title(query)
            try:
                data = await self._summary(lang, title)
            except Exception:
                await bot.privmsg(ev.target, f"{ev.nick}: Wikipedia check failed.")
                return

            page_type = (data.get("type") or "").lower()
            actual_title = data.get("title") or title
            url = (((data.get("content_urls") or {}).get("desktop") or {}).get("page")) or ""

            if data.get("detail") == "Not found." or data.get("type") == "https://mediawiki.org/wiki/HyperSwitch/errors/not_found":
                suggestions = await self._opensearch(lang, query, limit=5)
                s = ", ".join(suggestions[:5]) if suggestions else "no suggestions"
                await bot.privmsg(ev.target, f"WIKICHECK: not found. Suggestions: {s}")
                return

            if page_type == "disambiguation":
                suggestions = await self._opensearch(lang, query, limit=5)
                s = ", ".join(suggestions[:5]) if suggestions else "no suggestions"
                await bot.privmsg(ev.target, f"WIKICHECK: {actual_title} is disambiguation. Options: {s}")
                return

            await bot.privmsg(ev.target, f"WIKICHECK: OK — {actual_title}" + (f" ({url})" if url else ""))
            return

        # ------------------- !wikimon -------------------
        # Watchlist is a simple leobot-owned JSON file; collector reads it and emits events.
        if cmd == "wikimon":
            sub = parts[1].lower() if len(parts) >= 2 else ""
            rest = cmdline.split(maxsplit=2)
            arg = rest[2].strip() if len(rest) >= 3 else ""

            if sub in ("list", ""):
                wl = _load_watchlist()
                pages = wl.get("pages") or []
                if not pages:
                    await bot.privmsg(ev.target, "WIKIMON: watchlist empty.")
                    return
                # keep it short
                show = pages[:15]
                await bot.privmsg(ev.target, "WIKIMON: " + " | ".join(show) + ("" if len(pages) <= 15 else f" (+{len(pages)-15} more)"))
                return

            if sub == "add":
                if not arg:
                    await bot.privmsg(ev.target, f"{ev.nick}: usage: !wikimon add <Wikipedia page title>")
                    return
                title = _norm_title(arg)
                wl = _load_watchlist()
                wl.setdefault("lang", "en")
                wl.setdefault("pages", [])
                if title in wl["pages"]:
                    await bot.privmsg(ev.target, f"WIKIMON: already watching {title}")
                    return
                wl["pages"].append(title)
                wl["pages"] = sorted(set(wl["pages"]))
                _save_watchlist(wl)
                await bot.privmsg(ev.target, f"WIKIMON: added {title} (collector will track changes)")
                return

            if sub == "del":
                if not arg:
                    await bot.privmsg(ev.target, f"{ev.nick}: usage: !wikimon del <title>")
                    return
                title = _norm_title(arg)
                wl = _load_watchlist()
                pages = wl.get("pages") or []
                if title not in pages:
                    await bot.privmsg(ev.target, f"WIKIMON: not watching {title}")
                    return
                wl["pages"] = [p for p in pages if p != title]
                _save_watchlist(wl)
                await bot.privmsg(ev.target, f"WIKIMON: removed {title}")
                return

            if sub == "lang":
                # Optional: !wikimon lang en
                if not arg:
                    await bot.privmsg(ev.target, "WIKIMON: usage: !wikimon lang <en|de|fr|...>")
                    return
                wl = _load_watchlist()
                wl["lang"] = arg.strip().lower()
                _save_watchlist(wl)
                await bot.privmsg(ev.target, f"WIKIMON: language set to {wl['lang']}")
                return

            await bot.privmsg(ev.target, "WIKIMON: usage: !wikimon [list] | add <title> | del <title> | lang <code>")

def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command("wiki", min_role="user", mutating=False, help="Wikipedia lookup. Usage: !wiki <query> | !wiki random", category="Info")
        bot.register_command("wiki random", min_role="user", mutating=False, help="Random Wikipedia article. Usage: !wiki random", category="Info")
        bot.register_command("wikicheck", min_role="user", mutating=False, help="Check if a Wikipedia page exists. Usage: !wikicheck <query>", category="Info")
        bot.register_command("wikimon", min_role="contributor", mutating=True, help="Manage a Wikipedia watchlist used by the external collector. Usage: !wikimon [list]|add|del|lang", category="Info")
        bot.register_command("wikimon list", min_role="contributor", mutating=False, help="List watched pages. Usage: !wikimon list", category="Info")
        bot.register_command("wikimon add", min_role="contributor", mutating=True, help="Add a page to the watchlist. Usage: !wikimon add <title>", category="Info")
        bot.register_command("wikimon del", min_role="contributor", mutating=True, help="Remove a page from the watchlist. Usage: !wikimon del <title>", category="Info")
        bot.register_command("wikimon lang", min_role="contributor", mutating=True, help="Set the watchlist language. Usage: !wikimon lang <en|de|fr|...>", category="Info")
    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register("wiki", min_role="user", mutating=False, help="Wikipedia lookup. Usage: !wiki <query> | !wiki random", category="Info")
        bot.acl.register("wiki random", min_role="user", mutating=False, help="Random Wikipedia article. Usage: !wiki random", category="Info")
        bot.acl.register("wikicheck", min_role="user", mutating=False, help="Check if a Wikipedia page exists. Usage: !wikicheck <query>", category="Info")
        bot.acl.register("wikimon", min_role="contributor", mutating=True, help="Manage a Wikipedia watchlist used by the external collector. Usage: !wikimon [list]|add|del|lang", category="Info")
        bot.acl.register("wikimon list", min_role="contributor", mutating=False, help="List watched pages. Usage: !wikimon list", category="Info")
        bot.acl.register("wikimon add", min_role="contributor", mutating=True, help="Add a page to the watchlist. Usage: !wikimon add <title>", category="Info")
        bot.acl.register("wikimon del", min_role="contributor", mutating=True, help="Remove a page from the watchlist. Usage: !wikimon del <title>", category="Info")
        bot.acl.register("wikimon lang", min_role="contributor", mutating=True, help="Set the watchlist language. Usage: !wikimon lang <en|de|fr|...>", category="Info")

    return WikiService(bot.cfg.get('wiki', {}) if isinstance(bot.cfg, dict) else {})
