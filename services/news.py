from __future__ import annotations

import asyncio
import html
import logging
import re
import time
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("leobot.news")

UA = "Leonidas/2.0 (IRC bot; RSS reader)"


# ----------------------------
# Models
# ----------------------------

@dataclass(frozen=True)
class NewsItem:
    source_id: str
    source_name: str
    category: str
    title: str
    link: str
    published_utc: Optional[datetime]


@dataclass(frozen=True)
class CategoryRow:
    category: str
    url: str


@dataclass
class PendingSelection:
    created_ts: float
    target: str
    limit: int
    requested_category: str


@dataclass
class PendingCategorySelection:
    created_ts: float
    target: str
    limit: int
    source: dict
    requested_category: str
    categories: List[CategoryRow]


# ----------------------------
# Helpers: fetch + parse
# ----------------------------

def _fetch_url(url: str, timeout: int = 15) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _text(el: Optional[ET.Element]) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def _parse_date(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    try:
        iso = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _clean_title(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _parse_rss_or_atom(xml_bytes: bytes) -> List[Tuple[str, str, Optional[datetime]]]:
    root = ET.fromstring(xml_bytes)
    out: List[Tuple[str, str, Optional[datetime]]] = []

    # RSS 2.0
    channel = root.find("channel")
    if channel is not None:
        for it in channel.findall("item"):
            title = _clean_title(_text(it.find("title")))
            link = _text(it.find("link"))
            pub = _parse_date(_text(it.find("pubDate")))
            if title and link:
                out.append((title, link, pub))
        return out

    # Atom
    for entry in root.findall(".//{*}entry"):
        title = _clean_title(_text(entry.find("{*}title")))

        link = ""
        for link_el in entry.findall("{*}link"):
            href = link_el.attrib.get("href", "")
            rel = link_el.attrib.get("rel", "alternate")
            if rel == "alternate" and href:
                link = href
                break
            if not link and href:
                link = href

        pub = _parse_date(_text(entry.find("{*}published")) or _text(entry.find("{*}updated")))
        if title and link:
            out.append((title, link, pub))

    return out


# ----------------------------
# Service
# ----------------------------

def setup(bot):
    return NewsService(bot)


class NewsService:
    def __init__(self, bot):
        self.bot = bot

        ncfg = bot.cfg.get("news", {}) if isinstance(bot.cfg, dict) else {}
        self.default_limit = int(ncfg.get("default_limit", 5))
        self.max_limit = int(ncfg.get("max_limit", 10))
        self.cache_ttl = int(ncfg.get("cache_ttl_seconds", 3600))
        self.cooldown = int(ncfg.get("cooldown_seconds", 120))
        self.line_delay = float(ncfg.get("line_delay_seconds", 1.2))
        self.selection_timeout = int(ncfg.get("selection_timeout_seconds", 60))

        # cache: (source_id, category) -> (ts, items)
        self._cache: Dict[Tuple[str, str], Tuple[float, List[NewsItem]]] = {}

        # pending source selection: (nick, target) -> (sources, PendingSelection)
        self._pending_source: Dict[Tuple[str, str], Tuple[List[dict], PendingSelection]] = {}

        # pending category selection: (nick, target) -> PendingCategorySelection
        self._pending_cat: Dict[Tuple[str, str], PendingCategorySelection] = {}

        # Register commands
        bot.register_command("news", min_role="guest", mutating=False, help="Show headlines. Usage: !news [limit] [category]", category="News")
        bot.register_command("headlines", min_role="guest", mutating=False, help="Alias for !news", category="News")

        bot.register_command("news sources", min_role="contributor", mutating=False, help="List configured sources", category="News")
        bot.register_command("news categories", min_role="contributor", mutating=False, help="List categories for a source. Usage: !news categories <id>", category="News")

        bot.register_command("news addsource", min_role="contributor", mutating=True, help="Add/update a source. Usage: !news addsource <id> <name>", category="News")
        bot.register_command("news delsource", min_role="contributor", mutating=True, help="Delete a source. Usage: !news delsource <id>", category="News")
        bot.register_command("news enable", min_role="contributor", mutating=True, help="Enable a source. Usage: !news enable <id>", category="News")
        bot.register_command("news disable", min_role="contributor", mutating=True, help="Disable a source. Usage: !news disable <id>", category="News")

        bot.register_command("news addcat", min_role="contributor", mutating=True, help="Add/update a category URL. Usage: !news addcat <id> <category> <url>", category="News")
        bot.register_command("news delcat", min_role="contributor", mutating=True, help="Delete a category. Usage: !news delcat <id> <category>", category="News")

    # ----------------------------
    # DB-backed config helpers
    # ----------------------------

    async def _allowed(self, ev) -> bool:
        # Per-channel service enablement
        if ev.channel:
            return await self.bot.store.is_service_enabled(ev.channel, "news")
        return True

    async def _enabled_sources(self) -> List[dict]:
        rows = await self.bot.store.news_list_sources()
        return [{"id": r["id"], "name": r["name"], "enabled": bool(r["enabled"])} for r in rows if bool(r["enabled"])]

    async def _list_categories(self, source_id: str) -> List[CategoryRow]:
        rows = await self.bot.store.news_list_categories(source_id)
        out: List[CategoryRow] = []
        for r in rows:
            out.append(CategoryRow(category=str(r["category"]), url=str(r["url"])))
        return out

    def _find_category_url(self, categories: List[CategoryRow], category: str) -> Optional[str]:
        want = (category or "").strip().lower()
        for r in categories:
            if r.category.strip().lower() == want:
                return r.url
        return None

    # ----------------------------
    # Fetch/caching
    # ----------------------------

    async def _fetch_items(self, source_id: str, source_name: str, category: str, url: str) -> List[NewsItem]:
        key = (source_id, category.lower())
        now = time.time()
        if key in self._cache:
            ts, items = self._cache[key]
            if now - ts <= self.cache_ttl:
                return items

        xml_bytes = await asyncio.to_thread(_fetch_url, url)
        raw = _parse_rss_or_atom(xml_bytes)
        items = [NewsItem(source_id, source_name, category, t, l, p) for (t, l, p) in raw]
        self._cache[key] = (now, items)
        return items

    async def _post_items(self, target: str, items: List[NewsItem], limit: int) -> None:
        for it in items[:limit]:
            stamp = it.published_utc.strftime("%Y-%m-%d %H:%MZ ") if it.published_utc else ""
            await self.bot.privmsg(target, f"{stamp}{it.title} — {it.link}")
            await asyncio.sleep(self.line_delay)

    # ----------------------------
    # Category resolution + prompting
    # ----------------------------

    async def _resolve_or_prompt_category(self, ev, src: dict, requested_category: str, limit: int) -> Tuple[Optional[str], Optional[str]]:
        """
        Returns (category, url) if resolved.
        Returns (None, None) if it prompted the user (or errored).
        """
        categories = await self._list_categories(src["id"])
        if not categories:
            await self.bot.privmsg(ev.target, f"{ev.nick}: no categories configured for {src['id']}. Add with !news addcat {src['id']} <category> <url>")
            return (None, None)

        # direct hit
        url = self._find_category_url(categories, requested_category)
        if url:
            return (requested_category, url)

        # If only one category exists: use it silently (best UX for single-feed sources)
        if len(categories) == 1:
            c = categories[0]
            return (c.category, c.url)

        # Try fallbacks only if present (keeps behaviour sane)
        for fallback in ("top", "default"):
            url2 = self._find_category_url(categories, fallback)
            if url2:
                return (fallback, url2)

        # Otherwise: prompt category choice
        key = (ev.nick.lower(), ev.target)
        self._pending_cat[key] = PendingCategorySelection(
            created_ts=time.time(),
            target=ev.target,
            limit=limit,
            source=src,
            requested_category=requested_category,
            categories=categories,
        )

        choices = " | ".join([f"{i+1}) {c.category}" for i, c in enumerate(categories[:10])])
        suffix = f" (+{len(categories)-10} more)" if len(categories) > 10 else ""
        await self.bot.privmsg(
            ev.target,
            f"{ev.nick}: '{requested_category}' not available for {src['name']}[{src['id']}]. Pick category: {choices}{suffix} (reply 1-{min(len(categories),10)})"
        )
        return (None, None)

    # ----------------------------
    # Serve feed (cooldown + output)
    # ----------------------------

    async def _serve(self, ev, src: dict, requested_category: str, limit: int) -> None:
        resolved = await self._resolve_or_prompt_category(ev, src, requested_category, limit)
        category, url = resolved
        if not category or not url:
            return

        # cooldown key: channel if in a channel, else pm target
        chan_key = ev.channel or ev.target
        now = int(time.time())

        last = await self.bot.store.news_get_last_posted(chan_key, src["id"], category, limit)
        if last and now - last < self.cooldown:
            await self.bot.privmsg(ev.target, f"{ev.nick}: cooldown active ({self.cooldown - (now - last)}s)")
            return

        try:
            items = await self._fetch_items(src["id"], src["name"], category, url)
        except Exception as e:
            log.exception("Failed to fetch news feed: %s", e)
            await self.bot.privmsg(ev.target, f"{ev.nick}: failed to fetch feed ({type(e).__name__})")
            return

        if not items:
            await self.bot.privmsg(ev.target, f"{ev.nick}: no items found")
            return

        await self._post_items(ev.target, items, limit)
        await self.bot.store.news_set_last_posted(chan_key, src["id"], category, limit, ts=now)

    # ----------------------------
    # Main event handler
    # ----------------------------

    async def on_privmsg(self, bot, ev):
        # 1) pending category selection replies (reply: "1")
        key = (ev.nick.lower(), ev.target)
        if key in self._pending_cat:
            p = self._pending_cat[key]
            if time.time() - p.created_ts > self.selection_timeout:
                del self._pending_cat[key]
                await bot.privmsg(ev.target, f"{ev.nick}: selection timed out")
                return

            txt = (ev.text or "").strip()
            if txt.isdigit():
                idx = int(txt) - 1
                # Only first 10 are offered (to keep it readable)
                cats = p.categories[:10]
                if 0 <= idx < len(cats):
                    chosen = cats[idx]
                    del self._pending_cat[key]
                    await self._serve(ev, p.source, chosen.category, p.limit)
                    return

        # 2) pending source selection replies (reply: "1")
        if key in self._pending_source:
            sources, pending = self._pending_source[key]
            if time.time() - pending.created_ts > self.selection_timeout:
                del self._pending_source[key]
                await bot.privmsg(ev.target, f"{ev.nick}: selection timed out")
                return

            txt = (ev.text or "").strip()
            if txt.isdigit():
                idx = int(txt) - 1
                if 0 <= idx < len(sources):
                    src = sources[idx]
                    del self._pending_source[key]
                    await self._serve(ev, src, pending.requested_category, pending.limit)
                    return

        # 3) command parsing
        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()
        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return

        parts = cmdline.split()
        cmd = parts[0].lower()
        if cmd not in ("news", "headlines"):
            return

        if not await self._allowed(ev):
            return

        args = parts[1:]

        # ---- management subcommands ----
        if args and args[0].lower() == "sources":
            rows = await bot.store.news_list_sources()
            if not rows:
                await bot.privmsg(ev.target, f"{ev.nick}: no sources configured")
                return
            msg = ", ".join([f"{r['id']}({'on' if r['enabled'] else 'off'})" for r in rows])
            await bot.privmsg(ev.target, f"{ev.nick}: sources: {msg}")
            return

        if args and args[0].lower() == "categories":
            if len(args) < 2:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !news categories <source_id>")
                return
            sid = args[1]
            rows = await bot.store.news_list_categories(sid)
            if not rows:
                await bot.privmsg(ev.target, f"{ev.nick}: no categories for {sid}")
                return
            msg = ", ".join([str(r["category"]) for r in rows])
            await bot.privmsg(ev.target, f"{ev.nick}: categories for {sid}: {msg}")
            return

        if args and args[0].lower() == "addsource":
            if len(args) < 3:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !news addsource <id> <name>")
                return
            sid = args[1].strip()
            name = " ".join(args[2:]).strip()
            if not sid or not name:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !news addsource <id> <name>")
                return
            await bot.store.news_upsert_source(sid, name, enabled=True)
            await bot.privmsg(ev.target, f"{ev.nick}: source upserted: {sid} = {name} (enabled)")
            return

        if args and args[0].lower() == "delsource":
            if len(args) < 2:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !news delsource <id>")
                return
            sid = args[1].strip()
            row = await bot.store.news_get_source(sid)
            if not row:
                await bot.privmsg(ev.target, f"{ev.nick}: unknown source '{sid}'")
                return
            await bot.store.execute("DELETE FROM news_sources WHERE id=?", (sid,))
            await bot.privmsg(ev.target, f"{ev.nick}: deleted source '{sid}'")
            return

        if args and args[0].lower() in ("enable", "disable"):
            if len(args) < 2:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !news {args[0]} <id>")
                return
            sid = args[1].strip()
            row = await bot.store.news_get_source(sid)
            if not row:
                await bot.privmsg(ev.target, f"{ev.nick}: unknown source '{sid}'")
                return
            en = args[0].lower() == "enable"
            await bot.store.news_set_source_enabled(sid, enabled=en)
            await bot.privmsg(ev.target, f"{ev.nick}: source '{sid}' set to {'on' if en else 'off'}")
            return

        if args and args[0].lower() == "addcat":
            if len(args) < 4:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !news addcat <id> <category> <url>")
                return
            sid = args[1].strip()
            cat = args[2].strip().lower()
            url = args[3].strip()
            if not sid or not cat or not url:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !news addcat <id> <category> <url>")
                return
            row = await bot.store.news_get_source(sid)
            if not row:
                await bot.privmsg(ev.target, f"{ev.nick}: unknown source '{sid}' (add it first with !news addsource)")
                return
            await bot.store.news_set_category(sid, cat, url)
            await bot.privmsg(ev.target, f"{ev.nick}: category upserted: {sid}/{cat}")
            return

        if args and args[0].lower() == "delcat":
            if len(args) < 3:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !news delcat <id> <category>")
                return
            sid = args[1].strip()
            cat = args[2].strip().lower()
            await bot.store.execute("DELETE FROM news_source_categories WHERE source_id=? AND category=?", (sid, cat))
            await bot.privmsg(ev.target, f"{ev.nick}: deleted category: {sid}/{cat}")
            return

        # ---- headlines ----
        limit = self.default_limit
        requested_category = "top"

        # supported forms:
        #   !news
        #   !news 5
        #   !news 5 tech
        #   !news tech
        if args:
            if args[0].isdigit():
                limit = int(args[0])
                if len(args) >= 2:
                    requested_category = args[1].lower()
            else:
                requested_category = args[0].lower()

        limit = max(1, min(limit, self.max_limit))

        sources = await self._enabled_sources()
        if not sources:
            await bot.privmsg(ev.target, f"{ev.nick}: no enabled news sources. Add with !news addsource ...")
            return

        # If multiple sources enabled, prompt for source first.
        if len(sources) > 1:
            self._pending_source[key] = (sources, PendingSelection(time.time(), ev.target, limit, requested_category))
            choices = " | ".join([f"{i+1}) {s['name']}[{s['id']}]" for i, s in enumerate(sources[:10])])
            suffix = f" (+{len(sources)-10} more)" if len(sources) > 10 else ""
            await bot.privmsg(ev.target, f"{ev.nick}: pick source: {choices}{suffix} (reply 1-{min(len(sources),10)})")
            return

        await self._serve(ev, sources[0], requested_category, limit)