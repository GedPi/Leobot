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


@dataclass(frozen=True)
class NewsItem:
    source_id: str
    source_name: str
    category: str
    title: str
    link: str
    published_utc: Optional[datetime]


@dataclass
class PendingSelection:
    created_ts: float
    target: str
    limit: int
    category: str


def _fetch_url(url: str, timeout: int = 15) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "Leonidas/1.0 (IRC bot; RSS reader)"})
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

    channel = root.find("channel")
    if channel is not None:
        for it in channel.findall("item"):
            title = _clean_title(_text(it.find("title")))
            link = _text(it.find("link"))
            pub = _parse_date(_text(it.find("pubDate")))
            if title and link:
                out.append((title, link, pub))
        return out

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

        self._cache: Dict[Tuple[str, str], Tuple[float, List[NewsItem]]] = {}
        self._pending: Dict[Tuple[str, str], Tuple[List[dict], PendingSelection]] = {}

        bot.register_command("news", min_role="guest", mutating=False, help="Show headlines. Usage: !news [limit] [category]", category="News")
        bot.register_command("headlines", min_role="guest", mutating=False, help="Alias for !news", category="News")
        bot.register_command("news sources", min_role="contributor", mutating=False, help="List configured sources", category="News")
        bot.register_command("news categories", min_role="contributor", mutating=False, help="List categories for a source. Usage: !news categories <id>", category="News")
        bot.register_command("news addsource", min_role="contributor", mutating=True, help="Add/update a source. Usage: !news addsource <id> <name>", category="News")
        bot.register_command("news addcat", min_role="contributor", mutating=True, help="Add/update a category URL. Usage: !news addcat <id> <category> <url>", category="News")

    async def _allowed(self, ev) -> bool:
        if ev.channel:
            return await self.bot.store.is_service_enabled(ev.channel, "news")
        return True

    async def _enabled_sources(self) -> List[dict]:
        rows = await self.bot.store.news_list_sources()
        return [{"id": r["id"], "name": r["name"], "enabled": bool(r["enabled"])} for r in rows if bool(r["enabled"])]

    async def _category_url(self, source_id: str, category: str) -> Optional[str]:
        rows = await self.bot.store.news_list_categories(source_id)
        for r in rows:
            if str(r["category"]).lower() == category.lower():
                return str(r["url"])
        return None

    async def _fetch_items(self, source_id: str, source_name: str, category: str, url: str) -> List[NewsItem]:
        key = (source_id, category)
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

    async def _post(self, target: str, items: List[NewsItem], limit: int) -> None:
        for it in items[:limit]:
            stamp = it.published_utc.strftime("%Y-%m-%d %H:%MZ ") if it.published_utc else ""
            await self.bot.privmsg(target, f"{stamp}{it.title} — {it.link}")
            await asyncio.sleep(self.line_delay)

    async def _serve(self, ev, src: dict, category: str, limit: int) -> None:
        url = await self._category_url(src["id"], category)
        if not url:
            await self.bot.privmsg(ev.target, f"{ev.nick}: unknown category '{category}' for {src['id']}. Use !news categories {src['id']}")
            return

        chan = ev.channel or ev.target
        last = await self.bot.store.news_get_last_posted(chan, src["id"], category, limit)
        now = int(time.time())
        if last and now - last < self.cooldown:
            await self.bot.privmsg(ev.target, f"{ev.nick}: cooldown active ({self.cooldown - (now - last)}s)")
            return

        items = await self._fetch_items(src["id"], src["name"], category, url)
        if not items:
            await self.bot.privmsg(ev.target, f"{ev.nick}: no items found")
            return

        await self._post(ev.target, items, limit)
        await self.bot.store.news_set_last_posted(chan, src["id"], category, limit, ts=now)

    async def on_privmsg(self, bot, ev):
        # pending selection flow
        key = (ev.nick.lower(), ev.target)
        if key in self._pending:
            sources, pending = self._pending[key]
            if time.time() - pending.created_ts > self.selection_timeout:
                del self._pending[key]
                await bot.privmsg(ev.target, f"{ev.nick}: selection timed out")
                return

            txt = (ev.text or "").strip()
            if txt.isdigit():
                idx = int(txt) - 1
                if 0 <= idx < len(sources):
                    src = sources[idx]
                    del self._pending[key]
                    await self._serve(ev, src, pending.category, pending.limit)
                    return

        # command parsing
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

        args = [a for a in parts[1:]]

        # management
        if args and args[0].lower() == "sources":
            rows = await bot.store.news_list_sources()
            if not rows:
                await bot.privmsg(ev.target, f"{ev.nick}: no sources configured")
                return
            msg = ", ".join([f"{r['id']}({ 'on' if r['enabled'] else 'off' })" for r in rows])
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
            sid = args[1]
            name = " ".join(args[2:])
            await bot.store.news_upsert_source(sid, name, enabled=True)
            await bot.privmsg(ev.target, f"{ev.nick}: source upserted: {sid} = {name}")
            return

        if args and args[0].lower() == "addcat":
            if len(args) < 4:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !news addcat <id> <category> <url>")
                return
            sid = args[1]
            cat = args[2]
            url = args[3]
            await bot.store.news_set_category(sid, cat, url)
            await bot.privmsg(ev.target, f"{ev.nick}: category upserted: {sid}/{cat}")
            return

        # headlines
        limit = self.default_limit
        category = "top"
        if args:
            if args[0].isdigit():
                limit = int(args[0])
                if len(args) >= 2:
                    category = args[1].lower()
            else:
                category = args[0].lower()

        limit = max(1, min(limit, self.max_limit))

        sources = await self._enabled_sources()
        if not sources:
            await bot.privmsg(ev.target, f"{ev.nick}: no enabled news sources. Add with !news addsource ...")
            return

        if len(sources) > 1:
            self._pending[key] = (sources, PendingSelection(time.time(), ev.target, limit, category))
            choices = " | ".join([f"{i+1}) {s['name']}[{s['id']}]" for i, s in enumerate(sources)])
            await bot.privmsg(ev.target, f"{ev.nick}: pick source: {choices} (reply 1-{len(sources)})")
            return

        await self._serve(ev, sources[0], category, limit)
