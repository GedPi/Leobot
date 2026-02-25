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

from services.store import Store


# ----------------------------
# Data models
# ----------------------------

@dataclass(frozen=True)
class NewsItem:
    source_id: str
    source_name: str
    category: str
    title: str
    link: str
    published_utc: Optional[datetime]  # tz-aware UTC


@dataclass
class PendingSelection:
    created_ts: float
    target: str              # channel or nick (reply target)
    limit: int
    category: str


# ----------------------------
# Helpers: fetch + parse
# ----------------------------

def _fetch_url(url: str, timeout: int = 15) -> bytes:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Leonidas/1.0 (IRC bot; RSS reader)"},
    )
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

    # RSS pubDate (RFC822-ish)
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # Atom published/updated (ISO8601)
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
    """Returns list of (title, link, published_utc)."""
    root = ET.fromstring(xml_bytes)
    out: List[Tuple[str, str, Optional[datetime]]] = []

    # RSS2: <rss><channel><item>...
    channel = root.find("channel")
    if channel is not None:
        for it in channel.findall("item"):
            title = _clean_title(_text(it.find("title")))
            link = _text(it.find("link"))
            pub = _parse_date(_text(it.find("pubDate")))
            if title and link:
                out.append((title, link, pub))
        return out

    # Atom: <feed><entry>... with namespaces
    for entry in root.findall(".//{*}entry"):
        title_el = entry.find("{*}title")
        title = _clean_title(_text(title_el))

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

class NewsService:
    def __init__(self, bot, db_path: str):
        self.bot = bot
        self.store = getattr(bot, "store", None) or Store(db_path)

        # cache key: (source_id, category) -> (fetched_ts, [NewsItem...])
        self._cache: Dict[Tuple[str, str], Tuple[float, List[NewsItem]]] = {}

        # pending selections: (nick, target) -> (sources list, pending selection)
        self._pending: Dict[Tuple[str, str], Tuple[List[dict], PendingSelection]] = {}

        # loaded config
        self._loaded = False
        self._default_limit = 10
        self._max_limit = 10
        self._cache_ttl = 3600
        self._cooldown = 120
        self._line_delay = 1.2
        self._selection_timeout = 60
        self._sources_cache: List[dict] = []

    async def _ensure_loaded(self) -> None:
        if self._loaded:
            return

        # Import legacy config once (DB is authoritative after this)
        try:
            news_cfg = self.bot.cfg.get("news", {}) if isinstance(self.bot.cfg, dict) else {}
            await self.store.news_import_from_legacy_config(news_cfg)
        except Exception:
            pass

        # Load settings
        self._default_limit = await self.store.news_get_int("default_limit", 10)
        self._max_limit = await self.store.news_get_int("max_limit", 10)
        self._cache_ttl = await self.store.news_get_int("cache_ttl_seconds", 3600)
        self._cooldown = await self.store.news_get_int("cooldown_seconds", 120)
        self._line_delay = await self.store.news_get_float("line_delay_seconds", 1.2)
        self._selection_timeout = await self.store.news_get_int("selection_timeout_seconds", 60)

        # Load sources (enabled only)
        self._sources_cache = await self.store.news_list_sources(include_disabled=False)
        self._loaded = True

    # ---- config getters ----

    def _sources(self) -> List[dict]:
        return self._sources_cache

    # ---- command parsing ----

    def _parse_news_command(self, text: str) -> Optional[Tuple[int, str]]:
        """Parses:
          !news
          !news 5
          !news 10 sport
          !news sport
        Returns: (limit, category)
        """
        prefix = self.bot.cfg.get("command_prefix", "!")
        if not text.startswith(prefix):
            return None

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return None

        parts = cmdline.split()
        cmd = parts[0].lower()
        if cmd not in ("news", "headlines"):
            return None

        # Special subcommand
        if len(parts) >= 2 and parts[1].lower() == "categories":
            return (-1, "categories")

        limit = self._default_limit
        category = "top"

        # Accept forms:
        #   !news sport
        #   !news 5
        #   !news 5 sport
        if len(parts) >= 2:
            if parts[1].isdigit():
                limit = int(parts[1])
                if len(parts) >= 3:
                    category = parts[2].lower()
            else:
                category = parts[1].lower()

        limit = max(1, min(self._max_limit, limit))
        category = category.strip().lower() if category else "top"
        return (limit, category)

    # ---- public hook ----

    async def on_privmsg(self, bot, ev) -> None:
        await self._ensure_loaded()

        text = (ev.text or "").strip()
        parsed = self._parse_news_command(text)

        # 1) Handle pending selections first: user replies "1"
        if parsed is None:
            await self._maybe_handle_selection(bot, ev)
            return

        limit, category = parsed

        # 2) !news categories
        if limit == -1 and category == "categories":
            await self._handle_categories(bot, ev.target)
            return

        # 3) Start interactive source selection
        sources = self._sources()
        if not sources:
            await bot.privmsg(ev.target, "No news sources configured (DB empty).")
            return

        # Validate category exists anywhere; if not, tell user what's available
        available = self._all_categories(sources)
        if category not in available:
            await bot.privmsg(ev.target, f"Unknown category '{category}'. Try: !news categories")
            return

        # Store pending selection scoped to (nick, target) so channel/PM don’t collide
        key = (ev.nick.lower(), ev.target)
        self._pending[key] = (
            sources,
            PendingSelection(
                created_ts=time.time(),
                target=ev.target,
                limit=limit,
                category=category,
            ),
        )

        menu = " | ".join([f"{i}) {s.get('name','?')}" for i, s in enumerate(sources, start=1)])
        await bot.privmsg(ev.target, f"Choose a news source: {menu}")
        await bot.privmsg(ev.target, "Reply with the number (e.g. 1).")

    # ---- selection + output ----

    async def _maybe_handle_selection(self, bot, ev) -> None:
        msg = (ev.text or "").strip()
        if not msg.isdigit():
            return

        key = (ev.nick.lower(), ev.target)
        pending_tuple = self._pending.get(key)
        if not pending_tuple:
            return

        sources, pending = pending_tuple

        # Timeout pending requests
        if (time.time() - pending.created_ts) > self._selection_timeout:
            self._pending.pop(key, None)
            await bot.privmsg(ev.target, "News selection timed out. Run !news again.")
            return

        choice = int(msg)
        if choice < 1 or choice > len(sources):
            await bot.privmsg(ev.target, f"Invalid selection. Choose 1-{len(sources)}.")
            return

        src = sources[choice - 1]
        src_id = str(src.get("id", "")).strip() or f"src{choice}"
        src_name = str(src.get("name", src_id)).strip() or src_id
        category = pending.category
        limit = pending.limit

        # Clear pending now (so repeated digits don’t re-trigger)
        self._pending.pop(key, None)

        # Ensure selected source supports category
        cat_map = src.get("categories", {})
        if not isinstance(cat_map, dict) or category not in cat_map:
            await bot.privmsg(ev.target, f"{src_name} doesn’t support category '{category}'. Try !news categories.")
            return

        # Cooldown: block identical posts to same target (persisted in DB)
        last = await self.store.news_get_last_posted(
            target=pending.target,
            source_id=src_id,
            category=category,
            limit=limit,
        )
        now = int(time.time())
        if last is not None and (now - int(last)) < self._cooldown:
            remaining = int(self._cooldown - (now - int(last)))
            await bot.privmsg(
                ev.target,
                f"Already posted that recently. Try again in ~{remaining}s (or choose a different category/source).",
            )
            return

        await bot.privmsg(ev.target, f"Fetching {src_name} / {category} ({limit})…")

        items = await self._get_items(src_id, src_name, category, str(cat_map[category]))
        if not items:
            await bot.privmsg(ev.target, "No headlines returned (feed empty or failed).")
            return

        # De-dupe and take latest-ish items
        items = self._dedupe(items)
        items.sort(
            key=lambda x: (x.published_utc is None, x.published_utc or datetime.min.replace(tzinfo=timezone.utc)),
            reverse=True,
        )
        out = items[:limit]

        if not out:
            await bot.privmsg(ev.target, "No headlines available.")
            return

        await self.store.news_set_last_posted(
            target=pending.target,
            source_id=src_id,
            category=category,
            limit=limit,
            posted_ts=now,
        )

        await bot.privmsg(ev.target, f"Headlines: [{src_name}] ({category})")
        delay = self._line_delay
        for i, it in enumerate(out, start=1):
            await bot.privmsg(ev.target, f"{i}) {it.title} — {it.link}")
            if delay > 0 and i != len(out):
                await asyncio.sleep(delay)

    async def _get_items(self, source_id: str, source_name: str, category: str, url: str) -> List[NewsItem]:
        cache_key = (source_id, category)
        ttl = self._cache_ttl
        now = time.time()

        cached = self._cache.get(cache_key)
        if cached is not None:
            fetched_ts, items = cached
            if (now - fetched_ts) < ttl:
                return items

        try:
            xml_bytes = await asyncio.to_thread(_fetch_url, url)
            raw_items = _parse_rss_or_atom(xml_bytes)
            items = [
                NewsItem(
                    source_id=source_id,
                    source_name=source_name,
                    category=category,
                    title=title,
                    link=link,
                    published_utc=published,
                )
                for (title, link, published) in raw_items
            ]
            self._cache[cache_key] = (now, items)
            return items
        except Exception as e:
            logging.warning("News fetch failed for %s/%s: %s", source_id, category, e)
            return []

    def _dedupe(self, items: List[NewsItem]) -> List[NewsItem]:
        seen_links = set()
        seen_titles = set()
        out: List[NewsItem] = []
        for it in items:
            if it.link in seen_links:
                continue
            t = it.title.lower()
            if t in seen_titles:
                continue
            seen_links.add(it.link)
            seen_titles.add(t)
            out.append(it)
        return out

    # ---- categories command ----

    def _all_categories(self, sources: List[dict]) -> List[str]:
        cats = set()
        for s in sources:
            c = s.get("categories", {})
            if isinstance(c, dict):
                for k in c.keys():
                    cats.add(str(k).lower())
        ordered = []
        if "top" in cats:
            ordered.append("top")
        ordered.extend(sorted([c for c in cats if c != "top"]))
        return ordered

    async def _handle_categories(self, bot, target: str) -> None:
        sources = self._sources()
        if not sources:
            await bot.privmsg(target, "No news sources configured (DB empty).")
            return

        cats = self._all_categories(sources)
        await bot.privmsg(target, "Available categories:")
        await bot.privmsg(target, ", ".join(cats))

        for s in sources:
            name = s.get("name", s.get("id", "?"))
            c = s.get("categories", {})
            if isinstance(c, dict):
                scats = sorted([str(k).lower() for k in c.keys()])
                await bot.privmsg(target, f"{name}: {', '.join(scats)}")


def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command(
            "news",
            min_role="user",
            mutating=False,
            help="News headlines via RSS. Usage: !news [N] [category] (then pick source).",
            category="News",
        )
        bot.register_command(
            "news categories",
            min_role="user",
            mutating=False,
            help="List available news categories.",
            category="News",
        )
        bot.register_command(
            "headlines",
            min_role="user",
            mutating=False,
            help="Alias for !news",
            category="News",
        )
        bot.register_command(
            "headlines categories",
            min_role="user",
            mutating=False,
            help="Alias for !news categories",
            category="News",
        )

    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register(
            "news",
            min_role="user",
            mutating=False,
            help="News headlines via RSS. Usage: !news [N] [category] (then pick source).",
            category="News",
        )
        bot.acl.register(
            "news categories",
            min_role="user",
            mutating=False,
            help="List available news categories.",
            category="News",
        )
        bot.acl.register(
            "headlines",
            min_role="user",
            mutating=False,
            help="Alias for !news",
            category="News",
        )
        bot.acl.register(
            "headlines categories",
            min_role="user",
            mutating=False,
            help="Alias for !news categories",
            category="News",
        )

    db_path = str((bot.cfg.get("chatdb", {}) if isinstance(bot.cfg, dict) else {}).get("db_path", "/var/lib/leobot/db/leobot.db"))
    return NewsService(bot, db_path)
