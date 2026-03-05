from __future__ import annotations

import asyncio
import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

UA = "LeonidasIRCbot/2.0 (https://hairyoctopus.net; admin: Ged)"


def _http_get_json(url: str, timeout: int = 12) -> dict:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": UA, "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))


async def _get_json(url: str, timeout: int = 12) -> dict:
    return await asyncio.to_thread(_http_get_json, url, timeout)


def _now_ts() -> int:
    return int(time.time())


def _norm_space(s: str) -> str:
    return " ".join((s or "").strip().split())


def _norm_title(s: str) -> str:
    s = _norm_space(s)
    if not s:
        return s
    # Wikipedia titles are typically capitalized on first letter
    return s[0].upper() + s[1:]


def _parse_duration_to_minutes(raw: str) -> int | None:
    """
    Supports: 15m, 2h, 1d  -> minutes
    """
    r = (raw or "").strip().lower()
    if len(r) < 2:
        return None
    unit = r[-1]
    num = r[:-1]
    if not num.isdigit():
        return None
    n = int(num)
    if n <= 0:
        return None

    if unit == "m":
        if n > 24 * 60:
            return None
        return n
    if unit == "h":
        if n > 7 * 24:
            return None
        return n * 60
    if unit == "d":
        if n > 30:
            return None
        return n * 1440
    return None


def _wiki_page_url(lang: str, title: str) -> str:
    t = title.replace(" ", "_")
    return f"https://{lang}.wikipedia.org/wiki/{urllib.parse.quote(t, safe='')}"


def _wiki_diff_url(lang: str, title: str, oldid: int | None, revid: int) -> str:
    # Diff links are stable enough for operator use.
    t = title.replace(" ", "_")
    base = f"https://{lang}.wikipedia.org/w/index.php?title={urllib.parse.quote(t, safe='')}"
    if oldid:
        return f"{base}&diff={revid}&oldid={oldid}"
    return f"{base}&diff={revid}"


@dataclass(slots=True)
class _Revision:
    revid: int
    user: str
    ts: str
    comment: str


class WikiService:
    """
    Wikipedia lookup + DB-backed watchlist monitor.

    Per-channel enablement is handled by the core !service system:
      !service enable wiki #Channel

    Commands:
      !wiki <query>
      !wikicheck <query>
      !wikimon [list]
      !wikimon add <title> [15m|2h|1d]
      !wikimon del <id>
      !wikimon clear
      !wikimon lang <code>               (sets default language in settings)
      !wikimon interval <id> <15m|2h|1d>
    """

    def __init__(self, cfg: dict, *, service_name: str = "wiki"):
        self.cfg = cfg or {}
        self.service_name = service_name
        self._cooldown: dict[tuple[str, str], float] = {}
        self._mem_cache: dict[tuple, tuple[float, Any]] = {}

        self.poll_tick_s = float(self.cfg.get("poll_tick_seconds", 60))
        self.cache_ttl_s = int(self.cfg.get("cache_ttl_seconds", 900))
        self.cooldown_seconds = int(self.cfg.get("cooldown_seconds", 5))
        self.max_summary_chars = int(self.cfg.get("max_summary_chars", 320))
        self.max_changes_per_tick = int(self.cfg.get("max_changes_per_tick", 6))

        # default watch interval if user doesn’t specify
        self.default_watch_interval_minutes = int(self.cfg.get("default_watch_interval_minutes", 15))

    # ----------------------------
    # generic helpers
    # ----------------------------
    def _cooldown_ok(self, target: str, cmd: str, seconds: int) -> bool:
        now = time.time()
        k = (target, cmd)
        until = self._cooldown.get(k, 0.0)
        if now < until:
            return False
        self._cooldown[k] = now + seconds
        return True

    def _cache_get(self, key: tuple) -> Any | None:
        it = self._mem_cache.get(key)
        if not it:
            return None
        exp, val = it
        if time.time() >= exp:
            self._mem_cache.pop(key, None)
            return None
        return val

    def _cache_set(self, key: tuple, val: Any, ttl_s: int) -> None:
        self._mem_cache[key] = (time.time() + ttl_s, val)

    async def _default_lang(self, bot) -> str:
        # Stored in DB settings to avoid config reloads for simple changes.
        try:
            v = await bot.store.get_setting("wiki:default_lang", None)
            if v:
                return str(v).strip().lower()
        except Exception:
            pass
        return str(self.cfg.get("lang", "en")).strip().lower() or "en"

    # ----------------------------
    # Wikipedia REST lookup
    # ----------------------------
    async def _rest_search_title(self, lang: str, query: str) -> dict | None:
        q = _norm_space(query)
        if not q:
            return None
        key = ("rest_search", lang, q.lower())
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        url = f"https://{lang}.wikipedia.org/w/rest.php/v1/search/title?" + urllib.parse.urlencode(
            {"q": q, "limit": 1}
        )
        data = await _get_json(url, timeout=12)
        pages = data.get("pages") or []
        out = pages[0] if pages else None
        self._cache_set(key, out, self.cache_ttl_s)
        return out

    async def _rest_summary(self, lang: str, key_or_title: str) -> dict:
        k = key_or_title.strip()
        cache_key = ("rest_summary", lang, k.lower())
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        url = f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/{urllib.parse.quote(k)}"
        data = await _get_json(url, timeout=12)
        self._cache_set(cache_key, data, self.cache_ttl_s)
        return data

    async def _opensearch(self, lang: str, query: str, limit: int = 5) -> list[str]:
        q = _norm_space(query)
        if not q:
            return []
        cache_key = ("opensearch", lang, q.lower(), int(limit))
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        url = f"https://{lang}.wikipedia.org/w/api.php?" + urllib.parse.urlencode(
            {
                "action": "opensearch",
                "search": q,
                "limit": str(int(limit)),
                "namespace": "0",
                "format": "json",
            }
        )
        data = await _get_json(url, timeout=12)
        titles = data[1] if isinstance(data, list) and len(data) > 1 else []
        titles = [str(t) for t in titles if str(t).strip()]
        self._cache_set(cache_key, titles, self.cache_ttl_s)
        return titles

    # ----------------------------
    # Wikipedia revision check (monitor)
    # ----------------------------
    async def _latest_revision(self, lang: str, title: str) -> _Revision | None:
        t = title.replace(" ", "_")
        url = f"https://{lang}.wikipedia.org/w/api.php?" + urllib.parse.urlencode(
            {
                "action": "query",
                "format": "json",
                "prop": "revisions",
                "titles": t,
                "rvprop": "ids|timestamp|user|comment",
                "rvlimit": "1",
                "rvdir": "older",
            }
        )
        data = await _get_json(url, timeout=12)
        pages = ((data.get("query") or {}).get("pages") or {})
        # pages is a dict keyed by pageid; value contains revisions or missing
        for _, p in pages.items():
            if p.get("missing") is not None:
                return None
            revs = p.get("revisions") or []
            if not revs:
                return None
            r0 = revs[0]
            try:
                revid = int(r0.get("revid"))
            except Exception:
                return None
            return _Revision(
                revid=revid,
                user=str(r0.get("user") or ""),
                ts=str(r0.get("timestamp") or ""),
                comment=str(r0.get("comment") or "").strip(),
            )
        return None

    async def _channels_with_wiki_enabled(self, bot) -> list[str]:
        # Announce into any channel where wiki is enabled.
        rows = await bot.store.fetchall(
            "SELECT channel FROM service_enablement WHERE service=? AND enabled=1 ORDER BY channel",
            (self.service_name,),
        )
        out: list[str] = []
        for r in rows:
            ch = str(r[0])
            if ch.startswith("#"):
                out.append(ch)
        return out

    async def _watch_due_rows(self, bot, now_ts: int, limit: int = 30):
        # Due if never checked, or last_checked + interval <= now.
        # wiki_state is 1:1 with watch_id, but may be absent initially.
        return await bot.store.fetchall(
            """
            SELECT
              w.id, w.lang, w.title, w.interval_minutes, w.enabled,
              s.last_rev_id, s.last_checked_ts
            FROM wiki_watches w
            LEFT JOIN wiki_state s ON s.watch_id = w.id
            WHERE w.enabled=1
              AND (
                s.last_checked_ts IS NULL
                OR (s.last_checked_ts + (w.interval_minutes * 60)) <= ?
              )
            ORDER BY COALESCE(s.last_checked_ts, 0) ASC
            LIMIT ?
            """,
            (int(now_ts), int(limit)),
        )

    async def job_poll(self, bot) -> None:
        now = _now_ts()
        try:
            due = await self._watch_due_rows(bot, now_ts=now, limit=50)
        except Exception:
            # If DB is borked, don't crash the scheduler loop.
            return

        if not due:
            return

        targets = await self._channels_with_wiki_enabled(bot)
        if not targets:
            # If nowhere to announce, still update state so we don’t spam later.
            targets = []

        announced = 0
        for r in due:
            watch_id = int(r["id"])
            lang = str(r["lang"] or "en").strip().lower() or "en"
            title = str(r["title"] or "").strip()
            if not title:
                continue

            last_rev = r["last_rev_id"]
            last_rev_id = int(last_rev) if last_rev is not None else None

            rev = None
            try:
                rev = await self._latest_revision(lang, title)
            except Exception:
                rev = None

            # Mark checked even if API failed (prevents tight loop hammering)
            try:
                await bot.store.execute(
                    """
                    INSERT INTO wiki_state(watch_id,last_rev_id,last_checked_ts)
                    VALUES(?,?,?)
                    ON CONFLICT(watch_id) DO UPDATE SET
                      last_checked_ts=excluded.last_checked_ts
                    """,
                    (watch_id, last_rev_id if last_rev_id is not None else None, now),
                )
            except Exception:
                pass

            if not rev:
                continue

            # First-seen: set baseline, do not announce
            if last_rev_id is None:
                try:
                    await bot.store.execute(
                        """
                        INSERT INTO wiki_state(watch_id,last_rev_id,last_checked_ts)
                        VALUES(?,?,?)
                        ON CONFLICT(watch_id) DO UPDATE SET
                          last_rev_id=excluded.last_rev_id,
                          last_checked_ts=excluded.last_checked_ts
                        """,
                        (watch_id, int(rev.revid), now),
                    )
                except Exception:
                    pass
                continue

            if int(rev.revid) == int(last_rev_id):
                continue

            # Update state first (so if we get disconnected mid-spam we don't repeat forever)
            try:
                await bot.store.execute(
                    """
                    INSERT INTO wiki_state(watch_id,last_rev_id,last_checked_ts)
                    VALUES(?,?,?)
                    ON CONFLICT(watch_id) DO UPDATE SET
                      last_rev_id=excluded.last_rev_id,
                      last_checked_ts=excluded.last_checked_ts
                    """,
                    (watch_id, int(rev.revid), now),
                )
            except Exception:
                pass

            if not targets:
                continue

            # Announce
            diff = _wiki_diff_url(lang, title, oldid=last_rev_id, revid=rev.revid)
            page = _wiki_page_url(lang, title)

            msg = f"WIKIMON: {title} updated"
            if rev.user:
                msg += f" by {rev.user}"
            if rev.comment:
                # keep it short
                c = rev.comment
                if len(c) > 120:
                    c = c[:119].rstrip() + "…"
                msg += f" — {c}"
            msg += f" | {diff} | {page}"

            for ch in targets:
                try:
                    await bot.privmsg(ch, msg)
                except Exception:
                    pass

            announced += 1
            if announced >= self.max_changes_per_tick:
                break

    # ----------------------------
    # command handling
    # ----------------------------
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

        # wiki is per-channel enablement
        if ev.channel and not await bot.store.is_service_enabled(ev.channel, self.service_name):
            return

        # mild channel flood control
        if not ev.is_private and cmd in ("wiki", "wikicheck"):
            if not self._cooldown_ok(ev.target, cmd, seconds=self.cooldown_seconds):
                await bot.privmsg(ev.target, f"{ev.nick}: slow down.")
                return

        lang = await self._default_lang(bot)

        # -------------- !wiki --------------
        if cmd == "wiki":
            q = cmdline[len("wiki") :].strip()
            if not q:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !wiki <query>")
                return

            hit = await self._rest_search_title(lang, q)
            if not hit:
                await bot.privmsg(ev.target, f"{ev.nick}: no results")
                return

            title = str(hit.get("title") or "").strip() or _norm_title(q)
            key = str(hit.get("key") or title).strip() or title

            summ = await self._rest_summary(lang, key)
            page_type = str(summ.get("type") or "").lower()
            extract = (summ.get("extract") or "").strip()
            link = (((summ.get("content_urls") or {}).get("desktop") or {}).get("page")) or _wiki_page_url(lang, title)

            if page_type == "disambiguation":
                suggestions = await self._opensearch(lang, q, limit=5)
                s = ", ".join(suggestions[:5]) if suggestions else "no suggestions"
                await bot.privmsg(ev.target, f"WIKI: {title} is a disambiguation page. Try: {s}")
                return

            if not extract:
                suggestions = await self._opensearch(lang, q, limit=5)
                s = ", ".join(suggestions[:5]) if suggestions else "no suggestions"
                await bot.privmsg(ev.target, f"WIKI: no summary found. Suggestions: {s}")
                return

            if len(extract) > self.max_summary_chars:
                extract = extract[: self.max_summary_chars - 1].rstrip() + "…"

            await bot.privmsg(ev.target, f"WIKI: {title} — {extract} ({link})")
            return

        # -------------- !wikicheck --------------
        if cmd == "wikicheck":
            q = cmdline[len("wikicheck") :].strip()
            if not q:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !wikicheck <query>")
                return

            hit = await self._rest_search_title(lang, q)
            if not hit:
                suggestions = await self._opensearch(lang, q, limit=5)
                s = ", ".join(suggestions[:5]) if suggestions else "no suggestions"
                await bot.privmsg(ev.target, f"WIKICHECK: not found. Suggestions: {s}")
                return

            title = str(hit.get("title") or "").strip() or _norm_title(q)
            link = _wiki_page_url(lang, title)
            await bot.privmsg(ev.target, f"WIKICHECK: OK — {title} ({link})")
            return

        # -------------- !wikimon --------------
        # NOTE: watchers are GLOBAL; announcements go to channels where wiki is enabled.
        sub = parts[1].lower() if len(parts) >= 2 else "list"
        rest = cmdline.split(maxsplit=2)
        arg = rest[2].strip() if len(rest) >= 3 else ""

        if sub in ("list", ""):
            rows = await bot.store.fetchall(
                "SELECT id, lang, title, interval_minutes, enabled FROM wiki_watches ORDER BY id",
                (),
            )
            if not rows:
                await bot.privmsg(ev.target, "WIKIMON: watchlist empty.")
                return
            # compact output
            items = []
            for r in rows[:20]:
                items.append(f"{int(r['id'])}) {r['lang']}:{r['title']} ({int(r['interval_minutes'])}m){'' if int(r['enabled'])==1 else ' [off]'}")
            more = f" (+{len(rows)-20} more)" if len(rows) > 20 else ""
            await bot.privmsg(ev.target, "WIKIMON: " + " | ".join(items) + more)
            return

        if sub == "add":
            if not arg:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !wikimon add <title> [15m|2h|1d]")
                return
            bits = arg.split()
            title = _norm_title(" ".join(bits[:-1])) if len(bits) >= 2 and _parse_duration_to_minutes(bits[-1]) else _norm_title(arg)
            interval_m = _parse_duration_to_minutes(bits[-1]) if len(bits) >= 2 else None
            if interval_m is None:
                interval_m = self.default_watch_interval_minutes
            interval_m = max(1, min(24 * 60, int(interval_m)))

            # quick existence check (optional but prevents junk watches)
            rev = await self._latest_revision(lang, title)
            if not rev:
                await bot.privmsg(ev.target, f"WIKIMON: page not found: {lang}:{title}")
                return

            now = _now_ts()
            try:
                await bot.store.execute(
                    """
                    INSERT INTO wiki_watches(lang,title,enabled,interval_minutes,created_ts,created_by)
                    VALUES(?,?,?,?,?,?)
                    ON CONFLICT(lang,title) DO UPDATE SET
                      enabled=1,
                      interval_minutes=excluded.interval_minutes
                    """,
                    (lang, title, 1, interval_m, now, ev.nick),
                )
                # ensure state row exists; set baseline revision so we don't announce immediately
                row = await bot.store.fetchone("SELECT id FROM wiki_watches WHERE lang=? AND title=?", (lang, title))
                if row:
                    watch_id = int(row[0])
                    await bot.store.execute(
                        """
                        INSERT INTO wiki_state(watch_id,last_rev_id,last_checked_ts)
                        VALUES(?,?,?)
                        ON CONFLICT(watch_id) DO UPDATE SET
                          last_rev_id=excluded.last_rev_id
                        """,
                        (watch_id, int(rev.revid), now),
                    )
            except Exception:
                await bot.privmsg(ev.target, "WIKIMON: failed to add watch (db error).")
                return

            await bot.privmsg(ev.target, f"WIKIMON: added {lang}:{title} ({interval_m}m)")
            return

        if sub == "del":
            if not arg or not arg.isdigit():
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !wikimon del <id>")
                return
            wid = int(arg)
            async_rows = await bot.store.fetchall("SELECT id, lang, title FROM wiki_watches WHERE id=?", (wid,))
            if not async_rows:
                await bot.privmsg(ev.target, f"WIKIMON: unknown id {wid}")
                return
            await bot.store.execute("DELETE FROM wiki_watches WHERE id=?", (wid,))
            await bot.privmsg(ev.target, f"WIKIMON: removed watch {wid}")
            return

        if sub == "clear":
            await bot.store.execute("DELETE FROM wiki_watches", ())
            await bot.privmsg(ev.target, "WIKIMON: cleared watchlist.")
            return

        if sub == "lang":
            if not arg:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !wikimon lang <code>")
                return
            code = arg.strip().lower()
            if not code.isalpha() or len(code) > 12:
                await bot.privmsg(ev.target, f"{ev.nick}: invalid language code.")
                return
            await bot.store.set_setting("wiki:default_lang", code)
            await bot.privmsg(ev.target, f"WIKIMON: default language set to {code}")
            return

        if sub == "interval":
            # !wikimon interval <id> <15m|2h|1d>
            bits = arg.split()
            if len(bits) != 2 or (not bits[0].isdigit()):
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !wikimon interval <id> <15m|2h|1d>")
                return
            wid = int(bits[0])
            interval_m = _parse_duration_to_minutes(bits[1])
            if interval_m is None:
                await bot.privmsg(ev.target, f"{ev.nick}: invalid interval. Use 15m, 2h, 1d.")
                return
            interval_m = max(1, min(24 * 60, int(interval_m)))
            row = await bot.store.fetchone("SELECT id FROM wiki_watches WHERE id=?", (wid,))
            if not row:
                await bot.privmsg(ev.target, f"WIKIMON: unknown id {wid}")
                return
            await bot.store.execute("UPDATE wiki_watches SET interval_minutes=? WHERE id=?", (interval_m, wid))
            await bot.privmsg(ev.target, f"WIKIMON: updated {wid} interval to {interval_m}m")
            return

        await bot.privmsg(
            ev.target,
            "WIKIMON: usage: !wikimon [list] | add <title> [15m|2h|1d] | del <id> | clear | lang <code> | interval <id> <15m|2h|1d>",
        )


def setup(bot):
    # Register commands into core help/ACL registry
    if hasattr(bot, "register_command"):
        bot.register_command("wiki", min_role="guest", mutating=False, help="Wikipedia lookup. Usage: !wiki <query>", category="Wiki")
        bot.register_command("wikicheck", min_role="guest", mutating=False, help="Check a Wikipedia title exists. Usage: !wikicheck <query>", category="Wiki")
        bot.register_command("wikimon", min_role="user", mutating=False, help="List wiki watches. Usage: !wikimon [list]", category="Wiki")
        bot.register_command("wikimon add", min_role="user", mutating=True, help="Add a wiki watch. Usage: !wikimon add <title> [15m|2h|1d]", category="Wiki")
        bot.register_command("wikimon del", min_role="user", mutating=True, help="Delete a wiki watch. Usage: !wikimon del <id>", category="Wiki")
        bot.register_command("wikimon clear", min_role="contributor", mutating=True, help="Clear all wiki watches.", category="Wiki")
        bot.register_command("wikimon lang", min_role="contributor", mutating=True, help="Set default wiki language. Usage: !wikimon lang <code>", category="Wiki")
        bot.register_command("wikimon interval", min_role="user", mutating=True, help="Change watch interval. Usage: !wikimon interval <id> <15m|2h|1d>", category="Wiki")

    svc = WikiService(bot.cfg.get("wiki", {}) if isinstance(getattr(bot, "cfg", None), dict) else {})

    # Register scheduler job
    if getattr(bot, "scheduler", None) is not None and hasattr(bot.scheduler, "register_interval"):
        bot.scheduler.register_interval(
            "wiki:poll",
            svc.poll_tick_s,
            lambda: svc.job_poll(bot),
            jitter_seconds=1.0,
            run_on_start=False,
        )

    return svc