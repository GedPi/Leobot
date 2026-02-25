from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.chatdb import ChatDB


def _now() -> int:
    return int(time.time())


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _json_loads(s: str) -> Any:
    return json.loads(s)


class Store:
    """High-level DB accessors for services.

    Keep service modules dumb: they call Store methods with typed-ish parameters.
    Store translates that into SQL and JSON structures.
    """

    def __init__(self, db: ChatDB):
        self.db = db
        self._migrated = False
        self._mig_lock = asyncio.Lock()

    async def _migrate_once(self) -> None:
        """Apply lightweight migrations (ALTER TABLE ADD COLUMN) when needed."""
        if self._migrated:
            return
        async with self._mig_lock:
            if self._migrated:
                return

            # NEWS: add optional name column to news_sources
            cols = await self.db.fetchall("PRAGMA table_info(news_sources)")
            col_names = {r["name"] for r in cols} if cols else set()
            if "name" not in col_names:
                await self.db.execute("ALTER TABLE news_sources ADD COLUMN name TEXT NOT NULL DEFAULT ''")

            # NEWS: add url column to news_source_categories (category feed url)
            cols = await self.db.fetchall("PRAGMA table_info(news_source_categories)")
            col_names = {r["name"] for r in cols} if cols else set()
            if "url" not in col_names:
                await self.db.execute("ALTER TABLE news_source_categories ADD COLUMN url TEXT NOT NULL DEFAULT ''")

            self._migrated = True

    # -------------------------
    # ACL
    # -------------------------
    async def acl_prune(self) -> int:
        await self._migrate_once()
        now = _now()
        await self.db.execute("DELETE FROM acl_auth WHERE authed_until_ts <= ?", (now,))
        row = await self.db.fetchone("SELECT changes() AS n")
        return int(row["n"]) if row else 0

    # Back-compat name used by newer ACLService
    async def acl_prune_expired(self) -> int:
        return await self.acl_prune()

    async def acl_set_auth(self, identity_key: str, role: str, until_ts: int) -> None:
        await self._migrate_once()
        identity_key = (identity_key or "").strip().lower()
        role = (role or "").strip().lower()
        until_ts = int(until_ts)
        now = _now()
        await self.db.execute(
            """                INSERT INTO acl_auth(identity_key, role, authed_until_ts, authed_ts)
            VALUES(?,?,?,?)
            ON CONFLICT(identity_key) DO UPDATE SET
                role=excluded.role,
                authed_until_ts=excluded.authed_until_ts,
                authed_ts=excluded.authed_ts
            """,
            (identity_key, role, until_ts, now),
        )

    async def acl_clear_auth(self, identity_key: str) -> None:
        await self._migrate_once()
        identity_key = (identity_key or "").strip().lower()
        await self.db.execute("DELETE FROM acl_auth WHERE identity_key=?", (identity_key,))

    async def acl_get_auth(self, identity_key: str) -> tuple[str | None, int | None]:
        await self._migrate_once()
        identity_key = (identity_key or "").strip().lower()
        row = await self.db.fetchone(
            "SELECT role, authed_until_ts FROM acl_auth WHERE identity_key=?",
            (identity_key,),
        )
        if not row:
            return None, None
        return str(row["role"]), int(row["authed_until_ts"])

    async def acl_get_authed_until(self, identity_key: str) -> int | None:
        """Compatibility helper for callers that only care about the expiry."""
        _role, until = await self.acl_get_auth(identity_key)
        return until

    # -------------------------
    # GREET
    # -------------------------
    async def greet_list_rules(self) -> list[dict[str, Any]]:
        await self._migrate_once()
        rows = await self.db.fetchall(
            """                SELECT id, priority, enabled, match_json, greetings_json, updated_ts
            FROM greet_rules
            ORDER BY enabled DESC, priority DESC, id ASC
            """
        )
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                dict(
                    id=r["id"],
                    priority=int(r["priority"]),
                    enabled=bool(int(r["enabled"])),
                    match=_json_loads(r["match_json"]),
                    greetings=_json_loads(r["greetings_json"]),
                    updated_ts=int(r["updated_ts"]),
                )
            )
        return out

    async def greet_upsert_rule(
        self,
        rule_id: str,
        *,
        priority: int = 0,
        enabled: bool = True,
        match: dict[str, Any] | None = None,
        greetings: list[str] | None = None,
    ) -> None:
        await self._migrate_once()
        rule_id = (rule_id or "").strip()
        if not rule_id:
            raise ValueError("rule_id is required")
        match = match or {}
        greetings = greetings or []
        now = _now()
        await self.db.execute(
            """                INSERT INTO greet_rules(id, priority, enabled, match_json, greetings_json, updated_ts)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                priority=excluded.priority,
                enabled=excluded.enabled,
                match_json=excluded.match_json,
                greetings_json=excluded.greetings_json,
                updated_ts=excluded.updated_ts
            """,
            (
                rule_id,
                int(priority),
                1 if enabled else 0,
                _json_dumps(match),
                _json_dumps(greetings),
                now,
            ),
        )

    async def greet_delete_rule(self, rule_id: str) -> None:
        await self._migrate_once()
        rule_id = (rule_id or "").strip()
        await self.db.execute("DELETE FROM greet_rules WHERE id=?", (rule_id,))

    async def greet_import_from_legacy_file(self, path: str | Path) -> int:
        """Import /var/lib/leobot/greetings.json into greet_rules if DB is empty.

        Supports either:
        - list of rule dicts
        - dict mapping id -> rule dict
        """
        await self._migrate_once()
        p = Path(path)
        if not p.exists():
            return 0

        row = await self.db.fetchone("SELECT COUNT(*) AS n FROM greet_rules")
        if row and int(row["n"]) > 0:
            return 0

        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return 0

        rules: list[dict[str, Any]] = []
        if isinstance(raw, list):
            rules = [r for r in raw if isinstance(r, dict)]
        elif isinstance(raw, dict):
            # accept {"rules":[...]} or {"id":{...}, ...}
            if isinstance(raw.get("rules"), list):
                rules = [r for r in raw["rules"] if isinstance(r, dict)]
            else:
                for k, v in raw.items():
                    if isinstance(v, dict):
                        vv = dict(v)
                        vv.setdefault("id", k)
                        rules.append(vv)

        imported = 0
        for r in rules:
            rid = str(r.get("id", "")).strip() or str(r.get("name", "")).strip()
            if not rid:
                continue
            priority = int(r.get("priority", 0) or 0)
            enabled = bool(r.get("enabled", True))
            match = r.get("match") or r.get("match_json") or {}
            greetings = r.get("greetings") or r.get("greetings_json") or []
            # legacy shapes: {"hosts":[...]} etc
            if isinstance(match, str):
                try:
                    match = json.loads(match)
                except Exception:
                    match = {}
            if isinstance(greetings, str):
                try:
                    greetings = json.loads(greetings)
                except Exception:
                    greetings = []
            if not isinstance(match, dict):
                match = {}
            if not isinstance(greetings, list):
                greetings = []
            await self.greet_upsert_rule(
                rid,
                priority=priority,
                enabled=enabled,
                match=match,
                greetings=[str(x) for x in greetings if str(x).strip()],
            )
            imported += 1
        return imported

    # -------------------------
    # WIKI (wikimon)
    # -------------------------
    async def wiki_get_setting(self, key: str, default: str | None = None) -> str | None:
        await self._migrate_once()
        key = (key or "").strip()
        row = await self.db.fetchone("SELECT v FROM wiki_settings WHERE k=?", (key,))
        if not row:
            return default
        return str(row["v"])

    async def wiki_set_setting(self, key: str, value: str) -> None:
        await self._migrate_once()
        key = (key or "").strip()
        now = _now()
        await self.db.execute(
            """                INSERT INTO wiki_settings(k, v)
            VALUES(?, ?)
            ON CONFLICT(k) DO UPDATE SET v=excluded.v
            """,
            (key, str(value)),
        )

    async def wiki_get_lang(self, default: str = "en") -> str:
        v = await self.wiki_get_setting("lang", default)
        return (v or default).strip() or default

    async def wiki_set_lang(self, lang: str) -> None:
        await self.wiki_set_setting("lang", (lang or "en").strip().lower() or "en")

    # Names expected by WikiService
    async def wiki_list_pages(self) -> list[str]:
        await self._migrate_once()
        rows = await self.db.fetchall("SELECT title FROM wiki_watch ORDER BY title COLLATE NOCASE ASC")
        return [str(r["title"]) for r in rows]

    async def wiki_add_page(self, title: str) -> None:
        await self._migrate_once()
        title = (title or "").strip()
        if not title:
            raise ValueError("title required")
        await self.db.execute("INSERT OR IGNORE INTO wiki_watch(title) VALUES(?)", (title,))

    async def wiki_del_page(self, title: str) -> None:
        await self._migrate_once()
        title = (title or "").strip()
        await self.db.execute("DELETE FROM wiki_watch WHERE title=?", (title,))

    async def wiki_import_from_legacy_file(self, path: str | Path) -> int:
        """Import legacy wiki watchlist file if DB empty."""
        await self._migrate_once()
        p = Path(path)
        if not p.exists():
            return 0
        row = await self.db.fetchone("SELECT COUNT(*) AS n FROM wiki_watch")
        if row and int(row["n"]) > 0:
            return 0
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return 0
        titles: list[str] = []
        if isinstance(raw, list):
            titles = [str(x) for x in raw]
        elif isinstance(raw, dict):
            if isinstance(raw.get("watch"), list):
                titles = [str(x) for x in raw["watch"]]
            elif isinstance(raw.get("titles"), list):
                titles = [str(x) for x in raw["titles"]]
        imported = 0
        for t in titles:
            t = (t or "").strip()
            if not t:
                continue
            await self.wiki_add_page(t)
            imported += 1
        return imported

    # -------------------------
    # WEATHER WARN
    # -------------------------
    async def weather_list_watches(self) -> list[dict[str, Any]]:
        await self._migrate_once()
        rows = await self.db.fetchall(
            """                SELECT city, duration_hours, types_json, interval_minutes, created_ts, expires_ts
            FROM weather_watches
            ORDER BY expires_ts ASC
            """
        )
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                dict(
                    city=str(r["city"]),
                    duration_hours=int(r["duration_hours"]),
                    types=_json_loads(r["types_json"]),
                    interval_minutes=int(r["interval_minutes"]),
                    created_ts=int(r["created_ts"]),
                    expires_ts=int(r["expires_ts"]),
                )
            )
        return out

    async def weather_upsert_watch(
        self,
        city: str,
        *,
        duration_hours: int,
        types: list[str],
        interval_minutes: int,
    ) -> None:
        await self._migrate_once()
        city = (city or "").strip()
        if not city:
            raise ValueError("city required")
        now = _now()
        expires = now + int(duration_hours) * 3600
        await self.db.execute(
            """                INSERT INTO weather_watches(city, duration_hours, types_json, interval_minutes, created_ts, expires_ts)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(city) DO UPDATE SET
                duration_hours=excluded.duration_hours,
                types_json=excluded.types_json,
                interval_minutes=excluded.interval_minutes,
                created_ts=excluded.created_ts,
                expires_ts=excluded.expires_ts
            """,
            (city, int(duration_hours), _json_dumps(types), int(interval_minutes), now, expires),
        )

    async def weather_delete_watch(self, city: str) -> None:
        await self._migrate_once()
        city = (city or "").strip()
        await self.db.execute("DELETE FROM weather_watches WHERE city=?", (city,))

    async def weather_prune_expired(self) -> int:
        await self._migrate_once()
        now = _now()
        await self.db.execute("DELETE FROM weather_watches WHERE expires_ts <= ?", (now,))
        row = await self.db.fetchone("SELECT changes() AS n")
        return int(row["n"]) if row else 0

    async def weather_import_from_legacy_file(self, path: str | Path) -> int:
        """Import legacy weather watches file if DB empty."""
        await self._migrate_once()
        p = Path(path)
        if not p.exists():
            return 0
        row = await self.db.fetchone("SELECT COUNT(*) AS n FROM weather_watches")
        if row and int(row["n"]) > 0:
            return 0
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return 0

        watches: list[dict[str, Any]] = []
        if isinstance(raw, list):
            watches = [w for w in raw if isinstance(w, dict)]
        elif isinstance(raw, dict) and isinstance(raw.get("watches"), list):
            watches = [w for w in raw["watches"] if isinstance(w, dict)]

        imported = 0
        for w in watches:
            city = str(w.get("city", "")).strip()
            if not city:
                continue
            dur = int(w.get("duration_hours", w.get("duration", 1)) or 1)
            types = w.get("types", w.get("types_json", ["rain"]))
            if isinstance(types, str):
                try:
                    types = json.loads(types)
                except Exception:
                    types = ["rain"]
            if not isinstance(types, list):
                types = ["rain"]
            interval = int(w.get("interval_minutes", w.get("interval", 15)) or 15)
            await self.weather_upsert_watch(city, duration_hours=dur, types=[str(x) for x in types], interval_minutes=interval)
            imported += 1
        return imported

    # -------------------------
    # NEWS
    # -------------------------
    async def news_get_setting(self, key: str, default: str | None = None) -> str | None:
        await self._migrate_once()
        key = (key or "").strip()
        row = await self.db.fetchone("SELECT v FROM news_settings WHERE k=?", (key,))
        if not row:
            return default
        return str(row["v"])

    async def news_set_setting(self, key: str, value: str) -> None:
        await self._migrate_once()
        key = (key or "").strip()
        now = _now()
        await self.db.execute(
            """                INSERT INTO news_settings(k, v, updated_ts)
            VALUES(?,?,?)
            ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_ts=excluded.updated_ts
            """,
            (key, str(value), now),
        )

    async def news_get_int(self, key: str, default: int) -> int:
        v = await self.news_get_setting(key, None)
        try:
            return int(v) if v is not None else int(default)
        except Exception:
            return int(default)

    async def news_get_float(self, key: str, default: float) -> float:
        v = await self.news_get_setting(key, None)
        try:
            return float(v) if v is not None else float(default)
        except Exception:
            return float(default)

    async def news_list_sources(self) -> list[dict[str, Any]]:
        """Return sources as list of {id,name,categories:{cat:url}}."""
        await self._migrate_once()
        src_rows = await self.db.fetchall(
            "SELECT id, url, name, enabled, interval_minutes FROM news_sources WHERE enabled=1 ORDER BY id ASC"
        )
        out: list[dict[str, Any]] = []
        for s in src_rows:
            sid = str(s["id"])
            name = str(s.get("name") or "") or sid
            # categories: from table if populated, else fall back to single url as 'top'
            cat_rows = await self.db.fetchall(
                "SELECT category, url FROM news_source_categories WHERE source_id=? ORDER BY category ASC", (sid,)
            )
            cats: dict[str, str] = {}
            for c in cat_rows:
                cat = str(c["category"])
                url = str(c.get("url") or "")
                if url:
                    cats[cat] = url
            if not cats:
                base_url = str(s["url"])
                if base_url:
                    cats["top"] = base_url
            out.append({"id": sid, "name": name, "categories": cats})
        return out

    async def news_set_sources(self, sources: list[dict[str, Any]]) -> None:
        """Replace enabled sources set (used by admin tooling if added later)."""
        await self._migrate_once()
        now = _now()
        # naive replace
        await self.db.execute("DELETE FROM news_source_categories")
        await self.db.execute("DELETE FROM news_sources")
        for src in sources:
            sid = str(src.get("id", "")).strip()
            if not sid:
                continue
            name = str(src.get("name", sid)).strip() or sid
            cats = src.get("categories", {})
            if not isinstance(cats, dict):
                cats = {}
            # choose a representative url (first category url) for news_sources.url (legacy)
            rep_url = ""
            for _k, _v in cats.items():
                rep_url = str(_v)
                break
            await self.db.execute(
                """                    INSERT INTO news_sources(id, url, name, enabled, interval_minutes, created_ts, updated_ts)
                VALUES(?,?,?,?,?,?,?)
                """,
                (sid, rep_url, name, 1, 60, now, now),
            )
            for cat, url in cats.items():
                cat = str(cat).strip().lower()
                url = str(url).strip()
                if not cat or not url:
                    continue
                await self.db.execute(
                    "INSERT OR REPLACE INTO news_source_categories(source_id, category, url) VALUES(?,?,?)",
                    (sid, cat, url),
                )

    async def news_get_last_posted(self, *, target: str, source_id: str, category: str, limit_n: int) -> int | None:
        await self._migrate_once()
        row = await self.db.fetchone(
            """                SELECT last_ts FROM news_last_posted
            WHERE target=? AND source_id=? AND category=? AND limit_n=?
            """,
            ((target or "").strip(), (source_id or "").strip().lower(), (category or "").strip().lower(), int(limit_n)),
        )
        if not row:
            return None
        ts = int(row["last_ts"])
        return ts if ts > 0 else None

    async def news_set_last_posted(self, *, target: str, source_id: str, category: str, limit_n: int, posted_ts: int) -> None:
        await self._migrate_once()
        now = _now()
        await self.db.execute(
            """                INSERT INTO news_last_posted(target, source_id, category, limit_n, last_guid, last_ts)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(target, source_id, category, limit_n) DO UPDATE SET
                last_ts=excluded.last_ts
            """,
            (
                (target or "").strip(),
                (source_id or "").strip().lower(),
                (category or "").strip().lower(),
                int(limit_n),
                "",
                int(posted_ts),
            ),
        )

    async def news_import_from_legacy_config(self, news_cfg: dict[str, Any] | None) -> int:
        """Import sources/settings from config.json (once), if DB has no sources."""
        await self._migrate_once()
        if not news_cfg or not isinstance(news_cfg, dict):
            return 0

        row = await self.db.fetchone("SELECT COUNT(*) AS n FROM news_sources")
        if row and int(row["n"]) > 0:
            return 0

        now = _now()
        imported = 0

        # settings
        if "default_limit" in news_cfg:
            await self.news_set_setting("default_limit", str(int(news_cfg.get("default_limit") or 10)))
        if "cooldown_seconds" in news_cfg:
            await self.news_set_setting("cooldown_seconds", str(int(news_cfg.get("cooldown_seconds") or 120)))
        if "line_delay_seconds" in news_cfg:
            await self.news_set_setting("line_delay_seconds", str(float(news_cfg.get("line_delay_seconds") or 0.8)))
        if "selection_timeout_seconds" in news_cfg:
            await self.news_set_setting("selection_timeout_seconds", str(int(news_cfg.get("selection_timeout_seconds") or 60)))

        srcs = news_cfg.get("sources", [])
        if not isinstance(srcs, list):
            srcs = []

        for src in srcs:
            if not isinstance(src, dict):
                continue
            sid = str(src.get("id", "")).strip()
            if not sid:
                continue
            name = str(src.get("name", sid)).strip() or sid
            cats = src.get("categories", {})
            if not isinstance(cats, dict):
                cats = {}
            rep_url = ""
            for _k, _v in cats.items():
                rep_url = str(_v).strip()
                break
            if not rep_url:
                # allow a single url if no categories
                rep_url = str(src.get("url", "")).strip()
            await self.db.execute(
                """                    INSERT INTO news_sources(id, url, name, enabled, interval_minutes, created_ts, updated_ts)
                VALUES(?,?,?,?,?,?,?)
                """,
                (sid, rep_url, name, 1, int(src.get("interval_minutes") or 60), now, now),
            )
            if cats:
                for cat, url in cats.items():
                    cat = str(cat).strip().lower()
                    url = str(url).strip()
                    if not cat or not url:
                        continue
                    await self.db.execute(
                        "INSERT OR REPLACE INTO news_source_categories(source_id, category, url) VALUES(?,?,?)",
                        (sid, cat, url),
                    )
            else:
                # create 'top' category pointing to rep_url
                if rep_url:
                    await self.db.execute(
                        "INSERT OR REPLACE INTO news_source_categories(source_id, category, url) VALUES(?,?,?)",
                        (sid, "top", rep_url),
                    )
            imported += 1

        return imported

    # -------------------------
    # SYSTEM COLLECTOR DB (sysmon)
    # -------------------------
    async def sys_state_set(self, k: str, v_json: dict[str, Any]) -> None:
        await self._migrate_once()
        now = _now()
        await self.db.execute(
            """                INSERT INTO sys_state(k, v_json, updated_ts)
            VALUES(?,?,?)
            ON CONFLICT(k) DO UPDATE SET v_json=excluded.v_json, updated_ts=excluded.updated_ts
            """,
            ((k or "").strip(), _json_dumps(v_json), now),
        )

    async def sys_state_get(self, k: str) -> dict[str, Any] | None:
        await self._migrate_once()
        row = await self.db.fetchone("SELECT v_json FROM sys_state WHERE k=?", ((k or "").strip(),))
        if not row:
            return None
        try:
            return _json_loads(row["v_json"])
        except Exception:
            return None

    async def sys_event_add(self, message: str) -> None:
        await self._migrate_once()
        now = _now()
        await self.db.execute(
            "INSERT INTO sys_events(ts, message) VALUES(?,?)",
            (now, str(message)),
        )

    async def sys_health_add(self, payload: dict[str, Any]) -> None:
        await self._migrate_once()
        now = _now()
        await self.db.execute(
            "INSERT INTO sys_health_snapshots(ts, payload_json) VALUES(?,?)",
            (now, _json_dumps(payload)),
        )
