#!/usr/bin/env python3
# services/store.py
"""
High-level data access layer for Leonidas.

Rule: Services should talk to Store, Store talks to ChatDB.
Store can be constructed with either a ChatDB instance or a DB path string.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional

from services.chatdb import ChatDB, DBConfig


def _now() -> int:
    return int(time.time())


class Store:
    def __init__(self, db: ChatDB | str, *, db_config: Optional[DBConfig] = None):
        # Accept ChatDB or a filesystem path.
        if isinstance(db, ChatDB):
            self.db: ChatDB = db
        else:
            cfg = db_config or DBConfig(db_path=str(db))
            self.db = ChatDB(cfg)

        self._schema_checked = False

    async def _ensure_schema(self) -> None:
        """Lightweight migrations for columns that may be missing on existing DBs."""
        if self._schema_checked:
            return

        # NEWS: ensure optional 'name' column exists (older schema lacked it).
        try:
            cols = await self.db.fetchall("PRAGMA table_info(news_sources)")
            colnames = {c["name"] if isinstance(c, dict) else c[1] for c in cols}
            if "name" not in colnames:
                await self.db.execute("ALTER TABLE news_sources ADD COLUMN name TEXT")
        except Exception:
            # If the table doesn't exist yet, ChatDB schema will create it.
            pass

        self._schema_checked = True

    # -----------------
    # ACL
    # -----------------
    async def acl_prune_expired(self, now_ts: Optional[int] = None) -> int:
        await self._ensure_schema()
        now_ts = _now() if now_ts is None else int(now_ts)
        cur = await self.db.execute(
            "DELETE FROM acl_auth WHERE authed_until_ts < ?",
            (now_ts,),
        )
        return int(getattr(cur, "rowcount", 0) or 0)

    async def acl_set_auth(self, identity_key: str, role: str, authed_until_ts: int, authed_ts: Optional[int] = None) -> None:
        await self._ensure_schema()
        authed_ts = _now() if authed_ts is None else int(authed_ts)
        await self.db.execute(
            "INSERT INTO acl_auth(identity_key, role, authed_until_ts, authed_ts) "
            "VALUES(?,?,?,?) "
            "ON CONFLICT(identity_key) DO UPDATE SET role=excluded.role, authed_until_ts=excluded.authed_until_ts, authed_ts=excluded.authed_ts",
            (identity_key.lower().strip(), role, int(authed_until_ts), int(authed_ts)),
        )

    async def acl_get_auth(self, identity_key: str, now_ts: Optional[int] = None) -> Optional[Dict[str, Any]]:
        await self._ensure_schema()
        now_ts = _now() if now_ts is None else int(now_ts)
        row = await self.db.fetchone(
            "SELECT identity_key, role, authed_until_ts, authed_ts "
            "FROM acl_auth WHERE identity_key = ?",
            (identity_key.lower().strip(),),
        )
        if not row:
            return None
        until_ts = int(row["authed_until_ts"])
        if until_ts < now_ts:
            return None
        return dict(row)

    async def acl_clear_auth(self, identity_key: str) -> None:
        await self._ensure_schema()
        await self.db.execute("DELETE FROM acl_auth WHERE identity_key = ?", (identity_key.lower().strip(),))

    # -----------------
    # GREET
    # -----------------
    async def greet_import_from_legacy_file(self, path: str) -> None:
        """
        Legacy importer kept for compatibility.
        If the file doesn't exist, or DB already has rules, no-op.
        """
        await self._ensure_schema()
        if not path or not os.path.exists(path):
            return
        row = await self.db.fetchone("SELECT COUNT(*) AS n FROM greet_rules")
        if row and int(row["n"]) > 0:
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return

        # Expected legacy: dict of rules keyed by id OR list of dicts.
        rules: List[Dict[str, Any]] = []
        if isinstance(data, dict):
            # If it looks like {"rules":[...]} use it; else treat keys as ids
            if "rules" in data and isinstance(data["rules"], list):
                rules = data["rules"]
            else:
                for rid, rv in data.items():
                    if isinstance(rv, dict):
                        d = dict(rv)
                        d.setdefault("id", rid)
                        rules.append(d)
        elif isinstance(data, list):
            rules = [r for r in data if isinstance(r, dict)]

        now_ts = _now()
        for r in rules:
            rid = str(r.get("id") or "").strip()
            if not rid:
                continue
            priority = int(r.get("priority", 0) or 0)
            enabled = 1 if bool(r.get("enabled", True)) else 0
            match_obj = r.get("match") or r.get("match_json") or {}
            greets = r.get("greetings") or r.get("greetings_json") or []
            await self.greet_upsert_rule(rid, priority, enabled, match_obj, greets, now_ts)

    async def greet_upsert_rule(
        self,
        rule_id: str,
        priority: int,
        enabled: int,
        match_obj: Dict[str, Any],
        greetings: List[str],
        updated_ts: Optional[int] = None,
    ) -> None:
        await self._ensure_schema()
        updated_ts = _now() if updated_ts is None else int(updated_ts)
        await self.db.execute(
            "INSERT INTO greet_rules(id, priority, enabled, match_json, greetings_json, updated_ts) "
            "VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "priority=excluded.priority, enabled=excluded.enabled, match_json=excluded.match_json, greetings_json=excluded.greetings_json, updated_ts=excluded.updated_ts",
            (
                str(rule_id),
                int(priority),
                int(enabled),
                json.dumps(match_obj, ensure_ascii=False),
                json.dumps(greetings, ensure_ascii=False),
                int(updated_ts),
            ),
        )

    async def greet_delete_rule(self, rule_id: str) -> None:
        await self._ensure_schema()
        await self.db.execute("DELETE FROM greet_rules WHERE id = ?", (str(rule_id),))

    async def greet_list_rules(self) -> List[Dict[str, Any]]:
        await self._ensure_schema()
        rows = await self.db.fetchall(
            "SELECT id, priority, enabled, match_json, greetings_json, updated_ts "
            "FROM greet_rules ORDER BY enabled DESC, priority DESC, id ASC"
        )
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            try:
                d["match"] = json.loads(d.get("match_json") or "{}")
            except Exception:
                d["match"] = {}
            try:
                d["greetings"] = json.loads(d.get("greetings_json") or "[]")
            except Exception:
                d["greetings"] = []
            out.append(d)
        return out

    async def greet_get_enabled_rules(self) -> List[Dict[str, Any]]:
        await self._ensure_schema()
        rows = await self.db.fetchall(
            "SELECT id, priority, enabled, match_json, greetings_json, updated_ts "
            "FROM greet_rules WHERE enabled = 1 ORDER BY priority DESC, id ASC"
        )
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["match"] = json.loads(d.get("match_json") or "{}")
            d["greetings"] = json.loads(d.get("greetings_json") or "[]")
            out.append(d)
        return out

    # -----------------
    # WIKI
    # -----------------
    async def wiki_import_from_legacy_file(self, path: str) -> None:
        await self._ensure_schema()
        if not path or not os.path.exists(path):
            return
        # If DB already has watches, don't import.
        row = await self.db.fetchone("SELECT COUNT(*) AS n FROM wiki_watch")
        if row and int(row["n"]) > 0:
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                items = json.load(f)
        except Exception:
            return
        if isinstance(items, dict) and "watch" in items:
            items = items["watch"]
        if not isinstance(items, list):
            return
        for t in items:
            if isinstance(t, str) and t.strip():
                await self.wiki_add_watch(t.strip())

    async def wiki_get_lang(self, default: str = "en") -> str:
        await self._ensure_schema()
        row = await self.db.fetchone("SELECT v FROM wiki_settings WHERE k='lang'")
        if not row:
            return default
        return str(row["v"] or default)

    async def wiki_set_lang(self, lang: str) -> None:
        await self._ensure_schema()
        await self.db.execute(
            "INSERT INTO wiki_settings(k,v) VALUES('lang',?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (str(lang).strip(),),
        )

    async def wiki_add_watch(self, title: str) -> None:
        await self._ensure_schema()
        await self.db.execute("INSERT OR IGNORE INTO wiki_watch(title) VALUES(?)", (str(title).strip(),))

    async def wiki_remove_watch(self, title: str) -> None:
        await self._ensure_schema()
        await self.db.execute("DELETE FROM wiki_watch WHERE title = ?", (str(title).strip(),))

    async def wiki_list_watch(self) -> List[str]:
        await self._ensure_schema()
        rows = await self.db.fetchall("SELECT title FROM wiki_watch ORDER BY title COLLATE NOCASE ASC")
        return [str(r["title"]) for r in rows]

    # -----------------
    # WEATHER
    # -----------------
    async def weather_import_from_legacy_file(self, path: str) -> None:
        await self._ensure_schema()
        if not path or not os.path.exists(path):
            return
        row = await self.db.fetchone("SELECT COUNT(*) AS n FROM weather_watches")
        if row and int(row["n"]) > 0:
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return
        # legacy expected: {"watches":[{...}]}
        watches = data.get("watches") if isinstance(data, dict) else None
        if not isinstance(watches, list):
            return
        now_ts = _now()
        for w in watches:
            if not isinstance(w, dict):
                continue
            city = str(w.get("city") or "").strip()
            if not city:
                continue
            duration_hours = int(w.get("duration_hours", 1) or 1)
            types = w.get("types") or w.get("types_json") or ["rain"]
            if not isinstance(types, list):
                types = ["rain"]
            interval = int(w.get("interval_minutes", 15) or 15)
            expires = now_ts + duration_hours * 3600
            await self.weather_add_watch(city, duration_hours, types, interval, now_ts, expires)

    async def weather_add_watch(
        self,
        city: str,
        duration_hours: int,
        types: List[str],
        interval_minutes: int,
        created_ts: Optional[int] = None,
        expires_ts: Optional[int] = None,
    ) -> None:
        await self._ensure_schema()
        created_ts = _now() if created_ts is None else int(created_ts)
        expires_ts = created_ts + int(duration_hours) * 3600 if expires_ts is None else int(expires_ts)
        await self.db.execute(
            "INSERT INTO weather_watches(city, duration_hours, types_json, interval_minutes, created_ts, expires_ts) "
            "VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(city) DO UPDATE SET duration_hours=excluded.duration_hours, types_json=excluded.types_json, "
            "interval_minutes=excluded.interval_minutes, created_ts=excluded.created_ts, expires_ts=excluded.expires_ts",
            (
                str(city).strip(),
                int(duration_hours),
                json.dumps(types, ensure_ascii=False),
                int(interval_minutes),
                int(created_ts),
                int(expires_ts),
            ),
        )

    async def weather_remove_watch(self, city: str) -> None:
        await self._ensure_schema()
        await self.db.execute("DELETE FROM weather_watches WHERE city = ?", (str(city).strip(),))

    async def weather_list_watches(self, now_ts: Optional[int] = None) -> List[Dict[str, Any]]:
        await self._ensure_schema()
        now_ts = _now() if now_ts is None else int(now_ts)
        rows = await self.db.fetchall(
            "SELECT city, duration_hours, types_json, interval_minutes, created_ts, expires_ts "
            "FROM weather_watches WHERE expires_ts >= ? ORDER BY expires_ts ASC",
            (now_ts,),
        )
        out = []
        for r in rows:
            d = dict(r)
            d["types"] = json.loads(d.get("types_json") or "[]")
            out.append(d)
        return out

    async def weather_prune_expired(self, now_ts: Optional[int] = None) -> int:
        await self._ensure_schema()
        now_ts = _now() if now_ts is None else int(now_ts)
        cur = await self.db.execute("DELETE FROM weather_watches WHERE expires_ts < ?", (now_ts,))
        return int(getattr(cur, "rowcount", 0) or 0)

    async def weather_get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        await self._ensure_schema()
        row = await self.db.fetchone("SELECT v FROM weather_settings WHERE k = ?", (str(key).strip(),))
        if not row:
            return default
        return row["v"]

    async def weather_set_setting(self, key: str, value: str) -> None:
        await self._ensure_schema()
        await self.db.execute(
            "INSERT INTO weather_settings(k,v) VALUES(?,?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (str(key).strip(), str(value)),
        )

    # -----------------
    # NEWS
    # -----------------
    async def news_import_from_legacy_config(self, path: str) -> None:
        # Optional legacy import; safe no-op when file missing.
        await self._ensure_schema()
        if not path or not os.path.exists(path):
            return

    async def news_get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        await self._ensure_schema()
        row = await self.db.fetchone("SELECT v FROM news_settings WHERE k = ?", (str(key).strip(),))
        if not row:
            return default
        return row["v"]

    async def news_set_setting(self, key: str, value: str) -> None:
        await self._ensure_schema()
        await self.db.execute(
            "INSERT INTO news_settings(k,v) VALUES(?,?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (str(key).strip(), str(value)),
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

    async def news_list_sources(self) -> List[Dict[str, Any]]:
        await self._ensure_schema()
        # sources + categories
        src_rows = await self.db.fetchall(
            "SELECT id, COALESCE(name, id) AS name, url, enabled, interval_minutes, created_ts, updated_ts "
            "FROM news_sources ORDER BY enabled DESC, id ASC"
        )
        cat_rows = await self.db.fetchall(
            "SELECT source_id, category, url FROM news_source_categories ORDER BY source_id ASC, category ASC"
        )
        cats: Dict[str, Dict[str, str]] = {}
        for r in cat_rows:
            cats.setdefault(r["source_id"], {})[r["category"]] = r["url"]
        out = []
        for r in src_rows:
            d = dict(r)
            d["categories"] = cats.get(d["id"], {})
            out.append(d)
        return out

    async def news_add_source(
        self,
        source_id: str,
        name: str,
        url: str,
        enabled: int,
        interval_minutes: int,
        categories: Optional[Dict[str, str]] = None,
    ) -> None:
        await self._ensure_schema()
        ts = _now()
        await self.db.execute(
            "INSERT INTO news_sources(id, name, url, enabled, interval_minutes, created_ts, updated_ts) "
            "VALUES(?,?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET name=excluded.name, url=excluded.url, enabled=excluded.enabled, "
            "interval_minutes=excluded.interval_minutes, updated_ts=excluded.updated_ts",
            (str(source_id).strip(), str(name).strip(), str(url).strip(), int(enabled), int(interval_minutes), ts, ts),
        )
        if categories:
            for cat, cat_url in categories.items():
                await self.news_set_category_url(source_id, cat, cat_url)

    async def news_remove_source(self, source_id: str) -> None:
        await self._ensure_schema()
        sid = str(source_id).strip()
        await self.db.execute("DELETE FROM news_source_categories WHERE source_id = ?", (sid,))
        await self.db.execute("DELETE FROM news_sources WHERE id = ?", (sid,))

    async def news_set_source_enabled(self, source_id: str, enabled: int) -> None:
        await self._ensure_schema()
        await self.db.execute(
            "UPDATE news_sources SET enabled=?, updated_ts=? WHERE id=?",
            (int(enabled), _now(), str(source_id).strip()),
        )

    async def news_set_source_interval(self, source_id: str, interval_minutes: int) -> None:
        await self._ensure_schema()
        await self.db.execute(
            "UPDATE news_sources SET interval_minutes=?, updated_ts=? WHERE id=?",
            (int(interval_minutes), _now(), str(source_id).strip()),
        )

    async def news_set_source_name(self, source_id: str, name: str) -> None:
        await self._ensure_schema()
        await self.db.execute(
            "UPDATE news_sources SET name=?, updated_ts=? WHERE id=?",
            (str(name).strip(), _now(), str(source_id).strip()),
        )

    async def news_set_source_url(self, source_id: str, url: str) -> None:
        await self._ensure_schema()
        await self.db.execute(
            "UPDATE news_sources SET url=?, updated_ts=? WHERE id=?",
            (str(url).strip(), _now(), str(source_id).strip()),
        )

    async def news_set_category_url(self, source_id: str, category: str, url: str) -> None:
        await self._ensure_schema()
        await self.db.execute(
            "INSERT INTO news_source_categories(source_id, category, url) VALUES(?,?,?) "
            "ON CONFLICT(source_id, category) DO UPDATE SET url=excluded.url",
            (str(source_id).strip(), str(category).strip().lower(), str(url).strip()),
        )

    async def news_remove_category(self, source_id: str, category: str) -> None:
        await self._ensure_schema()
        await self.db.execute(
            "DELETE FROM news_source_categories WHERE source_id=? AND category=?",
            (str(source_id).strip(), str(category).strip().lower()),
        )
