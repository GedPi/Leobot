import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from services.chatdb import ChatDB, DBConfig


def _ts() -> int:
    return int(time.time())


@dataclass
class NewsLastPosted:
    last_guid: str
    last_ts: int


class Store:
    """
    Single, standardised DB access layer for services.
    Accepts either:
      - a ChatDB instance, or
      - a db path (str/pathlike)
    """

    def __init__(self, db: ChatDB | str):
        if isinstance(db, ChatDB):
            self.db = db
        else:
            self.db = ChatDB(DBConfig(path=str(db)))

    # ----------------------------
    # Greet
    # ----------------------------
    async def greet_get_rules(self) -> list[dict]:
        rows = await self.db.fetchall(
            "SELECT id, priority, enabled, match_json, greetings_json, updated_ts "
            "FROM greet_rules ORDER BY priority DESC, id ASC"
        )
        out = []
        for r in rows:
            out.append(
                {
                    "id": r[0],
                    "priority": int(r[1]),
                    "enabled": bool(r[2]),
                    "match": json.loads(r[3]),
                    "greetings": json.loads(r[4]),
                    "updated_ts": int(r[5]),
                }
            )
        return out

    async def greet_upsert_rule(self, rule_id: str, priority: int, enabled: bool, match: dict, greetings: list[str]) -> None:
        await self.db.execute(
            "INSERT INTO greet_rules(id, priority, enabled, match_json, greetings_json, updated_ts) "
            "VALUES(?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "priority=excluded.priority, enabled=excluded.enabled, match_json=excluded.match_json, "
            "greetings_json=excluded.greetings_json, updated_ts=excluded.updated_ts",
            (
                rule_id,
                int(priority),
                1 if enabled else 0,
                json.dumps(match, separators=(",", ":")),
                json.dumps(greetings, separators=(",", ":"), ensure_ascii=False),
                _ts(),
            ),
        )

    # ----------------------------
    # Wiki
    # ----------------------------
    async def wiki_add_watch(self, title: str, lang: str) -> None:
        await self.db.execute(
            "INSERT OR REPLACE INTO wiki_watch(title, lang, created_ts) VALUES(?, ?, ?)",
            (title, (lang or "en"), _ts()),
        )

    async def wiki_list_watch(self) -> list[tuple[str, str]]:
        rows = await self.db.fetchall("SELECT title, lang FROM wiki_watch ORDER BY title")
        return [(r[0], r[1]) for r in rows]

    async def wiki_set_setting(self, k: str, v: str) -> None:
        await self.db.execute(
            "INSERT INTO wiki_settings(k, v, updated_ts) VALUES(?, ?, ?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_ts=excluded.updated_ts",
            (k, v, _ts()),
        )

    async def wiki_get_setting(self, k: str, default: str = "") -> str:
        row = await self.db.fetchone("SELECT v FROM wiki_settings WHERE k=?", (k,))
        return row[0] if row else default

    # ----------------------------
    # Weather
    # ----------------------------
    async def weather_upsert_watch(self, city: str, duration_hours: int, types: list[str], interval_minutes: int) -> None:
        created = _ts()
        expires = created + int(duration_hours) * 3600
        await self.db.execute(
            "INSERT INTO weather_watches(city, duration_hours, types_json, interval_minutes, created_ts, expires_ts) "
            "VALUES(?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(city) DO UPDATE SET "
            "duration_hours=excluded.duration_hours, types_json=excluded.types_json, "
            "interval_minutes=excluded.interval_minutes, created_ts=excluded.created_ts, expires_ts=excluded.expires_ts",
            (city, int(duration_hours), json.dumps(types), int(interval_minutes), created, expires),
        )

    async def weather_list_watches(self) -> list[dict]:
        rows = await self.db.fetchall(
            "SELECT city, duration_hours, types_json, interval_minutes, created_ts, expires_ts "
            "FROM weather_watches ORDER BY city"
        )
        out = []
        for r in rows:
            out.append(
                {
                    "city": r[0],
                    "duration_hours": int(r[1]),
                    "types": json.loads(r[2]),
                    "interval_minutes": int(r[3]),
                    "created_ts": int(r[4]),
                    "expires_ts": int(r[5]),
                }
            )
        return out

    async def weather_prune_expired(self) -> int:
        now = _ts()
        # SQLite change count isn’t super reliable via python; do it as two steps:
        rows = await self.db.fetchall("SELECT city FROM weather_watches WHERE expires_ts <= ?", (now,))
        if not rows:
            return 0
        await self.db.execute("DELETE FROM weather_watches WHERE expires_ts <= ?", (now,))
        return len(rows)

    # ----------------------------
    # ACL (auth cache)
    # ----------------------------
    async def acl_set_auth(self, identity_key: str, role: str, authed_until_ts: int) -> None:
        await self.db.execute(
            "INSERT INTO acl_auth(identity_key, role, authed_until_ts, authed_ts) VALUES(?, ?, ?, ?) "
            "ON CONFLICT(identity_key) DO UPDATE SET role=excluded.role, authed_until_ts=excluded.authed_until_ts, authed_ts=excluded.authed_ts",
            ((identity_key or "").strip().lower(), role, int(authed_until_ts), _ts()),
        )

    async def acl_get_auth(self, identity_key: str) -> Optional[tuple[str, int]]:
        row = await self.db.fetchone(
            "SELECT role, authed_until_ts FROM acl_auth WHERE identity_key=?",
            ((identity_key or "").strip().lower(),),
        )
        if not row:
            return None
        return (row[0], int(row[1]))

    async def acl_prune_expired(self) -> int:
        now = _ts()
        rows = await self.db.fetchall("SELECT identity_key FROM acl_auth WHERE authed_until_ts <= ?", (now,))
        if not rows:
            return 0
        await self.db.execute("DELETE FROM acl_auth WHERE authed_until_ts <= ?", (now,))
        return len(rows)

    # ----------------------------
    # NEWS
    # ----------------------------
    async def news_set_setting(self, k: str, v: str) -> None:
        await self.db.execute(
            "INSERT INTO news_settings(k, v, updated_ts) VALUES(?, ?, ?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_ts=excluded.updated_ts",
            (k, v, _ts()),
        )

    async def news_get_setting(self, k: str, default: str = "") -> str:
        row = await self.db.fetchone("SELECT v FROM news_settings WHERE k=?", (k,))
        return row[0] if row else default

    async def news_upsert_source(self, source_id: str, url: str, enabled: bool = True, interval_minutes: int = 60) -> None:
        now = _ts()
        sid = (source_id or "").strip().lower()
        await self.db.execute(
            "INSERT INTO news_sources(id, url, enabled, interval_minutes, created_ts, updated_ts) VALUES(?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET url=excluded.url, enabled=excluded.enabled, interval_minutes=excluded.interval_minutes, updated_ts=excluded.updated_ts",
            (sid, url, 1 if enabled else 0, int(interval_minutes), now, now),
        )

    async def news_list_sources(self, enabled_only: bool = False) -> list[dict]:
        if enabled_only:
            rows = await self.db.fetchall("SELECT id, url, enabled, interval_minutes FROM news_sources WHERE enabled=1 ORDER BY id")
        else:
            rows = await self.db.fetchall("SELECT id, url, enabled, interval_minutes FROM news_sources ORDER BY id")
        return [{"id": r[0], "url": r[1], "enabled": bool(r[2]), "interval_minutes": int(r[3])} for r in rows]

    async def news_set_categories(self, source_id: str, categories: list[str]) -> None:
        sid = (source_id or "").strip().lower()
        cats = sorted({(c or "").strip().lower() for c in categories if (c or "").strip()})
        await self.db.execute("DELETE FROM news_source_categories WHERE source_id=?", (sid,))
        if cats:
            await self.db.executemany(
                "INSERT INTO news_source_categories(source_id, category) VALUES(?, ?)",
                [(sid, c) for c in cats],
            )

    async def news_get_categories(self, source_id: str) -> list[str]:
        sid = (source_id or "").strip().lower()
        rows = await self.db.fetchall("SELECT category FROM news_source_categories WHERE source_id=? ORDER BY category", (sid,))
        return [r[0] for r in rows]

    async def news_get_last_posted(self, target: str, source_id: str, category: str, limit_n: int) -> NewsLastPosted:
        # IMPORTANT: this uses limit_n, not "limit"
        row = await self.db.fetchone(
            "SELECT last_guid, last_ts FROM news_last_posted WHERE target=? AND source_id=? AND category=? AND limit_n=?",
            ((target or "").strip(), (source_id or "").strip().lower(), (category or "").strip().lower(), int(limit_n)),
        )
        if not row:
            return NewsLastPosted(last_guid="", last_ts=0)
        return NewsLastPosted(last_guid=row[0] or "", last_ts=int(row[1] or 0))

    async def news_set_last_posted(self, target: str, source_id: str, category: str, limit_n: int, last_guid: str, last_ts: int) -> None:
        await self.db.execute(
            "INSERT INTO news_last_posted(target, source_id, category, limit_n, last_guid, last_ts) VALUES(?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(target, source_id, category, limit_n) DO UPDATE SET last_guid=excluded.last_guid, last_ts=excluded.last_ts",
            ((target or "").strip(), (source_id or "").strip().lower(), (category or "").strip().lower(), int(limit_n), last_guid or "", int(last_ts)),
        )