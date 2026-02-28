from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable

from system.migrations import apply_migrations

log = logging.getLogger("leobot.store")


class Store:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            isolation_level=None,  # autocommit; we manage transactions explicitly when needed
        )
        self._conn.row_factory = sqlite3.Row
        self._lock = asyncio.Lock()

        # sane pragmas
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=3000")

        apply_migrations(self._conn)

    async def close(self) -> None:
        async with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass

    async def execute(self, sql: str, params: Iterable[Any] = ()) -> None:
        async with self._lock:
            self._conn.execute(sql, tuple(params))

    async def executemany(self, sql: str, seq: Iterable[Iterable[Any]]) -> None:
        async with self._lock:
            self._conn.executemany(sql, [tuple(x) for x in seq])

    async def fetchone(self, sql: str, params: Iterable[Any] = ()):
        async with self._lock:
            cur = self._conn.execute(sql, tuple(params))
            return cur.fetchone()

    async def fetchall(self, sql: str, params: Iterable[Any] = ()):
        async with self._lock:
            cur = self._conn.execute(sql, tuple(params))
            return cur.fetchall()

    # ---- settings ----
    async def get_setting(self, key: str, default: str | None = None) -> str | None:
        row = await self.fetchone("SELECT value FROM settings WHERE key=?", (key,))
        return row[0] if row else default

    async def set_setting(self, key: str, value: str) -> None:
        now = int(time.time())
        await self.execute(
            "INSERT INTO settings(key,value,updated_ts) VALUES(?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_ts=excluded.updated_ts",
            (key, value, now),
        )

    # ---- service enablement ----
    async def is_service_enabled(self, channel: str, service: str) -> bool:
        row = await self.fetchone(
            "SELECT enabled FROM service_enablement WHERE channel=? AND service=?",
            (channel, service),
        )
        return bool(row[0]) if row else False

    async def set_service_enabled(self, channel: str, service: str, enabled: bool, updated_by: str | None = None) -> None:
        now = int(time.time())
        await self.execute(
            "INSERT INTO service_enablement(channel,service,enabled,updated_ts,updated_by) VALUES(?,?,?,?,?) "
            "ON CONFLICT(channel,service) DO UPDATE SET enabled=excluded.enabled, updated_ts=excluded.updated_ts, updated_by=excluded.updated_by",
            (channel, service, 1 if enabled else 0, now, updated_by),
        )

    async def list_service_enablement(self, channel: str) -> list[tuple[str, bool]]:
        rows = await self.fetchall(
            "SELECT service, enabled FROM service_enablement WHERE channel=? ORDER BY service",
            (channel,),
        )
        return [(str(r[0]), bool(r[1])) for r in rows]

    # ---- ACL sessions ----
    async def get_acl_session(self, identity_key: str):
        return await self.fetchone(
            "SELECT role, auth_until_ts FROM acl_sessions WHERE identity_key=?",
            (identity_key,),
        )

    async def set_acl_session(self, identity_key: str, role: str, auth_until_ts: int) -> None:
        now = int(time.time())
        await self.execute(
            "INSERT INTO acl_sessions(identity_key,role,auth_until_ts,created_ts,updated_ts) VALUES(?,?,?,?,?) "
            "ON CONFLICT(identity_key) DO UPDATE SET role=excluded.role, auth_until_ts=excluded.auth_until_ts, updated_ts=excluded.updated_ts",
            (identity_key, role, int(auth_until_ts), now, now),
        )

    async def prune_acl_sessions(self, now_ts: int | None = None) -> int:
        now = int(now_ts or time.time())
        async with self._lock:
            cur = self._conn.execute("DELETE FROM acl_sessions WHERE auth_until_ts < ?", (now,))
            return cur.rowcount

    # ---- Greet selection ----
    async def greet_select_target(self, *, nick: str, hostmask: str, userhost: str, host: str, channel: str) -> sqlite3.Row | None:
        # Fetch enabled targets ordered by priority desc, id asc, then filter in python for AND semantics.
        rows = await self.fetchall(
            "SELECT * FROM greet_targets WHERE enabled=1 AND (channel IS NULL OR channel=?) ORDER BY priority DESC, id ASC",
            (channel,),
        )
        n_l = (nick or "").strip().lower()
        for r in rows:
            if r["match_nick"]:
                if r["match_nick"].strip().lower() != n_l:
                    continue
            if r["match_hostmask"]:
                import fnmatch
                if not fnmatch.fnmatch(hostmask or "", r["match_hostmask"].strip()):
                    continue
            if r["match_userhost"]:
                import fnmatch
                if not fnmatch.fnmatch(userhost or "", r["match_userhost"].strip()):
                    continue
            if r["match_host"]:
                import fnmatch
                if not fnmatch.fnmatch(host or "", r["match_host"].strip()):
                    continue
            return r
        return None

    async def greet_pick_greeting(self, target_id: int) -> str | None:
        row = await self.fetchone(
            "SELECT text FROM greetings WHERE target_id=? AND enabled=1 ORDER BY RANDOM() LIMIT 1",
            (int(target_id),),
        )
        return str(row[0]) if row else None

    # ---- News persistence ----
    async def news_list_sources(self) -> list[sqlite3.Row]:
        return await self.fetchall("SELECT id, name, enabled FROM news_sources ORDER BY name")

    async def news_get_source(self, source_id: str):
        return await self.fetchone("SELECT id, name, enabled FROM news_sources WHERE id=?", (source_id,))

    async def news_upsert_source(self, source_id: str, name: str, enabled: bool = True) -> None:
        now = int(time.time())
        await self.execute(
            "INSERT INTO news_sources(id,name,enabled,created_ts,updated_ts) VALUES(?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET name=excluded.name, enabled=excluded.enabled, updated_ts=excluded.updated_ts",
            (source_id, name, 1 if enabled else 0, now, now),
        )

    async def news_set_source_enabled(self, source_id: str, enabled: bool) -> None:
        now = int(time.time())
        await self.execute(
            "UPDATE news_sources SET enabled=?, updated_ts=? WHERE id=?",
            (1 if enabled else 0, now, source_id),
        )

    async def news_set_category(self, source_id: str, category: str, url: str) -> None:
        await self.execute(
            "INSERT OR REPLACE INTO news_source_categories(source_id,category,url) VALUES(?,?,?)",
            (source_id, category, url),
        )

    async def news_list_categories(self, source_id: str) -> list[sqlite3.Row]:
        return await self.fetchall(
            "SELECT category, url FROM news_source_categories WHERE source_id=? ORDER BY category",
            (source_id,),
        )

    async def news_get_last_posted(self, channel: str, source_id: str, category: str, limit_n: int) -> int | None:
        row = await self.fetchone(
            "SELECT last_posted_ts FROM news_posted WHERE channel=? AND source_id=? AND category=? AND limit_n=?",
            (channel, source_id, category, int(limit_n)),
        )
        return int(row[0]) if row else None

    async def news_set_last_posted(self, channel: str, source_id: str, category: str, limit_n: int, ts: int | None = None) -> None:
        t = int(ts or time.time())
        await self.execute(
            "INSERT INTO news_posted(channel,source_id,category,limit_n,last_posted_ts) VALUES(?,?,?,?,?) "
            "ON CONFLICT(channel,source_id,category,limit_n) DO UPDATE SET last_posted_ts=excluded.last_posted_ts",
            (channel, source_id, category, int(limit_n), t),
        )

    # ---------------------------------------------------------------------
    # Weather persistence (v2)
    # ---------------------------------------------------------------------

    @staticmethod
    def _norm_loc_query(q: str) -> str:
        return " ".join((q or "").strip().split()).lower()

    # ---- Weather location cache ----
    async def weather_location_get(self, query: str) -> sqlite3.Row | None:
        qn = self._norm_loc_query(query)
        return await self.fetchone(
            "SELECT query,name,country,country_code,lat,lon FROM weather_locations WHERE query=?",
            (qn,),
        )

    async def weather_location_upsert(
        self,
        *,
        query: str,
        name: str,
        lat: float,
        lon: float,
        country: str | None = None,
        country_code: str | None = None,
    ) -> None:
        qn = self._norm_loc_query(query)
        now = int(time.time())
        await self.execute(
            "INSERT INTO weather_locations(query,name,country,country_code,lat,lon,created_ts,updated_ts) "
            "VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(query) DO UPDATE SET "
            "name=excluded.name, country=excluded.country, country_code=excluded.country_code, "
            "lat=excluded.lat, lon=excluded.lon, updated_ts=excluded.updated_ts",
            (qn, name, country, country_code, float(lat), float(lon), now, now),
        )

    # ---- Weather watches ----
    async def weather_watch_add(
        self,
        *,
        target_channel: str,
        location_query: str,
        location_name: str,
        lat: float | None,
        lon: float | None,
        types_csv: str,
        duration_seconds: int,
        interval_seconds: int = 900,
        created_by: str | None = None,
        enabled: bool = True,
        now_ts: int | None = None,
    ) -> int:
        now = int(now_ts or time.time())
        expires_ts = now + int(duration_seconds)
        next_check_ts = now  # poller will run immediately on next tick
        async with self._lock:
            cur = self._conn.execute(
                "INSERT INTO weather_watches("
                "target_channel,location_query,location_name,country,country_code,lat,lon,"
                "types,interval_seconds,next_check_ts,expires_ts,created_ts,created_by,enabled"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    str(target_channel),
                    str(location_query),
                    str(location_name),
                    None,
                    None,
                    lat if lat is None else float(lat),
                    lon if lon is None else float(lon),
                    str(types_csv),
                    int(interval_seconds),
                    int(next_check_ts),
                    int(expires_ts),
                    int(now),
                    created_by,
                    1 if enabled else 0,
                ),
            )
            return int(cur.lastrowid)

    async def weather_watch_list(self, *, target_channel: str) -> list[sqlite3.Row]:
        return await self.fetchall(
            "SELECT id,target_channel,location_name,location_query,types,interval_seconds,next_check_ts,expires_ts,created_ts,created_by,enabled "
            "FROM weather_watches WHERE target_channel=? ORDER BY id",
            (str(target_channel),),
        )

    async def weather_watch_delete(self, *, target_channel: str, watch_id: int) -> int:
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM weather_watches WHERE target_channel=? AND id=?",
                (str(target_channel), int(watch_id)),
            )
            # also cascades alert state
            return int(cur.rowcount)

    async def weather_watch_clear(self, *, target_channel: str) -> int:
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM weather_watches WHERE target_channel=?",
                (str(target_channel),),
            )
            return int(cur.rowcount)

    async def weather_watch_due(self, *, now_ts: int | None = None, limit: int = 50) -> list[sqlite3.Row]:
        now = int(now_ts or time.time())
        return await self.fetchall(
            "SELECT * FROM weather_watches "
            "WHERE enabled=1 AND expires_ts > ? AND next_check_ts <= ? "
            "ORDER BY next_check_ts ASC LIMIT ?",
            (now, now, int(limit)),
        )

    async def weather_watch_mark_checked(self, *, watch_id: int, next_check_ts: int) -> None:
        await self.execute(
            "UPDATE weather_watches SET next_check_ts=? WHERE id=?",
            (int(next_check_ts), int(watch_id)),
        )

    async def weather_watch_prune_expired(self, *, now_ts: int | None = None) -> int:
        now = int(now_ts or time.time())
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM weather_watches WHERE expires_ts <= ? OR enabled=0",
                (now,),
            )
            return int(cur.rowcount)

    # ---- Weather alert state ----
    async def weather_alert_get(self, watch_id: int) -> sqlite3.Row | None:
        return await self.fetchone(
            "SELECT last_alert_ts,last_fingerprint FROM weather_alert_state WHERE watch_id=?",
            (int(watch_id),),
        )

    async def weather_alert_set(self, *, watch_id: int, last_alert_ts: int, last_fingerprint: str) -> None:
        await self.execute(
            "INSERT INTO weather_alert_state(watch_id,last_alert_ts,last_fingerprint) VALUES(?,?,?) "
            "ON CONFLICT(watch_id) DO UPDATE SET "
            "last_alert_ts=excluded.last_alert_ts, last_fingerprint=excluded.last_fingerprint",
            (int(watch_id), int(last_alert_ts), str(last_fingerprint)),
        )