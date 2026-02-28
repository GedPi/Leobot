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
            "INSERT INTO settings(key,value,updated_ts) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_ts=excluded.updated_ts",
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
