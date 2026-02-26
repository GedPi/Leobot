"""
services.chatdb

Async-friendly SQLite wrapper + schema for Leobot.

Design goals:
- Single persistent connection (WAL) with serialized access.
- Foreign keys ALWAYS ON (per connection).
- Schema includes control-plane (services/channels toggles) + service data tables.
- Control-plane API used by bot.py:
    - ensure_schema()
    - ensure_channel()
    - ensure_service()
    - set_service_channel_enabled()
    - is_service_enabled()
    - is_service_enabled_any()
    - list_service_status_for_channel()
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

# -----------------------------
# Schema
# -----------------------------

SCHEMA = r"""
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;
PRAGMA foreign_keys=ON;

-- Control-plane
CREATE TABLE IF NOT EXISTS channels (
  channel TEXT PRIMARY KEY,
  created_utc TEXT NOT NULL,
  created_ts INTEGER
);

CREATE TABLE IF NOT EXISTS services (
  service TEXT PRIMARY KEY,
  description TEXT DEFAULT '',
  enabled_by_default INTEGER NOT NULL DEFAULT 0,
  created_utc TEXT NOT NULL,
  created_ts INTEGER
);

CREATE TABLE IF NOT EXISTS service_channel (
  service TEXT NOT NULL,
  channel TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  updated_utc TEXT NOT NULL,
  updated_by TEXT DEFAULT '',
  PRIMARY KEY (service, channel),
  FOREIGN KEY (service) REFERENCES services(service) ON DELETE CASCADE,
  FOREIGN KEY (channel) REFERENCES channels(channel) ON DELETE CASCADE
);

-- Chat/history (used by lastseen/stats)
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,
  channel TEXT NOT NULL,
  nick TEXT NOT NULL,
  is_action INTEGER NOT NULL DEFAULT 0,
  has_link INTEGER NOT NULL DEFAULT 0,
  text TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_chan_ts ON messages(channel, ts);
CREATE INDEX IF NOT EXISTS idx_messages_nick_ts ON messages(nick, ts);

CREATE TABLE IF NOT EXISTS seen (
  nick TEXT PRIMARY KEY,
  ts INTEGER NOT NULL,
  event TEXT NOT NULL,
  channel TEXT,
  last_msg TEXT
);

CREATE TABLE IF NOT EXISTS stats_daily (
  day TEXT NOT NULL,        -- YYYY-MM-DD (UTC)
  channel TEXT NOT NULL,
  nick TEXT NOT NULL,
  msgs INTEGER NOT NULL DEFAULT 0,
  words INTEGER NOT NULL DEFAULT 0,
  links INTEGER NOT NULL DEFAULT 0,
  actions INTEGER NOT NULL DEFAULT 0,
  joins INTEGER NOT NULL DEFAULT 0,
  parts INTEGER NOT NULL DEFAULT 0,
  quits INTEGER NOT NULL DEFAULT 0,
  kicks INTEGER NOT NULL DEFAULT 0,
  nickchanges INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(day, channel, nick)
);

-- Full channel logging (human-readable stream)
CREATE TABLE IF NOT EXISTS channel_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,               -- epoch seconds
  channel TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT '',     -- ~ & @ % + (or '')
  nick TEXT NOT NULL,
  message TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_channel_log_chan_ts ON channel_log(channel, ts);
CREATE INDEX IF NOT EXISTS idx_channel_log_nick_ts ON channel_log(nick, ts);

-- greet/wiki/weather/acl/news/sysmon tables (kept for compatibility)
CREATE TABLE IF NOT EXISTS greet_rules (
  id TEXT PRIMARY KEY,
  priority INTEGER NOT NULL DEFAULT 0,
  enabled INTEGER NOT NULL DEFAULT 1,
  match_json TEXT NOT NULL,
  greetings_json TEXT NOT NULL,
  updated_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_greet_rules_enabled_pri ON greet_rules(enabled, priority);

CREATE TABLE IF NOT EXISTS wiki_watch (
  title TEXT PRIMARY KEY,
  lang TEXT NOT NULL DEFAULT 'en',
  created_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS wiki_settings (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL,
  updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS weather_watches (
  city TEXT PRIMARY KEY,
  duration_hours INTEGER NOT NULL,
  types_json TEXT NOT NULL,
  interval_minutes INTEGER NOT NULL,
  created_ts INTEGER NOT NULL,
  expires_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_weather_watches_expires ON weather_watches(expires_ts);

CREATE TABLE IF NOT EXISTS weather_settings (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL,
  updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS acl_auth (
  identity_key TEXT PRIMARY KEY,
  role TEXT NOT NULL,
  authed_until_ts INTEGER NOT NULL,
  authed_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_acl_auth_until ON acl_auth(authed_until_ts);

CREATE TABLE IF NOT EXISTS sys_health_snapshots (
  ts INTEGER PRIMARY KEY,
  payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sys_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,
  message TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sys_events_ts ON sys_events(ts);

CREATE TABLE IF NOT EXISTS sys_state (
  k TEXT PRIMARY KEY,
  v_json TEXT NOT NULL,
  updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS news_settings (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL,
  updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS news_sources (
  id TEXT PRIMARY KEY,
  url TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  interval_minutes INTEGER NOT NULL DEFAULT 60,
  created_ts INTEGER NOT NULL,
  updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS news_source_categories (
  source_id TEXT NOT NULL,
  category TEXT NOT NULL,
  PRIMARY KEY (source_id, category),
  FOREIGN KEY (source_id) REFERENCES news_sources(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS news_last_posted (
  target TEXT NOT NULL,
  source_id TEXT NOT NULL,
  category TEXT NOT NULL,
  limit_n INTEGER NOT NULL DEFAULT 10,
  last_guid TEXT DEFAULT '',
  last_ts INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (target, source_id, category, limit_n),
  FOREIGN KEY (source_id) REFERENCES news_sources(id) ON DELETE CASCADE
);
"""


def _now() -> int:
    return int(time.time())


@dataclass(frozen=True)
class DBConfig:
    db_path: str
    timeout: float = 30.0


class ChatDB:
    def __init__(self, cfg: DBConfig):
        self.cfg = cfg
        self._path = Path(cfg.db_path)
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = asyncio.Lock()
        self._schema_ready = False

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(
                str(self._path),
                timeout=float(self.cfg.timeout),
                isolation_level=None,      # autocommit
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            # Foreign keys are per-connection. This must be set every time.
            conn.execute("PRAGMA foreign_keys=ON;")
            self._conn = conn
        return self._conn

    async def ensure_schema(self) -> None:
        async with self._lock:
            if self._schema_ready:
                return
            conn = self._connect()
            conn.executescript(SCHEMA)
            # Make sure the connection still has FK ON after schema script.
            conn.execute("PRAGMA foreign_keys=ON;")
            self._schema_ready = True

    # -------------
    # Low-level IO
    # -------------

    async def execute(self, sql: str, params: Iterable[Any] = ()) -> None:
        await self.ensure_schema()
        async with self._lock:
            conn = self._connect()
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute(sql, tuple(params))

    async def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        await self.ensure_schema()
        async with self._lock:
            conn = self._connect()
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.executemany(sql, rows)

    async def fetchone(self, sql: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
        await self.ensure_schema()
        async with self._lock:
            conn = self._connect()
            conn.execute("PRAGMA foreign_keys=ON;")
            cur = conn.execute(sql, tuple(params))
            return cur.fetchone()

    async def fetchall(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        await self.ensure_schema()
        async with self._lock:
            conn = self._connect()
            conn.execute("PRAGMA foreign_keys=ON;")
            cur = conn.execute(sql, tuple(params))
            return cur.fetchall()

    async def close(self) -> None:
        async with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                finally:
                    self._conn = None
                    self._schema_ready = False

    # -----------------------------
    # Control-plane helpers
    # -----------------------------

    async def ensure_channel(self, channel: str) -> None:
        await self.ensure_schema()
        channel = (channel or "").strip()
        if not channel:
            return
        # created_utc is NOT NULL in your DB, so always satisfy it.
        await self.execute(
            "INSERT OR IGNORE INTO channels(channel, created_utc, created_ts) VALUES (?, datetime('now'), ?)",
            (channel, _now()),
        )

    async def ensure_service(self, service: str) -> None:
        await self.ensure_schema()
        service = (service or "").strip().lower()
        if not service:
            return
        # created_utc is NOT NULL in your DB, so always satisfy it.
        await self.execute(
            "INSERT OR IGNORE INTO services(service, created_utc, created_ts) VALUES (?, datetime('now'), ?)",
            (service, _now()),
        )

    async def set_service_channel_enabled(self, service: str, channel: str, enabled: bool, updated_by: str = "") -> None:
        await self.ensure_schema()
        service = (service or "").strip().lower()
        channel = (channel or "").strip()
        if not service or not channel:
            return

        # Hard guarantee FK targets exist (this is what prevents your FK failures).
        await self.ensure_service(service)
        await self.ensure_channel(channel)

        await self.execute(
            """
            INSERT INTO service_channel(service, channel, enabled, updated_utc, updated_by)
            VALUES (?, ?, ?, datetime('now'), ?)
            ON CONFLICT(service, channel) DO UPDATE SET
                enabled=excluded.enabled,
                updated_utc=datetime('now'),
                updated_by=excluded.updated_by
            """,
            (service, channel, 1 if enabled else 0, (updated_by or "")),
        )

    async def is_service_enabled(self, service: str, channel: str) -> bool:
        await self.ensure_schema()
        service = (service or "").strip().lower()
        channel = (channel or "").strip()
        if not service or not channel:
            return False

        row = await self.fetchone(
            "SELECT enabled FROM service_channel WHERE service=? AND channel=?",
            (service, channel),
        )
        if row is not None:
            return bool(int(row["enabled"]) == 1)

        # fallback to enabled_by_default
        row2 = await self.fetchone(
            "SELECT enabled_by_default FROM services WHERE service=?",
            (service,),
        )
        return bool(row2 is not None and int(row2["enabled_by_default"]) == 1)

    async def is_service_enabled_any(self, service: str) -> bool:
        await self.ensure_schema()
        service = (service or "").strip().lower()
        if not service:
            return False

        row = await self.fetchone(
            "SELECT 1 FROM service_channel WHERE service=? AND enabled=1 LIMIT 1",
            (service,),
        )
        if row is not None:
            return True

        row2 = await self.fetchone(
            "SELECT enabled_by_default FROM services WHERE service=?",
            (service,),
        )
        return bool(row2 is not None and int(row2["enabled_by_default"]) == 1)

    async def list_service_status_for_channel(self, channel: str) -> list[tuple[str, bool]]:
        await self.ensure_schema()
        channel = (channel or "").strip()
        if not channel:
            return []

        rows = await self.fetchall(
            """
            SELECT s.service AS service,
                   COALESCE(sc.enabled, s.enabled_by_default, 0) AS enabled
            FROM services s
            LEFT JOIN service_channel sc
              ON sc.service = s.service AND sc.channel = ?
            ORDER BY s.service ASC
            """,
            (channel,),
        )
        return [(str(r["service"]), bool(int(r["enabled"]))) for r in rows]