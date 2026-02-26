"""
services.chatdb

A small async-friendly SQLite wrapper used by Leonidas/Leobot services.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import re
import sqlite3
import time
from typing import Any, Iterable, List, Optional, Sequence, Tuple, Union

SCHEMA = r"""
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

-- Control-plane (services/channels)
CREATE TABLE IF NOT EXISTS channels (
  channel TEXT PRIMARY KEY,
  created_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS services (
  service TEXT PRIMARY KEY,
  description TEXT DEFAULT '',
  enabled_by_default INTEGER NOT NULL DEFAULT 0,
  created_utc TEXT NOT NULL
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

-- Chat/history
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
  day TEXT NOT NULL,
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

-- greet/wiki/weather/acl
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

-- sysmon
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

-- NEWS (safe schema: NO "limit" column name)
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

def utc_day(ts: Optional[int] = None) -> int:
    import datetime
    if ts is None:
        ts = _now()
    dt = datetime.datetime.utcfromtimestamp(int(ts))
    return dt.year * 10000 + dt.month * 100 + dt.day

_WORD_RE = re.compile(r"[A-Za-z0-9']+")
_URL_RE = re.compile(r"(https?://|www\.)", re.I)

def word_count(text: Optional[str]) -> int:
    if not text:
        return 0
    return len(_WORD_RE.findall(text))

def has_link(text: Optional[str]) -> int:
    if not text:
        return 0
    return 1 if _URL_RE.search(text) else 0


@dataclasses.dataclass(frozen=True)
class DBConfig:
    path: str
    timeout: float = 30.0
    pragmas: Tuple[Tuple[str, Union[str, int]], ...] = (
        ("journal_mode", "WAL"),
        ("synchronous", "NORMAL"),
        ("temp_store", "MEMORY"),
        ("foreign_keys", 1),
    )

    @staticmethod
    def from_any(*, path: Optional[str] = None, db_path: Optional[str] = None, db_file: Optional[str] = None, db: Optional[str] = None) -> "DBConfig":
        p = path or db_path or db_file or db
        if not p:
            raise TypeError("DBConfig requires path/db_path/db_file")
        return DBConfig(path=str(p))


class ChatDB:
    def __init__(self, config: Union[DBConfig, str, os.PathLike[str]]):
        if isinstance(config, DBConfig):
            self.cfg = config
        else:
            self.cfg = DBConfig.from_any(path=str(config))
        self._schema_ready = False
        self._schema_lock = asyncio.Lock()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.cfg.path, timeout=self.cfg.timeout)
        conn.row_factory = sqlite3.Row
        for k, v in self.cfg.pragmas:
            if isinstance(v, str):
                conn.execute(f"PRAGMA {k}={json.dumps(v)}")
            else:
                conn.execute(f"PRAGMA {k}={int(v)}")
        return conn

    def _ensure_schema_sync(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    async def ensure_schema(self) -> None:
        if self._schema_ready:
            return
        async with self._schema_lock:
            if self._schema_ready:
                return
            await asyncio.to_thread(self._ensure_schema_sync)
            self._schema_ready = True

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> int:
        """Execute a statement and return affected rowcount (best-effort)."""
        await self.ensure_schema()

        def _do() -> int:
            conn = self._connect()
            try:
                cur = conn.execute(sql, params)
                conn.commit()
                # sqlite3 rowcount is sometimes -1; treat that as 0
                return int(cur.rowcount) if cur.rowcount is not None and cur.rowcount >= 0 else 0
            finally:
                conn.close()

        return await asyncio.to_thread(_do)

    async def executemany(self, sql: str, seq_params: Iterable[Sequence[Any]]) -> int:
        """Execute many statements and return total affected rows (best-effort)."""
        await self.ensure_schema()
        seq_params = list(seq_params)

        def _do() -> int:
            conn = self._connect()
            try:
                cur = conn.executemany(sql, seq_params)
                conn.commit()
                return int(cur.rowcount) if cur.rowcount is not None and cur.rowcount >= 0 else 0
            finally:
                conn.close()

        return await asyncio.to_thread(_do)

    async def fetchone(self, sql: str, params: Sequence[Any] = ()) -> Optional[sqlite3.Row]:
        await self.ensure_schema()
        def _do():
            conn = self._connect()
            try:
                return conn.execute(sql, params).fetchone()
            finally:
                conn.close()
        return await asyncio.to_thread(_do)

    async def fetchall(self, sql: str, params: Sequence[Any] = ()) -> List[sqlite3.Row]:
        await self.ensure_schema()
        def _do():
            conn = self._connect()
            try:
                return conn.execute(sql, params).fetchall()
            finally:
                conn.close()
        return await asyncio.to_thread(_do)

    async def ensure_channel(self, channel: str) -> None:
        await self.ensure_schema()
        channel = (channel or "").strip()
        if not channel:
            return
        row = await self.fetchone("SELECT name FROM sqlite_master WHERE type='table' AND name='channels'")
        if not row:
            return
        await self.execute("INSERT OR IGNORE INTO channels(channel, created_ts) VALUES (?, ?)", (channel, _now()))

    async def ensure_service(self, service_name: str) -> None:
        await self.ensure_schema()
        service_name = (service_name or "").strip()
        if not service_name:
            return
        row = await self.fetchone("SELECT name FROM sqlite_master WHERE type='table' AND name='services'")
        if not row:
            return
        await self.execute("INSERT OR IGNORE INTO services(name, created_ts) VALUES (?, ?)", (service_name, _now()))
