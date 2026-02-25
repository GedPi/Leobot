import asyncio
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


SCHEMA = """
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

CREATE INDEX IF NOT EXISTS idx_service_channel_chan ON service_channel(channel, enabled);

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


@dataclass
class DBConfig:
    path: str


_LINK_RE = re.compile(r"(https?://|www\.)\S+", re.IGNORECASE)


def utc_day(ts: int) -> str:
    """Convert unix epoch seconds -> YYYY-MM-DD in UTC."""
    try:
        return time.strftime("%Y-%m-%d", time.gmtime(int(ts)))
    except Exception:
        return "1970-01-01"


def word_count(text: str) -> int:
    """Approximate word count for stats."""
    if not text:
        return 0
    return len([w for w in str(text).strip().split() if w])


def has_link(text: str) -> bool:
    """Detect if a message likely contains a URL."""
    if not text:
        return False
    return _LINK_RE.search(str(text)) is not None


class ChatDB:
    def __init__(self, cfg: DBConfig):
        self.path = Path(cfg.path)
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(
                str(self.path),
                timeout=30,
                isolation_level=None,
                check_same_thread=False,
            )
            self._conn.executescript(SCHEMA)
        return self._conn

    @staticmethod
    def _utc_now() -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    async def execute(self, sql: str, args: Iterable[Any] = ()) -> None:
        async with self._lock:
            self._connect().execute(sql, tuple(args))

    async def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        async with self._lock:
            self._connect().executemany(sql, rows)

    async def fetchone(self, sql: str, args: Iterable[Any] = ()) -> tuple[Any, ...] | None:
        async with self._lock:
            cur = self._connect().execute(sql, tuple(args))
            return cur.fetchone()

    async def fetchall(self, sql: str, args: Iterable[Any] = ()) -> list[tuple[Any, ...]]:
        async with self._lock:
            cur = self._connect().execute(sql, tuple(args))
            return cur.fetchall()

    # -----------------------------
    # Control-plane helpers used by bot.py
    # -----------------------------

    async def ensure_channel(self, channel: str) -> None:
        ch = (channel or "").strip()
        if not ch:
            return
        await self.execute(
            "INSERT OR IGNORE INTO channels(channel, created_utc) VALUES(?, ?)",
            (ch, self._utc_now()),
        )

    async def ensure_service(self, service: str, description: str = "", enabled_by_default: int = 0) -> None:
        s = (service or "").strip().lower()
        if not s:
            return
        await self.execute(
            "INSERT OR IGNORE INTO services(service, description, enabled_by_default, created_utc) VALUES(?, ?, ?, ?)",
            (s, (description or "").strip(), int(bool(enabled_by_default)), self._utc_now()),
        )

    async def set_service_channel_enabled(self, service: str, channel: str, enabled: bool, updated_by: str = "") -> None:
        s = (service or "").strip().lower()
        ch = (channel or "").strip()
        if not s or not ch:
            return
        await self.ensure_service(s)
        await self.ensure_channel(ch)
        await self.execute(
            "INSERT INTO service_channel(service, channel, enabled, updated_utc, updated_by) "
            "VALUES(?, ?, ?, ?, ?) "
            "ON CONFLICT(service, channel) DO UPDATE SET "
            "enabled=excluded.enabled, updated_utc=excluded.updated_utc, updated_by=excluded.updated_by",
            (s, ch, 1 if enabled else 0, self._utc_now(), (updated_by or "")[:128]),
        )

    async def is_service_enabled(self, service: str, channel: str) -> bool:
        s = (service or "").strip().lower()
        ch = (channel or "").strip()
        if not s or not ch:
            return True
        row = await self.fetchone(
            "SELECT COALESCE(sc.enabled, s.enabled_by_default) "
            "FROM services s "
            "LEFT JOIN service_channel sc ON sc.service=s.service AND sc.channel=? "
            "WHERE s.service=?",
            (ch, s),
        )
        if row is None:
            return False
        return bool(row[0])

    async def is_service_enabled_any(self, service: str) -> bool:
        s = (service or "").strip().lower()
        if not s:
            return True
        # If explicitly enabled in any channel, allow background tasks.
        row = await self.fetchone(
            "SELECT 1 FROM service_channel WHERE service=? AND enabled=1 LIMIT 1",
            (s,),
        )
        if row:
            return True
        # Otherwise fall back to enabled_by_default.
        row2 = await self.fetchone("SELECT enabled_by_default FROM services WHERE service=?", (s,))
        return bool(row2 and row2[0])

    async def list_service_status_for_channel(self, channel: str) -> list[tuple[str, bool]]:
        ch = (channel or "").strip()
        if not ch:
            return []
        await self.ensure_channel(ch)
        rows = await self.fetchall(
            "SELECT s.service, COALESCE(sc.enabled, s.enabled_by_default) AS enabled "
            "FROM services s "
            "LEFT JOIN service_channel sc ON sc.service=s.service AND sc.channel=? "
            "ORDER BY s.service",
            (ch,),
        )
        return [(r[0], bool(r[1])) for r in rows]

    async def close(self) -> None:
        async with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
