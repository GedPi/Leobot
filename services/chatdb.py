import asyncio
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

-- ---- Control-plane (migrations) ----
CREATE TABLE IF NOT EXISTS schema_migrations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  version INTEGER NOT NULL UNIQUE,
  applied_utc TEXT NOT NULL
);

-- ---- Control-plane (settings/events) ----
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_utc TEXT NOT NULL,
  level TEXT NOT NULL,
  source TEXT NOT NULL,
  channel TEXT,
  nick TEXT,
  message TEXT NOT NULL,
  data_json TEXT DEFAULT ''
);

-- ---- Control-plane (services/channels) ----
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

CREATE TABLE IF NOT EXISTS nick_changes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,
  channel TEXT,
  old_nick TEXT NOT NULL,
  new_nick TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nickchg_old_ts ON nick_changes(old_nick, ts);
CREATE INDEX IF NOT EXISTS idx_nickchg_new_ts ON nick_changes(new_nick, ts);

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

CREATE INDEX IF NOT EXISTS idx_stats_day_chan ON stats_daily(day, channel);
"""

# --- add to SCHEMA in services/chatdb.py ---

CREATE TABLE IF NOT EXISTS greet_rules (
  id TEXT PRIMARY KEY,
  priority INTEGER NOT NULL DEFAULT 0,
  enabled INTEGER NOT NULL DEFAULT 1,
  match_json TEXT NOT NULL,         -- {"nicks":[...], "hosts":[...]}
  greetings_json TEXT NOT NULL,     -- ["hi bob", "yo bob", ...]
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

-- weather watches (for collector / warn list/del/add)
CREATE TABLE IF NOT EXISTS weather_watches (
  city TEXT PRIMARY KEY,
  duration_hours INTEGER NOT NULL,
  types_json TEXT NOT NULL,         -- ["rain"] etc
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

-- persist ACL daily auth
CREATE TABLE IF NOT EXISTS acl_auth (
  identity_key TEXT PRIMARY KEY,    -- user@host lower, or nick lower fallback
  role TEXT NOT NULL,
  authed_until_ts INTEGER NOT NULL,
  authed_ts INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_acl_auth_until ON acl_auth(authed_until_ts);

@dataclass
class DBConfig:
    path: str


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
                isolation_level=None,     # autocommit
                check_same_thread=False,
            )
            self._conn.executescript(SCHEMA)
        return self._conn

    async def execute(self, sql: str, args: Iterable[Any] = ()) -> None:
        async with self._lock:
            conn = self._connect()
            conn.execute(sql, tuple(args))

    async def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        async with self._lock:
            conn = self._connect()
            conn.executemany(sql, rows)

    async def fetchone(self, sql: str, args: Iterable[Any] = ()) -> tuple[Any, ...] | None:
        async with self._lock:
            conn = self._connect()
            cur = conn.execute(sql, tuple(args))
            return cur.fetchone()

    async def fetchall(self, sql: str, args: Iterable[Any] = ()) -> list[tuple[Any, ...]]:
        async with self._lock:
            conn = self._connect()
            cur = conn.execute(sql, tuple(args))
            return cur.fetchall()

    async def close(self) -> None:
        async with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    # -----------------------------
    # Control-plane helpers
    # -----------------------------

    @staticmethod
    def _utc_now() -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

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
            "ON CONFLICT(service, channel) DO UPDATE SET enabled=excluded.enabled, updated_utc=excluded.updated_utc, updated_by=excluded.updated_by",
            (s, ch, int(bool(enabled)), self._utc_now(), (updated_by or "")[:128]),
        )

    async def is_service_enabled(self, service: str, channel: str) -> bool:
        """Default-off policy: if no row exists for (service, channel), treat as disabled."""
        s = (service or "").strip().lower()
        ch = (channel or "").strip()
        if not s or not ch:
            return True

        row = await self.fetchone(
            "SELECT enabled FROM service_channel WHERE service=? AND channel=?",
            (s, ch),
        )
        if row is None:
            return False
        return bool(row[0])

    async def is_service_enabled_any(self, service: str) -> bool:
        s = (service or "").strip().lower()
        if not s:
            return True
        row = await self.fetchone(
            "SELECT 1 FROM service_channel WHERE service=? AND enabled=1 LIMIT 1",
            (s,),
        )
        return row is not None

    async def list_services(self) -> list[str]:
        rows = await self.fetchall("SELECT service FROM services ORDER BY service")
        return [r[0] for r in rows]

    async def list_service_status_for_channel(self, channel: str) -> list[tuple[str, bool]]:
        ch = (channel or "").strip()
        if not ch:
            return []
        rows = await self.fetchall(
            "SELECT s.service, COALESCE(sc.enabled, 0) "
            "FROM services s "
            "LEFT JOIN service_channel sc ON sc.service=s.service AND sc.channel=? "
            "ORDER BY s.service",
            (ch,),
        )
        return [(r[0], bool(r[1])) for r in rows]


def utc_day(ts: int | None = None) -> str:
    ts = ts or int(time.time())
    return time.strftime("%Y-%m-%d", time.gmtime(ts))


def word_count(s: str) -> int:
    return len([w for w in (s or "").strip().split() if w])


def has_link(s: str) -> int:
    t = (s or "").lower()
    return 1 if ("http://" in t or "https://" in t) else 0
