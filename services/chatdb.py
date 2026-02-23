import asyncio
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

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


def utc_day(ts: int | None = None) -> str:
    ts = ts or int(time.time())
    return time.strftime("%Y-%m-%d", time.gmtime(ts))


def word_count(s: str) -> int:
    return len([w for w in (s or "").strip().split() if w])


def has_link(s: str) -> int:
    t = (s or "").lower()
    return 1 if ("http://" in t or "https://" in t) else 0
