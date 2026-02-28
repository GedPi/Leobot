from __future__ import annotations

import logging
import sqlite3
import time

log = logging.getLogger("leobot.migrations")


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    )
    return cur.fetchone() is not None


def get_schema_version(conn: sqlite3.Connection) -> int:
    if not _table_exists(conn, "meta"):
        return 0
    row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    if not row:
        return 0
    try:
        return int(row[0])
    except Exception:
        return 0


def set_schema_version(conn: sqlite3.Connection, v: int) -> None:
    conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version', ?)", (str(v),))


def migrate_v1(conn: sqlite3.Connection) -> None:
    # Core meta + settings
    conn.executescript(
        """
        PRAGMA foreign_keys=ON;

        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_ts INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS service_enablement (
            channel TEXT NOT NULL,
            service TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 0,
            updated_ts INTEGER NOT NULL,
            updated_by TEXT,
            PRIMARY KEY(channel, service)
        );

        -- ACL
        CREATE TABLE IF NOT EXISTS acl_sessions (
            identity_key TEXT PRIMARY KEY,
            role TEXT NOT NULL,
            auth_until_ts INTEGER NOT NULL,
            created_ts INTEGER NOT NULL,
            updated_ts INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_acl_sessions_until ON acl_sessions(auth_until_ts);

        -- Chat logs
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
        CREATE INDEX IF NOT EXISTS idx_stats_day_chan ON stats_daily(day, channel);

        -- Greet (normalized)
        CREATE TABLE IF NOT EXISTS greet_targets (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          enabled INTEGER NOT NULL DEFAULT 1,
          priority INTEGER NOT NULL DEFAULT 0,
          match_nick TEXT,
          match_hostmask TEXT,
          match_userhost TEXT,
          match_host TEXT,
          channel TEXT,
          cooldown_seconds INTEGER,
          created_ts INTEGER NOT NULL,
          updated_ts INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_greet_targets_enabled_pri ON greet_targets(enabled, priority DESC);
        CREATE INDEX IF NOT EXISTS idx_greet_targets_channel ON greet_targets(channel);

        CREATE TABLE IF NOT EXISTS greetings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          target_id INTEGER NOT NULL REFERENCES greet_targets(id) ON DELETE CASCADE,
          text TEXT NOT NULL,
          weight INTEGER NOT NULL DEFAULT 1,
          enabled INTEGER NOT NULL DEFAULT 1,
          created_ts INTEGER NOT NULL,
          updated_ts INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_greetings_target_enabled ON greetings(target_id, enabled);

        CREATE TABLE IF NOT EXISTS greet_cooldowns (
          key TEXT PRIMARY KEY,
          until_ts INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_greet_cooldowns_until ON greet_cooldowns(until_ts);

        -- Weather
        CREATE TABLE IF NOT EXISTS weather_watches (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          city TEXT NOT NULL,
          country TEXT,
          lat REAL,
          lon REAL,
          types TEXT NOT NULL,
          interval_minutes INTEGER NOT NULL DEFAULT 15,
          expires_ts INTEGER NOT NULL,
          created_ts INTEGER NOT NULL,
          created_by TEXT,
          enabled INTEGER NOT NULL DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_weather_watches_expires ON weather_watches(expires_ts);
        CREATE INDEX IF NOT EXISTS idx_weather_watches_city ON weather_watches(city);

        CREATE TABLE IF NOT EXISTS weather_alert_state (
          watch_id INTEGER PRIMARY KEY REFERENCES weather_watches(id) ON DELETE CASCADE,
          last_alert_ts INTEGER,
          last_alert_fingerprint TEXT
        );

        -- Wiki
        CREATE TABLE IF NOT EXISTS wiki_watches (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          lang TEXT NOT NULL DEFAULT 'en',
          title TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 1,
          interval_minutes INTEGER NOT NULL DEFAULT 15,
          created_ts INTEGER NOT NULL,
          created_by TEXT,
          UNIQUE(lang, title)
        );

        CREATE TABLE IF NOT EXISTS wiki_state (
          watch_id INTEGER PRIMARY KEY REFERENCES wiki_watches(id) ON DELETE CASCADE,
          last_rev_id INTEGER,
          last_checked_ts INTEGER
        );

        -- News (persistent)
        CREATE TABLE IF NOT EXISTS news_sources (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 1,
          created_ts INTEGER NOT NULL,
          updated_ts INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS news_source_categories (
          source_id TEXT NOT NULL REFERENCES news_sources(id) ON DELETE CASCADE,
          category TEXT NOT NULL,
          url TEXT NOT NULL,
          PRIMARY KEY (source_id, category)
        );

        CREATE TABLE IF NOT EXISTS news_posted (
          channel TEXT NOT NULL,
          source_id TEXT NOT NULL,
          category TEXT NOT NULL,
          limit_n INTEGER NOT NULL,
          last_posted_ts INTEGER NOT NULL,
          PRIMARY KEY (channel, source_id, category, limit_n)
        );

        -- Scheduler job state (optional but handy)
        CREATE TABLE IF NOT EXISTS jobs (
          name TEXT PRIMARY KEY,
          enabled INTEGER NOT NULL DEFAULT 1,
          last_run_ts INTEGER,
          last_ok_ts INTEGER,
          last_error TEXT,
          updated_ts INTEGER NOT NULL
        );
        """
    )

    now = int(time.time())
    conn.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('created_ts', ?)", (str(now),))


MIGRATIONS = {
    1: migrate_v1,
}


def apply_migrations(conn: sqlite3.Connection) -> None:
    current = get_schema_version(conn)
    target = max(MIGRATIONS.keys()) if MIGRATIONS else 0
    if current == 0:
        log.info("Bootstrapping new database schema (v1)")
    if current >= target:
        return

    for v in range(current + 1, target + 1):
        fn = MIGRATIONS.get(v)
        if not fn:
            raise RuntimeError(f"Missing migration function for version {v}")
        log.info("Applying migration v%s", v)
        fn(conn)
        set_schema_version(conn, v)

    conn.commit()
