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


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(r[1]) for r in rows}
    except Exception:
        return set()


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
    # Core meta + settings + initial services/tables
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

        -- Chat logs (legacy simple channel messages)
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

        -- Weather (v1 legacy shape; upgraded in v2)
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


def migrate_v2(conn: sqlite3.Connection) -> None:
    """
    Weather schema upgrade.
    """
    now = int(time.time())

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS weather_locations (
          query TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          country TEXT,
          country_code TEXT,
          lat REAL NOT NULL,
          lon REAL NOT NULL,
          created_ts INTEGER NOT NULL,
          updated_ts INTEGER NOT NULL
        );
        """
    )

    w_cols = _columns(conn, "weather_watches")
    already_v2 = "target_channel" in w_cols and "next_check_ts" in w_cols and "interval_seconds" in w_cols

    if already_v2:
        a_cols = _columns(conn, "weather_alert_state")
        if "last_fingerprint" not in a_cols:
            conn.executescript(
                """
                PRAGMA foreign_keys=OFF;

                ALTER TABLE weather_alert_state RENAME TO weather_alert_state_old;

                CREATE TABLE weather_alert_state (
                  watch_id INTEGER PRIMARY KEY REFERENCES weather_watches(id) ON DELETE CASCADE,
                  last_alert_ts INTEGER,
                  last_fingerprint TEXT
                );

                INSERT INTO weather_alert_state(watch_id,last_alert_ts,last_fingerprint)
                  SELECT watch_id,last_alert_ts,last_alert_fingerprint FROM weather_alert_state_old;

                DROP TABLE weather_alert_state_old;

                PRAGMA foreign_keys=ON;
                """
            )
        return

    conn.executescript(
        """
        PRAGMA foreign_keys=OFF;

        ALTER TABLE weather_watches RENAME TO weather_watches_old;
        """
    )

    conn.executescript(
        """
        CREATE TABLE weather_watches (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          target_channel TEXT NOT NULL,
          location_query TEXT NOT NULL,
          location_name TEXT NOT NULL,
          country TEXT,
          country_code TEXT,
          lat REAL,
          lon REAL,
          types TEXT NOT NULL,
          interval_seconds INTEGER NOT NULL DEFAULT 900,
          next_check_ts INTEGER NOT NULL,
          expires_ts INTEGER NOT NULL,
          created_ts INTEGER NOT NULL,
          created_by TEXT,
          enabled INTEGER NOT NULL DEFAULT 1
        );

        CREATE INDEX IF NOT EXISTS idx_weather_watches_enabled_nextcheck ON weather_watches(enabled, next_check_ts);
        CREATE INDEX IF NOT EXISTS idx_weather_watches_target_channel ON weather_watches(target_channel);
        """
    )

    if _table_exists(conn, "weather_alert_state"):
        conn.executescript("ALTER TABLE weather_alert_state RENAME TO weather_alert_state_old;")
    else:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS weather_alert_state_old (
              watch_id INTEGER PRIMARY KEY,
              last_alert_ts INTEGER,
              last_alert_fingerprint TEXT
            );
            """
        )

    conn.executescript(
        """
        CREATE TABLE weather_alert_state (
          watch_id INTEGER PRIMARY KEY REFERENCES weather_watches(id) ON DELETE CASCADE,
          last_alert_ts INTEGER,
          last_fingerprint TEXT
        );
        """
    )

    conn.execute(
        """
        INSERT INTO weather_watches(
          id, target_channel, location_query, location_name, country, country_code, lat, lon,
          types, interval_seconds, next_check_ts, expires_ts, created_ts, created_by, enabled
        )
        SELECT
          id,
          '' as target_channel,
          CASE
            WHEN country IS NOT NULL AND TRIM(country) <> '' THEN (city || ', ' || country)
            ELSE city
          END as location_query,
          CASE
            WHEN country IS NOT NULL AND TRIM(country) <> '' THEN (city || ', ' || country)
            ELSE city
          END as location_name,
          country,
          NULL as country_code,
          lat,
          lon,
          types,
          CAST(interval_minutes AS INTEGER) * 60 as interval_seconds,
          ? as next_check_ts,
          expires_ts,
          created_ts,
          created_by,
          0 as enabled
        FROM weather_watches_old
        """,
        (int(now),),
    )

    a_cols_old = _columns(conn, "weather_alert_state_old")
    if "last_alert_fingerprint" in a_cols_old:
        conn.execute(
            """
            INSERT INTO weather_alert_state(watch_id,last_alert_ts,last_fingerprint)
              SELECT watch_id,last_alert_ts,last_alert_fingerprint FROM weather_alert_state_old
            """
        )
    elif "last_fingerprint" in a_cols_old:
        conn.execute(
            """
            INSERT INTO weather_alert_state(watch_id,last_alert_ts,last_fingerprint)
              SELECT watch_id,last_alert_ts,last_fingerprint FROM weather_alert_state_old
            """
        )

    conn.executescript(
        """
        DROP TABLE weather_alert_state_old;
        DROP TABLE weather_watches_old;

        PRAGMA foreign_keys=ON;
        """
    )


def migrate_v3(conn: sqlite3.Connection) -> None:
    """
    Add canonical IRC event journal (irc_log).
    This is the single "posterity" log that lastseen/stats will derive from.
    """
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS irc_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts INTEGER NOT NULL,

          -- NULL for global events (e.g. QUIT). For channel events, this is the channel.
          channel TEXT,

          -- Canonical event name: PRIVMSG, ACTION, NOTICE, JOIN, PART, QUIT, NICK, KICK, MODE, TOPIC, ...
          event TEXT NOT NULL,

          -- Actor (source) identity
          actor_nick TEXT,
          actor_user TEXT,
          actor_host TEXT,
          actor_userhost TEXT,

          -- Target entity (victim, new nick, ban mask, mode target, etc.)
          target TEXT,

          -- Associated free text (message, quit reason, part reason, kick reason, topic text, mode args...)
          message TEXT,

          -- Human-readable rendered line (stable house format for display/export)
          rendered TEXT NOT NULL,

          -- Raw IRC line from server (verbatim)
          raw TEXT,

          -- Original IRC cmd and params (for future forensics/replay)
          cmd TEXT,
          params_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_irc_log_chan_ts ON irc_log(channel, ts);
        CREATE INDEX IF NOT EXISTS idx_irc_log_actor_ts ON irc_log(actor_nick, ts);
        CREATE INDEX IF NOT EXISTS idx_irc_log_target_ts ON irc_log(target, ts);
        CREATE INDEX IF NOT EXISTS idx_irc_log_event_ts ON irc_log(event, ts);
        """
    )

def migrate_v4(conn: sqlite3.Connection) -> None:
    # Sysmon: persistent event log for alerts / forensic trail
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sys_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts INTEGER NOT NULL,
          level TEXT NOT NULL,          -- INFO/WARN/ERROR
          source TEXT NOT NULL,         -- e.g. systemd, sshd, sysmon
          kind TEXT NOT NULL,           -- e.g. failed_units, service_down, auth_fail
          message TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sys_events_ts ON sys_events(ts);
        CREATE INDEX IF NOT EXISTS idx_sys_events_kind_ts ON sys_events(kind, ts);
        """
    )

def migrate_v5(conn: sqlite3.Connection) -> None:
    """
    Greet pools: allow multiple targets to share a greeting set.

    - Introduce greet_pools
    - Add greet_targets.pool_id
    - Re-home greetings from target_id -> pool_id
    """
    now = int(time.time())

    # If already migrated, no-op.
    if _table_exists(conn, "greet_pools"):
        tcols = _columns(conn, "greet_targets")
        gcols = _columns(conn, "greetings")
        if "pool_id" in tcols and "pool_id" in gcols:
            return

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS greet_pools (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          created_ts INTEGER NOT NULL,
          updated_ts INTEGER NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_greet_pools_name ON greet_pools(name);
        """
    )

    # Add pool_id to greet_targets (nullable; we'll backfill).
    tcols = _columns(conn, "greet_targets")
    if "pool_id" not in tcols:
        conn.execute("ALTER TABLE greet_targets ADD COLUMN pool_id INTEGER")

    # If greetings already has pool_id we can just backfill targets if needed.
    gcols = _columns(conn, "greetings")
    if "pool_id" in gcols:
        rows = conn.execute(
            "SELECT id, match_nick FROM greet_targets WHERE pool_id IS NULL ORDER BY id"
        ).fetchall()
        for tid, match_nick in rows:
            tid_i = int(tid)
            nick = (match_nick or "").strip()
            name = f"nick:{nick}" if nick else f"target:{tid_i}"
            existing = conn.execute("SELECT 1 FROM greet_pools WHERE name=? LIMIT 1", (name,)).fetchone()
            if existing:
                name = f"{name}#{tid_i}"
            conn.execute(
                "INSERT INTO greet_pools(name, created_ts, updated_ts) VALUES(?,?,?)",
                (name, now, now),
            )
            pid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            conn.execute("UPDATE greet_targets SET pool_id=?, updated_ts=? WHERE id=?", (pid, now, tid_i))
        return

    # Migrate greetings table from target_id -> pool_id
    conn.executescript(
        """
        PRAGMA foreign_keys=OFF;
        ALTER TABLE greetings RENAME TO greetings_old;

        CREATE TABLE greetings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          pool_id INTEGER NOT NULL REFERENCES greet_pools(id) ON DELETE CASCADE,
          text TEXT NOT NULL,
          weight INTEGER NOT NULL DEFAULT 1,
          enabled INTEGER NOT NULL DEFAULT 1,
          created_ts INTEGER NOT NULL,
          updated_ts INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_greetings_pool_enabled ON greetings(pool_id, enabled);
        """
    )

    # Create one pool per existing target and attach it.
    tgt_rows = conn.execute("SELECT id, match_nick FROM greet_targets ORDER BY id").fetchall()

    tgt_to_pool: dict[int, int] = {}
    for tid, match_nick in tgt_rows:
        tid_i = int(tid)
        nick = (match_nick or "").strip()
        name = f"nick:{nick}" if nick else f"target:{tid_i}"
        existing = conn.execute("SELECT 1 FROM greet_pools WHERE name=? LIMIT 1", (name,)).fetchone()
        if existing:
            name = f"{name}#{tid_i}"
        conn.execute(
            "INSERT INTO greet_pools(name, created_ts, updated_ts) VALUES(?,?,?)",
            (name, now, now),
        )
        pid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        tgt_to_pool[tid_i] = pid
        conn.execute("UPDATE greet_targets SET pool_id=?, updated_ts=? WHERE id=?", (pid, now, tid_i))

    # Move greetings across
    old_rows = conn.execute(
        "SELECT id, target_id, text, weight, enabled, created_ts, updated_ts FROM greetings_old ORDER BY id"
    ).fetchall()

    for gid, target_id, text, weight, enabled, cts, uts in old_rows:
        tid_i = int(target_id)
        pid = tgt_to_pool.get(tid_i)
        if not pid:
            name = f"target:{tid_i}"
            existing = conn.execute("SELECT 1 FROM greet_pools WHERE name=? LIMIT 1", (name,)).fetchone()
            if existing:
                name = f"{name}#{tid_i}"
            conn.execute(
                "INSERT INTO greet_pools(name, created_ts, updated_ts) VALUES(?,?,?)",
                (name, now, now),
            )
            pid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            tgt_to_pool[tid_i] = pid
            conn.execute("UPDATE greet_targets SET pool_id=?, updated_ts=? WHERE id=?", (pid, now, tid_i))

        conn.execute(
            """
            INSERT INTO greetings(id, pool_id, text, weight, enabled, created_ts, updated_ts)
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                int(gid),
                int(pid),
                text,
                int(weight) if weight is not None else 1,
                int(enabled) if enabled is not None else 1,
                int(cts) if cts is not None else now,
                int(uts) if uts is not None else now,
            ),
        )

    conn.executescript(
        """
        DROP TABLE greetings_old;
        PRAGMA foreign_keys=ON;
        """
    )

MIGRATIONS = {
    1: migrate_v1,
    2: migrate_v2,
    3: migrate_v3,
    4: migrate_v4,
    5: migrate_v5,
}


def apply_migrations(conn: sqlite3.Connection) -> None:
    current = get_schema_version(conn)
    target = max(MIGRATIONS.keys()) if MIGRATIONS else 0

    if current == 0:
        log.info("Bootstrapping new database schema (v1+)")

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