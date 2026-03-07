# Sync DB access for web UI (same SQLite file as bot; use when bot may be running).
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any


def get_conn(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def get_setting(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    now = int(time.time())
    conn.execute(
        "INSERT INTO settings(key,value,updated_ts) VALUES(?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_ts=excluded.updated_ts",
        (key, value, now),
    )
    conn.commit()


def list_settings(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    rows = conn.execute("SELECT key, value FROM settings ORDER BY key").fetchall()
    return [(r[0], r[1]) for r in rows]


def list_service_enablement_all(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT channel, service, enabled, updated_by FROM service_enablement ORDER BY channel, service"
    ).fetchall()
    return [{"channel": r[0], "service": r[1], "enabled": bool(r[2]), "updated_by": r[3]} for r in rows]


def list_service_enablement_channel(conn: sqlite3.Connection, channel: str) -> list[tuple[str, bool]]:
    rows = conn.execute(
        "SELECT service, enabled FROM service_enablement WHERE channel=? ORDER BY service",
        (channel,),
    ).fetchall()
    return [(r[0], bool(r[1])) for r in rows]


def set_service_enabled(conn: sqlite3.Connection, channel: str, service: str, enabled: bool, updated_by: str | None = None) -> None:
    now = int(time.time())
    conn.execute(
        "INSERT INTO service_enablement(channel,service,enabled,updated_ts,updated_by) VALUES(?,?,?,?,?) "
        "ON CONFLICT(channel,service) DO UPDATE SET enabled=excluded.enabled, updated_ts=excluded.updated_ts, updated_by=excluded.updated_by",
        (channel, service, 1 if enabled else 0, now, updated_by),
    )
    conn.commit()


def fact_list_categories(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT DISTINCT category FROM facts ORDER BY category").fetchall()
    return [r[0].strip() for r in rows]


def fact_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM facts").fetchone()
    return int(row[0]) if row else 0


def fact_count_by_category(conn: sqlite3.Connection) -> list[tuple[str, int]]:
    rows = conn.execute(
        "SELECT category, COUNT(*) FROM facts GROUP BY category ORDER BY category"
    ).fetchall()
    return [(r[0], r[1]) for r in rows]
