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

    async def clear_acl_session(self, identity_key: str) -> None:
        await self.execute("DELETE FROM acl_sessions WHERE identity_key=?", (identity_key,))

    # ---- News sources & categories ----
    async def news_list_sources(self):
        return await self.fetchall(
            "SELECT id,name,enabled,created_ts,updated_ts FROM news_sources ORDER BY id",
            (),
        )

    async def news_get_source(self, source_id: str):
        return await self.fetchone(
            "SELECT id,name,enabled,created_ts,updated_ts FROM news_sources WHERE id=?",
            (source_id,),
        )

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

    async def news_list_categories(self, source_id: str):
        return await self.fetchall(
            "SELECT source_id,category,url,created_ts,updated_ts FROM news_source_categories WHERE source_id=? ORDER BY category",
            (source_id,),
        )

    async def news_set_category(self, source_id: str, category: str, url: str) -> None:
        now = int(time.time())
        await self.execute(
            "INSERT INTO news_source_categories(source_id,category,url,created_ts,updated_ts) VALUES(?,?,?,?,?) "
            "ON CONFLICT(source_id,category) DO UPDATE SET url=excluded.url, updated_ts=excluded.updated_ts",
            (source_id, category, url, now, now),
        )

    async def news_get_last_posted(self, channel: str, source_id: str, category: str, limit: int) -> int | None:
        row = await self.fetchone(
            "SELECT last_posted_ts FROM news_posted WHERE channel=? AND source_id=? AND category=? AND limit_n=?",
            (channel, source_id, category, int(limit)),
        )
        return int(row[0]) if row else None

    async def news_set_last_posted(self, channel: str, source_id: str, category: str, limit: int, ts: int) -> None:
        await self.execute(
            "INSERT INTO news_posted(channel,source_id,category,limit_n,last_posted_ts) VALUES(?,?,?,?,?) "
            "ON CONFLICT(channel,source_id,category,limit_n) DO UPDATE SET last_posted_ts=excluded.last_posted_ts",
            (channel, source_id, category, int(limit), int(ts)),
        )

    # ---- Greet helpers ----
    async def greet_select_target(self, *, nick: str, hostmask: str, userhost: str, host: str, channel: str) -> sqlite3.Row | None:
        """Select the highest priority greeting target that matches this identity.

        Notes:
        - Channel comparison is case-insensitive (so '#General' and '#general' are equivalent).
        - match_host is intended for host-only patterns (e.g. '*.example.net').
          Back-compat: if match_host contains '!' or '@' (looks like a mask), we match it
          against hostmask/userhost/host so legacy/accidental patterns still work.
        """
        chan = (channel or "").strip()
        rows = await self.fetchall(
            """
            SELECT * FROM greet_targets
            WHERE enabled=1
              AND (
                channel IS NULL
                OR channel=''
                OR lower(channel)=lower(?)
              )
            ORDER BY priority DESC, id ASC
            """,
            (chan,),
        )

        import fnmatch

        n_l = (nick or "").strip().lower()
        hm = (hostmask or "").strip()
        uh = (userhost or "").strip()
        h = (host or "").strip()

        for r in rows:
            # AND semantics across provided match_* fields
            if r["match_nick"]:
                if str(r["match_nick"]).strip().lower() != n_l:
                    continue

            if r["match_hostmask"]:
                pat = str(r["match_hostmask"]).strip()
                if not fnmatch.fnmatch(hm, pat):
                    continue

            if r["match_userhost"]:
                pat = str(r["match_userhost"]).strip()
                if not fnmatch.fnmatch(uh, pat):
                    continue

            if r["match_host"]:
                pat = str(r["match_host"]).strip()
                # Back-compat: pattern looks like a mask => match it against mask/userhost/host.
                if ("!" in pat) or ("@" in pat):
                    if not (fnmatch.fnmatch(hm, pat) or fnmatch.fnmatch(uh, pat) or fnmatch.fnmatch(h, pat)):
                        continue
                else:
                    if not fnmatch.fnmatch(h, pat):
                        continue

            return r

        return None

    async def greet_pick_greeting(self, target_id: int) -> str | None:
        # weighted random selection
        rows = await self.fetchall(
            "SELECT id,text,weight FROM greetings WHERE target_id=? AND enabled=1",
            (int(target_id),),
        )
        if not rows:
            return None

        # deterministic-ish selection without importing random every time:
        # sum weights, pick based on current time ticks.
        total = 0
        items: list[tuple[str, int]] = []
        for r in rows:
            w = int(r["weight"] or 1)
            if w <= 0:
                continue
            total += w
            items.append((str(r["text"]), w))
        if total <= 0 or not items:
            return None

        pick = int(time.time() * 1000) % total
        acc = 0
        for txt, w in items:
            acc += w
            if pick < acc:
                return txt
        return items[-1][0]

    # ---- Weather watches ----

    async def weather_watch_add(
        self,
        *,
        target_channel: str,
        location_query: str,
        location_name: str,
        country: str | None = None,
        country_code: str | None = None,
        lat: float | None = None,
        lon: float | None = None,
        types_csv: str = "",
        duration_seconds: int = 3600,
        interval_seconds: int = 900,
        created_by: str | None = None,
        enabled: bool = True,
        next_check_ts: int | None = None,
    ) -> int:
        """Create a weather watch (schema v2)."""
        now = int(time.time())
        ncheck = int(next_check_ts or now)
        expires = now + int(duration_seconds)

        await self.execute(
            """
            INSERT INTO weather_watches(
              target_channel, location_query, location_name, country, country_code, lat, lon,
              types, interval_seconds, next_check_ts, expires_ts, created_ts, created_by, enabled
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                str(target_channel),
                str(location_query),
                str(location_name),
                (str(country) if country is not None else None),
                (str(country_code) if country_code is not None else None),
                (float(lat) if lat is not None else None),
                (float(lon) if lon is not None else None),
                str(types_csv),
                int(interval_seconds),
                int(ncheck),
                int(expires),
                now,
                created_by,
                1 if enabled else 0,
            ),
        )
        row = await self.fetchone("SELECT last_insert_rowid()", ())
        return int(row[0])

    async def weather_watch_get(self, watch_id: int) -> sqlite3.Row | None:
        return await self.fetchone("SELECT * FROM weather_watches WHERE id=?", (int(watch_id),))

    async def weather_watch_list(self, channel: str | None = None, *, target_channel: str | None = None) -> list[sqlite3.Row]:
        """
        List watches for a channel.

        Supports both:
          - weather_watch_list("#chan")
          - weather_watch_list(target_channel="#chan")
        """
        chan = target_channel if target_channel is not None else channel
        if not chan:
            return []
        rows = await self.fetchall(
            "SELECT * FROM weather_watches WHERE target_channel=? ORDER BY id",
            (str(chan),),
        )
        return list(rows)

    async def weather_watch_clear(self, channel: str | None = None, *, target_channel: str | None = None) -> int:
        """Delete all watches for a channel. Returns number of rows deleted."""
        chan = target_channel if target_channel is not None else channel
        if not chan:
            return 0
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM weather_watches WHERE target_channel=?",
                (str(chan),),
            )
            return int(cur.rowcount)

    async def weather_watch_delete(
        self,
        watch_id: int | None = None,
        channel: str | None = None,
        *,
        target_channel: str | None = None,
        watch_id_kw: int | None = None,
    ) -> int:
        """
        Delete a watch by id (scoped to a channel).

        Supports:
          - weather_watch_delete(target_channel="#chan", watch_id=123)  (what weather.py uses)
          - weather_watch_delete(123, "#chan")                          (legacy positional)
        """
        wid = watch_id_kw if watch_id_kw is not None else watch_id
        chan = target_channel if target_channel is not None else channel
        if wid is None or not chan:
            return 0
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM weather_watches WHERE id=? AND target_channel=?",
                (int(wid), str(chan)),
            )
            return int(cur.rowcount)

    async def weather_watch_set_enabled(self, watch_id: int, enabled: bool) -> None:
        await self.execute(
            "UPDATE weather_watches SET enabled=? WHERE id=?",
            (1 if enabled else 0, int(watch_id)),
        )

    async def weather_watch_due(self, *, now_ts: int, limit: int = 10) -> list[sqlite3.Row]:
        rows = await self.fetchall(
            """
            SELECT * FROM weather_watches
            WHERE enabled=1
              AND next_check_ts <= ?
              AND expires_ts > ?
            ORDER BY next_check_ts ASC
            LIMIT ?
            """,
            (int(now_ts), int(now_ts), int(limit)),
        )
        return list(rows)

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