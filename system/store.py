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

    # ----------------------------
    # generic helpers
    # ----------------------------

    async def execute(self, sql: str, params: Iterable[Any] = ()) -> None:
        async with self._lock:
            self._conn.execute(sql, tuple(params))

    async def executemany(self, sql: str, seq: Iterable[Iterable[Any]]) -> None:
        async with self._lock:
            self._conn.executemany(sql, [tuple(x) for x in seq])

    async def fetchone(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
        async with self._lock:
            cur = self._conn.execute(sql, tuple(params))
            return cur.fetchone()

    async def fetchall(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        async with self._lock:
            cur = self._conn.execute(sql, tuple(params))
            return cur.fetchall()

    # ----------------------------
    # settings
    # ----------------------------

    async def settings_get(self, key: str, default: str | None = None) -> str | None:
        row = await self.fetchone("SELECT value FROM settings WHERE key=?", (key,))
        return str(row[0]) if row else default

    async def settings_set(self, key: str, value: str) -> None:
        await self.execute(
            """
            INSERT INTO settings(key,value,updated_ts)
            VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_ts=excluded.updated_ts
            """,
            (key, str(value), int(time.time())),
        )

    # ----------------------------
    # services enablement
    # ----------------------------

    async def is_service_enabled(self, channel: str, service: str) -> bool:
        row = await self.fetchone(
            "SELECT enabled FROM service_enablement WHERE lower(channel)=lower(?) AND service=?",
            (channel or "", service or ""),
        )
        return bool(row and int(row[0]) == 1)

    async def set_service_enabled(self, channel: str, service: str, enabled: bool, updated_by: str | None = None) -> None:
        await self.execute(
            """
            INSERT INTO service_enablement(channel, service, enabled, updated_ts, updated_by)
            VALUES(?,?,?,?,?)
            ON CONFLICT(channel, service) DO UPDATE SET enabled=excluded.enabled, updated_ts=excluded.updated_ts, updated_by=excluded.updated_by
            """,
            (channel or "", service or "", 1 if enabled else 0, int(time.time()), updated_by),
        )

    async def list_service_enablement(self, channel: str) -> list[sqlite3.Row]:
        return await self.fetchall(
            """
            SELECT service, enabled, updated_ts, updated_by
            FROM service_enablement
            WHERE lower(channel)=lower(?)
            ORDER BY service ASC
            """,
            (channel or "",),
        )

    async def list_enabled_services(self, channel: str) -> list[str]:
        rows = await self.fetchall(
            """
            SELECT service
            FROM service_enablement
            WHERE lower(channel)=lower(?) AND enabled=1
            ORDER BY service ASC
            """,
            (channel or "",),
        )
        return [str(r[0]) for r in rows]

    # ----------------------------
    # greet
    # ----------------------------

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
            # AND semantics across provided match_* fields.
            if r["match_nick"]:
                if (r["match_nick"] or "").strip().lower() != n_l:
                    continue

            if r["match_hostmask"]:
                pat = (r["match_hostmask"] or "").strip()
                if not pat:
                    continue
                if not fnmatch.fnmatch(hm, pat):
                    continue

            if r["match_userhost"]:
                pat = (r["match_userhost"] or "").strip()
                if not pat:
                    continue
                if not fnmatch.fnmatch(uh, pat):
                    continue

            if r["match_host"]:
                pat = (r["match_host"] or "").strip()
                if not pat:
                    continue
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
        """Pick a greeting for a target via its assigned pool (greet_pools)."""
        row = await self.fetchone("SELECT pool_id FROM greet_targets WHERE id=?", (int(target_id),))
        if not row:
            return None
        pool_id = row[0]
        if pool_id is None:
            return None

        rows = await self.fetchall(
            "SELECT id,text,weight FROM greetings WHERE pool_id=? AND enabled=1",
            (int(pool_id),),
        )
        if not rows:
            return None

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
        country: str | None,
        country_code: str | None,
        lat: float | None,
        lon: float | None,
        types: str,
        interval_seconds: int,
        next_check_ts: int,
        expires_ts: int,
        created_by: str | None,
        enabled: bool = True,
    ) -> int:
        await self.execute(
            """
            INSERT INTO weather_watches(
              target_channel, location_query, location_name, country, country_code, lat, lon, types,
              interval_seconds, next_check_ts, expires_ts, created_ts, created_by, enabled
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                target_channel,
                location_query,
                location_name,
                country,
                country_code,
                lat,
                lon,
                types,
                int(interval_seconds),
                int(next_check_ts),
                int(expires_ts),
                int(time.time()),
                created_by,
                1 if enabled else 0,
            ),
        )
        row = await self.fetchone("SELECT last_insert_rowid()", ())
        return int(row[0])

    async def weather_watch_update_next(self, watch_id: int, next_check_ts: int) -> None:
        await self.execute(
            "UPDATE weather_watches SET next_check_ts=? WHERE id=?",
            (int(next_check_ts), int(watch_id)),
        )

    async def weather_watch_set_enabled(self, watch_id: int, enabled: bool) -> None:
        await self.execute(
            "UPDATE weather_watches SET enabled=? WHERE id=?",
            (1 if enabled else 0, int(watch_id)),
        )

    async def weather_watch_delete(self, watch_id: int) -> None:
        await self.execute("DELETE FROM weather_watches WHERE id=?", (int(watch_id),))

    async def weather_watch_get(self, watch_id: int) -> sqlite3.Row | None:
        return await self.fetchone("SELECT * FROM weather_watches WHERE id=?", (int(watch_id),))

    async def weather_watch_list_due(self, *, limit: int = 50) -> list[sqlite3.Row]:
        now = int(time.time())
        return await self.fetchall(
            """
            SELECT *
            FROM weather_watches
            WHERE enabled=1 AND next_check_ts<=? AND expires_ts>?
            ORDER BY next_check_ts ASC
            LIMIT ?
            """,
            (now, now, int(limit)),
        )

    async def weather_watch_list(self, *, target_channel: str | None = None) -> list[sqlite3.Row]:
        if target_channel:
            return await self.fetchall(
                """
                SELECT *
                FROM weather_watches
                WHERE lower(target_channel)=lower(?)
                ORDER BY id ASC
                """,
                (target_channel,),
            )
        return await self.fetchall("SELECT * FROM weather_watches ORDER BY id ASC")

    async def weather_watch_list_active(self, *, target_channel: str | None = None) -> list[sqlite3.Row]:
        now = int(time.time())
        if target_channel:
            return await self.fetchall(
                """
                SELECT *
                FROM weather_watches
                WHERE enabled=1 AND expires_ts>? AND lower(target_channel)=lower(?)
                ORDER BY next_check_ts ASC
                """,
                (now, target_channel),
            )
        return await self.fetchall(
            """
            SELECT *
            FROM weather_watches
            WHERE enabled=1 AND expires_ts>?
            ORDER BY next_check_ts ASC
            """,
            (now,),
        )

    async def weather_watch_set_fingerprint(self, watch_id: int, ts: int | None, fingerprint: str | None) -> None:
        await self.execute(
            """
            INSERT INTO weather_alert_state(watch_id,last_alert_ts,last_fingerprint)
            VALUES(?,?,?)
            ON CONFLICT(watch_id) DO UPDATE SET last_alert_ts=excluded.last_alert_ts, last_fingerprint=excluded.last_fingerprint
            """,
            (int(watch_id), ts, fingerprint),
        )

    async def weather_watch_get_state(self, watch_id: int) -> sqlite3.Row | None:
        return await self.fetchone("SELECT * FROM weather_alert_state WHERE watch_id=?", (int(watch_id),))

    async def weather_location_cache_get(self, query: str) -> sqlite3.Row | None:
        return await self.fetchone("SELECT * FROM weather_locations WHERE lower(query)=lower(?)", (query or "",))

    async def weather_location_cache_set(
        self,
        *,
        query: str,
        name: str,
        country: str | None,
        country_code: str | None,
        lat: float,
        lon: float,
    ) -> None:
        now = int(time.time())
        await self.execute(
            """
            INSERT INTO weather_locations(query,name,country,country_code,lat,lon,created_ts,updated_ts)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(query) DO UPDATE SET name=excluded.name, country=excluded.country, country_code=excluded.country_code, lat=excluded.lat, lon=excluded.lon, updated_ts=excluded.updated_ts
            """,
            (query, name, country, country_code, float(lat), float(lon), now, now),
        )

    # ---- Wiki ----

    async def wiki_watch_add(self, *, lang: str, title: str, interval_minutes: int, created_by: str | None) -> None:
        await self.execute(
            """
            INSERT OR IGNORE INTO wiki_watches(lang,title,enabled,interval_minutes,created_ts,created_by)
            VALUES(?,?,1,?,?,?)
            """,
            (lang or "en", title, int(interval_minutes), int(time.time()), created_by),
        )

    async def wiki_watch_delete(self, *, lang: str, title: str) -> None:
        await self.execute("DELETE FROM wiki_watches WHERE lang=? AND title=?", (lang or "en", title))

    async def wiki_watch_set_enabled(self, *, lang: str, title: str, enabled: bool) -> None:
        await self.execute("UPDATE wiki_watches SET enabled=? WHERE lang=? AND title=?", (1 if enabled else 0, lang or "en", title))

    async def wiki_watch_list(self) -> list[sqlite3.Row]:
        return await self.fetchall("SELECT * FROM wiki_watches ORDER BY lang ASC, title ASC")

    async def wiki_state_get(self, watch_id: int) -> sqlite3.Row | None:
        return await self.fetchone("SELECT * FROM wiki_state WHERE watch_id=?", (int(watch_id),))

    async def wiki_state_set(self, watch_id: int, *, last_rev_id: int | None, last_checked_ts: int | None) -> None:
        await self.execute(
            """
            INSERT INTO wiki_state(watch_id,last_rev_id,last_checked_ts)
            VALUES(?,?,?)
            ON CONFLICT(watch_id) DO UPDATE SET last_rev_id=excluded.last_rev_id, last_checked_ts=excluded.last_checked_ts
            """,
            (int(watch_id), last_rev_id, last_checked_ts),
        )

    # ---- News ----

    async def news_upsert_source(self, source_id: str, name: str, enabled: bool = True) -> None:
        now = int(time.time())
        await self.execute(
            """
            INSERT INTO news_sources(id,name,enabled,created_ts,updated_ts)
            VALUES(?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET name=excluded.name, enabled=excluded.enabled, updated_ts=excluded.updated_ts
            """,
            (source_id, name, 1 if enabled else 0, now, now),
        )

    async def news_set_category(self, source_id: str, category: str, url: str) -> None:
        await self.execute(
            """
            INSERT INTO news_source_categories(source_id,category,url)
            VALUES(?,?,?)
            ON CONFLICT(source_id,category) DO UPDATE SET url=excluded.url
            """,
            (source_id, category, url),
        )

    async def news_clear_categories(self, source_id: str) -> None:
        await self.execute("DELETE FROM news_source_categories WHERE source_id=?", (source_id,))

    async def news_list_categories(self, source_id: str) -> list[sqlite3.Row]:
        return await self.fetchall(
            "SELECT category,url FROM news_source_categories WHERE source_id=? ORDER BY category ASC",
            (source_id,),
        )

    async def news_get_posted(self, *, channel: str, source_id: str, category: str, limit_n: int) -> sqlite3.Row | None:
        return await self.fetchone(
            """
            SELECT * FROM news_posted
            WHERE lower(channel)=lower(?) AND source_id=? AND category=? AND limit_n=?
            """,
            (channel or "", source_id, category, int(limit_n)),
        )

    async def news_set_posted(self, *, channel: str, source_id: str, category: str, limit_n: int) -> None:
        await self.execute(
            """
            INSERT INTO news_posted(channel,source_id,category,limit_n,last_posted_ts)
            VALUES(?,?,?,?,?)
            ON CONFLICT(channel,source_id,category,limit_n) DO UPDATE SET last_posted_ts=excluded.last_posted_ts
            """,
            (channel or "", source_id, category, int(limit_n), int(time.time())),
        )

    async def news_list_sources(self) -> list[sqlite3.Row]:
        return await self.fetchall("SELECT * FROM news_sources WHERE enabled=1 ORDER BY id ASC")

    # ---- Sysmon ----

    async def sys_event_add(self, *, level: str, source: str, kind: str, message: str) -> None:
        await self.execute(
            "INSERT INTO sys_events(ts,level,source,kind,message) VALUES(?,?,?,?,?)",
            (int(time.time()), level, source, kind, message),
        )

    async def sys_event_list(self, *, since_ts: int, limit: int = 50) -> list[sqlite3.Row]:
        return await self.fetchall(
            "SELECT * FROM sys_events WHERE ts>=? ORDER BY ts DESC LIMIT ?",
            (int(since_ts), int(limit)),
        )