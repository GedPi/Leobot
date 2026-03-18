from __future__ import annotations

# SQLite persistence: applies migrations, exposes async execute/fetch; provides settings, service enablement,
# ACL sessions/identities/command_perms/policies, and service-specific tables (news, greet, etc.).

import asyncio
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable

from system.migrations import apply_migrations

log = logging.getLogger("leobot.store")


# Single DB connection with asyncio lock; all access via execute/fetchone/fetchall or typed helpers.
class Store:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row
        self._lock = asyncio.Lock()
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=3000")

        apply_migrations(self._conn)

    # Closes the DB connection; safe to call from shutdown.
    async def close(self) -> None:
        async with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass

    # Runs a single SQL statement with params under the store lock; no return value.
    async def execute(self, sql: str, params: Iterable[Any] = ()) -> None:
        async with self._lock:
            self._conn.execute(sql, tuple(params))

    # Runs a statement once per row in seq under the store lock.
    async def executemany(self, sql: str, seq: Iterable[Iterable[Any]]) -> None:
        async with self._lock:
            self._conn.executemany(sql, [tuple(x) for x in seq])

    # Executes query and returns one row (or None) under the store lock.
    async def fetchone(self, sql: str, params: Iterable[Any] = ()):
        async with self._lock:
            cur = self._conn.execute(sql, tuple(params))
            return cur.fetchone()

    # Executes query and returns all rows under the store lock.
    async def fetchall(self, sql: str, params: Iterable[Any] = ()):
        async with self._lock:
            cur = self._conn.execute(sql, tuple(params))
            return cur.fetchall()

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

    async def is_service_enabled(self, channel: str, service: str) -> bool:
        row = await self.fetchone(
            "SELECT enabled FROM service_enablement WHERE channel=? AND service=?",
            (channel, service),
        )
        return bool(row[0]) if row else False

    async def set_service_enabled(
        self,
        channel: str,
        service: str,
        enabled: bool,
        updated_by: str | None = None,
    ) -> None:
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

    # Deletes acl_sessions with auth_until_ts <= now; returns number of rows deleted.
    async def prune_acl_sessions(self) -> int:
        now = int(time.time())
        async with self._lock:
            cur = self._conn.execute("DELETE FROM acl_sessions WHERE auth_until_ts<=?", (now,))
            return int(cur.rowcount or 0)

    async def acl_count_admins(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM acl_identities WHERE role='admin'", ())
        return int(row[0]) if row else 0

    async def acl_get_identity_role(self, ident: str) -> str | None:
        ident_l = (ident or "").strip().lower()
        if not ident_l:
            return None
        row = await self.fetchone("SELECT role FROM acl_identities WHERE ident=?", (ident_l,))
        return str(row[0]) if row else None

    async def acl_set_identity_role(self, ident: str, role: str) -> None:
        ident_l = (ident or "").strip().lower()
        if not ident_l:
            return
        now = int(time.time())
        await self.execute(
            "INSERT INTO acl_identities(ident,role,created_ts) VALUES(?,?,?) "
            "ON CONFLICT(ident) DO UPDATE SET role=excluded.role",
            (ident_l, role, now),
        )

    async def acl_del_identity(self, ident: str) -> None:
        ident_l = (ident or "").strip().lower()
        if not ident_l:
            return
        await self.execute("DELETE FROM acl_identities WHERE ident=?", (ident_l,))

    async def acl_list_identities(self, role: str) -> list[str]:
        rows = await self.fetchall(
            "SELECT ident FROM acl_identities WHERE role=? ORDER BY ident ASC",
            (role,),
        )
        return [str(r[0]) for r in rows] if rows else []

    async def acl_get_command_min_role(self, command: str) -> str | None:
        cmd = (command or "").strip().lower()
        if not cmd:
            return None
        row = await self.fetchone("SELECT min_role FROM acl_command_perms WHERE command=?", (cmd,))
        return str(row[0]) if row else None

    async def acl_set_command_min_role(self, command: str, min_role: str) -> None:
        cmd = (command or "").strip().lower()
        if not cmd:
            return
        now = int(time.time())
        await self.execute(
            "INSERT INTO acl_command_perms(command,min_role,updated_ts) VALUES(?,?,?) "
            "ON CONFLICT(command) DO UPDATE SET min_role=excluded.min_role, updated_ts=excluded.updated_ts",
            (cmd, min_role, now),
        )

    async def acl_del_command_min_role(self, command: str) -> None:
        cmd = (command or "").strip().lower()
        if not cmd:
            return
        await self.execute("DELETE FROM acl_command_perms WHERE command=?", (cmd,))

    async def acl_list_command_perms(self) -> list[tuple[str, str]]:
        rows = await self.fetchall(
            "SELECT command, min_role FROM acl_command_perms ORDER BY min_role DESC, command ASC",
            (),
        )
        return [(str(r[0]), str(r[1])) for r in rows] if rows else []

    async def acl_get_policy(self, channel: str, service_id: str, capability: str) -> str | None:
        if not channel or not service_id or not capability:
            return None
        row = await self.fetchone(
            "SELECT min_role FROM acl_policies WHERE channel=? AND service_id=? AND capability=?",
            (channel, service_id.strip().lower(), capability.strip().lower()),
        )
        return str(row[0]) if row else None

    async def acl_set_policy(self, channel: str, service_id: str, capability: str, min_role: str) -> None:
        if not channel or not service_id or not capability:
            return
        now = int(time.time())
        ch, sid, cap = channel, service_id.strip().lower(), capability.strip().lower()
        await self.execute(
            "INSERT INTO acl_policies(channel, service_id, capability, min_role, updated_ts) VALUES(?,?,?,?,?) "
            "ON CONFLICT(channel, service_id, capability) DO UPDATE SET min_role=excluded.min_role, updated_ts=excluded.updated_ts",
            (ch, sid, cap, (min_role or "user").strip().lower(), now),
        )

    async def acl_del_policy(self, channel: str, service_id: str, capability: str) -> None:
        if not channel or not service_id or not capability:
            return
        await self.execute(
            "DELETE FROM acl_policies WHERE channel=? AND service_id=? AND capability=?",
            (channel, service_id.strip().lower(), capability.strip().lower()),
        )

    async def acl_list_policies(self, channel: str | None = None) -> list[tuple[str, str, str, str]]:
        if channel is not None and channel != "":
            rows = await self.fetchall(
                "SELECT channel, service_id, capability, min_role FROM acl_policies WHERE channel=? ORDER BY service_id, capability",
                (channel,),
            )
        else:
            rows = await self.fetchall(
                "SELECT channel, service_id, capability, min_role FROM acl_policies ORDER BY channel, service_id, capability",
                (),
            )
        return [(str(r[0]), str(r[1]), str(r[2]), str(r[3])) for r in rows] if rows else []

    async def fact_insert(self, category: str, fact: str) -> None:
        cat = (category or "").strip()
        txt = (fact or "").strip()
        if not cat or not txt:
            return
        await self.execute("INSERT INTO facts(category, fact) VALUES(?, ?)", (cat, txt))

    async def fact_get_random(self) -> tuple[str, str] | None:
        row = await self.fetchone("SELECT category, fact FROM facts ORDER BY RANDOM() LIMIT 1", ())
        return (str(row[0]).strip(), str(row[1])) if row else None

    async def fact_get_random_by_category(self, category: str) -> tuple[str, str] | None:
        cat = (category or "").strip().lower()
        if not cat:
            return None
        row = await self.fetchone(
            "SELECT category, fact FROM facts WHERE LOWER(TRIM(category)) = ? ORDER BY RANDOM() LIMIT 1",
            (cat,),
        )
        return (str(row[0]).strip(), str(row[1])) if row else None

    async def fact_list_categories(self) -> list[str]:
        rows = await self.fetchall("SELECT DISTINCT category FROM facts ORDER BY category", ())
        return [str(r[0]).strip() for r in rows] if rows else []

    # ---- Fact auto (random facts with min/max per day) ----
    async def fact_auto_is_enabled(self, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT enabled FROM fact_auto_enablement WHERE channel=?",
            (channel.strip(),),
        )
        return bool(row and row[0]) if row else False

    async def fact_auto_set_enabled(
        self, channel: str, enabled: bool, *, updated_by: str | None = None
    ) -> None:
        ch = channel.strip()
        now = int(time.time())
        await self.execute(
            "INSERT INTO fact_auto_enablement(channel, enabled, updated_ts, updated_by) VALUES(?,?,?,?) "
            "ON CONFLICT(channel) DO UPDATE SET enabled=excluded.enabled, updated_ts=excluded.updated_ts, updated_by=excluded.updated_by",
            (ch, 1 if enabled else 0, now, updated_by or None),
        )

    async def fact_auto_get_posted_count(self, channel: str, day: str) -> int:
        row = await self.fetchone(
            "SELECT count FROM fact_auto_posted WHERE channel=? AND day=?",
            (channel.strip(), day),
        )
        return int(row[0]) if row else 0

    async def fact_auto_increment_posted(self, channel: str, day: str) -> None:
        ch = channel.strip()
        await self.execute(
            "INSERT INTO fact_auto_posted(channel, day, count) VALUES(?,?,1) "
            "ON CONFLICT(channel, day) DO UPDATE SET count=count+1",
            (ch, day),
        )

    async def fact_auto_get_min_max(self) -> tuple[int, int]:
        min_v = await self.get_setting("fact_auto_min_per_day", "6")
        max_v = await self.get_setting("fact_auto_max_per_day", "12")
        try:
            mi, ma = int(min_v), int(max_v)
            if mi < 1 or ma < mi:
                mi, ma = 6, 12
            return mi, ma
        except (TypeError, ValueError):
            return 6, 12

    async def fact_auto_list_enabled_channels(self) -> list[str]:
        rows = await self.fetchall(
            "SELECT channel FROM fact_auto_enablement WHERE enabled=1", ()
        )
        return [str(r[0]).strip() for r in rows] if rows else []

    async def fact_irc_log_recent_speaker(self, channel: str, exclude_nick: str) -> str | None:
        """Returns a random nick from recent irc_log speakers in channel, or None."""
        since_ts = int(time.time()) - (7 * 24 * 3600)
        ex = (exclude_nick or "").strip().lower()
        row = await self.fetchone(
            """
            SELECT actor_nick FROM irc_log
            WHERE channel=? AND actor_nick IS NOT NULL AND lower(actor_nick) != ?
              AND ts > ? AND event IN ('PRIVMSG','ACTION')
            GROUP BY lower(actor_nick)
            ORDER BY RANDOM()
            LIMIT 1
            """,
            (channel.strip(), ex or "__none__", since_ts),
        )
        return str(row[0]).strip() if row else None

    # ---- Lover service (pickup lines + targets + pacing) ----
    async def lover_line_insert(self, line: str, enabled: bool = True) -> None:
        txt = (line or "").strip()
        if not txt:
            return
        await self.execute(
            "INSERT INTO lover_lines(line, enabled) VALUES(?, ?)",
            (txt, 1 if enabled else 0),
        )

    async def lover_line_get_random(self) -> str | None:
        row = await self.fetchone(
            "SELECT line FROM lover_lines WHERE enabled=1 ORDER BY RANDOM() LIMIT 1",
            (),
        )
        return str(row[0]).strip() if row else None

    async def lover_line_count_enabled(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM lover_lines WHERE enabled=1", ())
        return int(row[0]) if row else 0

    async def lover_target_upsert(
        self,
        nick: str,
        channel: str,
        *,
        enabled: bool = True,
        created_by: str | None = None,
    ) -> None:
        n = (nick or "").strip()
        ch = (channel or "").strip()
        if not n or not ch:
            return
        now = int(time.time())
        await self.execute(
            "INSERT INTO lover_targets(nick, channel, enabled, created_ts, updated_ts, created_by) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(nick, channel) DO UPDATE SET enabled=excluded.enabled, updated_ts=excluded.updated_ts, created_by=COALESCE(lover_targets.created_by, excluded.created_by)",
            (n, ch, 1 if enabled else 0, now, now, created_by),
        )

    async def lover_target_set_enabled(self, nick: str, channel: str, enabled: bool) -> None:
        n = (nick or "").strip()
        ch = (channel or "").strip()
        if not n or not ch:
            return
        now = int(time.time())
        await self.execute(
            "UPDATE lover_targets SET enabled=?, updated_ts=? WHERE nick=? AND channel=?",
            (1 if enabled else 0, now, n, ch),
        )

    async def lover_target_exists(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM lover_targets WHERE nick=? AND channel=? LIMIT 1",
            ((nick or "").strip(), (channel or "").strip()),
        )
        return row is not None

    async def lover_targets_list(self, channel: str | None = None, enabled_only: bool = True) -> list[tuple[str, str]]:
        if channel:
            if enabled_only:
                rows = await self.fetchall(
                    "SELECT nick, channel FROM lover_targets WHERE channel=? AND enabled=1 ORDER BY channel, nick",
                    (channel.strip(),),
                )
            else:
                rows = await self.fetchall(
                    "SELECT nick, channel FROM lover_targets WHERE channel=? ORDER BY channel, nick",
                    (channel.strip(),),
                )
        else:
            if enabled_only:
                rows = await self.fetchall(
                    "SELECT nick, channel FROM lover_targets WHERE enabled=1 ORDER BY channel, nick",
                    (),
                )
            else:
                rows = await self.fetchall(
                    "SELECT nick, channel FROM lover_targets ORDER BY channel, nick",
                    (),
                )
        return [(str(r[0]).strip(), str(r[1]).strip()) for r in rows] if rows else []

    async def lover_enablement_is_enabled(self, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT enabled FROM lover_enablement WHERE channel=?",
            ((channel or "").strip(),),
        )
        return bool(row and row[0]) if row else False

    async def lover_enablement_set(
        self, channel: str, enabled: bool, *, updated_by: str | None = None
    ) -> None:
        ch = (channel or "").strip()
        if not ch:
            return
        now = int(time.time())
        await self.execute(
            "INSERT INTO lover_enablement(channel, enabled, updated_ts, updated_by) VALUES(?,?,?,?) "
            "ON CONFLICT(channel) DO UPDATE SET enabled=excluded.enabled, updated_ts=excluded.updated_ts, updated_by=excluded.updated_by",
            (ch, 1 if enabled else 0, now, updated_by),
        )

    async def lover_enablement_list_enabled_channels(self) -> list[str]:
        rows = await self.fetchall(
            "SELECT channel FROM lover_enablement WHERE enabled=1 ORDER BY channel",
            (),
        )
        return [str(r[0]).strip() for r in rows] if rows else []

    async def lover_daily_count_get(self, nick: str, day: str) -> int:
        row = await self.fetchone(
            "SELECT count FROM lover_daily_counts WHERE nick=? AND day=?",
            ((nick or "").strip().lower(), day),
        )
        return int(row[0]) if row else 0

    async def lover_daily_count_increment(self, nick: str, day: str) -> None:
        n = (nick or "").strip().lower()
        if not n:
            return
        await self.execute(
            "INSERT INTO lover_daily_counts(nick, day, count) VALUES(?,?,1) "
            "ON CONFLICT(nick, day) DO UPDATE SET count=count+1",
            (n, day),
        )

    async def lover_get_min_max(self) -> tuple[int, int]:
        min_v = await self.get_setting("lover_min_per_user_per_day", "4")
        max_v = await self.get_setting("lover_max_per_user_per_day", "8")
        try:
            mi = int(min_v)
            ma = int(max_v)
            if mi < 1 or ma < mi:
                return 4, 8
            return mi, ma
        except (TypeError, ValueError):
            return 4, 8

    async def lover_set_min_max(self, minimum: int | None = None, maximum: int | None = None) -> None:
        if minimum is not None:
            await self.set_setting("lover_min_per_user_per_day", str(int(minimum)))
        if maximum is not None:
            await self.set_setting("lover_max_per_user_per_day", str(int(maximum)))

    async def lover_public_cooldown_ready(self, channel: str, cooldown_seconds: int) -> bool:
        row = await self.fetchone(
            "SELECT last_public_ts FROM lover_public_cooldowns WHERE channel=?",
            ((channel or "").strip(),),
        )
        if not row:
            return True
        last_ts = int(row[0] or 0)
        return int(time.time()) >= (last_ts + int(cooldown_seconds))

    async def lover_public_cooldown_mark_now(self, channel: str) -> None:
        ch = (channel or "").strip()
        if not ch:
            return
        now = int(time.time())
        await self.execute(
            "INSERT INTO lover_public_cooldowns(channel, last_public_ts) VALUES(?,?) "
            "ON CONFLICT(channel) DO UPDATE SET last_public_ts=excluded.last_public_ts",
            (ch, now),
        )

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

    async def news_set_category(self, source_id: str, category: str, url: str) -> None:
        """
        Upsert category URL for a source.

        NOTE: Some older schemas don't have created_ts/updated_ts on news_source_categories.
        We deliberately only depend on (source_id, category, url) to stay compatible.
        """
        await self.execute(
            "INSERT INTO news_source_categories(source_id,category,url) VALUES(?,?,?) "
            "ON CONFLICT(source_id,category) DO UPDATE SET url=excluded.url",
            (source_id, category, url),
        )

    async def news_list_categories(self, source_id: str):
        """List categories for a source (used by services/news.py)."""
        return await self.fetchall(
            "SELECT category, url FROM news_source_categories WHERE source_id=? ORDER BY category ASC",
            (source_id,),
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

    # ---- Greet helpers (pool approach handled in greet.py + migrations; store keeps target selection) ----
    async def greet_select_target(
        self,
        *,
        nick: str,
        hostmask: str,
        userhost: str,
        host: str,
        channel: str,
    ) -> sqlite3.Row | None:
        """Select the highest priority greeting target that matches this identity."""
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
                # Back-compat: if pattern looks like a mask => match it against mask/userhost/host.
                if ("!" in pat) or ("@" in pat):
                    if not (fnmatch.fnmatch(hm, pat) or fnmatch.fnmatch(uh, pat) or fnmatch.fnmatch(h, pat)):
                        continue
                else:
                    if not fnmatch.fnmatch(h, pat):
                        continue

            return r

        return None

    async def greet_pick_greeting(self, target_id: int) -> str | None:
        # NOTE: If you're running greet pools, greet.py should be resolving to pool_id and storing greetings by pool.
        # This method is kept for compatibility; if your DB has greetings.pool_id instead of target_id,
        # greet.py should not call this path.
        cols = await self._table_columns("greetings")

        if "pool_id" in cols:
            row = await self.fetchone("SELECT pool_id FROM greet_targets WHERE id=?", (int(target_id),))
            if not row or row[0] is None:
                return None
            rows = await self.fetchall(
                "SELECT id,text,weight FROM greetings WHERE pool_id=? AND enabled=1",
                (int(row[0]),),
            )
        else:
            rows = await self.fetchall(
                "SELECT id,text,weight FROM greetings WHERE target_id=? AND enabled=1",
                (int(target_id),),
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

    async def _table_columns(self, table: str) -> set[str]:
        async with self._lock:
            try:
                rows = self._conn.execute(f"PRAGMA table_info({table})").fetchall()
                return {str(r[1]) for r in rows}
            except Exception:
                return set()

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
        chan = target_channel if target_channel is not None else channel
        if not chan:
            return []
        rows = await self.fetchall(
            "SELECT * FROM weather_watches WHERE target_channel=? ORDER BY id",
            (str(chan),),
        )
        return list(rows)

    async def weather_watch_clear(self, channel: str | None = None, *, target_channel: str | None = None) -> int:
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

    # ---- Pokemon service ----
    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_species_get_random(self, limit: int = 5) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows) if rows else []

    async def pokemon_species_get(self, species_id: int):
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))

    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return row is not None

    async def pokemon_trainer_create(
        self,
        nick: str,
        channel: str,
        *,
        starter_species_ids: list[int],
        starter_items: list[tuple[str, int]] | None = None,
    ) -> None:
        now = int(time.time())
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        if not nick_l or not chan:
            return
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick_l, chan, now),
        )
        items = starter_items or [
            ("potion", 5),
            ("revive", 2),
            ("pokeball", 10),
        ]
        for item_id, qty in items:
            await self.execute(
                """INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity)
                   VALUES(?,?,?,?) ON CONFLICT(trainer_nick,channel,item_id) DO UPDATE
                   SET quantity=quantity+excluded.quantity""",
                (nick_l, chan, item_id, qty),
            )
        for slot, sid in enumerate(starter_species_ids[:6], 1):
            sp = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(sid),))
            if not sp:
                continue
            hp = max(10, int(sp["hp_base"] or 50) + (int(sp["hp_base"] or 50) * 2) // 10)
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(
                   trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (nick_l, chan, int(sid), 5, hp, hp, slot, now),
            )

    async def pokemon_trainer_get_pokemon(self, nick: str, channel: str) -> list:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        if not nick_l or not chan:
            return []
        rows = await self.fetchall(
            """SELECT p.*, s.name as species_name, s.type1, s.type2
               FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON p.species_id = s.id
               WHERE p.trainer_nick=? AND p.channel=? ORDER BY p.slot""",
            (nick_l, chan),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_get_items(self, nick: str, channel: str) -> list:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        if not nick_l or not chan:
            return []
        rows = await self.fetchall(
            """SELECT ti.item_id, ti.quantity, i.name, i.item_type, i.effect_value
               FROM pokemon_trainer_items ti
               JOIN pokemon_items i ON ti.item_id = i.id
               WHERE ti.trainer_nick=? AND ti.channel=? AND ti.quantity>0
               ORDER BY i.item_type, i.name""",
            (nick_l, chan),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_use_item(
        self,
        nick: str,
        channel: str,
        item_id: str,
        pokemon_id: int | None = None,
    ) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        if not nick_l or not chan:
            return False, "Invalid trainer."
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, chan, (item_id or "").strip()),
        )
        if not row or int(row[0] or 0) < 1:
            return False, "You don't have that item."
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", ((item_id or "").strip(),))
        if not item:
            return False, "Unknown item."
        itype = str(item["item_type"] or "").lower()
        if itype == "heal":
            if pokemon_id is None:
                return False, "Specify a Pokémon: !use potion <slot>"
            poke = await self.fetchone(
                "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
                (int(pokemon_id), nick_l, chan),
            )
            if not poke:
                return False, "Pokémon not found."
            if poke["is_fainted"]:
                return False, "That Pokémon has fainted. Use a Revive."
            healed = min(int(item["effect_value"] or 0), int(poke["max_hp"] or 0) - int(poke["current_hp"] or 0))
            if healed <= 0:
                return False, "That Pokémon is already at full HP."
            new_hp = int(poke["current_hp"] or 0) + healed
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?",
                (min(new_hp, int(poke["max_hp"] or 0)), int(pokemon_id)),
            )
        elif itype == "revive":
            if pokemon_id is None:
                return False, "Specify a Pokémon: !use revive <slot>"
            poke = await self.fetchone(
                "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
                (int(pokemon_id), nick_l, chan),
            )
            if not poke:
                return False, "Pokémon not found."
            if not poke["is_fainted"]:
                return False, "That Pokémon is not fainted."
            pct = int(item["effect_value"] or 50) / 100.0
            new_hp = int(int(poke["max_hp"] or 0) * pct)
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
                (new_hp, int(pokemon_id)),
            )
        elif itype == "ball":
            return False, "Use Poké Balls in the channel with !capture when a wild Pokémon appears."
        else:
            return False, "That item can't be used that way."
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, chan, (item_id or "").strip()),
        )
        return True, "Used successfully."

    async def pokemon_wild_spawn_get(self, channel: str):
        return await self.fetchone(
            """SELECT * FROM pokemon_wild_spawns w
               JOIN pokemon_species s ON w.species_id = s.id
               WHERE w.channel=? AND w.expires_ts>? AND w.captured_by IS NULL""",
            ((channel or "").strip(), int(time.time())),
        )

    async def pokemon_wild_spawn_create(self, channel: str, species_id: int, level: int = 5) -> bool:
        now = int(time.time())
        chan = (channel or "").strip()
        expires = now + 600
        try:
            await self.execute(
                """INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
                   VALUES(?,?,?,?,?)""",
                (chan, int(species_id), int(level), now, expires),
            )
            return True
        except Exception:
            return False

    async def pokemon_wild_spawn_capture(
        self,
        channel: str,
        nick: str,
        *,
        ball_modifier: float = 1.0,
    ) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        spawn = await self.fetchone(
            """SELECT w.*, s.name, s.capture_rate FROM pokemon_wild_spawns w
               JOIN pokemon_species s ON w.species_id = s.id
               WHERE w.channel=? AND w.expires_ts>? AND w.captured_by IS NULL""",
            (chan, int(time.time())),
        )
        if not spawn:
            return False, "No wild Pokémon here (or it already fled)."
        import random
        cap_rate = min(255, int(spawn["capture_rate"] or 255))
        effective_rate = min(255, int(cap_rate * ball_modifier))
        roll = random.randint(1, 255)
        if roll <= effective_rate:
            await self.execute(
                "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?",
                (nick_l, int(spawn["id"])),
            )
            now = int(time.time())
            await self.execute(
                """INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)""",
                (nick_l, chan, now),
            )
            max_slot = await self.fetchone(
                "SELECT COALESCE(MAX(slot),0) FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
                (nick_l, chan),
            )
            slot = int(max_slot[0] or 0) + 1
            if slot > 6:
                await self.execute(
                    "UPDATE pokemon_wild_spawns SET captured_by=NULL WHERE id=?",
                    (int(spawn["id"]),),
                )
                return False, "Your party is full! Release a Pokémon first."
            sp = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(spawn["species_id"]),))
            hp = max(10, int(sp["hp_base"] or 50) + (int(sp["hp_base"] or 50) * 2) // 10)
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(
                   trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (nick_l, chan, int(spawn["species_id"]), int(spawn["level"] or 5), hp, hp, slot, now),
            )
            await self.execute(
                "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?",
                (nick_l, int(spawn["id"])),
            )
            return True, str(spawn["name"])
        return False, "The Pokémon broke free!"

    async def pokemon_wild_spawn_count_24h(self, channel: str) -> int:
        since = int(time.time()) - 86400
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts>=?",
            ((channel or "").strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_get_spawns_per_day(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(1000, int(v)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_ensure_trainer(
        self,
        nick: str,
        channel: str,
        *,
        starter_species_ids: list[int] | None = None,
    ) -> bool:
        if await self.pokemon_trainer_exists(nick, channel):
            return False
        if starter_species_ids is None:
            species = await self.pokemon_species_get_random(5)
            starter_species_ids = [int(s["id"]) for s in species] if species else []
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        if not nick_l or not chan:
            return False
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick_l, chan, now),
        )
        for item_id, qty in [("potion", 5), ("revive", 2), ("pokeball", 10)]:
            await self.execute(
                """INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity)
                   VALUES(?,?,?,?) ON CONFLICT(trainer_nick,channel,item_id) DO UPDATE
                   SET quantity=quantity+excluded.quantity""",
                (nick_l, chan, item_id, qty),
            )
        for slot, sid in enumerate(starter_species_ids[:6], 1):
            sp = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(sid),))
            if not sp:
                continue
            hp = max(10, int(sp["hp_base"] or 50) + (int(sp["hp_base"] or 50) * 2) // 10)
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(
                   trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (nick_l, chan, int(sid), 5, hp, hp, slot, now),
            )
        return True

    async def pokemon_trainer_add_item(self, nick: str, channel: str, item_id: str, quantity: int = 1) -> None:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        await self.execute(
            """INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity)
               VALUES(?,?,?,?) ON CONFLICT(trainer_nick,channel,item_id) DO UPDATE
               SET quantity=quantity+excluded.quantity""",
            (nick_l, chan, (item_id or "").strip(), int(quantity)),
        )

    async def pokemon_trainer_deduct_item(
        self, nick: str, channel: str, item_id: str, quantity: int = 1
    ) -> bool:
        """Deduct item from trainer; returns True if had enough and deducted."""
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, chan, (item_id or "").strip()),
        )
        if not row or int(row[0] or 0) < quantity:
            return False
        await self.execute(
            """UPDATE pokemon_trainer_items SET quantity=quantity-? 
               WHERE trainer_nick=? AND channel=? AND item_id=? AND quantity>=?""",
            (quantity, nick_l, chan, (item_id or "").strip(), quantity),
        )
        return True

    # ---- Pokemon service (spawns config) ----
    async def pokemon_get_spawns_per_day(self) -> int:
        val = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(1440, int(val)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_spawns_per_day(self, n: int) -> None:
        n = max(1, min(1440, int(n)))
        await self.set_setting("pokemon_wild_spawns_per_day", str(n))

    async def pokemon_count_species(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_get_random_species(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return row is not None

    async def pokemon_trainer_create(
        self, nick: str, channel: str, *, species_ids: list[int], starter_items: list[tuple[str, int]]
    ) -> None:
        now = int(time.time())
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick_l, ch, now),
        )
        for slot, sp_id in enumerate(species_ids[:6], 1):
            species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(sp_id),))
            if species:
                hp = max(10, int(species["hp_base"] or 50) * 2 + 50)
                await self.execute(
                    """INSERT INTO pokemon_trainer_pokemon(
                        trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts
                    ) VALUES(?,?,?,5,?,?,?,?)""",
                    (nick_l, ch, int(sp_id), hp, hp, slot, now),
                )
        for item_id, qty in starter_items:
            await self.execute(
                "INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity) VALUES(?,?,?,?) "
                "ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+excluded.quantity",
                (nick_l, ch, str(item_id), int(qty)),
            )

    async def pokemon_trainer_get_pokemon(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT p.*, s.name as species_name, s.type1, s.type2
               FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON p.species_id = s.id
               WHERE p.trainer_nick=? AND p.channel=?
               ORDER BY p.slot""",
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_get_items(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT i.id, i.name, i.item_type, i.effect_value, t.quantity
               FROM pokemon_trainer_items t
               JOIN pokemon_items i ON t.item_id = i.id
               WHERE t.trainer_nick=? AND t.channel=? AND t.quantity > 0
               ORDER BY i.name""",
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_use_heal(
        self, nick: str, channel: str, item_id: str, pokemon_id: int
    ) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=? AND item_type='heal'", (item_id,))
        if not item:
            return False, "Invalid or wrong item type."
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        if not row or int(row[0] or 0) < 1:
            return False, "You don't have that item."
        pkmn = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
            (pokemon_id, nick_l, ch),
        )
        if not pkmn:
            return False, "Pokemon not found."
        if pkmn["is_fainted"]:
            return False, "Use a Revive on fainted Pokemon."
        heal = int(item["effect_value"] or 0)
        cur_hp = int(pkmn["current_hp"] or 0)
        max_hp = int(pkmn["max_hp"] or 1)
        new_hp = min(max_hp, cur_hp + heal)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?", (new_hp, pokemon_id)
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        return True, f"Healed {heal} HP. ({new_hp}/{max_hp})"

    async def pokemon_use_revive(
        self, nick: str, channel: str, item_id: str, pokemon_id: int
    ) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=? AND item_type='revive'", (item_id,))
        if not item:
            return False, "Invalid or wrong item type."
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        if not row or int(row[0] or 0) < 1:
            return False, "You don't have that item."
        pkmn = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
            (pokemon_id, nick_l, ch),
        )
        if not pkmn:
            return False, "Pokemon not found."
        if not pkmn["is_fainted"]:
            return False, "Pokemon is not fainted."
        pct = int(item["effect_value"] or 50)
        max_hp = int(pkmn["max_hp"] or 1)
        new_hp = max(1, (max_hp * pct) // 100)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
            (new_hp, pokemon_id),
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        return True, f"Revived! HP: {new_hp}/{max_hp}"

    async def pokemon_wild_get_active(self, channel: str):
        return await self.fetchone(
            """SELECT w.*, s.name as species_name, s.type1, s.type2, s.capture_rate
               FROM pokemon_wild_spawns w
               JOIN pokemon_species s ON w.species_id = s.id
               WHERE w.channel=? AND w.expires_ts > ? AND w.captured_by IS NULL""",
            ((channel or "").strip(), int(time.time())),
        )

    async def pokemon_wild_spawn(self, channel: str, species_id: int, level: int, duration_s: int) -> bool:
        now = int(time.time())
        ch = (channel or "").strip()
        existing = await self.fetchone(
            "SELECT 1 FROM pokemon_wild_spawns WHERE channel=? AND captured_by IS NULL AND expires_ts > ?",
            (ch, now),
        )
        if existing:
            return False
        expires = now + int(duration_s)
        await self.execute(
            """INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
               VALUES(?,?,?,?,?)""",
            (ch, int(species_id), int(level), now, expires),
        )
        return True

    async def pokemon_wild_count_last_24h(self, channel: str) -> int:
        since = int(time.time()) - 86400
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            ((channel or "").strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_wild_capture(
        self, channel: str, nick: str, spawn_id: int, ball_id: str
    ) -> tuple[bool, str]:
        ch = (channel or "").strip()
        nick_l = (nick or "").strip().lower()
        spawn = await self.fetchone(
            "SELECT * FROM pokemon_wild_spawns WHERE id=? AND channel=? AND captured_by IS NULL AND expires_ts > ?",
            (spawn_id, ch, int(time.time())),
        )
        if not spawn:
            return False, "That Pokemon is gone or already captured."
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=? AND item_type='ball'", (ball_id,))
        if not item:
            return False, "Invalid ball."
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, ball_id),
        )
        if not row or int(row[0] or 0) < 1:
            return False, "You don't have that ball."
        import random
        rate = int(spawn["capture_rate"] or 255) if hasattr(spawn, "keys") else 255
        if isinstance(spawn, dict):
            species_row = await self.fetchone("SELECT capture_rate FROM pokemon_species WHERE id=?", (spawn["species_id"],))
            rate = int(species_row["capture_rate"] or 255) if species_row else 255
        bonus = {"pokeball": 1, "great_ball": 1.5, "ultra_ball": 2}.get(ball_id, 1)
        roll = random.random()
        threshold = min(0.95, (rate / 255) * bonus * 0.4)
        if roll > threshold:
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (nick_l, ch, ball_id),
            )
            return False, "The Pokemon broke free!"
        now = int(time.time())
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?", (nick_l, spawn_id)
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, ball_id),
        )
        trainer_exists = await self.pokemon_trainer_exists(nick_l, ch)
        if not trainer_exists:
            await self.execute(
                "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
                (nick_l, ch, now),
            )
        species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (spawn["species_id"],))
        max_hp = max(10, int(species["hp_base"] or 50) * 2 + 50) if species else 50
        slot = 1
        row = await self.fetchone(
            "SELECT COALESCE(MAX(slot),0)+1 FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
            (nick_l, ch),
        )
        if row:
            slot = min(6, int(row[0] or 1))
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon(
                trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts
            ) VALUES(?,?,?,?,?,?,?,?)""",
            (nick_l, ch, spawn["species_id"], spawn["level"], max_hp, max_hp, slot, now),
        )
        return True, "Caught!"

    async def pokemon_level_up(self, nick: str, channel: str, pokemon_id: int) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        pkmn = await self.fetchone(
            """SELECT p.*, s.hp_base FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON p.species_id = s.id
               WHERE p.id=? AND p.trainer_nick=? AND p.channel=?""",
            (pokemon_id, nick_l, ch),
        )
        if not pkmn:
            return False, "Pokemon not found."
        lvl = int(pkmn["level"] or 5)
        if lvl >= 100:
            return False, "Already max level."
        new_lvl = lvl + 1
        hp_base = int(pkmn["hp_base"] or 50)
        new_max_hp = max(10, hp_base * 2 + (new_lvl * 2))
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, max_hp=?, current_hp=? WHERE id=?",
            (new_lvl, new_max_hp, new_max_hp, pokemon_id),
        )
        return True, f"Level up! Now level {new_lvl} (HP: {new_max_hp})"

    async def pokemon_delete_expired_wild(self) -> int:
        now = int(time.time())
        async with self._lock:
            cur = self._conn.execute("DELETE FROM pokemon_wild_spawns WHERE expires_ts <= ?", (now,))
            return int(cur.rowcount or 0)

    # ---- Pokemon service ----
    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_species_get_random(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return False
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            (nick_l, ch),
        )
        return row is not None

    async def pokemon_trainer_create(self, nick: str, channel: str) -> None:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick_l, ch, now),
        )

    async def pokemon_trainer_add_pokemon(
        self,
        nick: str,
        channel: str,
        species_id: int,
        *,
        level: int = 5,
        nickname: str | None = None,
    ) -> int:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return 0
        now = int(time.time())
        species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))
        if not species:
            return 0
        hp = max(10, int(species["hp_base"]) + (level - 1) * 2)
        slot = 1
        row = await self.fetchone(
            "SELECT COALESCE(MAX(slot), 0) + 1 FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
            (nick_l, ch),
        )
        if row and row[0]:
            slot = int(row[0])
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon(
                trainer_nick, channel, species_id, nickname, level, current_hp, max_hp, experience, is_fainted, slot, created_ts
            ) VALUES(?,?,?,?,?,?,?,0,0,?,?)""",
            (nick_l, ch, int(species_id), nickname or None, level, hp, hp, min(6, slot), now),
        )
        row = await self.fetchone("SELECT last_insert_rowid()", ())
        return int(row[0]) if row else 0

    async def pokemon_trainer_add_items(self, nick: str, channel: str, items: list[tuple[str, int]]) -> None:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch or not items:
            return
        for item_id, qty in items:
            await self.execute(
                """INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity)
                VALUES(?,?,?,?) ON CONFLICT(trainer_nick, channel, item_id)
                DO UPDATE SET quantity=quantity+excluded.quantity""",
                (nick_l, ch, str(item_id), int(qty)),
            )

    async def pokemon_trainer_get_pokemon(self, nick: str, channel: str) -> list:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return []
        rows = await self.fetchall(
            """SELECT p.*, s.name as species_name, s.type1, s.type2
            FROM pokemon_trainer_pokemon p
            JOIN pokemon_species s ON p.species_id = s.id
            WHERE p.trainer_nick=? AND p.channel=?
            ORDER BY p.slot ASC""",
            (nick_l, ch),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_get_items(self, nick: str, channel: str) -> list:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return []
        rows = await self.fetchall(
            """SELECT ti.item_id, ti.quantity, i.name, i.item_type, i.effect_value
            FROM pokemon_trainer_items ti
            JOIN pokemon_items i ON ti.item_id = i.id
            WHERE ti.trainer_nick=? AND ti.channel=? AND ti.quantity > 0
            ORDER BY i.item_type, i.name""",
            (nick_l, ch),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_use_heal(
        self, nick: str, channel: str, item_id: str, pokemon_slot: int
    ) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return False, "Invalid trainer."
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=? AND item_type='heal'", (item_id,))
        if not item:
            return False, "Invalid or unavailable item."
        inv = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        if not inv or int(inv[0]) < 1:
            return False, "You don't have any."
        poke = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? AND slot=?",
            (nick_l, ch, pokemon_slot),
        )
        if not poke:
            return False, "No Pokémon in that slot."
        if poke["is_fainted"]:
            return False, "Use a Revive on fainted Pokémon."
        heal_amt = int(item["effect_value"])
        new_hp = min(int(poke["max_hp"]), int(poke["current_hp"]) + heal_amt)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?",
            (new_hp, poke["id"]),
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        return True, f"Healed {heal_amt} HP. {poke['nickname'] or 'Pokémon'} now has {new_hp}/{poke['max_hp']} HP."

    async def pokemon_trainer_use_revive(
        self, nick: str, channel: str, item_id: str, pokemon_slot: int
    ) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return False, "Invalid trainer."
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=? AND item_type='revive'", (item_id,))
        if not item:
            return False, "Invalid or unavailable item."
        inv = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        if not inv or int(inv[0]) < 1:
            return False, "You don't have any."
        poke = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? AND slot=?",
            (nick_l, ch, pokemon_slot),
        )
        if not poke:
            return False, "No Pokémon in that slot."
        if not poke["is_fainted"]:
            return False, "That Pokémon isn't fainted."
        pct = int(item["effect_value"])
        new_hp = max(1, int(poke["max_hp"]) * pct // 100)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
            (new_hp, poke["id"]),
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        return True, f"Revived! {poke['nickname'] or 'Pokémon'} has {new_hp}/{poke['max_hp']} HP."

    async def pokemon_wild_get(self, channel: str) -> sqlite3.Row | None:
        ch = (channel or "").strip()
        if not ch:
            return None
        now = int(time.time())
        return await self.fetchone(
            """SELECT w.*, s.name as species_name, s.type1, s.type2, s.capture_rate
            FROM pokemon_wild_spawns w
            JOIN pokemon_species s ON w.species_id = s.id
            WHERE w.channel=? AND w.expires_ts > ? AND w.captured_by IS NULL""",
            (ch, now),
        )

    async def pokemon_wild_spawn_count_24h(self, channel: str) -> int:
        ch = (channel or "").strip()
        if not ch:
            return 0
        since = int(time.time()) - 86400
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            (ch, since),
        )
        return int(row[0]) if row else 0

    async def pokemon_wild_spawn(
        self, channel: str, species_id: int, level: int, duration_seconds: int = 600
    ) -> bool:
        ch = (channel or "").strip()
        if not ch:
            return False
        now = int(time.time())
        n_per_day = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            max_per_day = int(n_per_day)
        except Exception:
            max_per_day = 24
        if await self.pokemon_wild_spawn_count_24h(ch) >= max_per_day:
            return False
        existing = await self.pokemon_wild_get(ch)
        if existing:
            return False
        expires = now + duration_seconds
        await self.execute(
            "INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts) VALUES(?,?,?,?,?)",
            (ch, int(species_id), int(level), now, expires),
        )
        return True

    async def pokemon_wild_capture(
        self, channel: str, spawn_id: int, trainer_nick: str, ball_modifier: int = 255
    ) -> tuple[bool, str]:
        nick_l = (trainer_nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return False, "Invalid trainer."
        spawn = await self.fetchone(
            "SELECT * FROM pokemon_wild_spawns WHERE id=? AND channel=? AND captured_by IS NULL AND expires_ts > ?",
            (spawn_id, ch, int(time.time())),
        )
        if not spawn:
            return False, "That wild Pokémon is gone."
        species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (spawn["species_id"],))
        if not species:
            return False, "Unknown species."
        capture_rate = int(species["capture_rate"])
        # Simplified: (rate * ball_mod) / 255, then random check
        effective = (capture_rate * ball_modifier) // 255
        import random
        roll = random.randint(1, 255)
        caught = roll <= effective
        if caught:
            await self.execute(
                "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?",
                (nick_l, spawn_id),
            )
            await self.pokemon_trainer_create(nick_l, ch)
            await self.pokemon_trainer_add_pokemon(nick_l, ch, int(spawn["species_id"]), level=int(spawn["level"]))
            return True, f"Caught {species['name']}!"
        return False, "The wild Pokémon broke free."

    async def pokemon_wild_prune_expired(self, now_ts: int | None = None) -> int:
        now = int(now_ts or time.time())
        async with self._lock:
            cur = self._conn.execute("DELETE FROM pokemon_wild_spawns WHERE expires_ts <= ?", (now,))
            return int(cur.rowcount)

    async def pokemon_get_setting_spawns_per_day(self) -> int:
        val = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return int(val)
        except Exception:
            return 24

    async def pokemon_set_spawns_per_day(self, n: int) -> None:
        now = int(time.time())
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, min(1440, n))))

    # ---- Pokemon service ----
    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_species_get_random(self, limit: int = 5) -> list[sqlite3.Row]:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?", (int(limit),)
        )
        return list(rows)

    async def pokemon_species_get(self, species_id: int) -> sqlite3.Row | None:
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))

    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return False
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE lower(nick)=? AND channel=?",
            (nick_l, ch),
        )
        return row is not None

    async def pokemon_trainer_create(
        self, nick: str, channel: str, *, species_ids: list[int], starter_items: list[tuple[str, int]]
    ) -> None:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch or not species_ids:
            return
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick_l, ch, now),
        )
        for slot, sp_id in enumerate(species_ids[:6], 1):
            sp = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(sp_id),))
            if not sp:
                continue
            hp = max(10, (int(sp["hp_base"] or 50) * 2 + 10))
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(
                    trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts
                ) VALUES(?,?,?,5,?,?,?,?)""",
                (nick_l, ch, int(sp_id), hp, hp, slot, now),
            )
        for item_id, qty in starter_items:
            await self.execute(
                "INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity) VALUES(?,?,?,?) "
                "ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+excluded.quantity",
                (nick_l, ch, str(item_id), int(qty)),
            )

    async def pokemon_trainer_get_pokemon(self, nick: str, channel: str) -> list[sqlite3.Row]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return []
        rows = await self.fetchall(
            """SELECT p.*, s.name as species_name, s.type1, s.type2
               FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON p.species_id = s.id
               WHERE lower(p.trainer_nick)=? AND p.channel=? ORDER BY p.slot""",
            (nick_l, ch),
        )
        return list(rows)

    async def pokemon_trainer_get_items(self, nick: str, channel: str) -> list[sqlite3.Row]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return []
        rows = await self.fetchall(
            """SELECT ti.item_id, ti.quantity, i.name, i.item_type, i.effect_value
               FROM pokemon_trainer_items ti
               JOIN pokemon_items i ON ti.item_id = i.id
               WHERE lower(ti.trainer_nick)=? AND ti.channel=? AND ti.quantity > 0""",
            (nick_l, ch),
        )
        return list(rows)

    async def pokemon_trainer_use_item(
        self, nick: str, channel: str, item_id: str, *, target_slot: int | None = None
    ) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch or not item_id:
            return False, "Invalid parameters"
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (str(item_id),))
        if not item:
            return False, "Unknown item"
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE lower(trainer_nick)=? AND channel=? AND item_id=?",
            (nick_l, ch, str(item_id)),
        )
        if not row or int(row[0] or 0) < 1:
            return False, f"No {item['name']} left"
        item_type = str(item["item_type"] or "")
        effect = int(item["effect_value"] or 0)
        if item_type == "heal":
            pokemon = await self._pokemon_get_by_slot(nick_l, ch, target_slot)
            if not pokemon:
                return False, "No Pokemon to heal. Use: heal <slot>"
            if int(pokemon["is_fainted"] or 0):
                return False, "Can't heal a fainted Pokemon. Use a Revive first."
            current = int(pokemon["current_hp"] or 0)
            maximum = int(pokemon["max_hp"] or 1)
            if current >= maximum:
                return False, "Pokemon already at full HP"
            new_hp = min(maximum, current + effect)
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?",
                (new_hp, int(pokemon["id"])),
            )
            await self._pokemon_consume_item(nick_l, ch, item_id, 1)
            return True, f"Healed to {new_hp}/{maximum} HP"
        if item_type == "revive":
            pokemon = await self._pokemon_get_by_slot(nick_l, ch, target_slot)
            if not pokemon:
                return False, "No Pokemon to revive. Use: revive <slot>"
            if not int(pokemon["is_fainted"] or 0):
                return False, "Pokemon is not fainted"
            maximum = int(pokemon["max_hp"] or 1)
            restore = (maximum * effect) // 100
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
                (restore, int(pokemon["id"])),
            )
            await self._pokemon_consume_item(nick_l, ch, item_id, 1)
            return True, f"Revived with {restore}/{maximum} HP"
        return False, "Item not usable here"

    async def _pokemon_get_by_slot(
        self, nick_l: str, channel: str, slot: int | None
    ) -> sqlite3.Row | None:
        if slot is None or slot < 1 or slot > 6:
            return await self.fetchone(
                """SELECT * FROM pokemon_trainer_pokemon
                   WHERE lower(trainer_nick)=? AND channel=? AND is_fainted=0
                   ORDER BY slot LIMIT 1""",
                (nick_l, channel),
            )
        return await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE lower(trainer_nick)=? AND channel=? AND slot=?",
            (nick_l, channel, int(slot)),
        )

    async def _pokemon_consume_item(self, nick_l: str, channel: str, item_id: str, qty: int) -> None:
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-? WHERE lower(trainer_nick)=? AND channel=? AND item_id=? AND quantity>=?",
            (qty, nick_l, channel, item_id, qty),
        )
        await self.execute(
            "DELETE FROM pokemon_trainer_items WHERE lower(trainer_nick)=? AND channel=? AND item_id=? AND quantity<=0",
            (nick_l, channel, item_id),
        )

    async def pokemon_wild_spawns_today_count(self, channel: str) -> int:
        ch = (channel or "").strip()
        if not ch:
            return 0
        day_start = int(time.time()) - (24 * 3600)
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts>=?",
            (ch, day_start),
        )
        return int(row[0]) if row else 0

    async def pokemon_wild_get_active(self, channel: str) -> sqlite3.Row | None:
        ch = (channel or "").strip()
        if not ch:
            return None
        now = int(time.time())
        return await self.fetchone(
            """SELECT w.*, s.name as species_name, s.type1, s.type2, s.capture_rate
               FROM pokemon_wild_spawns w
               JOIN pokemon_species s ON w.species_id = s.id
               WHERE w.channel=? AND w.expires_ts > ? AND w.captured_by IS NULL""",
            (ch, now),
        )

    async def pokemon_wild_spawn(
        self, channel: str, species_id: int, level: int, *, duration_seconds: int = 900
    ) -> bool:
        ch = (channel or "").strip()
        if not ch:
            return False
        existing = await self.pokemon_wild_get_active(ch)
        if existing:
            return False
        now = int(time.time())
        expires = now + duration_seconds
        await self.execute(
            """INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
               VALUES(?,?,?,?,?)""",
            (ch, int(species_id), int(level), now, expires),
        )
        return True

    async def pokemon_wild_capture(
        self, channel: str, nick: str, *, ball_modifier: int = 255
    ) -> tuple[bool, str]:
        spawn = await self.pokemon_wild_get_active(channel)
        if not spawn:
            return False, "No wild Pokemon here"
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not await self.pokemon_trainer_exists(nick_l, ch):
            return False, "You're not a trainer. Join the channel and use !pokemon to get started."
        ball = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE lower(trainer_nick)=? AND channel=? AND item_id IN ('ultra_ball','great_ball','pokeball') AND quantity>0 ORDER BY CASE item_id WHEN 'ultra_ball' THEN 1 WHEN 'great_ball' THEN 2 ELSE 3 END LIMIT 1",
            (nick_l, ch),
        )
        if not ball:
            return False, "You need a Poké Ball to capture. Get some from the Poké Mart (PM the bot: items)."
        ball_id = "pokeball"
        ball_row = await self.fetchone(
            "SELECT item_id, quantity FROM pokemon_trainer_items WHERE lower(trainer_nick)=? AND channel=? AND item_id='ultra_ball' AND quantity>0",
            (nick_l, ch),
        )
        if ball_row:
            ball_id = "ultra_ball"
        else:
            br = await self.fetchone(
                "SELECT item_id FROM pokemon_trainer_items WHERE lower(trainer_nick)=? AND channel=? AND item_id='great_ball' AND quantity>0",
                (nick_l, ch),
            )
            if br:
                ball_id = "great_ball"
        ball_item = await self.fetchone("SELECT effect_value FROM pokemon_items WHERE id=?", (ball_id,))
        modifier = int(ball_item["effect_value"]) if ball_item else 255
        import random
        capture_rate = int(spawn["capture_rate"] or 255)
        catch_chance = min(255, (capture_rate * modifier) // 255)
        roll = random.randint(1, 255)
        if roll <= catch_chance:
            now = int(time.time())
            max_hp = max(10, (int(spawn["hp_base"] or 50) * 2 + 10) * int(spawn["level"] or 5) // 5)
            rows = await self.fetchall(
                "SELECT slot FROM pokemon_trainer_pokemon WHERE lower(trainer_nick)=? AND channel=? ORDER BY slot",
                (nick_l, ch),
            )
            slots = {int(r[0]) for r in rows}
            slot = 1
            for s in range(1, 7):
                if s not in slots:
                    slot = s
                    break
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(
                    trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts
                ) VALUES(?,?,?,?,?,?,?,?)""",
                (nick_l, ch, int(spawn["species_id"]), int(spawn["level"] or 5), max_hp, max_hp, slot, now),
            )
            await self.execute(
                "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?", (nick_l, int(spawn["id"])),
            )
            await self._pokemon_consume_item(nick_l, ch, ball_id, 1)
            return True, f"Caught {spawn['species_name']}!"
        await self._pokemon_consume_item(nick_l, ch, ball_id, 1)
        return False, "The Pokemon broke free!"

    async def pokemon_get_spawns_per_day(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(100, int(v)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_spawns_per_day(self, n: int) -> None:
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, min(100, int(n)))))

    async def pokemon_trainer_channels_for_nick(self, nick: str) -> list[str]:
        nick_l = (nick or "").strip().lower()
        if not nick_l:
            return []
        rows = await self.fetchall(
            "SELECT channel FROM pokemon_trainers WHERE lower(nick)=?", (nick_l,)
        )
        return [str(r[0]) for r in rows]

    async def pokemon_trainer_level_up(self, nick: str, channel: str, slot: int) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch or slot < 1 or slot > 6:
            return False, "Invalid slot"
        pkmn = await self.fetchone(
            """SELECT p.*, s.hp_base FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON p.species_id = s.id
               WHERE lower(p.trainer_nick)=? AND p.channel=? AND p.slot=?""",
            (nick_l, ch, slot),
        )
        if not pkmn:
            return False, "No Pokemon in that slot"
        level = int(pkmn["level"] or 5)
        if level >= 100:
            return False, "Already max level"
        new_level = level + 1
        hp_base = int(pkmn["hp_base"] or 50)
        new_max_hp = max(10, (hp_base * 2 + 10) * new_level // 5)
        current_hp = int(pkmn["current_hp"] or 0)
        is_fainted = int(pkmn["is_fainted"] or 0)
        new_current = new_max_hp if not is_fainted else 0
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, max_hp=?, current_hp=?, experience=level*level*10 WHERE id=?",
            (new_level, new_max_hp, new_current, int(pkmn["id"])),
        )
        return True, f"Leveled up to {new_level}!"

    # ---- Pokemon service ----
    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_species_get_random(self, limit: int = 5) -> list:
        rows = await self.fetchall(
            "SELECT id, pokedex_number, name, type1, type2, hp_base, atk_base, def_base, "
            "sp_atk_base, sp_def_base, speed_base, capture_rate FROM pokemon_species "
            "ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows) if rows else []

    async def pokemon_species_get(self, species_id: int):
        return await self.fetchone(
            "SELECT * FROM pokemon_species WHERE id=?", (int(species_id),)
        )

    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            (nick.strip().lower(), channel.strip()),
        )
        return row is not None

    async def pokemon_trainer_create(
        self, nick: str, channel: str, species_ids: list[int], *, starter_items: list[tuple[str, int]] | None = None
    ) -> None:
        now = int(time.time())
        n, ch = nick.strip().lower(), channel.strip()
        await self.execute(
            "INSERT INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (n, ch, now),
        )
        for slot, sid in enumerate(species_ids[:6], 1):
            sp = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (sid,))
            hp = int(sp["hp_base"] * 1.5 + 10) if sp else 20
            await self.execute(
                "INSERT INTO pokemon_trainer_pokemon(trainer_nick,channel,species_id,level,current_hp,max_hp,slot,created_ts) "
                "VALUES(?,?,?,5,?,?,?,?)",
                (n, ch, sid, hp, hp, slot, now),
            )
        for item_id, qty in starter_items or [("potion", 5), ("revive", 2), ("pokeball", 5)]:
            await self.execute(
                "INSERT INTO pokemon_trainer_items(trainer_nick,channel,item_id,quantity) VALUES(?,?,?,?)",
                (n, ch, item_id, int(qty)),
            )

    async def pokemon_trainer_pokemon_list(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """
            SELECT tp.id, tp.species_id, s.name as species_name, tp.nickname, tp.level, tp.current_hp, tp.max_hp,
                   tp.is_fainted, tp.slot, s.type1, s.type2
            FROM pokemon_trainer_pokemon tp
            JOIN pokemon_species s ON tp.species_id=s.id
            WHERE tp.trainer_nick=? AND tp.channel=?
            ORDER BY tp.slot
            """,
            (nick.strip().lower(), channel.strip()),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_items_list(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """
            SELECT ti.item_id, i.name, ti.quantity, i.item_type, i.effect_value
            FROM pokemon_trainer_items ti
            JOIN pokemon_items i ON ti.item_id=i.id
            WHERE ti.trainer_nick=? AND ti.channel=? AND ti.quantity>0
            ORDER BY i.item_type, i.name
            """,
            (nick.strip().lower(), channel.strip()),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_use_heal(
        self, nick: str, channel: str, item_id: str, pokemon_slot: int | None = None
    ) -> tuple[bool, str]:
        n, ch = nick.strip().lower(), channel.strip()
        row = await self.fetchone(
            "SELECT quantity, effect_value FROM pokemon_trainer_items ti "
            "JOIN pokemon_items i ON ti.item_id=i.id WHERE ti.trainer_nick=? AND ti.channel=? AND ti.item_id=? AND i.item_type='heal'",
            (n, ch, item_id),
        )
        if not row or int(row["quantity"]) < 1:
            return False, "You don't have that item or it's not a healing item."
        heal = int(row["effect_value"] or 20)
        rows = await self.fetchall(
            "SELECT tp.id, tp.slot, tp.current_hp, tp.max_hp, tp.is_fainted FROM pokemon_trainer_pokemon tp "
            "WHERE tp.trainer_nick=? AND tp.channel=? AND tp.is_fainted=0 ORDER BY tp.slot",
            (n, ch),
        )
        if not rows:
            return False, "No conscious Pokemon to heal."
        target = next((r for r in rows if int(r["slot"]) == pokemon_slot), rows[0]) if pokemon_slot else rows[0]
        new_hp = min(int(target["current_hp"]) + heal, int(target["max_hp"]))
        if new_hp == int(target["current_hp"]):
            return False, "That Pokemon is already at full HP."
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?", (new_hp, int(target["id"])),
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (n, ch, item_id),
        )
        return True, f"Healed {heal} HP. Now at {new_hp}/{target['max_hp']}."

    async def pokemon_trainer_use_revive(
        self, nick: str, channel: str, item_id: str, pokemon_slot: int | None = None
    ) -> tuple[bool, str]:
        n, ch = nick.strip().lower(), channel.strip()
        row = await self.fetchone(
            "SELECT quantity, effect_value FROM pokemon_trainer_items ti "
            "JOIN pokemon_items i ON ti.item_id=i.id WHERE ti.trainer_nick=? AND ti.channel=? AND ti.item_id=? AND i.item_type='revive'",
            (n, ch, item_id),
        )
        if not row or int(row["quantity"]) < 1:
            return False, "You don't have that item or it's not a revive."
        pct = int(row["effect_value"] or 50)
        rows = await self.fetchall(
            "SELECT tp.id, tp.slot, tp.max_hp FROM pokemon_trainer_pokemon tp "
            "WHERE tp.trainer_nick=? AND tp.channel=? AND tp.is_fainted=1 ORDER BY tp.slot",
            (n, ch),
        )
        if not rows:
            return False, "No fainted Pokemon to revive."
        target = next((r for r in rows if int(r["slot"]) == pokemon_slot), rows[0]) if pokemon_slot else rows[0]
        new_hp = max(1, int(target["max_hp"]) * pct // 100)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?", (new_hp, int(target["id"])),
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (n, ch, item_id),
        )
        return True, f"Revived! HP restored to {new_hp}/{target['max_hp']}."

    async def pokemon_trainer_level_up(self, nick: str, channel: str, pokemon_slot: int) -> tuple[bool, str]:
        n, ch = nick.strip().lower(), channel.strip()
        row = await self.fetchone(
            "SELECT id, level, max_hp, current_hp FROM pokemon_trainer_pokemon "
            "WHERE trainer_nick=? AND channel=? AND slot=?", (n, ch, int(pokemon_slot)),
        )
        if not row:
            return False, "No Pokemon in that slot."
        new_level = int(row["level"]) + 1
        new_max_hp = min(100, int(row["max_hp"]) + 5)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, max_hp=?, current_hp=? WHERE id=?",
            (new_level, new_max_hp, new_max_hp, int(row["id"])),
        )
        return True, f"Level up! Now level {new_level}. Max HP: {new_max_hp}."

    async def pokemon_wild_spawns_count_24h(self, channel: str) -> int:
        since = int(time.time()) - 86400
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts>=?", (channel.strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_wild_spawn_get_active(self, channel: str):
        return await self.fetchone(
            """
            SELECT w.*, s.name as species_name, s.type1, s.type2, s.capture_rate
            FROM pokemon_wild_spawns w
            JOIN pokemon_species s ON w.species_id=s.id
            WHERE w.channel=? AND w.expires_ts>? AND w.captured_by IS NULL
            """,
            (channel.strip(), int(time.time())),
        )

    async def pokemon_wild_spawn_create(self, channel: str, species_id: int, level: int, duration_s: int = 900) -> int:
        now = int(time.time())
        expires = now + duration_s
        await self.execute(
            "INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts) VALUES(?,?,?,?,?)",
            (channel.strip(), species_id, level, now, expires),
        )
        row = await self.fetchone("SELECT last_insert_rowid()", ())
        return int(row[0])

    async def pokemon_wild_spawn_capture(
        self, channel: str, spawn_id: int, nick: str
    ) -> tuple[bool, str]:
        n = nick.strip().lower()
        ch = channel.strip()
        spawn = await self.fetchone("SELECT * FROM pokemon_wild_spawns WHERE id=? AND channel=? AND captured_by IS NULL", (spawn_id, ch))
        if not spawn:
            return False, "That Pokemon is not available to capture."
        if spawn["expires_ts"] and int(spawn["expires_ts"]) <= int(time.time()):
            return False, "That Pokemon has fled!"
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id IN ('pokeball','great_ball','ultra_ball') AND quantity>0",
            (n, ch),
        )
        if not row:
            return False, "You need a Poké Ball, Great Ball or Ultra Ball."
        ball_row = await self.fetchone(
            "SELECT item_id, quantity, effect_value FROM pokemon_trainer_items ti "
            "JOIN pokemon_items i ON ti.item_id=i.id "
            "WHERE ti.trainer_nick=? AND ti.channel=? AND ti.item_id IN ('ultra_ball','great_ball','pokeball') AND ti.quantity>0 "
            "ORDER BY i.effect_value ASC LIMIT 1",
            (n, ch),
        )
        if not ball_row:
            return False, "You need a Poké Ball to capture."
        import random
        rate = int(spawn.get("capture_rate", 255) or 255)
        roll = random.randint(1, 255)
        captured = roll <= rate
        if captured:
            now = int(time.time())
            await self.execute(
                "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?", (n, spawn_id),
            )
            await self.execute(
                "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)", (n, ch, now),
            )
            slot = await self.fetchone(
                "SELECT COALESCE(MAX(slot),0)+1 FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
                (n, ch),
            )
            slot_n = int(slot[0] or 1)
            sp = await self.pokemon_species_get(int(spawn["species_id"]))
            hp = int(sp["hp_base"] * 1.5 + 10) if sp else 20
            await self.execute(
                "INSERT INTO pokemon_trainer_pokemon(trainer_nick,channel,species_id,level,current_hp,max_hp,slot,created_ts) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (n, ch, int(spawn["species_id"]), int(spawn["level"] or 5), hp, hp, slot_n, now),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (n, ch, ball_row["item_id"]),
            )
            return True, f"Gotcha! {spawn['species_name']} was caught!"
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (n, ch, ball_row["item_id"]),
        )
        return False, "The Pokemon broke free! Try again."

    async def pokemon_get_wild_spawns_per_day_setting(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, int(v))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_wild_spawns_per_day(self, n: int) -> None:
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, n)))

    # ---- Pokemon service ----
    async def pokemon_get_wild_spawns_per_day(self) -> int:
        val = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(100, int(val)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_wild_spawns_per_day(self, n: int) -> None:
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, min(100, n))))

    async def pokemon_count_species(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_get_random_species(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows)

    async def pokemon_get_species(self, species_id: int):
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))

    async def pokemon_is_trainer(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            ((nick or "").strip(), (channel or "").strip()),
        )
        return row is not None

    async def pokemon_create_trainer(self, nick: str, channel: str, *, species_ids: list[int]) -> None:
        now = int(time.time())
        nick = (nick or "").strip()
        channel = (channel or "").strip()
        if not nick or not channel or not species_ids:
            return
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick, channel, now),
        )
        for slot, sid in enumerate(species_ids[:6], 1):
            species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(sid),))
            if not species:
                continue
            base_hp = int(species["hp_base"] or 50)
            max_hp = max(10, base_hp + (int(species["hp_base"] or 0) * (5 - 1) // 50))
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(
                    trainer_nick, channel, species_id, level, current_hp, max_hp, experience, slot, created_ts
                ) VALUES(?,?,?,5,?,?,0,?,?)""",
                (nick, channel, int(sid), max_hp, max_hp, slot, now),
            )
        starter_items = [("potion", 5), ("revive", 2), ("pokeball", 10)]
        for item_id, qty in starter_items:
            await self.execute(
                "INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity) VALUES(?,?,?,?) "
                "ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+excluded.quantity",
                (nick, channel, item_id, qty),
            )

    async def pokemon_list_trainer_pokemon(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT p.*, s.name as species_name, s.type1, s.type2
               FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON p.species_id = s.id
               WHERE p.trainer_nick=? AND p.channel=? ORDER BY p.slot""",
            ((nick or "").strip(), (channel or "").strip()),
        )
        return list(rows)

    async def pokemon_get_trainer_item(self, nick: str, channel: str, item_id: str) -> int:
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            ((nick or "").strip(), (channel or "").strip(), (item_id or "").strip()),
        )
        return int(row[0]) if row and row[0] is not None else 0

    async def pokemon_list_trainer_items(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT ti.item_id, ti.quantity, i.name
               FROM pokemon_trainer_items ti
               JOIN pokemon_items i ON ti.item_id = i.id
               WHERE ti.trainer_nick=? AND ti.channel=? AND ti.quantity > 0
               ORDER BY i.item_type, i.name""",
            ((nick or "").strip(), (channel or "").strip()),
        )
        return list(rows)

    async def pokemon_get_item(self, item_id: str):
        return await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", ((item_id or "").strip(),))

    async def pokemon_use_item(self, nick: str, channel: str, item_id: str, pokemon_id: int | None = None) -> tuple[bool, str]:
        nick = (nick or "").strip()
        channel = (channel or "").strip()
        item_id = (item_id or "").strip()
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (item_id,))
        if not item:
            return False, "Unknown item."
        qty = await self.pokemon_get_trainer_item(nick, channel, item_id)
        if qty <= 0:
            return False, f"You don't have any {item['name']}."
        item_type = str(item["item_type"] or "").lower()
        if item_type == "heal":
            pokemon = await self.fetchone(
                "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=? AND is_fainted=0",
                (int(pokemon_id or 0), nick, channel),
            ) if pokemon_id else None
            if not pokemon:
                return False, "Specify a non-fainted Pokemon: !heal <pokemon_id>"
            heal = int(item["effect_value"] or 20)
            new_hp = min(int(pokemon["max_hp"]), int(pokemon["current_hp"] or 0) + heal)
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?",
                (new_hp, int(pokemon["id"])),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (nick, channel, item_id),
            )
            return True, f"Healed {int(new_hp) - int(pokemon['current_hp'] or 0)} HP."
        if item_type == "revive":
            pokemon = await self.fetchone(
                "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=? AND is_fainted=1",
                (int(pokemon_id or 0), nick, channel),
            ) if pokemon_id else None
            if not pokemon:
                return False, "Specify a fainted Pokemon: !revive <pokemon_id>"
            pct = int(item["effect_value"] or 50)
            new_hp = max(1, int(pokemon["max_hp"] or 1) * pct // 100)
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
                (new_hp, int(pokemon["id"])),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (nick, channel, item_id),
            )
            return True, f"Revived to {new_hp} HP."
        return False, "That item cannot be used here."

    async def pokemon_get_active_wild_spawn(self, channel: str) -> sqlite3.Row | None:
        now = int(time.time())
        return await self.fetchone(
            "SELECT * FROM pokemon_wild_spawns WHERE channel=? AND expires_ts>? AND captured_by IS NULL",
            ((channel or "").strip(), now),
        )

    async def pokemon_count_spawns_last_24h(self, channel: str) -> int:
        since = int(time.time()) - (24 * 3600)
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts>=?",
            ((channel or "").strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_create_wild_spawn(self, channel: str, species_id: int, level: int, duration_s: int = 900) -> int | None:
        now = int(time.time())
        channel = (channel or "").strip()
        limit = await self.pokemon_get_wild_spawns_per_day()
        count = await self.pokemon_count_spawns_last_24h(channel)
        if count >= limit:
            return None
        existing = await self.pokemon_get_active_wild_spawn(channel)
        if existing:
            return None
        expires = now + int(duration_s)
        await self.execute(
            "INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts) VALUES(?,?,?,?,?)",
            (channel, int(species_id), int(level), now, expires),
        )
        row = await self.fetchone("SELECT last_insert_rowid()", ())
        return int(row[0]) if row else None

    async def pokemon_capture_wild(self, channel: str, nick: str) -> tuple[bool, str]:
        spawn = await self.pokemon_get_active_wild_spawn(channel)
        if not spawn:
            return False, "No wild Pokemon here."
        nick = (nick or "").strip()
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?",
            (nick, int(spawn["id"])),
        )
        species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(spawn["species_id"]),))
        species_name = species["name"] if species else "Unknown"
        level = int(spawn["level"] or 5)
        base_hp = int(species["hp_base"] or 50) if species else 50
        max_hp = max(10, base_hp + (base_hp * (level - 1) // 50))
        if not await self.pokemon_is_trainer(nick, channel):
            await self.pokemon_create_trainer(nick, channel, species_ids=[])
        trainer = await self.fetchone("SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?", (nick, channel))
        if not trainer:
            return False, "Trainer not found."
        cnt = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
            (nick, channel),
        )
        slot = int(cnt[0]) + 1 if cnt else 1
        if slot > 6:
            return False, "Your party is full!"
        now = int(time.time())
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon(
                trainer_nick, channel, species_id, level, current_hp, max_hp, experience, slot, created_ts
            ) VALUES(?,?,?,?,?,?,0,?,?)""",
            (nick, channel, int(spawn["species_id"]), level, max_hp, max_hp, slot, now),
        )
        return True, f"Caught {species_name} (Lv.{level})!"

    async def pokemon_expire_old_spawns(self) -> int:
        now = int(time.time())
        async with self._lock:
            cur = self._conn.execute("DELETE FROM pokemon_wild_spawns WHERE expires_ts<=?", (now,))
            return int(cur.rowcount or 0)

    async def pokemon_level_up(self, nick: str, channel: str, pokemon_id: int) -> tuple[bool, str]:
        row = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
            (int(pokemon_id), (nick or "").strip(), (channel or "").strip()),
        )
        if not row:
            return False, "Pokemon not found."
        level = int(row["level"] or 5)
        if level >= 100:
            return False, "Pokemon is already max level."
        exp = int(row["experience"] or 0)
        exp_needed = level * 50
        if exp < exp_needed:
            return False, f"Needs {exp_needed - exp} more XP to level up."
        species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(row["species_id"]),))
        base_hp = int(species["hp_base"] or 50) if species else 50
        new_level = level + 1
        new_max_hp = max(10, base_hp + (base_hp * (new_level - 1) // 50))
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, max_hp=?, current_hp=?, experience=experience-? WHERE id=?",
            (new_level, new_max_hp, new_max_hp, exp_needed, int(row["id"])),
        )
        return True, f"Leveled up to Lv.{new_level}!"

    async def pokemon_add_experience(self, nick: str, channel: str, pokemon_id: int, exp: int) -> None:
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET experience=experience+? WHERE id=? AND trainer_nick=? AND channel=?",
            (int(exp), int(pokemon_id), (nick or "").strip(), (channel or "").strip()),
        )

    # ---- Pokemon service ----
    async def pokemon_get_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_get_random_species(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows)

    async def pokemon_get_species_by_id(self, species_id: int):
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))

    async def pokemon_is_trainer(self, nick: str, channel: str) -> bool:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return False
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE lower(nick)=? AND channel=?",
            (nick_l, ch),
        )
        return row is not None

    async def pokemon_create_trainer(self, nick: str, channel: str, species_ids: list[int]) -> None:
        nick_s = (nick or "").strip()
        ch = (channel or "").strip()
        if not nick_s or not ch or not species_ids:
            return
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick_s, ch, now),
        )
        starter_items = [("potion", 5), ("revive", 2), ("pokeball", 10)]
        for item_id, qty in starter_items:
            await self.execute(
                """INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity)
                   VALUES(?,?,?,?) ON CONFLICT(trainer_nick, channel, item_id)
                   DO UPDATE SET quantity=quantity+excluded.quantity""",
                (nick_s, ch, item_id, qty),
            )
        for slot, sid in enumerate(species_ids[:6], 1):
            sp = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(sid),))
            if not sp:
                continue
            hp = max(10, (int(sp["hp_base"]) or 50) * 2 + 10)
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(
                   trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts)
                   VALUES(?,?,?,5,?,?,?,?)""",
                (nick_s, ch, int(sid), hp, hp, slot, now),
            )

    async def pokemon_get_trainer_pokemon(self, nick: str, channel: str) -> list:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return []
        rows = await self.fetchall(
            """SELECT p.*, s.name as species_name, s.type1, s.type2
               FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON p.species_id = s.id
               WHERE lower(p.trainer_nick)=? AND p.channel=?
               ORDER BY p.slot""",
            (nick_l, ch),
        )
        return list(rows)

    async def pokemon_get_trainer_items(self, nick: str, channel: str) -> list:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return []
        rows = await self.fetchall(
            """SELECT ti.*, i.name as item_name, i.item_type, i.effect_value
               FROM pokemon_trainer_items ti
               JOIN pokemon_items i ON ti.item_id = i.id
               WHERE lower(ti.trainer_nick)=? AND ti.channel=? AND ti.quantity > 0
               ORDER BY i.item_type, i.name""",
            (nick_l, ch),
        )
        return list(rows)

    async def pokemon_get_trainer_nick(self, nick: str, channel: str) -> str | None:
        row = await self.fetchone(
            "SELECT nick FROM pokemon_trainers WHERE lower(nick)=? AND channel=?",
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return str(row[0]) if row else None

    async def pokemon_heal_pokemon(self, nick: str, channel: str, pokemon_id: int, amount: int) -> bool:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return False
        await self.execute(
            """UPDATE pokemon_trainer_pokemon SET
               current_hp = min(max_hp, current_hp + ?), is_fainted = 0
               WHERE id=? AND lower(trainer_nick)=? AND channel=? AND is_fainted=0""",
            (int(amount), int(pokemon_id), nick_l, ch),
        )
        row = await self.fetchone("SELECT changes()", ())
        return row and row[0] and int(row[0]) > 0

    async def pokemon_revive_pokemon(self, nick: str, channel: str, pokemon_id: int, hp_percent: int) -> bool:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return False
        row = await self.fetchone(
            "SELECT max_hp FROM pokemon_trainer_pokemon WHERE id=? AND lower(trainer_nick)=? AND channel=?",
            (int(pokemon_id), nick_l, ch),
        )
        if not row:
            return False
        restore = max(1, int(row[0]) * hp_percent // 100)
        await self.execute(
            """UPDATE pokemon_trainer_pokemon SET
               current_hp=?, is_fainted=0
               WHERE id=? AND lower(trainer_nick)=? AND channel=? AND is_fainted=1""",
            (restore, int(pokemon_id), nick_l, ch),
        )
        r2 = await self.fetchone("SELECT changes()", ())
        return r2 and r2[0] and int(r2[0]) > 0

    async def pokemon_use_item(self, nick: str, channel: str, item_id: str, target_id: int | None = None) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch or not item_id:
            return False, "Invalid target."
        inv = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE lower(trainer_nick)=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        if not inv or int(inv[0] or 0) < 1:
            return False, "You don't have that item."
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (item_id,))
        if not item:
            return False, "Unknown item."
        itype = str(item["item_type"])
        effect = int(item["effect_value"] or 0)
        if itype == "heal":
            if target_id is None:
                return False, "Specify a Pokémon: !heal <pokemon_id>"
            ok = await self.pokemon_heal_pokemon(nick, ch, target_id, effect)
            if not ok:
                return False, "Could not heal (Pokémon not found or not damaged)."
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE lower(trainer_nick)=? AND channel=? AND item_id=? AND quantity>0",
                (nick_l, ch, item_id),
            )
            return True, f"Healed by {effect} HP."
        if itype == "revive":
            if target_id is None:
                return False, "Specify a Pokémon: !revive <pokemon_id>"
            ok = await self.pokemon_revive_pokemon(nick, ch, target_id, effect)
            if not ok:
                return False, "Could not revive (Pokémon not found or not fainted)."
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE lower(trainer_nick)=? AND channel=? AND item_id=? AND quantity>0",
                (nick_l, ch, item_id),
            )
            return True, f"Revived to {effect}% HP."
        return False, "That item can't be used here."

    async def pokemon_get_wild_spawn(self, channel: str):
        ch = (channel or "").strip()
        if not ch:
            return None
        now = int(time.time())
        return await self.fetchone(
            """SELECT w.*, s.name as species_name, s.type1, s.type2, s.capture_rate
               FROM pokemon_wild_spawns w
               JOIN pokemon_species s ON w.species_id = s.id
               WHERE w.channel=? AND w.expires_ts > ? AND w.captured_by IS NULL""",
            (ch, now),
        )

    async def pokemon_count_spawns_last_24h(self, channel: str) -> int:
        ch = (channel or "").strip()
        if not ch:
            return 0
        since = int(time.time()) - 86400
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            (ch, since),
        )
        return int(row[0]) if row else 0

    async def pokemon_spawn_wild(self, channel: str, species_id: int, level: int, duration_s: int = 600) -> bool:
        ch = (channel or "").strip()
        if not ch:
            return False
        now = int(time.time())
        n_per_day = int((await self.get_setting("pokemon_wild_spawns_per_day", "24")) or 24)
        if await self.pokemon_count_spawns_last_24h(ch) >= n_per_day:
            return False
        try:
            await self.execute(
                """INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
                   VALUES(?,?,?,?,?)""",
                (ch, int(species_id), int(level), now, now + int(duration_s)),
            )
            return True
        except Exception:
            return False

    async def pokemon_capture_wild(self, nick: str, channel: str, spawn_id: int) -> tuple[bool, str]:
        nick_s = (nick or "").strip()
        nick_l = nick_s.lower()
        ch = (channel or "").strip()
        if not nick_s or not ch:
            return False, "Invalid."
        spawn = await self.fetchone(
            "SELECT * FROM pokemon_wild_spawns WHERE id=? AND channel=? AND captured_by IS NULL AND expires_ts > ?",
            (int(spawn_id), ch, int(time.time())),
        )
        if not spawn:
            return False, "That wild Pokémon is gone or already captured."
        species_id = int(spawn["species_id"])
        level = int(spawn["level"] or 5)
        sp = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (species_id,))
        if not sp:
            return False, "Species not found."
        now = int(time.time())
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?", (nick_s, int(spawn_id)),
        )
        slot = 1
        rows = await self.fetchall(
            "SELECT slot FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? ORDER BY slot",
            (nick_s, ch),
        )
        for r in rows:
            if int(r[0]) != slot:
                break
            slot += 1
        if slot > 6:
            slot = 6
        hp = max(10, (int(sp["hp_base"]) or 50) * 2 + 10)
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon(
               trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts)
               VALUES(?,?,?,?,?,?,?,?)""",
            (nick_s, ch, species_id, level, hp, hp, slot, now),
        )
        return True, str(sp["name"])

    async def pokemon_expire_old_spawns(self, now_ts: int | None = None) -> int:
        now = int(now_ts or time.time())
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM pokemon_wild_spawns WHERE expires_ts <= ?",
                (now,),
            )
            return int(cur.rowcount or 0)

    async def pokemon_set_spawns_per_day(self, n: int) -> None:
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, min(100, n))))

    async def pokemon_get_spawns_per_day(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(100, int(v)))
        except (TypeError, ValueError):
            return 24

    # ---- Pokemon service ----
    async def pokemon_get_wild_spawns_per_day(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(1000, int(v)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_wild_spawns_per_day(self, n: int) -> None:
        n = max(1, min(1000, int(n)))
        await self.set_setting("pokemon_wild_spawns_per_day", str(n))

    async def pokemon_count_spawns_last_24h(self, channel: str) -> int:
        since = int(time.time()) - (24 * 3600)
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts>=?",
            (channel.strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            (nick.strip().lower(), channel.strip()),
        )
        return row is not None

    async def pokemon_create_trainer(self, nick: str, channel: str) -> None:
        nick_l = nick.strip().lower()
        ch = channel.strip()
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick_l, ch, now),
        )

    async def pokemon_add_starter(self, nick: str, channel: str, species_id: int, level: int = 5) -> None:
        nick_l = nick.strip().lower()
        ch = channel.strip()
        now = int(time.time())
        row = await self.fetchone(
            "SELECT hp_base FROM pokemon_species WHERE id=?",
            (int(species_id),),
        )
        hp = int(row["hp_base"]) * 2 + 110 if row else 50
        max_hp = hp  # Simplified: base * 2 + 110 at level 5
        max_hp = max(20, min(max_hp, 999))
        slot = 1
        rows = await self.fetchall(
            "SELECT COALESCE(MAX(slot),0) FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
            (nick_l, ch),
        )
        if rows and rows[0][0] is not None:
            slot = int(rows[0][0] or 0) + 1
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon(
                trainer_nick, channel, species_id, level, current_hp, max_hp, experience, is_fainted, slot, created_ts
            ) VALUES(?,?,?,?,?,?,0,0,?,?)""",
            (nick_l, ch, species_id, level, max_hp, max_hp, slot, now),
        )

    async def pokemon_add_starter_items(self, nick: str, channel: str) -> None:
        nick_l = nick.strip().lower()
        ch = channel.strip()
        starters = [("potion", 5), ("revive", 2), ("pokeball", 10)]
        for item_id, qty in starters:
            await self.execute(
                """INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity)
                   VALUES(?,?,?,?) ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+?""",
                (nick_l, ch, item_id, qty, qty),
            )

    async def pokemon_get_random_species(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT id, name, type1, type2 FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows)

    async def pokemon_get_trainer_pokemon(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT p.id, p.species_id, p.nickname, p.level, p.current_hp, p.max_hp, p.is_fainted, p.slot,
                      s.name as species_name, s.type1, s.type2
               FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON p.species_id = s.id
               WHERE p.trainer_nick=? AND p.channel=?
               ORDER BY p.slot""",
            (nick.strip().lower(), channel.strip()),
        )
        return list(rows)

    async def pokemon_get_trainer_items(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT ti.item_id, ti.quantity, i.name, i.item_type, i.effect_value
               FROM pokemon_trainer_items ti
               JOIN pokemon_items i ON ti.item_id = i.id
               WHERE ti.trainer_nick=? AND ti.channel=? AND ti.quantity>0
               ORDER BY i.item_type, i.name""",
            (nick.strip().lower(), channel.strip()),
        )
        return list(rows)

    async def pokemon_get_wild_spawn(self, channel: str) -> sqlite3.Row | None:
        now = int(time.time())
        return await self.fetchone(
            """SELECT w.*, s.name as species_name, s.type1, s.type2
               FROM pokemon_wild_spawns w
               JOIN pokemon_species s ON w.species_id = s.id
               WHERE w.channel=? AND w.expires_ts>? AND w.captured_by IS NULL""",
            (channel.strip(), now),
        )

    async def pokemon_insert_wild_spawn(self, channel: str, species_id: int, level: int, duration_sec: int = 600) -> bool:
        now = int(time.time())
        expires = now + duration_sec
        try:
            await self.execute(
                """INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
                   VALUES(?,?,?,?,?)""",
                (channel.strip(), species_id, level, now, expires),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    async def pokemon_capture_wild(self, channel: str, nick: str) -> bool:
        now = int(time.time())
        nick_l = nick.strip().lower()
        row = await self.fetchone(
            "SELECT id, species_id, level FROM pokemon_wild_spawns WHERE channel=? AND expires_ts>? AND captured_by IS NULL",
            (channel.strip(), now),
        )
        if not row:
            return False
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?",
            (nick_l, row["id"]),
        )
        sp = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (row["species_id"],))
        hp_base = int(sp["hp_base"]) if sp else 50
        max_hp = max(20, min(hp_base * 2 + 50, 999))
        slot_rows = await self.fetchall(
            "SELECT COALESCE(MAX(slot),0) FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
            (nick_l, channel.strip()),
        )
        slot = int(slot_rows[0][0] or 0) + 1 if slot_rows else 1
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon(
                trainer_nick, channel, species_id, level, current_hp, max_hp, experience, is_fainted, slot, created_ts
            ) VALUES(?,?,?,?,?,?,0,0,?,?)""",
            (nick_l, channel.strip(), row["species_id"], row["level"], max_hp, max_hp, slot, now),
        )
        return True

    async def pokemon_use_heal(self, nick: str, channel: str, item_id: str, pokemon_slot: int) -> tuple[bool, str]:
        nick_l = nick.strip().lower()
        ch = channel.strip()
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        if not row or int(row["quantity"] or 0) < 1:
            return False, "No such item"
        item = await self.fetchone("SELECT item_type, effect_value FROM pokemon_items WHERE id=?", (item_id,))
        if not item or item["item_type"] != "heal":
            return False, "Item cannot heal"
        poke = await self.fetchone(
            "SELECT id, current_hp, max_hp, is_fainted FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? AND slot=?",
            (nick_l, ch, pokemon_slot),
        )
        if not poke:
            return False, "No Pokemon in that slot"
        if int(poke["is_fainted"] or 0):
            return False, "Pokemon is fainted; use a Revive"
        heal = min(int(item["effect_value"] or 20), int(poke["max_hp"]) - int(poke["current_hp"]))
        if heal <= 0:
            return False, "Pokemon is already at full HP"
        new_hp = min(int(poke["current_hp"]) + heal, int(poke["max_hp"]))
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?",
            (new_hp, poke["id"]),
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        return True, f"restored {heal} HP"

    async def pokemon_use_revive(self, nick: str, channel: str, item_id: str, pokemon_slot: int) -> tuple[bool, str]:
        nick_l = nick.strip().lower()
        ch = channel.strip()
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        if not row or int(row["quantity"] or 0) < 1:
            return False, "No such item"
        item = await self.fetchone("SELECT item_type, effect_value FROM pokemon_items WHERE id=?", (item_id,))
        if not item or item["item_type"] != "revive":
            return False, "Item cannot revive"
        poke = await self.fetchone(
            "SELECT id, current_hp, max_hp, is_fainted FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? AND slot=?",
            (nick_l, ch, pokemon_slot),
        )
        if not poke:
            return False, "No Pokemon in that slot"
        if not int(poke["is_fainted"] or 0):
            return False, "Pokemon is not fainted"
        pct = int(item["effect_value"] or 50)
        new_hp = max(1, int(poke["max_hp"]) * pct // 100)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
            (new_hp, poke["id"]),
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        return True, f"revived to {new_hp} HP"

    async def pokemon_try_capture_with_ball(self, nick: str, channel: str, item_id: str) -> tuple[bool, str]:
        nick_l = nick.strip().lower()
        ch = channel.strip()
        spawn = await self.pokemon_get_wild_spawn(ch)
        if not spawn:
            return False, "No wild Pokemon to capture"
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        if not row or int(row["quantity"] or 0) < 1:
            return False, "No such item"
        item = await self.fetchone("SELECT item_type, effect_value FROM pokemon_items WHERE id=?", (item_id,))
        if not item or item["item_type"] != "ball":
            return False, "Item is not a Poké Ball"
        ball_mod = int(item["effect_value"] or 255)
        sp = await self.fetchone("SELECT capture_rate FROM pokemon_species WHERE id=?", (spawn["species_id"],))
        cap = int(sp["capture_rate"]) if sp else 255
        import random
        roll = random.randint(1, 255)
        success = roll <= (cap * ball_mod // 255)
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        if success:
            await self.execute(
                "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?", (nick_l, spawn["id"])
            )
            max_hp = max(20, 50)
            slot_rows = await self.fetchall(
                "SELECT COALESCE(MAX(slot),0) FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
                (nick_l, ch),
            )
            slot = int(slot_rows[0][0] or 0) + 1 if slot_rows else 1
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(
                    trainer_nick, channel, species_id, level, current_hp, max_hp, experience, is_fainted, slot, created_ts
                ) VALUES(?,?,?,?,?,?,0,0,?,?)""",
                (nick_l, ch, spawn["species_id"], spawn["level"], max_hp, max_hp, slot, int(time.time())),
            )
            return True, "captured"
        return False, "broke free"

    async def pokemon_delete_expired_spawns(self, now_ts: int | None = None) -> int:
        now = int(now_ts or time.time())
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM pokemon_wild_spawns WHERE expires_ts<=?",
                (now,),
            )
            return int(cur.rowcount or 0)

    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    # ---- Pokemon service ----
    async def pokemon_get_species(self, species_id: int) -> sqlite3.Row | None:
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))

    async def pokemon_get_species_by_name(self, name: str) -> sqlite3.Row | None:
        n = (name or "").strip().lower()
        if not n:
            return None
        return await self.fetchone("SELECT * FROM pokemon_species WHERE LOWER(name)=?", (n,))

    async def pokemon_get_random_species(self, limit: int = 1) -> list:
        return await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?", (int(limit),)
        )

    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        n = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not n or not ch:
            return False
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE LOWER(nick)=? AND channel=?",
            (n, ch),
        )
        return row is not None

    async def pokemon_trainer_create(self, nick: str, channel: str, starter_species_ids: list[int]) -> None:
        n = (nick or "").strip()
        ch = (channel or "").strip()
        if not n or not ch or not starter_species_ids:
            return
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (n, ch, now),
        )
        for slot, sid in enumerate(starter_species_ids[:6], 1):
            species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(sid),))
            if not species:
                continue
            hp = int(species["hp_base"] or 50) * 2 + 60  # approximate level 5 HP
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(
                    trainer_nick, channel, species_id, level, current_hp, max_hp,
                    experience, is_fainted, slot, created_ts)
                VALUES(?,?,?,5,?,?,0,0,?,?)""",
                (n, ch, int(sid), hp, hp, slot, now),
            )
        for item_id, qty in [("potion", 5), ("revive", 3), ("pokeball", 10)]:
            await self.execute(
                """INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity)
                VALUES(?,?,?,?) ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+?""",
                (n, ch, item_id, qty, qty),
            )

    async def pokemon_trainer_get_pokemon(self, nick: str, channel: str) -> list:
        n = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not n or not ch:
            return []
        rows = await self.fetchall(
            """SELECT tp.*, ps.name as species_name, ps.type1, ps.type2
            FROM pokemon_trainer_pokemon tp
            JOIN pokemon_species ps ON tp.species_id = ps.id
            WHERE LOWER(tp.trainer_nick)=? AND tp.channel=?
            ORDER BY tp.slot""",
            (n, ch),
        )
        return list(rows)

    async def pokemon_trainer_get_items(self, nick: str, channel: str) -> list:
        n = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not n or not ch:
            return []
        rows = await self.fetchall(
            """SELECT ti.*, i.name, i.item_type, i.effect_value
            FROM pokemon_trainer_items ti
            JOIN pokemon_items i ON ti.item_id = i.id
            WHERE LOWER(ti.trainer_nick)=? AND ti.channel=? AND ti.quantity > 0
            ORDER BY i.item_type, i.name""",
            (n, ch),
        )
        return list(rows)

    async def pokemon_trainer_heal(self, nick: str, channel: str, item_id: str, pokemon_id: int | None = None) -> tuple[bool, str]:
        n = (nick or "").strip()
        ch = (channel or "").strip()
        if not n or not ch or not item_id:
            return False, "Invalid parameters"
        row = await self.fetchone(
            "SELECT quantity, effect_value FROM pokemon_trainer_items ti JOIN pokemon_items i ON ti.item_id=i.id WHERE LOWER(ti.trainer_nick)=? AND ti.channel=? AND ti.item_id=? AND i.item_type='heal'",
            (n.lower(), ch, item_id),
        )
        if not row or int(row[0]) <= 0:
            return False, "No heal items available"
        heal_amt = int(row[1] or 20)
        pokemon_rows = await self.fetchall(
            "SELECT id, current_hp, max_hp, is_fainted FROM pokemon_trainer_pokemon WHERE LOWER(trainer_nick)=? AND channel=? AND is_fainted=0 ORDER BY slot",
            (n.lower(), ch),
        )
        if not pokemon_rows:
            return False, "No healthy Pokemon to heal"
        target = None
        if pokemon_id:
            for r in pokemon_rows:
                if int(r["id"]) == int(pokemon_id):
                    target = r
                    break
        if not target:
            target = pokemon_rows[0]
        new_hp = min(int(target["current_hp"]) + heal_amt, int(target["max_hp"]))
        if new_hp == int(target["current_hp"]) and int(target["current_hp"]) == int(target["max_hp"]):
            return False, "Pokemon already at full HP"
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?",
            (new_hp, int(target["id"])),
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE LOWER(trainer_nick)=? AND channel=? AND item_id=?",
            (n.lower(), ch, item_id),
        )
        return True, f"Healed {heal_amt} HP (now {new_hp}/{target['max_hp']})"

    async def pokemon_trainer_revive(self, nick: str, channel: str, item_id: str, pokemon_id: int | None = None) -> tuple[bool, str]:
        n = (nick or "").strip()
        ch = (channel or "").strip()
        if not n or not ch or not item_id:
            return False, "Invalid parameters"
        row = await self.fetchone(
            "SELECT quantity, effect_value FROM pokemon_trainer_items ti JOIN pokemon_items i ON ti.item_id=i.id WHERE LOWER(ti.trainer_nick)=? AND ti.channel=? AND ti.item_id=? AND i.item_type='revive'",
            (n.lower(), ch, item_id),
        )
        if not row or int(row[0]) <= 0:
            return False, "No revive items available"
        restores_pct = int(row[1] or 50)
        fainted = await self.fetchall(
            "SELECT id, max_hp FROM pokemon_trainer_pokemon WHERE LOWER(trainer_nick)=? AND channel=? AND is_fainted=1 ORDER BY slot",
            (n.lower(), ch),
        )
        if not fainted:
            return False, "No fainted Pokemon to revive"
        target = None
        if pokemon_id:
            for r in fainted:
                if int(r["id"]) == int(pokemon_id):
                    target = r
                    break
        if not target:
            target = fainted[0]
        new_hp = max(1, int(target["max_hp"]) * restores_pct // 100)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
            (new_hp, int(target["id"])),
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE LOWER(trainer_nick)=? AND channel=? AND item_id=?",
            (n.lower(), ch, item_id),
        )
        return True, f"Revived! HP restored to {new_hp}/{target['max_hp']} ({restores_pct}%)"

    async def pokemon_wild_get(self, channel: str) -> sqlite3.Row | None:
        ch = (channel or "").strip()
        if not ch:
            return None
        now = int(time.time())
        return await self.fetchone(
            """SELECT w.*, ps.name as species_name, ps.type1, ps.type2, ps.capture_rate
            FROM pokemon_wild_spawns w
            JOIN pokemon_species ps ON w.species_id = ps.id
            WHERE w.channel=? AND w.expires_ts > ? AND w.captured_by IS NULL""",
            (ch, now),
        )

    async def pokemon_wild_spawn(self, channel: str, species_id: int, level: int, duration_s: int = 600) -> bool:
        ch = (channel or "").strip()
        if not ch:
            return False
        now = int(time.time())
        existing = await self.fetchone("SELECT 1 FROM pokemon_wild_spawns WHERE channel=? AND expires_ts > ? AND captured_by IS NULL", (ch, now))
        if existing:
            return False
        await self.execute(
            """INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
            VALUES(?,?,?,?,?)""",
            (ch, int(species_id), int(level), now, now + int(duration_s)),
        )
        return True

    async def pokemon_wild_capture(self, nick: str, channel: str, spawn_id: int, ball_capture_mod: int = 255) -> tuple[bool, str]:
        n = (nick or "").strip()
        ch = (channel or "").strip()
        if not n or not ch:
            return False, "Invalid"
        now = int(time.time())
        row = await self.fetchone(
            "SELECT * FROM pokemon_wild_spawns WHERE id=? AND channel=? AND expires_ts > ? AND captured_by IS NULL",
            (int(spawn_id), ch, now),
        )
        if not row:
            return False, "No wild Pokemon here or it fled"
        import random
        rate = int(row["capture_rate"] or 255) * ball_capture_mod // 255
        roll = random.randint(1, 255)
        if roll > rate:
            return False, "The Pokemon broke free!"
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?",
            (n, int(spawn_id)),
        )
        species_id = int(row["species_id"])
        level = int(row["level"])
        species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (species_id,))
        hp_base = int(species["hp_base"] or 50)
        max_hp = hp_base * 2 + 60
        await self.execute(
            """INSERT INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)
            ON CONFLICT(nick, channel) DO NOTHING""",
            (n, ch, now),
        )
        slot = 1
        slots = await self.fetchall("SELECT slot FROM pokemon_trainer_pokemon WHERE LOWER(trainer_nick)=? AND channel=? ORDER BY slot DESC LIMIT 1", (n.lower(), ch))
        if slots:
            slot = int(slots[0]["slot"] or 0) + 1
            if slot > 6:
                slot = 6
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon(trainer_nick, channel, species_id, level, current_hp, max_hp, experience, is_fainted, slot, created_ts)
            VALUES(?,?,?,?,?,?,0,0,?,?)""",
            (n, ch, species_id, level, max_hp, max_hp, slot, now),
        )
        species_name = str(row["species_name"] or species["name"] or "?")
        return True, species_name

    async def pokemon_wild_count_24h(self, channel: str) -> int:
        ch = (channel or "").strip()
        if not ch:
            return 0
        cutoff = int(time.time()) - (24 * 3600)
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            (ch, cutoff),
        )
        return int(row[0]) if row else 0

    async def pokemon_get_wild_spawns_per_day(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, int(v))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_wild_spawns_per_day(self, n: int) -> None:
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, n)))

    # ---- Pokemon ----
    async def pokemon_get_wild_spawns_per_day(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, int(v))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_wild_spawns_per_day(self, n: int) -> None:
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, n)))

    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            (nick.strip().lower(), channel.strip()),
        )
        return row is not None

    async def pokemon_create_trainer(self, nick: str, channel: str, species_ids: list[int]) -> None:
        nick_l = nick.strip().lower()
        channel = channel.strip()
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick_l, channel, now),
        )
        default_items = [
            ("potion", 5), ("revive", 3), ("pokeball", 10),
        ]
        for item_id, qty in default_items:
            await self.execute(
                "INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity) VALUES(?,?,?,?) "
                "ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+excluded.quantity",
                (nick_l, channel, item_id, qty),
            )
        for slot, species_id in enumerate(species_ids[:6], 1):
            sp = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (species_id,))
            hp = int(sp["hp_base"] * 2 + 110) if sp else 20
            hp = max(10, min(hp, 999))
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(trainer_nick, channel, species_id, level, current_hp, max_hp, experience, slot, created_ts)
                VALUES(?,?,?,5,?,?,0,?,?)""",
                (nick_l, channel, species_id, hp, hp, slot, now),
            )

    async def pokemon_get_random_species(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT id, pokedex_number, name, type1, type2, hp_base, atk_base, def_base, sp_atk_base, sp_def_base, speed_base, capture_rate FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (limit,),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_pokemon_list(self, nick: str, channel: str) -> list:
        return await self.fetchall(
            """SELECT p.id, p.species_id, s.name as species_name, p.nickname, p.level, p.current_hp, p.max_hp, p.experience, p.is_fainted, p.slot, s.type1, s.type2
               FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON p.species_id = s.id
               WHERE p.trainer_nick=? AND p.channel=? ORDER BY p.slot""",
            (nick.strip().lower(), channel.strip()),
        )

    async def pokemon_trainer_items_list(self, nick: str, channel: str) -> list:
        return await self.fetchall(
            """SELECT i.id, i.name, i.item_type, i.effect_value, t.quantity
               FROM pokemon_trainer_items t
               JOIN pokemon_items i ON t.item_id = i.id
               WHERE t.trainer_nick=? AND t.channel=? AND t.quantity > 0 ORDER BY i.item_type, i.name""",
            (nick.strip().lower(), channel.strip()),
        )

    async def pokemon_trainer_use_item(self, nick: str, channel: str, item_id: str, pokemon_slot: int | None = None) -> tuple[bool, str]:
        nick_l = nick.strip().lower()
        channel = channel.strip()
        row = await self.fetchone(
            "SELECT i.item_type, i.effect_value, t.quantity FROM pokemon_trainer_items t JOIN pokemon_items i ON t.item_id=i.id WHERE t.trainer_nick=? AND t.channel=? AND t.item_id=?",
            (nick_l, channel, item_id.strip().lower()),
        )
        if not row or int(row["quantity"] or 0) <= 0:
            return False, "You don't have that item."
        if row["item_type"] == "heal":
            pokemon = await self.fetchone(
                "SELECT id, current_hp, max_hp, is_fainted FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? AND slot=?",
                (nick_l, channel, int(pokemon_slot or 1)),
            )
            if not pokemon:
                return False, "No Pokémon in that slot."
            if pokemon["is_fainted"]:
                return False, "Use a Revive on fainted Pokémon."
            curr, mx = int(pokemon["current_hp"]), int(pokemon["max_hp"])
            heal = min(int(row["effect_value"]), mx - curr)
            if heal <= 0:
                return False, "Pokémon is already at full HP."
            await self.execute("UPDATE pokemon_trainer_pokemon SET current_hp=current_hp+? WHERE id=?", (heal, pokemon["id"]))
            await self.execute("UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?", (nick_l, channel, item_id))
            return True, f"Restored {heal} HP!"
        if row["item_type"] == "revive":
            pokemon = await self.fetchone(
                "SELECT id, max_hp, is_fainted FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? AND slot=?",
                (nick_l, channel, int(pokemon_slot or 1)),
            )
            if not pokemon:
                return False, "No Pokémon in that slot."
            if not pokemon["is_fainted"]:
                return False, "Pokémon is not fainted."
            pct = int(row["effect_value"])
            hp = max(1, int(pokemon["max_hp"] * pct / 100))
            await self.execute("UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?", (hp, pokemon["id"]))
            await self.execute("UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?", (nick_l, channel, item_id))
            return True, f"Revived! HP restored to {hp}."
        return False, "That item can't be used that way."

    async def pokemon_wild_get_active(self, channel: str) -> object | None:
        now = int(time.time())
        return await self.fetchone(
            "SELECT w.*, s.name as species_name, s.type1, s.type2 FROM pokemon_wild_spawns w JOIN pokemon_species s ON w.species_id=s.id WHERE w.channel=? AND w.expires_ts>? AND w.captured_by IS NULL",
            (channel.strip(), now),
        )

    async def pokemon_wild_spawn_count_24h(self, channel: str) -> int:
        since = int(time.time()) - 86400
        row = await self.fetchone(
            "SELECT COUNT(*) as c FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts>=?",
            (channel.strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_wild_insert(self, channel: str, species_id: int, level: int, duration_s: int = 600) -> bool:
        now = int(time.time())
        expires = now + duration_s
        try:
            await self.execute(
                "INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts) VALUES(?,?,?,?,?)",
                (channel.strip(), species_id, level, now, expires),
            )
            return True
        except Exception:
            return False

    async def pokemon_wild_capture(self, spawn_id: int, nick: str, channel: str) -> tuple[bool, str]:
        nick_l = nick.strip().lower()
        spawn = await self.fetchone("SELECT * FROM pokemon_wild_spawns WHERE id=? AND captured_by IS NULL", (spawn_id,))
        if not spawn:
            return False, "That Pokémon is gone."
        if str(spawn["channel"]) != channel.strip():
            return False, "Wrong channel."
        trainer = await self.fetchone("SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?", (nick_l, channel))
        if not trainer:
            return False, "You need to be a trainer first. Use !pokemon in a channel."
        ball_row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id IN ('pokeball','great_ball','ultra_ball') AND quantity>0 ORDER BY CASE item_id WHEN 'ultra_ball' THEN 1 WHEN 'great_ball' THEN 2 ELSE 3 END LIMIT 1",
            (nick_l, channel),
        )
        if not ball_row:
            return False, "You need a Poké Ball."
        ball_id = "pokeball"
        ball_rows = await self.fetchall("SELECT item_id, quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id IN ('ultra_ball','great_ball','pokeball') AND quantity>0", (nick_l, channel))
        for r in ball_rows:
            if r["item_id"] == "ultra_ball" and int(r["quantity"] or 0) > 0:
                ball_id = "ultra_ball"
                break
            if r["item_id"] == "great_ball" and int(r["quantity"] or 0) > 0:
                ball_id = "great_ball"
                break
            ball_id = "pokeball"
        import random
        rate = int(await self.fetchone("SELECT effect_value FROM pokemon_items WHERE id=?", (ball_id,)) or [255])[0] if isinstance((await self.fetchone("SELECT effect_value FROM pokemon_items WHERE id=?", (ball_id,))), tuple) else 255
        cap_rate = int(spawn["species_id"])
        sp = await self.fetchone("SELECT capture_rate FROM pokemon_species WHERE id=?", (spawn["species_id"],))
        cap_rate = int(sp["capture_rate"]) if sp else 255
        roll = random.randint(1, 255)
        if roll > cap_rate:
            await self.execute("UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?", (nick_l, channel, ball_id))
            return False, "The Pokémon broke free!"
        slots = await self.fetchall("SELECT slot FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?", (nick_l, channel))
        used = {r["slot"] for r in slots}
        slot = 1
        for s in range(1, 7):
            if s not in used:
                slot = s
                break
        now_ts = int(time.time())
        sp = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (spawn["species_id"],))
        hp = int(sp["hp_base"] * 2 + 110) if sp else 20
        hp = max(10, min(hp, 999))
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon(trainer_nick, channel, species_id, level, current_hp, max_hp, experience, slot, created_ts)
            VALUES(?,?,?,?,?,?,0,?,?)""",
            (nick_l, channel, spawn["species_id"], int(spawn["level"] or 5), hp, hp, slot, now_ts),
        )
        await self.execute("UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?", (nick_l, spawn_id))
        await self.execute("UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?", (nick_l, channel, ball_id))
        return True, f"Congratulations! You caught {spawn['species_name']}!"

    async def pokemon_wild_expire_old(self, now_ts: int | None = None) -> int:
        now = int(now_ts or time.time())
        async with self._lock:
            cur = self._conn.execute("DELETE FROM pokemon_wild_spawns WHERE expires_ts<=? OR captured_by IS NOT NULL", (now,))
            return int(cur.rowcount or 0)

    async def pokemon_level_up(self, nick: str, channel: str, pokemon_id: int) -> tuple[bool, str]:
        row = await self.fetchone(
            "SELECT p.*, s.hp_base FROM pokemon_trainer_pokemon p JOIN pokemon_species s ON p.species_id=s.id WHERE p.id=? AND p.trainer_nick=? AND p.channel=?",
            (pokemon_id, nick.strip().lower(), channel.strip()),
        )
        if not row:
            return False, "Pokémon not found."
        exp = int(row["experience"] or 0) + 100
        lvl = int(row["level"] or 5)
        new_lvl = min(100, lvl + (exp // 500))
        hp_base = int(row["hp_base"] or 50)
        new_max_hp = max(10, min(999, int(hp_base * 2 * new_lvl / 50 + 110)))
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET experience=?, level=?, max_hp=?, current_hp=? WHERE id=?",
            (exp, new_lvl, new_max_hp, new_max_hp, pokemon_id),
        )
        return True, f"Level up! Now level {new_lvl}!"

    # ---- Pokemon service ----
    async def pokemon_get_wild_spawns_per_day(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(1000, int(v)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_wild_spawns_per_day(self, n: int) -> None:
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, min(1000, n))))

    async def pokemon_count_species(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_get_random_species(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?", (limit,)
        )
        return list(rows) if rows else []

    async def pokemon_get_species(self, species_id: int) -> sqlite3.Row | None:
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (species_id,))

    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            (nick.strip().lower(), channel.strip()),
        )
        return row is not None

    async def pokemon_create_trainer(self, nick: str, channel: str, species_ids: list[int]) -> None:
        now = int(time.time())
        n = nick.strip().lower()
        ch = channel.strip()
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (n, ch, now),
        )
        default_items = [("potion", 5), ("revive", 2), ("pokeball", 10)]
        for item_id, qty in default_items:
            await self.execute(
                "INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity) VALUES(?,?,?,?) "
                "ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+excluded.quantity",
                (n, ch, item_id, qty),
            )
        for slot, sid in enumerate(species_ids[:6], 1):
            species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (sid,))
            if species:
                hp = max(10, int(species["hp_base"] or 50) + (5 * (5 - 1)))
                await self.execute(
                    """INSERT INTO pokemon_trainer_pokemon(
                        trainer_nick, channel, species_id, level, current_hp, max_hp, experience, slot, created_ts
                    ) VALUES(?,?,?,?,?,?,?,?,?)""",
                    (n, ch, sid, 5, hp, hp, 0, slot, now),
                )

    async def pokemon_get_trainer_pokemon(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT p.*, s.name as species_name, s.type1, s.type2
               FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON p.species_id = s.id
               WHERE p.trainer_nick=? AND p.channel=?
               ORDER BY p.slot""",
            (nick.strip().lower(), channel.strip()),
        )
        return list(rows) if rows else []

    async def pokemon_get_trainer_items(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT t.item_id, t.quantity, i.name, i.item_type, i.effect_value
               FROM pokemon_trainer_items t
               JOIN pokemon_items i ON t.item_id = i.id
               WHERE t.trainer_nick=? AND t.channel=? AND t.quantity > 0
               ORDER BY i.item_type, i.name""",
            (nick.strip().lower(), channel.strip()),
        )
        return list(rows) if rows else []

    async def pokemon_use_heal(self, nick: str, channel: str, item_id: str, pokemon_id: int) -> tuple[bool, str]:
        n, ch = nick.strip().lower(), channel.strip()
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (n, ch, item_id),
        )
        if not row or int(row[0] or 0) < 1:
            return False, "You don't have that item."
        pokemon = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
            (pokemon_id, n, ch),
        )
        if not pokemon:
            return False, "Pokemon not found."
        if pokemon["is_fainted"]:
            return False, "Use a Revive on fainted Pokemon."
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (item_id,))
        if not item or item["item_type"] != "heal":
            return False, "That item can't heal."
        heal_amt = int(item["effect_value"] or 0)
        new_hp = min(int(pokemon["max_hp"]), int(pokemon["current_hp"]) + heal_amt)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?",
            (new_hp, pokemon_id),
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (n, ch, item_id),
        )
        return True, f"Healed {heal_amt} HP. {pokemon['nickname'] or pokemon['species_name']} now has {new_hp}/{pokemon['max_hp']} HP."

    async def pokemon_use_revive(self, nick: str, channel: str, item_id: str, pokemon_id: int) -> tuple[bool, str]:
        n, ch = nick.strip().lower(), channel.strip()
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (n, ch, item_id),
        )
        if not row or int(row[0] or 0) < 1:
            return False, "You don't have that item."
        pokemon = await self.fetchone(
            "SELECT p.*, s.name as species_name FROM pokemon_trainer_pokemon p "
            "JOIN pokemon_species s ON p.species_id = s.id "
            "WHERE p.id=? AND p.trainer_nick=? AND p.channel=?",
            (pokemon_id, n, ch),
        )
        if not pokemon:
            return False, "Pokemon not found."
        if not pokemon["is_fainted"]:
            return False, "That Pokemon isn't fainted."
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (item_id,))
        if not item or item["item_type"] != "revive":
            return False, "That item isn't a Revive."
        pct = int(item["effect_value"] or 50) / 100.0
        new_hp = max(1, int(int(pokemon["max_hp"]) * pct))
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
            (new_hp, pokemon_id),
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (n, ch, item_id),
        )
        name = pokemon["nickname"] or pokemon["species_name"]
        return True, f"{name} was revived with {new_hp}/{pokemon['max_hp']} HP!"

    async def pokemon_level_up(self, nick: str, channel: str, pokemon_id: int) -> tuple[bool, str]:
        n, ch = nick.strip().lower(), channel.strip()
        pokemon = await self.fetchone(
            "SELECT p.*, s.name as species_name, s.hp_base FROM pokemon_trainer_pokemon p "
            "JOIN pokemon_species s ON p.species_id = s.id "
            "WHERE p.id=? AND p.trainer_nick=? AND p.channel=?",
            (pokemon_id, n, ch),
        )
        if not pokemon:
            return False, "Pokemon not found."
        level = int(pokemon["level"] or 5)
        exp = int(pokemon["experience"] or 0)
        exp_needed = level * 100
        if exp < exp_needed:
            return False, f"Needs {exp_needed - exp} more EXP to level up (currently {level})."
        new_level = level + 1
        hp_base = int(pokemon["hp_base"] or 50)
        new_max_hp = max(10, hp_base + (5 * (new_level - 1)))
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, experience=experience-?, max_hp=?, current_hp=? WHERE id=?",
            (new_level, exp_needed, new_max_hp, new_max_hp, pokemon_id),
        )
        name = pokemon["nickname"] or pokemon["species_name"]
        return True, f"{name} grew to level {new_level}! HP: {new_max_hp}/{new_max_hp}"

    async def pokemon_get_wild_spawn(self, channel: str) -> sqlite3.Row | None:
        return await self.fetchone(
            "SELECT w.*, s.name as species_name, s.type1, s.type2 FROM pokemon_wild_spawns w "
            "JOIN pokemon_species s ON w.species_id = s.id "
            "WHERE w.channel=? AND w.captured_by IS NULL AND w.expires_ts > ?",
            (channel.strip(), int(time.time())),
        )

    async def pokemon_wild_spawn_count_24h(self, channel: str) -> int:
        since = int(time.time()) - (24 * 3600)
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            (channel.strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_create_wild_spawn(self, channel: str, species_id: int, level: int, duration_s: int = 900) -> bool:
        now = int(time.time())
        ch = channel.strip()
        existing = await self.fetchone(
            "SELECT 1 FROM pokemon_wild_spawns WHERE channel=? AND captured_by IS NULL AND expires_ts > ?",
            (ch, now),
        )
        if existing:
            return False
        per_day = await self.pokemon_get_wild_spawns_per_day()
        count = await self.pokemon_wild_spawn_count_24h(ch)
        if count >= per_day:
            return False
        expires = now + duration_s
        await self.execute(
            "INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts) VALUES(?,?,?,?,?)",
            (ch, species_id, level, now, expires),
        )
        return True

    async def pokemon_capture_attempt(
        self, nick: str, channel: str, ball_id: str
    ) -> tuple[bool, str, int | None]:
        n, ch = nick.strip().lower(), channel.strip()
        spawn = await self.pokemon_get_wild_spawn(ch)
        if not spawn:
            return False, "No wild Pokemon here right now.", None
        ball = await self.fetchone("SELECT * FROM pokemon_items WHERE id=? AND item_type='ball'", (ball_id,))
        if not ball:
            return False, "Invalid ball.", None
        inv = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (n, ch, ball_id),
        )
        if not inv or int(inv[0] or 0) < 1:
            return False, f"You don't have a {ball['name']}.", None
        capture_rate = int(spawn.get("capture_rate", 255) or 255)
        ball_mod = {255: 1.0, 200: 1.5, 150: 2.0}.get(int(ball.get("effect_value", 255) or 255), 1.0)
        chance = min(0.95, (capture_rate / 255.0) * ball_mod * 0.5)
        import random
        if random.random() < chance:
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (n, ch, ball_id),
            )
            await self.execute(
                "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?", (n, spawn["id"])
            )
            species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (spawn["species_id"],))
            hp = max(10, int(species["hp_base"] or 50) + (5 * (int(spawn["level"]) - 1)))
            slot = await self.fetchone(
                "SELECT COALESCE(MAX(slot), 0) + 1 FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
                (n, ch),
            )
            slot_num = int(slot[0]) if slot else 1
            if slot_num <= 6:
                await self.execute(
                    """INSERT INTO pokemon_trainer_pokemon(
                        trainer_nick, channel, species_id, level, current_hp, max_hp, experience, slot, created_ts
                    ) VALUES(?,?,?,?,?,?,?,?,?)""",
                    (n, ch, spawn["species_id"], spawn["level"], hp, hp, 0, slot_num, now),
                )
            return True, f"Gotcha! {spawn['species_name']} was caught!", spawn["species_id"]
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (n, ch, ball_id),
        )
        return False, f"The wild {spawn['species_name']} broke free!", spawn["species_id"]

    # ---- Pokemon ----
    async def pokemon_get_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_get_random_species(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?", (int(limit),)
        )
        return list(rows) if rows else []

    async def pokemon_get_species_by_id(self, species_id: int):
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))

    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return row is not None

    async def pokemon_trainer_create(
        self, nick: str, channel: str, *, species_ids: list[int], starter_items: list[tuple[str, int]]
    ) -> None:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick_l, ch, now),
        )
        for slot, sid in enumerate(species_ids[:6], 1):
            spec = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(sid),))
            if spec:
                hp = self._pokemon_hp_for_level(int(spec["hp_base"] or 50), 5)
                await self.execute(
                    """INSERT INTO pokemon_trainer_pokemon(
                        trainer_nick, channel, species_id, level, current_hp, max_hp, experience, slot, created_ts
                    ) VALUES(?,?,?,5,?,?,0,?,?)""",
                    (nick_l, ch, int(sid), hp, hp, slot, now),
                )
        for item_id, qty in starter_items:
            await self.execute(
                """INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity)
                VALUES(?,?,?,?) ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+?""",
                (nick_l, ch, item_id, int(qty), int(qty)),
            )

    def _pokemon_hp_for_level(self, base_hp: int, level: int) -> int:
        return max(10, int((2 * base_hp + 50) * level / 50) + level + 10)

    async def pokemon_trainer_get_pokemon(self, nick: str, channel: str) -> list:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        rows = await self.fetchall(
            """SELECT p.*, s.name as species_name, s.type1, s.type2
            FROM pokemon_trainer_pokemon p
            JOIN pokemon_species s ON p.species_id = s.id
            WHERE p.trainer_nick=? AND p.channel=?
            ORDER BY p.slot""",
            (nick_l, ch),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_get_items(self, nick: str, channel: str) -> list:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        rows = await self.fetchall(
            """SELECT ti.item_id, ti.quantity, i.name, i.item_type, i.effect_value
            FROM pokemon_trainer_items ti
            JOIN pokemon_items i ON ti.item_id = i.id
            WHERE ti.trainer_nick=? AND ti.channel=? AND ti.quantity > 0
            ORDER BY i.item_type, i.name""",
            (nick_l, ch),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_use_heal(
        self, nick: str, channel: str, item_id: str, pokemon_id: int
    ) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        item = await self.fetchone(
            "SELECT * FROM pokemon_items WHERE id=? AND item_type='heal'", (item_id,)
        )
        if not item:
            return False, "Invalid or non-healing item."
        row = await self.fetchone(
            "SELECT * FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        if not row or int(row["quantity"] or 0) < 1:
            return False, "You don't have any of that item."
        pokemon = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
            (pokemon_id, nick_l, ch),
        )
        if not pokemon:
            return False, "Pokémon not found."
        if pokemon["is_fainted"]:
            return False, "Use a Revive on fainted Pokémon."
        heal_amt = int(item["effect_value"] or 20)
        new_hp = min(int(pokemon["current_hp"] or 0) + heal_amt, int(pokemon["max_hp"] or 0))
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?", (new_hp, pokemon_id)
        )
        await self.execute(
            """UPDATE pokemon_trainer_items SET quantity=quantity-1
            WHERE trainer_nick=? AND channel=? AND item_id=?""",
            (nick_l, ch, item_id),
        )
        return True, f"Healed {heal_amt} HP. {pokemon['nickname'] or pokemon['species_name']} now has {new_hp}/{pokemon['max_hp']} HP."

    async def pokemon_trainer_use_revive(
        self, nick: str, channel: str, item_id: str, pokemon_id: int
    ) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        item = await self.fetchone(
            "SELECT * FROM pokemon_items WHERE id=? AND item_type='revive'", (item_id,)
        )
        if not item:
            return False, "Invalid or non-revive item."
        row = await self.fetchone(
            "SELECT * FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        if not row or int(row["quantity"] or 0) < 1:
            return False, "You don't have any of that item."
        pokemon = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon p JOIN pokemon_species s ON p.species_id=s.id WHERE p.id=? AND p.trainer_nick=? AND p.channel=?",
            (pokemon_id, nick_l, ch),
        )
        if not pokemon:
            return False, "Pokémon not found."
        if not pokemon["is_fainted"]:
            return False, "Pokémon is not fainted."
        pct = int(item["effect_value"] or 50) / 100.0
        new_hp = max(1, int(int(pokemon["max_hp"] or 0) * pct))
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
            (new_hp, pokemon_id),
        )
        await self.execute(
            """UPDATE pokemon_trainer_items SET quantity=quantity-1
            WHERE trainer_nick=? AND channel=? AND item_id=?""",
            (nick_l, ch, item_id),
        )
        return True, f"Revived! {pokemon.get('nickname') or pokemon.get('species_name', '?')} has {new_hp} HP."

    async def pokemon_trainer_heal_all(self, nick: str, channel: str) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=max_hp, is_fainted=0 WHERE trainer_nick=? AND channel=?",
            (nick_l, ch),
        )
        return True, "All Pokémon have been fully healed!"

    async def pokemon_wild_get_active(self, channel: str):
        return await self.fetchone(
            """SELECT w.*, s.name as species_name, s.type1, s.type2
            FROM pokemon_wild_spawns w
            JOIN pokemon_species s ON w.species_id = s.id
            WHERE w.channel=? AND w.expires_ts > ? AND w.captured_by IS NULL""",
            (channel.strip(), int(time.time())),
        )

    async def pokemon_wild_spawns_count_24h(self, channel: str) -> int:
        since = int(time.time()) - 86400
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            (channel.strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_wild_spawn(self, channel: str, species_id: int, level: int) -> bool:
        now = int(time.time())
        per_day = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            limit = int(per_day)
        except (TypeError, ValueError):
            limit = 24
        count = await self.pokemon_wild_spawns_count_24h(channel)
        if count >= limit:
            return False
        expires = now + 600
        await self.execute(
            """INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
            VALUES(?,?,?,?,?)""",
            (channel.strip(), int(species_id), int(level), now, expires),
        )
        return True

    async def pokemon_wild_capture(
        self, channel: str, spawn_id: int, nick: str
    ) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        spawn = await self.fetchone(
            "SELECT * FROM pokemon_wild_spawns WHERE id=? AND channel=? AND captured_by IS NULL AND expires_ts > ?",
            (spawn_id, channel.strip(), int(time.time())),
        )
        if not spawn:
            return False, "That Pokémon has fled or was already caught!"
        trainer_exists = await self.pokemon_trainer_exists(nick_l, channel)
        if not trainer_exists:
            return False, "You're not a trainer yet. Say !pokemon in a channel to get started!"
        spec = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (spawn["species_id"],))
        if not spec:
            return False, "Invalid species."
        hp = self._pokemon_hp_for_level(int(spec["hp_base"] or 50), int(spawn["level"] or 5))
        now = int(time.time())
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon(
                trainer_nick, channel, species_id, level, current_hp, max_hp, experience, slot, created_ts
            ) SELECT ?, ?, ?, ?, ?, ?, 0,
                COALESCE((SELECT MAX(slot) FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?) + 1, 1),
                ?
            FROM (SELECT 1)
            WHERE (SELECT COUNT(*) FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?) < 6""",
            (nick_l, channel, spawn["species_id"], spawn["level"], hp, hp, nick_l, channel, now, nick_l, channel),
        )
        cur = await self.fetchone("SELECT changes()", ())
        if cur and int(cur[0] or 0) == 0:
            return False, "Your party is full! Store a Pokémon first."
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?", (nick_l, spawn_id)
        )
        return True, f"Caught {spec['name']}!"

    async def pokemon_wild_expire_old(self) -> int:
        now = int(time.time())
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM pokemon_wild_spawns WHERE expires_ts <= ? OR captured_by IS NOT NULL",
                (now,),
            )
            return int(cur.rowcount or 0)

    async def pokemon_get_setting_spawns_per_day(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, int(v))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_spawns_per_day(self, n: int) -> None:
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, int(n))))

    async def pokemon_level_up(self, nick: str, channel: str, pokemon_id: int) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        row = await self.fetchone(
            """SELECT p.*, s.hp_base, s.name as species_name FROM pokemon_trainer_pokemon p
            JOIN pokemon_species s ON p.species_id = s.id
            WHERE p.id=? AND p.trainer_nick=? AND p.channel=?""",
            (pokemon_id, nick_l, ch),
        )
        if not row:
            return False, "Pokémon not found."
        level = int(row["level"] or 5)
        exp = int(row["experience"] or 0)
        exp_needed = level * 50
        if exp < exp_needed:
            return False, f"Needs {exp_needed - exp} more XP to level up."
        new_level = level + 1
        new_max_hp = self._pokemon_hp_for_level(int(row["hp_base"] or 50), new_level)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, max_hp=?, current_hp=?, experience=experience-? WHERE id=?",
            (new_level, new_max_hp, new_max_hp, exp_needed, pokemon_id),
        )
        return True, f"Level up! {row.get('nickname') or row['species_name']} is now level {new_level}!"

    # ---- Pokemon ----
    async def pokemon_get_spawns_per_day(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, int(v))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_spawns_per_day(self, n: int) -> None:
        now = int(time.time())
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, n)))

    async def pokemon_is_trainer(self, nick: str, channel: str) -> bool:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return False
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE lower(nick)=? AND channel=?",
            (nick_l, ch),
        )
        return row is not None

    async def pokemon_create_trainer(self, nick: str, channel: str) -> None:
        nick_n = (nick or "").strip()
        nick_l = nick_n.lower()
        ch = (channel or "").strip()
        if not nick_n or not ch:
            return
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick_n, ch, now),
        )

    async def pokemon_add_starter(self, nick: str, channel: str, species_id: int, slot: int, level: int = 5) -> None:
        nick_n = (nick or "").strip()
        ch = (channel or "").strip()
        if not nick_n or not ch or species_id <= 0:
            return
        now = int(time.time())
        sp = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (species_id,))
        hp = int(sp["hp_base"] * (level / 50)) + 10 if sp else 20
        hp = max(10, hp)
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon
               (trainer_nick, channel, species_id, level, current_hp, max_hp, experience, slot, created_ts)
               VALUES(?,?,?,?,?,?,0,?,?)""",
            (nick_n, ch, species_id, level, hp, hp, slot, now),
        )

    async def pokemon_add_trainer_item(self, nick: str, channel: str, item_id: str, quantity: int) -> None:
        nick_n = (nick or "").strip()
        ch = (channel or "").strip()
        if not nick_n or not ch or not item_id:
            return
        await self.execute(
            "INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity) VALUES(?,?,?,?) "
            "ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+excluded.quantity",
            (nick_n, ch, item_id, quantity),
        )

    async def pokemon_get_random_species(self, limit: int = 5) -> list[sqlite3.Row]:
        rows = await self.fetchall(
            "SELECT id, name, type1, type2, hp_base FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (limit,),
        )
        return list(rows) if rows else []

    async def pokemon_get_trainer_pokemon(self, nick: str, channel: str) -> list[sqlite3.Row]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return []
        rows = await self.fetchall(
            """SELECT p.*, s.name as species_name, s.type1, s.type2
               FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON p.species_id = s.id
               WHERE lower(p.trainer_nick)=? AND p.channel=? ORDER BY p.slot""",
            (nick_l, ch),
        )
        return list(rows) if rows else []

    async def pokemon_get_trainer_items(self, nick: str, channel: str) -> list[sqlite3.Row]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return []
        rows = await self.fetchall(
            """SELECT ti.*, i.name as item_name, i.item_type, i.effect_value
               FROM pokemon_trainer_items ti
               JOIN pokemon_items i ON ti.item_id = i.id
               WHERE lower(ti.trainer_nick)=? AND ti.channel=? AND ti.quantity > 0
               ORDER BY i.item_type, i.name""",
            (nick_l, ch),
        )
        return list(rows) if rows else []

    async def pokemon_get_wild_spawn(self, channel: str) -> sqlite3.Row | None:
        ch = (channel or "").strip()
        if not ch:
            return None
        now = int(time.time())
        return await self.fetchone(
            """SELECT w.*, s.name as species_name, s.type1, s.type2, s.capture_rate
               FROM pokemon_wild_spawns w
               JOIN pokemon_species s ON w.species_id = s.id
               WHERE w.channel=? AND w.expires_ts > ? AND w.captured_by IS NULL""",
            (ch, now),
        )

    async def pokemon_count_spawns_last_24h(self, channel: str) -> int:
        ch = (channel or "").strip()
        if not ch:
            return 0
        since = int(time.time()) - (24 * 3600)
        row = await self.fetchone(
            "SELECT COUNT(*) as c FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            (ch, since),
        )
        return int(row["c"]) if row else 0

    async def pokemon_insert_wild_spawn(self, channel: str, species_id: int, level: int, duration_s: int) -> bool:
        ch = (channel or "").strip()
        if not ch or species_id <= 0:
            return False
        now = int(time.time())
        expires = now + duration_s
        try:
            await self.execute(
                """INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
                   VALUES(?,?,?,?,?)""",
                (ch, species_id, level, now, expires),
            )
            return True
        except Exception:
            return False

    async def pokemon_capture_wild(self, channel: str, nick: str) -> sqlite3.Row | None:
        ch = (channel or "").strip()
        nick_n = (nick or "").strip()
        if not ch or not nick_n:
            return None
        spawn = await self.pokemon_get_wild_spawn(ch)
        if not spawn:
            return None
        now = int(time.time())
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE channel=? AND id=? AND captured_by IS NULL",
            (nick_n, ch, spawn["id"]),
        )
        slot = 1
        rows = await self.pokemon_get_trainer_pokemon(nick_n, ch)
        if rows:
            slots = {r["slot"] for r in rows}
            for s in range(1, 7):
                if s not in slots:
                    slot = s
                    break
            else:
                slot = len(rows) + 1 if len(rows) < 6 else 6
        await self.pokemon_add_starter(
            nick_n, ch, int(spawn["species_id"]), slot, int(spawn["level"] or 5)
        )
        return await self.fetchone(
            """SELECT p.*, s.name as species_name FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON p.species_id = s.id
               WHERE p.trainer_nick=? AND p.channel=? AND p.species_id=?
               ORDER BY p.id DESC LIMIT 1""",
            (nick_n, ch, spawn["species_id"]),
        )

    async def pokemon_use_item(self, nick: str, channel: str, item_id: str, pokemon_id: int) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch or not item_id:
            return False, "Invalid parameters"
        row = await self.fetchone(
            "SELECT quantity, item_type, effect_value FROM pokemon_trainer_items ti "
            "JOIN pokemon_items i ON ti.item_id = i.id "
            "WHERE lower(ti.trainer_nick)=? AND ti.channel=? AND ti.item_id=? AND ti.quantity > 0",
            (nick_l, ch, item_id),
        )
        if not row:
            return False, "You don't have that item."
        pkmn = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND lower(trainer_nick)=? AND channel=?",
            (pokemon_id, nick_l, ch),
        )
        if not pkmn:
            return False, "Pokemon not found."
        itype = str(row["item_type"])
        if itype == "heal":
            if pkmn["is_fainted"]:
                return False, "Use a Revive on fainted Pokemon."
            heal = int(row["effect_value"])
            new_hp = min(pkmn["max_hp"], pkmn["current_hp"] + heal)
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?",
                (new_hp, pokemon_id),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick IN (SELECT nick FROM pokemon_trainers WHERE lower(nick)=? AND channel=?) AND channel=? AND item_id=?",
                (nick_l, ch, ch, item_id),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE lower(trainer_nick)=? AND channel=? AND item_id=?",
                (nick_l, ch, item_id),
            )
            return True, f"Healed for {min(heal, pkmn['max_hp'] - pkmn['current_hp'])} HP!"
        if itype == "revive":
            if not pkmn["is_fainted"]:
                return False, "That Pokemon isn't fainted."
            pct = int(row["effect_value"])  # 50 or 100
            new_hp = int(pkmn["max_hp"] * pct / 100)
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
                (new_hp, pokemon_id),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE lower(trainer_nick)=? AND channel=? AND item_id=?",
                (nick_l, ch, item_id),
            )
            return True, f"Revived to {pct}% HP!"
        return False, "Can't use that item on a Pokemon."

    async def pokemon_use_item_simple(self, nick: str, channel: str, item_id: str, pokemon_id: int) -> tuple[bool, str]:
        nick_n = (nick or "").strip()
        nick_l = nick_n.lower()
        ch = (channel or "").strip()
        if not nick_n or not ch or not item_id:
            return False, "Invalid parameters"
        inv = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE lower(trainer_nick)=? AND channel=? AND item_id=?",
            (nick_l, ch, item_id),
        )
        if not inv or inv["quantity"] <= 0:
            return False, "You don't have that item."
        item = await self.fetchone("SELECT item_type, effect_value FROM pokemon_items WHERE id=?", (item_id,))
        if not item:
            return False, "Unknown item."
        pkmn = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND lower(trainer_nick)=? AND channel=?",
            (pokemon_id, nick_l, ch),
        )
        if not pkmn:
            return False, "Pokemon not found."
        itype = str(item["item_type"])
        if itype == "heal":
            if pkmn["is_fainted"]:
                return False, "Use a Revive on fainted Pokemon."
            heal = int(item["effect_value"])
            new_hp = min(pkmn["max_hp"], pkmn["current_hp"] + heal)
            await self.execute("UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?", (new_hp, pokemon_id))
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE lower(trainer_nick)=? AND channel=? AND item_id=?",
                (nick_l, ch, item_id),
            )
            return True, f"Healed for {min(heal, pkmn['max_hp'] - pkmn['current_hp'])} HP!"
        if itype == "revive":
            if not pkmn["is_fainted"]:
                return False, "That Pokemon isn't fainted."
            pct = int(item["effect_value"])
            new_hp = max(1, int(pkmn["max_hp"] * pct / 100))
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
                (new_hp, pokemon_id),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE lower(trainer_nick)=? AND channel=? AND item_id=?",
                (nick_l, ch, item_id),
            )
            return True, f"Revived to {pct}% HP!"
        return False, "Can't use that item on a Pokemon."

    async def pokemon_level_up(self, nick: str, channel: str, pokemon_id: int) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return False, "Invalid parameters"
        pkmn = await self.fetchone(
            "SELECT p.*, s.hp_base FROM pokemon_trainer_pokemon p "
            "JOIN pokemon_species s ON p.species_id = s.id "
            "WHERE p.id=? AND lower(p.trainer_nick)=? AND p.channel=?",
            (pokemon_id, nick_l, ch),
        )
        if not pkmn:
            return False, "Pokemon not found."
        new_level = min(100, pkmn["level"] + 1)
        if new_level == pkmn["level"]:
            return False, "Pokemon is already max level."
        new_max_hp = max(10, int(pkmn["hp_base"] * (new_level / 50)) + 10)
        new_hp = pkmn["current_hp"]
        if not pkmn["is_fainted"]:
            new_hp = min(new_max_hp, new_hp + max(1, new_max_hp - pkmn["max_hp"]))
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, max_hp=?, current_hp=? WHERE id=?",
            (new_level, new_max_hp, new_hp, pokemon_id),
        )
        return True, f"Leveled up to {new_level}!"

    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) as c FROM pokemon_species", ())
        return int(row["c"]) if row else 0

    async def pokemon_delete_expired_spawns(self) -> int:
        now = int(time.time())
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM pokemon_wild_spawns WHERE expires_ts <= ? OR captured_by IS NOT NULL",
                (now,),
            )
            return int(cur.rowcount)

    # ---- Pokemon ----
    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_species_get_random(self, limit: int = 5) -> list[sqlite3.Row]:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows) if rows else []

    async def pokemon_species_get(self, species_id: int) -> sqlite3.Row | None:
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))

    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        if not nick_l or not chan:
            return False
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE lower(nick)=? AND channel=?",
            (nick_l, chan),
        )
        return row is not None

    async def pokemon_trainer_create(
        self,
        nick: str,
        channel: str,
        *,
        starter_species_ids: list[int],
        starter_items: list[tuple[str, int]] | None = None,
    ) -> None:
        now = int(time.time())
        nick_n = (nick or "").strip()
        nick_l = nick_n.lower()
        chan = (channel or "").strip()
        if not nick_n or not chan:
            return
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?, ?, ?)",
            (nick_n, chan, now),
        )
        for slot, species_id in enumerate(starter_species_ids[:6], 1):
            species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))
            if not species:
                continue
            hp = max(10, int(species["hp_base"] or 50) * 2 + 50)
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(
                    trainer_nick, channel, species_id, level, current_hp, max_hp,
                    experience, is_fainted, slot, created_ts)
                VALUES(?,?,?,5,?,?,0,0,?,?)""",
                (nick_n, chan, int(species_id), hp, hp, slot, now),
            )
        starter_items = starter_items or [
            ("potion", 5),
            ("revive", 3),
            ("pokeball", 10),
        ]
        for item_id, qty in starter_items:
            await self.execute(
                """INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity)
                VALUES(?,?,?,?) ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+?""",
                (nick_n, chan, item_id, qty, qty),
            )

    async def pokemon_trainer_get_pokemon(
        self, nick: str, channel: str
    ) -> list[sqlite3.Row]:
        nick_n = (nick or "").strip()
        chan = (channel or "").strip()
        if not nick_n or not chan:
            return []
        rows = await self.fetchall(
            """SELECT tp.*, s.name as species_name, s.type1, s.type2, s.pokedex_number
               FROM pokemon_trainer_pokemon tp
               JOIN pokemon_species s ON tp.species_id = s.id
               WHERE lower(tp.trainer_nick)=lower(?) AND tp.channel=?
               ORDER BY tp.slot""",
            (nick_n, chan),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_get_items(
        self, nick: str, channel: str
    ) -> list[sqlite3.Row]:
        nick_n = (nick or "").strip()
        chan = (channel or "").strip()
        if not nick_n or not chan:
            return []
        rows = await self.fetchall(
            """SELECT ti.*, i.name, i.item_type, i.effect_value
               FROM pokemon_trainer_items ti
               JOIN pokemon_items i ON ti.item_id = i.id
               WHERE lower(ti.trainer_nick)=lower(?) AND ti.channel=? AND ti.quantity > 0
               ORDER BY i.item_type, i.name""",
            (nick_n, chan),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_use_item(
        self, nick: str, channel: str, item_id: str, target_slot: int | None
    ) -> tuple[bool, str]:
        nick_n = (nick or "").strip()
        chan = (channel or "").strip()
        if not nick_n or not chan:
            return False, "Invalid trainer."
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE lower(trainer_nick)=lower(?) AND channel=? AND item_id=?",
            (nick_n, chan, item_id),
        )
        if not row or int(row[0] or 0) < 1:
            return False, f"You don't have that item."
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (item_id,))
        if not item:
            return False, "Unknown item."
        itype = str(item["item_type"] or "").lower()
        if itype == "heal":
            if target_slot is None:
                return False, "Specify a slot: !heal <slot> or !use potion <slot>"
            poke = await self.fetchone(
                """SELECT * FROM pokemon_trainer_pokemon
                   WHERE lower(trainer_nick)=lower(?) AND channel=? AND slot=?""",
                (nick_n, chan, target_slot),
            )
            if not poke:
                return False, f"No Pokémon in slot {target_slot}."
            if poke["is_fainted"]:
                return False, "That Pokémon is fainted. Use a Revive."
            heal = int(item["effect_value"] or 20)
            new_hp = min(poke["max_hp"], poke["current_hp"] + heal)
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?",
                (new_hp, poke["id"]),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE lower(trainer_nick)=lower(?) AND channel=? AND item_id=?",
                (nick_n, chan, item_id),
            )
            return True, f"Healed {poke['species_name']} to {new_hp}/{poke['max_hp']} HP."
        if itype == "revive":
            if target_slot is None:
                return False, "Specify a slot: !revive <slot>"
            poke = await self.fetchone(
                """SELECT * FROM pokemon_trainer_pokemon
                   WHERE lower(trainer_nick)=lower(?) AND channel=? AND slot=?""",
                (nick_n, chan, target_slot),
            )
            if not poke:
                return False, f"No Pokémon in slot {target_slot}."
            if not poke["is_fainted"]:
                return False, "That Pokémon is not fainted."
            pct = int(item["effect_value"] or 50)
            new_hp = max(1, int(poke["max_hp"] * pct / 100))
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
                (new_hp, poke["id"]),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE lower(trainer_nick)=lower(?) AND channel=? AND item_id=?",
                (nick_n, chan, item_id),
            )
            return True, f"Revived {poke['species_name']} to {new_hp} HP."
        return False, f"Can't use {item['name']} that way."

    async def pokemon_wild_get(self, channel: str) -> sqlite3.Row | None:
        chan = (channel or "").strip()
        if not chan:
            return None
        return await self.fetchone(
            """SELECT w.*, s.name as species_name, s.type1, s.type2, s.capture_rate
               FROM pokemon_wild_spawns w
               JOIN pokemon_species s ON w.species_id = s.id
               WHERE w.channel=? AND w.captured_by IS NULL AND w.expires_ts > ?""",
            (chan, int(time.time())),
        )

    async def pokemon_wild_spawn_count_24h(self, channel: str) -> int:
        since = int(time.time()) - 86400
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            ((channel or "").strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_wild_spawns_per_day_get(self) -> int:
        val = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(1440, int(val)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_wild_spawn(
        self, channel: str, species_id: int, level: int, duration_seconds: int = 900
    ) -> bool:
        now = int(time.time())
        chan = (channel or "").strip()
        if not chan:
            return False
        existing = await self.fetchone(
            "SELECT 1 FROM pokemon_wild_spawns WHERE channel=? AND captured_by IS NULL AND expires_ts > ?",
            (chan, now),
        )
        if existing:
            return False
        per_day = await self.pokemon_wild_spawns_per_day_get()
        count = await self.pokemon_wild_spawn_count_24h(chan)
        if count >= per_day:
            return False
        expires = now + duration_seconds
        await self.execute(
            """INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
            VALUES(?,?,?,?,?)""",
            (chan, int(species_id), int(level), now, expires),
        )
        return True

    async def pokemon_wild_capture(
        self, channel: str, nick: str
    ) -> tuple[bool, str]:
        chan = (channel or "").strip()
        nick_n = (nick or "").strip()
        if not chan or not nick_n:
            return False, "Invalid."
        spawn = await self.pokemon_wild_get(chan)
        if not spawn:
            return False, "No wild Pokémon here right now."
        species_id = int(spawn["species_id"])
        level = int(spawn["level"] or 5)
        capture_rate = int(spawn["capture_rate"] or 255)
        if not await self.pokemon_trainer_exists(nick_n, chan):
            species_ids = [s["id"] for s in await self.pokemon_species_get_random(5)]
            await self.pokemon_trainer_create(nick_n, chan, starter_species_ids=species_ids or [1])
        has_ball = await self.fetchone(
            """SELECT item_id FROM pokemon_trainer_items
               WHERE lower(trainer_nick)=lower(?) AND channel=? AND quantity > 0
               AND item_id IN ('pokeball','great_ball','ultra_ball')
               ORDER BY CASE item_id WHEN 'ultra_ball' THEN 1 WHEN 'great_ball' THEN 2 ELSE 3 END
               LIMIT 1""",
            (nick_n, chan),
        )
        if not has_ball:
            return False, "You need a Poké Ball to catch Pokémon."
        roll = (capture_rate * 100) // 255
        import random
        if random.randint(1, 100) > roll:
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE lower(trainer_nick)=lower(?) AND channel=? AND item_id=?",
                (nick_n, chan, has_ball["item_id"]),
            )
            return False, "The wild Pokémon broke free!"
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE channel=?", (nick_n, chan)
        )
        species = await self.pokemon_species_get(species_id)
        hp = max(10, int(species["hp_base"] or 50) * 2 + 50) if species else 50
        max_slots = 6
        count = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_trainer_pokemon WHERE lower(trainer_nick)=lower(?) AND channel=?",
            (nick_n, chan),
        )
        cnt = int(count[0]) if count else 0
        if cnt >= max_slots:
            return True, f"Caught! But your party is full (6). {species['name']} was sent to the PC."
        now = int(time.time())
        slot = cnt + 1
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon(trainer_nick, channel, species_id, level, current_hp, max_hp, experience, is_fainted, slot, created_ts)
            VALUES(?,?,5,?,?,0,0,?,?)""",
            (nick_n, chan, species_id, hp, hp, slot, now),
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE lower(trainer_nick)=lower(?) AND channel=? AND item_id=?",
            (nick_n, chan, has_ball["item_id"]),
        )
        sp_name = species["name"] if species else "?"
        return True, f"Gotcha! {sp_name} was caught! (slot {slot})"

    async def pokemon_trainer_level_up(self, nick: str, channel: str, slot: int) -> tuple[bool, str]:
        nick_n = (nick or "").strip()
        chan = (channel or "").strip()
        if not nick_n or not chan:
            return False, "Invalid."
        poke = await self.fetchone(
            """SELECT tp.*, s.name as species_name, s.hp_base FROM pokemon_trainer_pokemon tp
               JOIN pokemon_species s ON tp.species_id = s.id
               WHERE lower(tp.trainer_nick)=lower(?) AND tp.channel=? AND tp.slot=?""",
            (nick_n, chan, slot),
        )
        if not poke:
            return False, f"No Pokémon in slot {slot}."
        xp = int(poke["experience"] or 0)
        level = int(poke["level"] or 5)
        xp_needed = level * 100
        if xp < xp_needed:
            return False, f"{poke['species_name']} needs {xp_needed - xp} more XP to level up (current: {xp}/{xp_needed})."
        new_level = level + 1
        hp_base = int(poke["hp_base"] or 50)
        new_max_hp = max(20, hp_base * 2 + 50 + (new_level - 5) * 5)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, max_hp=?, experience=experience-? WHERE id=?",
            (new_level, new_max_hp, xp_needed, poke["id"]),
        )
        return True, f"{poke['species_name']} grew to level {new_level}!"

    # ---- Pokemon ----
    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_species_get_random(self, limit: int = 5) -> list:
        rows = await self.fetchall(
            "SELECT id,pokedex_number,name,type1,type2,hp_base,atk_base,def_base,sp_atk_base,sp_def_base,speed_base,capture_rate FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows) if rows else []

    async def pokemon_species_get(self, species_id: int):
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))

    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return row is not None

    async def pokemon_trainer_create(self, nick: str, channel: str, species_ids: list[int]) -> None:
        now = int(time.time())
        n = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not n or not ch:
            return
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick,channel,created_ts) VALUES(?,?,?)",
            (n, ch, now),
        )
        for slot, sid in enumerate(species_ids[:6], 1):
            species = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (int(sid),))
            hp = int(species["hp_base"] * 0.4 + 10) if species else 20
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(trainer_nick,channel,species_id,nickname,level,current_hp,max_hp,experience,is_fainted,slot,created_ts)
                   VALUES(?,?,?,?,5,?,?,0,0,?,?)""",
                (n, ch, int(sid), None, hp, hp, slot, now),
            )
        starter_items = [("potion", 5), ("revive", 2), ("pokeball", 10)]
        for item_id, qty in starter_items:
            await self.execute(
                "INSERT INTO pokemon_trainer_items(trainer_nick,channel,item_id,quantity) VALUES(?,?,?,?) "
                "ON CONFLICT(trainer_nick,channel,item_id) DO UPDATE SET quantity=quantity+excluded.quantity",
                (n, ch, item_id, qty),
            )

    async def pokemon_trainer_get_team(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT p.id,p.species_id,p.nickname,p.level,p.current_hp,p.max_hp,p.is_fainted,p.slot,s.name as species_name,s.type1,s.type2
               FROM pokemon_trainer_pokemon p JOIN pokemon_species s ON p.species_id=s.id
               WHERE p.trainer_nick=? AND p.channel=? ORDER BY p.slot""",
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_get_items(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT ti.item_id,ti.quantity,i.name,i.item_type,i.effect_value
               FROM pokemon_trainer_items ti JOIN pokemon_items i ON ti.item_id=i.id
               WHERE ti.trainer_nick=? AND ti.channel=? AND ti.quantity>0 ORDER BY i.item_type,i.name""",
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_get_item_qty(self, nick: str, channel: str, item_id: str) -> int:
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            ((nick or "").strip().lower(), (channel or "").strip(), (item_id or "").strip()),
        )
        return int(row[0]) if row else 0

    async def pokemon_trainer_use_item(self, nick: str, channel: str, item_id: str) -> bool:
        n, ch = (nick or "").strip().lower(), (channel or "").strip()
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (n, ch, (item_id or "").strip()),
        )
        if not row or int(row[0]) < 1:
            return False
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=? AND quantity>0",
            (n, ch, item_id),
        )
        return True

    async def pokemon_heal_party(self, nick: str, channel: str, heal_amount: int) -> int:
        n, ch = (nick or "").strip().lower(), (channel or "").strip()
        rows = await self.fetchall(
            "SELECT id,current_hp,max_hp FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? AND is_fainted=0",
            (n, ch),
        )
        healed = 0
        for r in rows:
            cur, mx = int(r["current_hp"]), int(r["max_hp"])
            if cur < mx:
                new_hp = min(cur + heal_amount, mx)
                await self.execute("UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?", (new_hp, int(r["id"])))
                healed += new_hp - cur
        return healed

    async def pokemon_revive_party(self, nick: str, channel: str, restore_pct: int) -> int:
        n, ch = (nick or "").strip().lower(), (channel or "").strip()
        rows = await self.fetchall(
            "SELECT id,max_hp FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? AND is_fainted=1",
            (n, ch),
        )
        revived = 0
        for r in rows:
            mx = int(r["max_hp"])
            new_hp = max(1, (mx * restore_pct) // 100)
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
                (new_hp, int(r["id"])),
            )
            revived += 1
        return revived

    async def pokemon_wild_spawns_in_24h(self, channel: str) -> int:
        cutoff = int(time.time()) - (24 * 3600)
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts>=?",
            ((channel or "").strip(), cutoff),
        )
        return int(row[0]) if row else 0

    async def pokemon_wild_spawns_per_day_get(self) -> int:
        val = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, int(val))
        except (TypeError, ValueError):
            return 24

    async def pokemon_wild_spawns_per_day_set(self, n: int) -> None:
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, int(n))))

    async def pokemon_wild_get_active(self, channel: str):
        return await self.fetchone(
            "SELECT * FROM pokemon_wild_spawns WHERE channel=? AND expires_ts>? AND captured_by IS NULL",
            ((channel or "").strip(), int(time.time())),
        )

    async def pokemon_wild_spawn(self, channel: str, species_id: int, level: int, duration_seconds: int) -> bool:
        now = int(time.time())
        ch = (channel or "").strip()
        existing = await self.fetchone(
            "SELECT 1 FROM pokemon_wild_spawns WHERE channel=? AND captured_by IS NULL AND expires_ts>?",
            (ch, now),
        )
        if existing:
            return False
        expires = now + int(duration_seconds)
        await self.execute(
            "INSERT INTO pokemon_wild_spawns(channel,species_id,level,appeared_ts,expires_ts,captured_by) VALUES(?,?,?,?,?,NULL)",
            (ch, int(species_id), int(level), now, expires),
        )
        return True

    async def pokemon_wild_capture(self, channel: str, captor_nick: str) -> bool:
        ch = (channel or "").strip()
        n = (captor_nick or "").strip().lower()
        row = await self.fetchone(
            "SELECT id,species_id,level FROM pokemon_wild_spawns WHERE channel=? AND expires_ts>? AND captured_by IS NULL",
            (ch, int(time.time())),
        )
        if not row:
            return False
        now = int(time.time())
        await self.execute("UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?", (n, int(row["id"])))
        species = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (int(row["species_id"]),))
        hp = int(species["hp_base"] * 0.4 * int(row["level"]) / 5 + 10) if species else 20
        max_slot = await self.fetchone(
            "SELECT COALESCE(MAX(slot),0) as m FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
            (n, ch),
        )
        slot = int(max_slot["m"]) + 1 if max_slot else 1
        if slot > 6:
            return False
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon(trainer_nick,channel,species_id,nickname,level,current_hp,max_hp,experience,is_fainted,slot,created_ts)
           VALUES(?,?,?,?,?,?,?,0,0,?,?)""",
            (n, ch, int(row["species_id"]), None, int(row["level"]), hp, hp, slot, now),
        )
        return True

    async def pokemon_wild_expire_old(self, now_ts: int | None = None) -> int:
        now = int(now_ts or time.time())
        async with self._lock:
            cur = self._conn.execute("DELETE FROM pokemon_wild_spawns WHERE expires_ts<=? AND captured_by IS NULL", (now,))
            return int(cur.rowcount)

    # ---- Pokemon ----
    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            ((nick or "").strip(), (channel or "").strip()),
        )
        return row is not None

    async def pokemon_trainer_create(
        self,
        nick: str,
        channel: str,
        *,
        species_ids: list[int],
        starter_items: list[tuple[str, int]] | None = None,
    ) -> None:
        now = int(time.time())
        nick = (nick or "").strip()
        channel = (channel or "").strip()
        if not nick or not channel:
            return

        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick, channel, now),
        )

        for slot, species_id in enumerate(species_ids[:6], start=1):
            # Base stats for level 5
            sp = await self.fetchone("SELECT hp_base,atk_base,def_base,sp_atk_base,sp_def_base,speed_base FROM pokemon_species WHERE id=?", (int(species_id),))
            if not sp:
                continue
            hp = max(5, int(sp[0]) or 20)
            max_hp = hp
            await self.execute(
                "INSERT INTO pokemon_trainer_pokemon(trainer_nick,channel,species_id,level,current_hp,max_hp,slot,created_ts) VALUES(?,?,?,5,?,?,?,?)",
                (nick, channel, species_id, hp, max_hp, slot, now),
            )

        for item_id, qty in (starter_items or []):
            await self.execute(
                "INSERT INTO pokemon_trainer_items(trainer_nick,channel,item_id,quantity) VALUES(?,?,?,?) "
                "ON CONFLICT(trainer_nick,channel,item_id) DO UPDATE SET quantity=quantity+excluded.quantity",
                (nick, channel, str(item_id), int(qty)),
            )

    async def pokemon_get_random_species(self, limit: int = 1) -> list[int]:
        rows = await self.fetchall(
            "SELECT id FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return [int(r[0]) for r in rows] if rows else []

    async def pokemon_trainer_get_pokemon(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT p.id, p.species_id, p.nickname, p.level, p.current_hp, p.max_hp, p.is_fainted, p.slot, s.name as species_name, s.type1, s.type2
               FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON p.species_id = s.id
               WHERE p.trainer_nick=? AND p.channel=? ORDER BY p.slot""",
            ((nick or "").strip(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_get_items(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT i.id, i.name, t.quantity
               FROM pokemon_trainer_items t
               JOIN pokemon_items i ON t.item_id = i.id
               WHERE t.trainer_nick=? AND t.channel=? AND t.quantity > 0
               ORDER BY i.item_type, i.name""",
            ((nick or "").strip(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_use_item(
        self, nick: str, channel: str, item_id: str, target_pokemon_id: int | None = None
    ) -> tuple[bool, str]:
        nick = (nick or "").strip()
        channel = (channel or "").strip()
        if not nick or not channel:
            return False, "Invalid trainer."

        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (str(item_id),))
        if not item:
            return False, "Unknown item."

        qty_row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick, channel, item_id),
        )
        if not qty_row or int(qty_row[0] or 0) < 1:
            return False, f"No {item['name']} left."

        item_type = str(item["item_type"] or "")
        effect_value = int(item["effect_value"] or 0)

        if item_type == "heal":
            if target_pokemon_id is None:
                return False, "Specify a Pokémon to heal (e.g. !heal 1)."
            pk = await self.fetchone(
                "SELECT id, current_hp, max_hp, is_fainted FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
                (target_pokemon_id, nick, channel),
            )
            if not pk:
                return False, "Pokémon not found."
            if pk["is_fainted"]:
                return False, "Use a Revive first!"
            current_hp = int(pk["current_hp"] or 0)
            max_hp = int(pk["max_hp"] or 1)
            new_hp = min(max_hp, current_hp + effect_value)
            await self.execute("UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?", (new_hp, target_pokemon_id))
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (nick, channel, item_id),
            )
            return True, f"Healed {new_hp - current_hp} HP."

        if item_type == "revive":
            if target_pokemon_id is None:
                return False, "Specify a Pokémon to revive (e.g. !revive 1)."
            pk = await self.fetchone(
                "SELECT id, max_hp, is_fainted FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
                (target_pokemon_id, nick, channel),
            )
            if not pk:
                return False, "Pokémon not found."
            if not pk["is_fainted"]:
                return False, "Pokémon is not fainted."
            hp_restore = int(pk["max_hp"] or 20) * effect_value // 100
            hp_restore = max(1, hp_restore)
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
                (hp_restore, target_pokemon_id),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (nick, channel, item_id),
            )
            return True, f"Revived! Restored {hp_restore} HP."

        return False, "Can't use that item here."

    async def pokemon_wild_spawn_count_24h(self, channel: str) -> int:
        since = int(time.time()) - (24 * 3600)
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            ((channel or "").strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_wild_spawn_create(self, channel: str, species_id: int, level: int, duration_seconds: int) -> int | None:
        now = int(time.time())
        expires = now + duration_seconds
        try:
            await self.execute(
                "INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts) VALUES(?,?,?,?,?)",
                ((channel or "").strip(), int(species_id), int(level), now, expires),
            )
            row = await self.fetchone("SELECT last_insert_rowid()", ())
            return int(row[0]) if row else None
        except Exception:
            return None

    async def pokemon_wild_spawn_get_active(self, channel: str) -> object | None:
        now = int(time.time())
        return await self.fetchone(
            """SELECT w.*, s.name as species_name, s.type1, s.type2
               FROM pokemon_wild_spawns w
               JOIN pokemon_species s ON w.species_id = s.id
               WHERE w.channel=? AND w.expires_ts > ? AND w.captured_by IS NULL""",
            ((channel or "").strip(), now),
        )

    async def pokemon_wild_spawn_capture(self, spawn_id: int, nick: str) -> bool:
        row = await self.fetchone(
            "SELECT species_id, level, channel FROM pokemon_wild_spawns WHERE id=? AND captured_by IS NULL",
            (int(spawn_id),),
        )
        if not row:
            return False

        # Add to trainer (or create trainer if needed)
        trainer_exists = await self.pokemon_trainer_exists(nick, row["channel"])
        if not trainer_exists:
            await self.pokemon_trainer_create(nick, row["channel"], species_ids=[row["species_id"]], starter_items=[])
            # Override: we're adding this one. Actually create gave 1 species - we need to add this spawn's species
            # If trainer was created with 5 randoms, we'd have 5. Here we're capturing - so we need to add this pokemon
            # The trainer_create with species_ids=[row["species_id"]] would create 1 pokemon. So actually we need
            # to NOT call trainer_create here - the trainer might already exist. Let me fix.
            # Actually: if trainer doesn't exist, we create them with just this one pokemon as "starter". OK.
        else:
            # Get next slot
            slots = await self.fetchall(
                "SELECT slot FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
                (nick, row["channel"]),
            )
            used = {int(r["slot"]) for r in slots}
            slot = 1
            while slot <= 6 and slot in used:
                slot += 1
            if slot > 6:
                return False  # Party full

            sp = await self.fetchone(
                "SELECT hp_base FROM pokemon_species WHERE id=?",
                (row["species_id"],),
            )
            hp = max(5, int(sp["hp_base"]) if sp else 20)
            now = int(time.time())
            await self.execute(
                "INSERT INTO pokemon_trainer_pokemon(trainer_nick,channel,species_id,level,current_hp,max_hp,slot,created_ts) VALUES(?,?,?,?,?,?,?,?)",
                (nick, row["channel"], row["species_id"], row["level"], hp, hp, slot, now),
            )

        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?",
            (nick, int(spawn_id)),
        )
        return True

    async def pokemon_get_wild_spawns_per_day(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(100, int(v)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_wild_spawns_per_day(self, n: int) -> None:
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, min(100, int(n)))))

    async def pokemon_level_up(self, pokemon_id: int, nick: str, channel: str) -> tuple[bool, str]:
        pk = await self.fetchone(
            """SELECT p.*, s.hp_base, s.atk_base, s.def_base, s.sp_atk_base, s.sp_def_base, s.speed_base
               FROM pokemon_trainer_pokemon p JOIN pokemon_species s ON p.species_id = s.id
               WHERE p.id=? AND p.trainer_nick=? AND p.channel=?""",
            (pokemon_id, (nick or "").strip(), (channel or "").strip()),
        )
        if not pk:
            return False, "Pokémon not found."

        level = int(pk["level"] or 5)
        # Simple formula: max_hp = hp_base + level*2, clamp
        hp_base = int(pk["hp_base"] or 50)
        new_max_hp = hp_base + level * 2
        new_max_hp = max(20, min(999, new_max_hp))

        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, max_hp=?, current_hp=least(current_hp, ?) WHERE id=?",
            (level + 1, new_max_hp, new_max_hp, pokemon_id),
        )
        return True, f"Level up! Now level {level + 1}."

    # ---- Pokemon ----
    async def pokemon_get_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_get_random_species(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows) if rows else []

    async def pokemon_get_species_by_id(self, species_id: int) -> sqlite3.Row | None:
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))

    async def pokemon_is_trainer(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            (str(nick).strip().lower(), str(channel).strip()),
        )
        return row is not None

    async def pokemon_create_trainer(self, nick: str, channel: str, *, species_ids: list[int]) -> None:
        n = str(nick).strip().lower()
        ch = str(channel).strip()
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (n, ch, now),
        )
        for idx, sid in enumerate(species_ids[:6]):
            if sid <= 0:
                continue
            spec = await self.pokemon_get_species_by_id(sid)
            if not spec:
                continue
            hp = max(10, int(spec["hp_base"] or 50) + 10)
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(
                    trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts
                ) VALUES(?,?,?,5,?,?,?,?)""",
                (n, ch, sid, hp, hp, idx + 1, now),
            )
        for item_id, qty in [("potion", 5), ("revive", 2), ("pokeball", 5)]:
            await self.execute(
                """INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity)
                   VALUES(?,?,?,?)""",
                (n, ch, item_id, qty),
            )

    async def pokemon_get_trainer_pokemon(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT tp.*, s.name as species_name, s.type1, s.type2, s.pokedex_number
               FROM pokemon_trainer_pokemon tp
               JOIN pokemon_species s ON s.id=tp.species_id
               WHERE tp.trainer_nick=? AND tp.channel=?
               ORDER BY tp.slot""",
            (str(nick).strip().lower(), str(channel).strip()),
        )
        return list(rows) if rows else []

    async def pokemon_get_trainer_items(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT i.id, i.name, i.item_type, i.effect_value, ti.quantity
               FROM pokemon_trainer_items ti
               JOIN pokemon_items i ON i.id=ti.item_id
               WHERE ti.trainer_nick=? AND ti.channel=? AND ti.quantity>0
               ORDER BY i.item_type, i.name""",
            (str(nick).strip().lower(), str(channel).strip()),
        )
        return list(rows) if rows else []

    async def pokemon_heal_pokemon(self, nick: str, channel: str, pokemon_id: int, heal_amount: int) -> bool:
        n = str(nick).strip().lower()
        ch = str(channel).strip()
        row = await self.fetchone(
            "SELECT id, current_hp, max_hp FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
            (int(pokemon_id), n, ch),
        )
        if not row or row["current_hp"] >= row["max_hp"]:
            return False
        new_hp = min(row["max_hp"], row["current_hp"] + int(heal_amount))
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?",
            (new_hp, int(pokemon_id)),
        )
        return True

    async def pokemon_revive_pokemon(self, nick: str, channel: str, pokemon_id: int, percent: int) -> bool:
        n = str(nick).strip().lower()
        ch = str(channel).strip()
        row = await self.fetchone(
            "SELECT id, max_hp, is_fainted FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
            (int(pokemon_id), n, ch),
        )
        if not row or not row["is_fainted"]:
            return False
        hp = max(1, int(row["max_hp"] * percent / 100))
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
            (hp, int(pokemon_id)),
        )
        return True

    async def pokemon_use_item(self, nick: str, channel: str, item_id: str, target_id: int | None) -> tuple[bool, str]:
        n = str(nick).strip().lower()
        ch = str(channel).strip()
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (n, ch, str(item_id)),
        )
        if not row or row["quantity"] <= 0:
            return False, "You don't have that item."
        item_def = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (str(item_id),))
        if not item_def:
            return False, "Unknown item."
        itype = str(item_def["item_type"] or "").lower()
        if itype == "heal":
            if not target_id:
                return False, "Specify which Pokémon to heal (e.g. !heal 1)."
            ok = await self.pokemon_heal_pokemon(n, ch, target_id, int(item_def["effect_value"] or 0))
            if not ok:
                return False, "That Pokémon is already at full HP or doesn't exist."
        elif itype == "revive":
            if not target_id:
                return False, "Specify which Pokémon to revive (e.g. !revive 1)."
            ok = await self.pokemon_revive_pokemon(n, ch, target_id, int(item_def["effect_value"] or 50))
            if not ok:
                return False, "That Pokémon isn't fainted or doesn't exist."
        else:
            return False, "That item can't be used here. Use it when capturing."
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (n, ch, item_id),
        )
        return True, "OK"

    async def pokemon_get_wild_spawn(self, channel: str) -> sqlite3.Row | None:
        now = int(time.time())
        return await self.fetchone(
            """SELECT w.*, s.name as species_name, s.type1, s.type2, s.capture_rate, s.pokedex_number
               FROM pokemon_wild_spawns w
               JOIN pokemon_species s ON s.id=w.species_id
               WHERE w.channel=? AND w.expires_ts>? AND w.captured_by IS NULL""",
            (str(channel).strip(), now),
        )

    async def pokemon_count_spawns_today(self, channel: str) -> int:
        now = int(time.time())
        cutoff = now - (24 * 3600)
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts>?",
            (str(channel).strip(), cutoff),
        )
        return int(row[0]) if row else 0

    async def pokemon_spawn_wild(self, channel: str, species_id: int, level: int = 5, duration_s: int = 600) -> bool:
        ch = str(channel).strip()
        now = int(time.time())
        expires = now + int(duration_s)
        try:
            await self.execute(
                """INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
                   VALUES(?,?,?,?,?)""",
                (ch, int(species_id), int(level), now, expires),
            )
            return True
        except Exception:
            return False

    async def pokemon_capture_wild(self, nick: str, channel: str) -> tuple[bool, str]:
        n = str(nick).strip().lower()
        ch = str(channel).strip()
        spawn = await self.pokemon_get_wild_spawn(ch)
        if not spawn:
            return False, "No wild Pokémon here."
        now = int(time.time())
        if spawn["expires_ts"] <= now:
            return False, "That Pokémon fled."
        species_id = int(spawn["species_id"])
        level = int(spawn["level"] or 5)
        spec = await self.pokemon_get_species_by_id(species_id)
        if not spec:
            return False, "Invalid spawn."
        max_hp = max(10, int(spec["hp_base"] or 50) + (level * 2))
        slot = 1
        rows = await self.fetchall(
            "SELECT slot FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? ORDER BY slot DESC LIMIT 1",
            (n, ch),
        )
        if rows:
            slot = int(rows[0]["slot"] or 0) + 1
        if slot > 6:
            return False, "Your party is full. Deposit a Pokémon first (not implemented yet)."
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?",
            (n, int(spawn["id"])),
        )
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon(
                trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts
            ) VALUES(?,?,?,?,?,?,?,?)""",
            (n, ch, species_id, level, max_hp, max_hp, slot, now),
        )
        return True, str(spawn["species_name"] or "Pokémon")

    async def pokemon_delete_expired_spawns(self, now_ts: int | None = None) -> int:
        now = int(now_ts or time.time())
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM pokemon_wild_spawns WHERE expires_ts<=? OR captured_by IS NOT NULL",
                (now,),
            )
            return int(cur.rowcount or 0)

    async def pokemon_get_spawns_per_day_setting(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(100, int(v)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_spawns_per_day(self, value: int) -> None:
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, min(100, value))))

    async def pokemon_level_up(self, nick: str, channel: str, pokemon_id: int) -> tuple[bool, str]:
        n = str(nick).strip().lower()
        ch = str(channel).strip()
        row = await self.fetchone(
            """SELECT tp.*, s.name as species_name, s.hp_base, s.atk_base, s.def_base
               FROM pokemon_trainer_pokemon tp
               JOIN pokemon_species s ON s.id=tp.species_id
               WHERE tp.id=? AND tp.trainer_nick=? AND tp.channel=?""",
            (int(pokemon_id), n, ch),
        )
        if not row:
            return False, "Pokémon not found."
        level = int(row["level"] or 5)
        if level >= 100:
            return False, "Already max level."
        exp = int(row["experience"] or 0)
        needed = (level ** 2) * 10
        if exp < needed:
            return False, f"Needs {needed - exp} more XP to level up."
        level += 1
        hp_base = int(row["hp_base"] or 50)
        old_max = int(row["max_hp"] or 20)
        new_max = max(old_max + 2, hp_base + level * 2)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, experience=0, max_hp=?, current_hp=? WHERE id=?",
            (level, new_max, new_max, int(pokemon_id)),
        )
        return True, str(row["species_name"] or "Pokémon")

    # ---- Pokemon service ----
    async def pokemon_get_wild_spawns_per_day(self) -> int:
        val = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(1440, int(val)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_wild_spawns_per_day(self, n: int) -> None:
        n = max(1, min(1440, int(n)))
        await self.set_setting("pokemon_wild_spawns_per_day", str(n))

    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_species_get_random(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows)

    async def pokemon_species_get(self, species_id: int):
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))

    async def pokemon_species_get_by_name(self, name: str):
        n = (name or "").strip().lower()
        if not n:
            return None
        return await self.fetchone("SELECT * FROM pokemon_species WHERE LOWER(name)=?", (n,))

    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        n, ch = (nick or "").strip().lower(), (channel or "").strip()
        if not n or not ch:
            return False
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            (n, ch),
        )
        return row is not None

    async def pokemon_trainer_create(
        self,
        nick: str,
        channel: str,
        *,
        starter_species_ids: list[int],
        starter_items: list[tuple[str, int]] | None = None,
    ) -> None:
        n, ch = (nick or "").strip().lower(), (channel or "").strip()
        if not n or not ch or not starter_species_ids:
            return
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (n, ch, now),
        )
        for slot, sid in enumerate(starter_species_ids[:6], 1):
            sp = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(sid),))
            if not sp:
                continue
            hp = max(10, int(sp["hp_base"]) * 2 // 5 + 10)
            await self.execute(
                """
                INSERT INTO pokemon_trainer_pokemon(
                    trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts
                ) VALUES(?,?,?,5,?,?,?,?)
                """,
                (n, ch, int(sid), hp, hp, slot, now),
            )
        items = starter_items or [
            ("potion", 5),
            ("revive", 3),
            ("pokeball", 10),
        ]
        for item_id, qty in items:
            await self.execute(
                """
                INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity)
                VALUES(?,?,?,?)
                ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+excluded.quantity
                """,
                (n, ch, item_id, int(qty)),
            )

    async def pokemon_trainer_get_pokemon(self, nick: str, channel: str) -> list:
        n, ch = (nick or "").strip().lower(), (channel or "").strip()
        if not n or not ch:
            return []
        rows = await self.fetchall(
            """
            SELECT p.*, s.name as species_name, s.type1, s.type2
            FROM pokemon_trainer_pokemon p
            JOIN pokemon_species s ON p.species_id = s.id
            WHERE p.trainer_nick=? AND p.channel=?
            ORDER BY p.slot
            """,
            (n, ch),
        )
        return list(rows)

    async def pokemon_trainer_get_items(self, nick: str, channel: str) -> list:
        n, ch = (nick or "").strip().lower(), (channel or "").strip()
        if not n or not ch:
            return []
        rows = await self.fetchall(
            """
            SELECT i.id, i.name, i.item_type, i.effect_value, t.quantity
            FROM pokemon_trainer_items t
            JOIN pokemon_items i ON t.item_id = i.id
            WHERE t.trainer_nick=? AND t.channel=? AND t.quantity > 0
            ORDER BY i.item_type, i.name
            """,
            (n, ch),
        )
        return list(rows)

    async def pokemon_trainer_ensure(self, nick: str, channel: str, starter_ids: list[int]) -> bool:
        if await self.pokemon_trainer_exists(nick, channel):
            return False
        await self.pokemon_trainer_create(nick, channel, starter_species_ids=starter_ids)
        return True

    async def pokemon_get_hp_at_level(self, species_id: int, level: int) -> int:
        sp = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (int(species_id),))
        if not sp:
            return 20
        base = int(sp["hp_base"] or 50)
        return max(10, (2 * base * level // 100) + level + 10)

    async def pokemon_heal(self, nick: str, channel: str, pokemon_id: int, amount: int) -> tuple[bool, str]:
        n, ch = (nick or "").strip().lower(), (channel or "").strip()
        row = await self.fetchone(
            """
            SELECT p.id, p.current_hp, p.max_hp, p.is_fainted
            FROM pokemon_trainer_pokemon p
            WHERE p.trainer_nick=? AND p.channel=? AND p.id=?
            """,
            (n, ch, int(pokemon_id)),
        )
        if not row:
            return False, "Pokemon not found."
        if row["is_fainted"]:
            return False, "Use a Revive on fainted Pokemon."
        cur, mx = int(row["current_hp"]), int(row["max_hp"])
        if cur >= mx:
            return False, "Already at full HP."
        new_hp = min(mx, cur + int(amount))
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?",
            (new_hp, int(pokemon_id)),
        )
        return True, f"HP: {cur} → {new_hp}/{mx}"

    async def pokemon_revive(self, nick: str, channel: str, pokemon_id: int, percent: int) -> tuple[bool, str]:
        n, ch = (nick or "").strip().lower(), (channel or "").strip()
        row = await self.fetchone(
            """
            SELECT p.id, p.max_hp, p.is_fainted
            FROM pokemon_trainer_pokemon p
            WHERE p.trainer_nick=? AND p.channel=? AND p.id=?
            """,
            (n, ch, int(pokemon_id)),
        )
        if not row:
            return False, "Pokemon not found."
        if not row["is_fainted"]:
            return False, "Pokemon is not fainted."
        mx = int(row["max_hp"])
        new_hp = max(1, mx * int(percent) // 100)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET is_fainted=0, current_hp=? WHERE id=?",
            (new_hp, int(pokemon_id)),
        )
        return True, f"Revived! HP: {new_hp}/{mx}"

    async def pokemon_use_item(
        self,
        nick: str,
        channel: str,
        item_id: str,
        target_pokemon_id: int | None = None,
    ) -> tuple[bool, str]:
        n, ch = (nick or "").strip().lower(), (channel or "").strip()
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (item_id,))
        if not item:
            return False, "Unknown item."
        inv = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (n, ch, item_id),
        )
        if not inv or int(inv["quantity"] or 0) <= 0:
            return False, f"You don't have any {item['name']}."
        itype = str(item["item_type"])
        effect = int(item["effect_value"] or 0)
        if itype == "heal":
            if target_pokemon_id is None:
                return False, "Specify a Pokemon slot, e.g. !heal 1"
            ok, msg = await self.pokemon_heal(n, ch, target_pokemon_id, effect)
            if not ok:
                return False, msg
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=? AND quantity>0",
                (n, ch, item_id),
            )
            return True, msg
        if itype == "revive":
            if target_pokemon_id is None:
                return False, "Specify a Pokemon slot, e.g. !revive 1"
            ok, msg = await self.pokemon_revive(n, ch, target_pokemon_id, effect)
            if not ok:
                return False, msg
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=? AND quantity>0",
                (n, ch, item_id),
            )
            return True, msg
        return False, f"{item['name']} cannot be used this way."

    async def pokemon_level_up(self, nick: str, channel: str, pokemon_id: int) -> tuple[bool, str]:
        n, ch = (nick or "").strip().lower(), (channel or "").strip()
        row = await self.fetchone(
            """
            SELECT p.id, p.species_id, p.level, p.max_hp, p.current_hp, p.experience
            FROM pokemon_trainer_pokemon p
            WHERE p.trainer_nick=? AND p.channel=? AND p.id=?
            """,
            (n, ch, int(pokemon_id)),
        )
        if not row:
            return False, "Pokemon not found."
        level = int(row["level"] or 5)
        exp = int(row["experience"] or 0)
        exp_needed = level * level * 10
        if exp < exp_needed:
            return False, f"Needs {exp_needed - exp} more XP (current: {exp})"
        new_level = level + 1
        new_max_hp = await self.pokemon_get_hp_at_level(int(row["species_id"]), new_level)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, max_hp=?, current_hp=?, experience=experience-? WHERE id=?",
            (new_level, new_max_hp, new_max_hp, exp_needed, int(pokemon_id)),
        )
        return True, f"Level up! Level {level} → {new_level}. HP: {new_max_hp}"

    async def pokemon_wild_get_active(self, channel: str):
        ch = (channel or "").strip()
        if not ch:
            return None
        return await self.fetchone(
            """
            SELECT w.*, s.name as species_name, s.type1, s.type2, s.capture_rate
            FROM pokemon_wild_spawns w
            JOIN pokemon_species s ON w.species_id = s.id
            WHERE w.channel=? AND w.expires_ts > ? AND w.captured_by IS NULL
            """,
            (ch, int(time.time())),
        )

    async def pokemon_wild_spawns_in_last_24h(self, channel: str) -> int:
        ch = (channel or "").strip()
        if not ch:
            return 0
        since = int(time.time()) - 86400
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            (ch, since),
        )
        return int(row[0]) if row else 0

    async def pokemon_wild_insert(self, channel: str, species_id: int, level: int, duration_s: int = 600) -> int:
        ch = (channel or "").strip()
        if not ch:
            return 0
        now = int(time.time())
        expires = now + int(duration_s)
        await self.execute(
            """
            INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
            VALUES(?,?,?,?,?)
            """,
            (ch, int(species_id), int(level), now, expires),
        )
        row = await self.fetchone("SELECT last_insert_rowid()", ())
        return int(row[0]) if row else 0

    async def pokemon_wild_capture(self, spawn_id: int, nick: str) -> tuple[bool, str]:
        n = (nick or "").strip().lower()
        if not n:
            return False, "Invalid nick."
        row = await self.fetchone(
            "SELECT * FROM pokemon_wild_spawns WHERE id=? AND captured_by IS NULL AND expires_ts > ?",
            (int(spawn_id), int(time.time())),
        )
        if not row:
            return False, "Spawn not found or already captured."
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?",
            (n, int(spawn_id)),
        )
        return True, str(row["species_id"])

    async def pokemon_wild_add_captured_to_trainer(self, nick: str, channel: str, species_id: int, level: int) -> bool:
        n, ch = (nick or "").strip().lower(), (channel or "").strip()
        if not n or not ch:
            return False
        if not await self.pokemon_trainer_exists(nick, ch):
            return False
        slots = await self.fetchall(
            "SELECT slot FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? ORDER BY slot",
            (n, ch),
        )
        used = {int(r["slot"]) for r in slots}
        slot = 1
        while slot in used:
            slot += 1
        if slot > 6:
            return False
        sp = await self.pokemon_species_get(species_id)
        if not sp:
            return False
        max_hp = await self.pokemon_get_hp_at_level(species_id, level)
        now = int(time.time())
        await self.execute(
            """
            INSERT INTO pokemon_trainer_pokemon(
                trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (n, ch, species_id, level, max_hp, max_hp, slot, now),
        )
        return True

    async def pokemon_wild_prune_expired(self, now_ts: int | None = None) -> int:
        now = int(now_ts or time.time())
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM pokemon_wild_spawns WHERE expires_ts <= ?",
                (now,),
            )
            return int(cur.rowcount)

    async def pokemon_get_trainer_channel_from_nick(self, nick: str) -> str | None:
        n = (nick or "").strip().lower()
        if not n:
            return None
        row = await self.fetchone(
            "SELECT channel FROM pokemon_trainers WHERE nick=? ORDER BY created_ts DESC LIMIT 1",
            (n,),
        )
        return str(row[0]) if row else None

    # ---- Pokemon ----
    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_species_get_random(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows)

    async def pokemon_species_get(self, species_id: int):
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))

    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return row is not None

    async def pokemon_trainer_create(self, nick: str, channel: str) -> None:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick_l, ch, now),
        )
        # Default starter items
        for item_id, qty in [("potion", 5), ("revive", 2), ("pokeball", 10)]:
            await self.execute(
                "INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity) VALUES(?,?,?,?) "
                "ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+?",
                (nick_l, ch, item_id, qty, qty),
            )

    async def pokemon_trainer_add_pokemon(
        self,
        nick: str,
        channel: str,
        species_id: int,
        level: int = 5,
    ) -> None:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return
        species = await self.pokemon_species_get(species_id)
        if not species:
            return
        now = int(time.time())
        hp = max(1, int(species["hp_base"] or 50) + (level - 1) * 2)
        slot = 1
        rows = await self.fetchall(
            "SELECT slot FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? ORDER BY slot DESC LIMIT 1",
            (nick_l, ch),
        )
        if rows:
            slot = int(rows[0]["slot"] or 0) + 1
        if slot > 6:
            slot = 6
        await self.execute(
            """
            INSERT INTO pokemon_trainer_pokemon(trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (nick_l, ch, int(species_id), int(level), hp, hp, slot, now),
        )

    async def pokemon_trainer_get_pokemon(self, nick: str, channel: str) -> list:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return []
        rows = await self.fetchall(
            """
            SELECT p.*, s.name as species_name, s.type1, s.type2
            FROM pokemon_trainer_pokemon p
            JOIN pokemon_species s ON p.species_id = s.id
            WHERE p.trainer_nick=? AND p.channel=?
            ORDER BY p.slot, p.id
            """,
            (nick_l, ch),
        )
        return list(rows)

    async def pokemon_trainer_get_items(self, nick: str, channel: str) -> list:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return []
        rows = await self.fetchall(
            """
            SELECT ti.*, i.name, i.item_type, i.effect_value
            FROM pokemon_trainer_items ti
            JOIN pokemon_items i ON ti.item_id = i.id
            WHERE ti.trainer_nick=? AND ti.channel=? AND ti.quantity > 0
            ORDER BY i.item_type, i.name
            """,
            (nick_l, ch),
        )
        return list(rows)

    async def pokemon_trainer_get_item(self, nick: str, channel: str, item_id: str) -> sqlite3.Row | None:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch or not item_id:
            return None
        return await self.fetchone(
            "SELECT * FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, ch, str(item_id)),
        )

    async def pokemon_trainer_use_item(
        self,
        nick: str,
        channel: str,
        item_id: str,
        pokemon_id: int | None = None,
    ) -> int:
        """Use item; returns 1 if used, 0 if not enough/ invalid, -1 if pokemon full."""
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch or not item_id:
            return 0
        inv = await self.pokemon_trainer_get_item(nick_l, ch, item_id)
        if not inv or int(inv["quantity"] or 0) < 1:
            return 0
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (str(item_id),))
        if not item:
            return 0
        itype = str(item["item_type"] or "").lower()
        if itype == "heal":
            if pokemon_id is None:
                return 0
            row = await self.fetchone(
                "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
                (int(pokemon_id), nick_l, ch),
            )
            if not row or int(row["is_fainted"] or 0):
                return 0
            cur_hp = int(row["current_hp"] or 0)
            max_hp = int(row["max_hp"] or 1)
            if cur_hp >= max_hp:
                return -1
            heal = min(int(item["effect_value"] or 20), max_hp - cur_hp)
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=current_hp+? WHERE id=?",
                (heal, int(pokemon_id)),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (nick_l, ch, item_id),
            )
            return 1
        if itype == "revive":
            if pokemon_id is None:
                return 0
            row = await self.fetchone(
                "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
                (int(pokemon_id), nick_l, ch),
            )
            if not row or not int(row["is_fainted"] or 0):
                return 0
            pct = int(item["effect_value"] or 50)  # 50 or 100
            max_hp = int(row["max_hp"] or 1)
            restore = max(1, (max_hp * pct) // 100)
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET is_fainted=0, current_hp=? WHERE id=?",
                (restore, int(pokemon_id)),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (nick_l, ch, item_id),
            )
            return 1
        return 0

    async def pokemon_wild_get_active(self, channel: str):
        ch = (channel or "").strip()
        if not ch:
            return None
        now = int(time.time())
        return await self.fetchone(
            """
            SELECT w.*, s.name as species_name, s.type1, s.type2
            FROM pokemon_wild_spawns w
            JOIN pokemon_species s ON w.species_id = s.id
            WHERE w.channel=? AND w.expires_ts > ? AND w.captured_by IS NULL
            """,
            (ch, now),
        )

    async def pokemon_wild_spawns_in_last_24h(self, channel: str) -> int:
        ch = (channel or "").strip()
        if not ch:
            return 0
        since = int(time.time()) - (24 * 3600)
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            (ch, since),
        )
        return int(row[0]) if row else 0

    async def pokemon_wild_spawn(
        self,
        channel: str,
        species_id: int,
        level: int = 5,
        duration_seconds: int = 600,
    ) -> bool:
        ch = (channel or "").strip()
        if not ch:
            return False
        now = int(time.time())
        existing = await self.pokemon_wild_get_active(ch)
        if existing:
            return False
        await self.execute(
            """
            INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
            VALUES(?,?,?,?,?)
            """,
            (ch, int(species_id), int(level), now, now + int(duration_seconds)),
        )
        return True

    async def pokemon_wild_capture(
        self,
        channel: str,
        spawn_id: int,
        nick: str,
    ) -> bool:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        if not nick_l or not ch:
            return False
        row = await self.fetchone(
            "SELECT * FROM pokemon_wild_spawns WHERE id=? AND channel=? AND captured_by IS NULL",
            (int(spawn_id), ch),
        )
        if not row:
            return False
        species_id = int(row["species_id"])
        level = int(row["level"] or 5)
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?",
            (nick_l, int(spawn_id)),
        )
        await self.pokemon_trainer_create(nick_l, ch)  # ensures trainer exists
        await self.pokemon_trainer_add_pokemon(nick_l, ch, species_id, level)
        return True

    async def pokemon_wild_prune_expired(self) -> int:
        now = int(time.time())
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM pokemon_wild_spawns WHERE expires_ts <= ?",
                (now,),
            )
            return int(cur.rowcount)

    async def pokemon_get_wild_spawns_per_day(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(100, int(v)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_pokemon_level_up(self, pokemon_id: int, nick: str, channel: str) -> bool:
        nick_l = (nick or "").strip().lower()
        ch = (channel or "").strip()
        row = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
            (int(pokemon_id), nick_l, ch),
        )
        if not row or int(row["is_fainted"] or 0):
            return False
        level = int(row["level"] or 5)
        if level >= 100:
            return False
        new_level = level + 1
        species = await self.pokemon_species_get(int(row["species_id"]))
        if not species:
            return False
        hp_base = int(species["hp_base"] or 50)
        old_max = int(row["max_hp"] or 1)
        new_max = max(1, hp_base + (new_level - 1) * 2)
        cur_hp = int(row["current_hp"] or 0)
        new_cur = min(cur_hp + (new_max - old_max), new_max)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, max_hp=?, current_hp=? WHERE id=?",
            (new_level, new_max, new_cur, int(pokemon_id)),
        )
        return True

    # ---- Pokemon service ----
    async def pokemon_get_wild_spawns_per_day(self) -> int:
        row = await self.fetchone("SELECT value FROM settings WHERE key='pokemon_wild_spawns_per_day'", ())
        try:
            return int(row[0]) if row and row[0] else 24
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_wild_spawns_per_day(self, count: int) -> None:
        now = int(time.time())
        await self.execute(
            "INSERT INTO settings(key, value, updated_ts) VALUES('pokemon_wild_spawns_per_day', ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_ts=excluded.updated_ts",
            (str(max(1, count)), now),
        )

    async def pokemon_count_spawns_last_24h(self, channel: str) -> int:
        since = int(time.time()) - 86400
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            (channel, since),
        )
        return int(row[0]) if row else 0

    async def pokemon_get_random_species(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (limit,),
        )
        return list(rows) if rows else []

    async def pokemon_is_trainer(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            (nick.strip(), channel.strip()),
        )
        return row is not None

    async def pokemon_create_trainer(self, nick: str, channel: str, species_ids: list[int]) -> None:
        nick = nick.strip()
        channel = channel.strip()
        if not nick or not channel:
            return
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick, channel, now),
        )
        for slot, sid in enumerate(species_ids[:6], 1):
            species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(sid),))
            if not species:
                continue
            hp = int(species["hp_base"] or 50) + (int(species["level"] or 5) - 1) * 2
            hp = max(1, hp)
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts)
                   VALUES(?,?,?,5,?,?,?,?)""",
                (nick, channel, int(sid), hp, hp, slot, now),
            )
        starter_items = [("potion", 5), ("revive", 2), ("pokeball", 10)]
        for item_id, qty in starter_items:
            await self.execute(
                """INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity) VALUES(?,?,?,?)
                   ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+?""",
                (nick, channel, item_id, qty, qty),
            )

    async def pokemon_get_trainer_pokemon(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT tp.*, s.name as species_name, s.type1, s.type2
               FROM pokemon_trainer_pokemon tp
               JOIN pokemon_species s ON tp.species_id = s.id
               WHERE tp.trainer_nick=? AND tp.channel=? ORDER BY tp.slot""",
            (nick.strip(), channel.strip()),
        )
        return list(rows) if rows else []

    async def pokemon_get_trainer_items(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT ti.item_id, ti.quantity, i.name, i.item_type, i.effect_value
               FROM pokemon_trainer_items ti
               JOIN pokemon_items i ON ti.item_id = i.id
               WHERE ti.trainer_nick=? AND ti.channel=? AND ti.quantity > 0
               ORDER BY i.item_type, i.name""",
            (nick.strip(), channel.strip()),
        )
        return list(rows) if rows else []

    async def pokemon_get_active_wild_spawn(self, channel: str) -> sqlite3.Row | None:
        now = int(time.time())
        return await self.fetchone(
            """SELECT ws.*, s.name as species_name, s.type1, s.type2, s.capture_rate
               FROM pokemon_wild_spawns ws
               JOIN pokemon_species s ON ws.species_id = s.id
               WHERE ws.channel=? AND ws.captured_by IS NULL AND ws.expires_ts > ?""",
            (channel.strip(), now),
        )

    async def pokemon_create_wild_spawn(self, channel: str, species_id: int, level: int, duration_s: int = 900) -> int | None:
        now = int(time.time())
        expires = now + duration_s
        try:
            await self.execute(
                """INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
                   VALUES(?,?,?,?,?)""",
                (channel.strip(), int(species_id), int(level), now, expires),
            )
            row = await self.fetchone("SELECT last_insert_rowid()", ())
            return int(row[0]) if row else None
        except sqlite3.IntegrityError:
            return None

    async def pokemon_capture_wild(self, channel: str, nick: str) -> int | None:
        spawn = await self.pokemon_get_active_wild_spawn(channel)
        if not spawn:
            return None
        nick = nick.strip()
        # Ensure trainer exists
        row = await self.fetchone("SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?", (nick, channel.strip()))
        if not row:
            return None
        now = int(time.time())
        species = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (int(spawn["species_id"]),))
        hp_base = int(species["hp_base"] or 50) if species else 50
        level = int(spawn["level"] or 5)
        max_hp = max(1, hp_base + (level - 1) * 2)
        slot = await self.fetchone(
            "SELECT COALESCE(MAX(slot),0)+1 FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
            (nick, channel.strip()),
        )
        slot_num = int(slot[0]) if slot and slot[0] else 1
        slot_num = min(6, slot_num)
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon(trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts)
               VALUES(?,?,?,?,?,?,?,?)""",
            (nick, channel.strip(), int(spawn["species_id"]), level, max_hp, max_hp, slot_num, now),
        )
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?",
            (nick, int(spawn["id"])),
        )
        return int(spawn["species_id"])

    async def pokemon_use_item(self, nick: str, channel: str, item_id: str, pokemon_slot: int) -> tuple[bool, str]:
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick.strip(), channel.strip(), item_id),
        )
        if not row or int(row[0] or 0) < 1:
            return False, "You don't have that item."
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (item_id,))
        if not item:
            return False, "Unknown item."
        pokemon = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? AND slot=?",
            (nick.strip(), channel.strip(), int(pokemon_slot)),
        )
        if not pokemon:
            return False, "No Pokemon in that slot."
        item_type = str(item["item_type"] or "")
        if item_type == "heal":
            if int(pokemon["is_fainted"] or 0):
                return False, "That Pokemon has fainted. Use a Revive."
            current = int(pokemon["current_hp"] or 0)
            max_hp = int(pokemon["max_hp"] or 1)
            heal = min(int(item["effect_value"] or 0), max_hp - current)
            if heal <= 0:
                return False, "That Pokemon is already at full HP."
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=current_hp+? WHERE id=?",
                (heal, int(pokemon["id"])),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (nick.strip(), channel.strip(), item_id),
            )
            return True, f"Healed for {heal} HP!"
        if item_type == "revive":
            if not int(pokemon["is_fainted"] or 0):
                return False, "That Pokemon isn't fainted."
            pct = int(item["effect_value"] or 50)
            max_hp = int(pokemon["max_hp"] or 1)
            restore = max(1, max_hp * pct // 100)
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET is_fainted=0, current_hp=? WHERE id=?",
                (restore, int(pokemon["id"])),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (nick.strip(), channel.strip(), item_id),
            )
            return True, f"Revived with {restore} HP!"
        return False, "You can't use that item on a Pokemon here."

    async def pokemon_level_up(self, nick: str, channel: str, pokemon_slot: int) -> tuple[bool, str]:
        pokemon = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? AND slot=?",
            (nick.strip(), channel.strip(), int(pokemon_slot)),
        )
        if not pokemon:
            return False, "No Pokemon in that slot."
        if int(pokemon["is_fainted"] or 0):
            return False, "Can't level up a fainted Pokemon. Revive it first."
        species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(pokemon["species_id"]),))
        if not species:
            return False, "Species not found."
        level = int(pokemon["level"] or 5)
        new_level = min(100, level + 1)
        hp_base = int(species["hp_base"] or 50)
        new_max_hp = max(1, hp_base + (new_level - 1) * 2)
        exp_needed = level * 100
        current_exp = int(pokemon["experience"] or 0)
        if current_exp < exp_needed:
            return False, f"Need {exp_needed} XP to level up (have {current_exp}). Gain XP by battling!"
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, max_hp=?, current_hp=?, experience=experience-? WHERE id=?",
            (new_level, new_max_hp, new_max_hp, exp_needed, int(pokemon["id"])),
        )
        return True, f"Level up! Now level {new_level}!"

    async def pokemon_add_experience(self, nick: str, channel: str, pokemon_slot: int, amount: int) -> None:
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET experience=experience+? WHERE trainer_nick=? AND channel=? AND slot=?",
            (int(amount), nick.strip(), channel.strip(), int(pokemon_slot)),
        )

    async def pokemon_channels_with_spawns_enabled(self) -> list[str]:
        rows = await self.fetchall(
            """SELECT DISTINCT channel FROM service_enablement
               WHERE service='pokemon' AND enabled=1 AND channel LIKE '#%'""",
            (),
        )
        return [str(r[0]) for r in rows] if rows else []

    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    # ---- Pokemon service ----
    async def pokemon_get_spawns_per_day(self) -> int:
        val = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(500, int(val)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_spawns_per_day(self, n: int) -> None:
        n = max(1, min(500, int(n)))
        await self.set_setting("pokemon_wild_spawns_per_day", str(n))

    async def pokemon_count_spawns_last_24h(self, channel: str) -> int:
        cutoff = int(time.time()) - (24 * 3600)
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts>=?",
            (channel.strip(), cutoff),
        )
        return int(row[0]) if row else 0

    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_species_random(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows) if rows else []

    async def pokemon_get_wild_spawn(self, channel: str) -> sqlite3.Row | None:
        now = int(time.time())
        return await self.fetchone(
            """
            SELECT w.*, s.name as species_name, s.type1, s.type2, s.capture_rate
            FROM pokemon_wild_spawns w
            JOIN pokemon_species s ON w.species_id = s.id
            WHERE w.channel=? AND w.expires_ts>? AND w.captured_by IS NULL
            """,
            (channel.strip(), now),
        )

    async def pokemon_create_trainer(self, nick: str, channel: str, species_ids: list[int]) -> bool:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        if not nick_l or not chan or not species_ids:
            return False
        now = int(time.time())
        try:
            await self.execute(
                "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
                (nick_l, chan, now),
            )
            row = await self.fetchone(
                "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
                (nick_l, chan),
            )
            if not row:
                return False
            for i, sid in enumerate(species_ids[:6]):
                species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (sid,))
                if not species:
                    continue
                lvl = 5
                hp = int(species["hp_base"]) + 10 + (lvl * 2)
                await self.execute(
                    """
                    INSERT INTO pokemon_trainer_pokemon(
                        trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts
                    ) VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (nick_l, chan, sid, lvl, hp, hp, i + 1, now),
                )
            for item_id, qty in [("potion", 5), ("revive", 2), ("pokeball", 10)]:
                await self.execute(
                    "INSERT OR IGNORE INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity) VALUES(?,?,?,?) "
                    "ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+excluded.quantity",
                    (nick_l, chan, item_id, qty),
                )
            return True
        except Exception:
            return False

    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            (nick_l, chan),
        )
        return row is not None

    async def pokemon_trainer_team(self, nick: str, channel: str) -> list:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        rows = await self.fetchall(
            """
            SELECT p.*, s.name as species_name, s.type1, s.type2
            FROM pokemon_trainer_pokemon p
            JOIN pokemon_species s ON p.species_id = s.id
            WHERE p.trainer_nick=? AND p.channel=?
            ORDER BY p.slot
            """,
            (nick_l, chan),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_items(self, nick: str, channel: str) -> list:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        rows = await self.fetchall(
            """
            SELECT ti.*, i.name, i.item_type, i.effect_value
            FROM pokemon_trainer_items ti
            JOIN pokemon_items i ON ti.item_id = i.id
            WHERE ti.trainer_nick=? AND ti.channel=? AND ti.quantity>0
            ORDER BY i.item_type, i.name
            """,
            (nick_l, chan),
        )
        return list(rows) if rows else []

    async def pokemon_heal_pokemon(self, nick: str, channel: str, pokemon_id: int, amount: int) -> bool:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        row = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=? AND is_fainted=0",
            (pokemon_id, nick_l, chan),
        )
        if not row:
            return False
        new_hp = min(int(row["max_hp"]), int(row["current_hp"]) + amount)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?",
            (new_hp, pokemon_id),
        )
        return True

    async def pokemon_revive_pokemon(self, nick: str, channel: str, pokemon_id: int, hp_percent: int) -> bool:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        row = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=? AND is_fainted=1",
            (pokemon_id, nick_l, chan),
        )
        if not row:
            return False
        restored = int(row["max_hp"] * hp_percent / 100)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
            (restored, pokemon_id),
        )
        return True

    async def pokemon_use_item(self, nick: str, channel: str, item_id: str) -> bool:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        row = await self.fetchone(
            "SELECT * FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=? AND quantity>0",
            (nick_l, chan, item_id),
        )
        if not row:
            return False
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, chan, item_id),
        )
        return True

    async def pokemon_capture_wild(self, channel: str, spawn_id: int, nick: str) -> bool:
        now = int(time.time())
        nick_l = (nick or "").strip().lower()
        spawn = await self.fetchone(
            "SELECT * FROM pokemon_wild_spawns WHERE id=? AND channel=? AND expires_ts>? AND captured_by IS NULL",
            (spawn_id, channel.strip(), now),
        )
        if not spawn:
            return False
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?",
            (nick_l, spawn_id),
        )
        species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (spawn["species_id"],))
        if not species:
            return True
        lvl = int(spawn["level"])
        hp = int(species["hp_base"]) + 10 + (lvl * 2)
        await self.execute(
            """
            INSERT INTO pokemon_trainer_pokemon(
                trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts
            ) VALUES(?,?,?,?,?,?,
                COALESCE((SELECT MAX(slot) FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?), 0) + 1,
                ?
            )
            """,
            (nick_l, channel.strip(), spawn["species_id"], lvl, hp, hp, nick_l, channel.strip(), now),
        )
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM pokemon_wild_spawns WHERE id=?",
                (spawn_id,),
            )
        return True

    async def pokemon_insert_wild_spawn(self, channel: str, species_id: int, level: int, duration_s: int) -> int | None:
        now = int(time.time())
        expires = now + duration_s
        try:
            await self.execute(
                "INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts) VALUES(?,?,?,?,?)",
                (channel.strip(), species_id, level, now, expires),
            )
            row = await self.fetchone("SELECT last_insert_rowid()", ())
            return int(row[0]) if row else None
        except Exception:
            return None

    async def pokemon_delete_expired_spawns(self) -> int:
        now = int(time.time())
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM pokemon_wild_spawns WHERE expires_ts<=?",
                (now,),
            )
            return int(cur.rowcount or 0)

    async def pokemon_level_up(self, nick: str, channel: str, pokemon_id: int) -> bool:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        row = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
            (pokemon_id, nick_l, chan),
        )
        if not row:
            return False
        species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (row["species_id"],))
        if not species:
            return False
        lvl = int(row["level"])
        if lvl >= 100:
            return False
        new_lvl = lvl + 1
        new_max_hp = int(species["hp_base"]) + 10 + (new_lvl * 2)
        new_hp = int(row["current_hp"]) + (new_max_hp - int(row["max_hp"]))
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, max_hp=?, current_hp=? WHERE id=?",
            (new_lvl, new_max_hp, min(new_max_hp, new_hp), pokemon_id),
        )
        return True

    async def pokemon_get_channels_with_trainers(self) -> list[str]:
        rows = await self.fetchall(
            "SELECT DISTINCT channel FROM pokemon_trainers",
            (),
        )
        return [str(r[0]) for r in rows] if rows else []

    # ---- Pokemon ----
    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            ((nick or "").strip(), (channel or "").strip()),
        )
        return row is not None

    async def pokemon_trainer_create(self, nick: str, channel: str, *, species_ids: list[int]) -> None:
        nick_s = (nick or "").strip()
        chan_s = (channel or "").strip()
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick_s, chan_s, now),
        )
        for slot, sid in enumerate(species_ids[:6], 1):
            species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(sid),))
            if not species:
                continue
            hp = max(10, int(species["hp_base"]) + int(species["level"] or 5) * 2)
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (nick_s, chan_s, int(sid), 5, hp, hp, slot, now),
            )
        for item_id, qty in [("potion", 5), ("revive", 3), ("pokeball", 10)]:
            await self.execute(
                "INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity) VALUES(?,?,?,?) "
                "ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+excluded.quantity",
                (nick_s, chan_s, item_id, qty),
            )

    async def pokemon_trainer_get_party(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT p.*, s.name as species_name, s.type1, s.type2, s.pokedex_number
               FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON p.species_id = s.id
               WHERE p.trainer_nick=? AND p.channel=? ORDER BY p.slot""",
            ((nick or "").strip(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_get_items(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT i.id, i.name, i.item_type, i.effect_value, t.quantity
               FROM pokemon_trainer_items t JOIN pokemon_items i ON t.item_id = i.id
               WHERE t.trainer_nick=? AND t.channel=? AND t.quantity>0 ORDER BY i.item_type, i.name""",
            ((nick or "").strip(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_use_item(self, nick: str, channel: str, item_id: str, pokemon_id: int) -> tuple[bool, str]:
        nick_s = (nick or "").strip()
        chan_s = (channel or "").strip()
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_s, chan_s, item_id),
        )
        if not row or int(row[0] or 0) < 1:
            return False, "You don't have that item."
        poke = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
            (int(pokemon_id), nick_s, chan_s),
        )
        if not poke:
            return False, "Pokemon not found."
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (item_id,))
        if not item:
            return False, "Invalid item."
        itype = str(item["item_type"])
        if itype == "heal":
            if poke["is_fainted"]:
                return False, "Use a Revive on fainted Pokemon."
            healed = min(int(item["effect_value"]), int(poke["max_hp"]) - int(poke["current_hp"]))
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=MIN(max_hp, current_hp+?) WHERE id=?",
                (int(item["effect_value"]), int(pokemon_id)),
            )
        elif itype == "revive":
            if not poke["is_fainted"]:
                return False, "Pokemon is not fainted."
            restore_pct = int(item["effect_value"])
            hp = int(poke["max_hp"]) * restore_pct // 100
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
                (hp, int(pokemon_id)),
            )
        else:
            return False, "That item cannot be used here."
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=? AND quantity>0",
            (nick_s, chan_s, item_id),
        )
        return True, "Done."

    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_species_get_random(self, limit: int = 5, *, exclude_ids: list[int] | None = None) -> list:
        exclude = exclude_ids or []
        if exclude:
            placeholders = ",".join("?" * len(exclude))
            rows = await self.fetchall(
                f"SELECT * FROM pokemon_species WHERE id NOT IN ({placeholders}) ORDER BY RANDOM() LIMIT ?",
                (*exclude, int(limit)),
            )
        else:
            rows = await self.fetchall(
                "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?", (int(limit),),
            )
        return list(rows) if rows else []

    async def pokemon_wild_spawn_count_24h(self, channel: str) -> int:
        since = int(time.time()) - 86400
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts>=?",
            ((channel or "").strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_wild_spawn_get_active(self, channel: str):
        return await self.fetchone(
            """SELECT w.*, s.name as species_name, s.type1, s.type2, s.capture_rate
               FROM pokemon_wild_spawns w JOIN pokemon_species s ON w.species_id = s.id
               WHERE w.channel=? AND w.expires_ts>? AND w.captured_by IS NULL""",
            ((channel or "").strip(), int(time.time())),
        )

    async def pokemon_wild_spawn_create(self, channel: str, species_id: int, level: int, duration_seconds: int) -> int:
        now = int(time.time())
        expires = now + int(duration_seconds)
        await self.execute(
            "INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts) VALUES(?,?,?,?,?)",
            ((channel or "").strip(), int(species_id), int(level), now, expires),
        )
        row = await self.fetchone("SELECT last_insert_rowid()", ())
        return int(row[0]) if row else 0

    async def pokemon_wild_spawn_capture(self, spawn_id: int, nick: str) -> bool:
        async with self._lock:
            row = self._conn.execute(
                "SELECT * FROM pokemon_wild_spawns WHERE id=? AND captured_by IS NULL AND expires_ts>?",
                (int(spawn_id), int(time.time())),
            ).fetchone()
            if not row:
                return False
            chan = str(row["channel"])
            species_id = int(row["species_id"])
            level = int(row["level"])
            nick_s = (nick or "").strip()
            now = int(time.time())
            self._conn.execute(
                "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
                (nick_s, chan, now),
            )
            slot = len(self._conn.execute(
                "SELECT id FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
                (nick_s, chan),
            ).fetchall()) + 1
            if slot > 6:
                return False
            species = self._conn.execute("SELECT * FROM pokemon_species WHERE id=?", (species_id,)).fetchone()
            hp = max(10, (species["hp_base"] or 50) + level * 2) if species else 20
            self._conn.execute(
                """INSERT INTO pokemon_trainer_pokemon(trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (nick_s, chan, species_id, level, hp, hp, slot, now),
            )
            self._conn.execute(
                "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?", (nick_s, int(spawn_id)),
            )
        return True

    async def pokemon_wild_spawn_prune_expired(self) -> int:
        now = int(time.time())
        async with self._lock:
            cur = self._conn.execute("DELETE FROM pokemon_wild_spawns WHERE expires_ts<=?", (now,))
            return int(cur.rowcount or 0)

    async def pokemon_get_setting_spawns_per_day(self) -> int:
        val = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(1000, int(val)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_spawns_per_day(self, n: int) -> None:
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, min(1000, int(n)))))

    async def pokemon_level_up(self, nick: str, channel: str, pokemon_id: int) -> tuple[bool, str]:
        poke = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
            (int(pokemon_id), (nick or "").strip(), (channel or "").strip()),
        )
        if not poke:
            return False, "Pokemon not found."
        species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (poke["species_id"],))
        if not species:
            return False, "Species not found."
        lvl = int(poke["level"]) + 1
        if lvl > 100:
            return False, "Already max level."
        hp_boost = 2
        new_max = int(poke["max_hp"]) + hp_boost
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, max_hp=?, current_hp=current_hp+? WHERE id=?",
            (lvl, new_max, hp_boost, int(pokemon_id)),
        )
        return True, f"Level up! Now level {lvl}."

    # ---- Pokemon service ----
    async def pokemon_get_species_random(self, limit: int = 1) -> list:
        """Return up to limit random species (for starters or wild spawns)."""
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows)

    async def pokemon_get_species(self, species_id: int):
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))

    async def pokemon_is_trainer(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            (nick.strip().lower(), channel.strip()),
        )
        return row is not None

    async def pokemon_create_trainer(self, nick: str, channel: str, *, starter_species_ids: list[int]) -> None:
        now = int(time.time())
        n = nick.strip().lower()
        ch = channel.strip()
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (n, ch, now),
        )
        for i, sid in enumerate(starter_species_ids[:6], start=1):
            species = await self.pokemon_get_species(sid)
            if not species:
                continue
            max_hp = self._pokemon_calc_hp(int(species["hp_base"]), 5)
            await self.execute(
                """
                INSERT INTO pokemon_trainer_pokemon(
                    trainer_nick, channel, species_id, level, current_hp, max_hp, experience, slot, created_ts
                ) VALUES(?,?,?,5,?,?,0,?,?)
                """,
                (n, ch, sid, max_hp, max_hp, i, now),
            )
        # Default starter items
        for item_id, qty in [("potion", 5), ("revive", 2), ("pokeball", 10)]:
            await self.execute(
                """
                INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity)
                VALUES(?,?,?,?) ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+excluded.quantity
                """,
                (n, ch, item_id, qty),
            )

    def _pokemon_calc_hp(self, base_hp: int, level: int) -> int:
        return max(10, int((2 * base_hp * level) / 100) + level + 10)

    def _pokemon_calc_stat(self, base: int, level: int) -> int:
        return max(5, int((2 * base * level) / 100) + 5)

    async def pokemon_trainer_pokemon_list(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """
            SELECT tp.*, s.name as species_name, s.type1, s.type2
            FROM pokemon_trainer_pokemon tp
            JOIN pokemon_species s ON tp.species_id = s.id
            WHERE tp.trainer_nick=? AND tp.channel=?
            ORDER BY tp.slot
            """,
            (nick.strip().lower(), channel.strip()),
        )
        return list(rows)

    async def pokemon_trainer_items_list(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """
            SELECT ti.item_id, ti.quantity, i.name, i.item_type, i.effect_value
            FROM pokemon_trainer_items ti
            JOIN pokemon_items i ON ti.item_id = i.id
            WHERE ti.trainer_nick=? AND ti.channel=? AND ti.quantity > 0
            ORDER BY i.item_type, i.name
            """,
            (nick.strip().lower(), channel.strip()),
        )
        return list(rows)

    async def pokemon_trainer_get_item_qty(self, nick: str, channel: str, item_id: str) -> int:
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick.strip().lower(), channel.strip(), item_id.strip()),
        )
        return int(row[0]) if row else 0

    async def pokemon_trainer_use_item(
        self, nick: str, channel: str, item_id: str, pokemon_id: int | None = None
    ) -> tuple[bool, str]:
        """Use item. Returns (success, message)."""
        n = nick.strip().lower()
        ch = channel.strip()
        item_row = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (item_id.strip(),))
        if not item_row:
            return False, "Unknown item."
        qty = await self.pokemon_trainer_get_item_qty(n, ch, item_id)
        if qty <= 0:
            return False, "You don't have any of that item."

        item_type = str(item_row["item_type"])
        effect = int(item_row["effect_value"] or 0)

        if item_type == "heal":
            if pokemon_id is None:
                return False, "Specify which Pokémon to heal: !heal <slot#>"
            row = await self.fetchone(
                "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
                (pokemon_id, n, ch),
            )
            if not row:
                return False, "Pokémon not found."
            if row["is_fainted"]:
                return False, "Use a Revive on fainted Pokémon."
            cur_hp = int(row["current_hp"])
            max_hp = int(row["max_hp"])
            if cur_hp >= max_hp:
                return False, "That Pokémon is already at full HP."
            new_hp = min(max_hp, cur_hp + effect)
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?",
                (new_hp, pokemon_id),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (n, ch, item_id),
            )
            return True, f"Healed {new_hp - cur_hp} HP."

        if item_type == "revive":
            if pokemon_id is None:
                return False, "Specify which Pokémon to revive: !revive <slot#>"
            row = await self.fetchone(
                "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
                (pokemon_id, n, ch),
            )
            if not row:
                return False, "Pokémon not found."
            if not row["is_fainted"]:
                return False, "That Pokémon isn't fainted."
            max_hp = int(row["max_hp"])
            restored = int(max_hp * effect / 100)
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
                (restored, pokemon_id),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (n, ch, item_id),
            )
            return True, f"Revived to {restored} HP."

        return False, "Can't use that item here."

    async def pokemon_wild_spawn_get(self, channel: str):
        now = int(time.time())
        return await self.fetchone(
            """
            SELECT * FROM pokemon_wild_spawns
            WHERE channel=? AND expires_ts > ? AND captured_by IS NULL
            """,
            (channel.strip(), now),
        )

    async def pokemon_wild_spawn_count_last_24h(self, channel: str) -> int:
        cutoff = int(time.time()) - (24 * 3600)
        row = await self.fetchone(
            "SELECT COUNT(*) as n FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            (channel.strip(), cutoff),
        )
        return int(row[0]) if row else 0

    async def pokemon_wild_spawn_create(
        self, channel: str, species_id: int, level: int = 5, duration_seconds: int = 600
    ) -> bool:
        now = int(time.time())
        ch = channel.strip()
        existing = await self.pokemon_wild_spawn_get(ch)
        if existing:
            return False
        expires = now + duration_seconds
        await self.execute(
            "INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts) VALUES(?,?,?,?,?)",
            (ch, int(species_id), int(level), now, expires),
        )
        return True

    async def pokemon_wild_spawn_capture(
        self, channel: str, spawn_id: int, nick: str
    ) -> tuple[bool, str]:
        """Attempt capture. Returns (success, message). Uses simplified capture formula."""
        row = await self.fetchone(
            "SELECT * FROM pokemon_wild_spawns WHERE id=? AND channel=? AND captured_by IS NULL AND expires_ts > ?",
            (spawn_id, channel.strip(), int(time.time())),
        )
        if not row:
            return False, "That Pokémon is no longer available."
        species = await self.pokemon_get_species(row["species_id"])
        if not species:
            return False, "Species data missing."
        capture_rate = int(species.get("capture_rate", 255))
        # Simplified: base 50% + capture_rate/255 * 50% => up to 100%
        import random
        roll = random.random() * 100
        threshold = 50 + (capture_rate / 255) * 50
        if roll > threshold:
            return False, "The Pokémon broke free!"
        n = nick.strip().lower()
        ch = channel.strip()
        level = int(row["level"] or 5)
        max_hp = self._pokemon_calc_hp(int(species["hp_base"]), level)
        now = int(time.time())
        # Ensure trainer exists
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (n, ch, now),
        )
        slots = await self.fetchall(
            "SELECT slot FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
            (n, ch),
        )
        next_slot = max([r[0] for r in slots], default=0) + 1
        if next_slot > 6:
            return False, "Your party is full (6 Pokémon max)."
        await self.execute(
            """
            INSERT INTO pokemon_trainer_pokemon(
                trainer_nick, channel, species_id, level, current_hp, max_hp, experience, slot, created_ts
            ) VALUES(?,?,?,?,?,?,0,?,?)
            """,
            (n, ch, row["species_id"], level, max_hp, max_hp, next_slot, now),
        )
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?",
            (nick, spawn_id),
        )
        return True, f"Gotcha! {species['name']} was caught!"

    async def pokemon_wild_spawn_cleanup_expired(self) -> int:
        now = int(time.time())
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM pokemon_wild_spawns WHERE expires_ts <= ?",
                (now,),
            )
            return int(cur.rowcount or 0)

    async def pokemon_level_up(self, pokemon_id: int, nick: str, channel: str) -> tuple[bool, str]:
        row = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
            (pokemon_id, nick.strip().lower(), channel.strip()),
        )
        if not row:
            return False, "Pokémon not found."
        exp = int(row["experience"] or 0)
        level = int(row["level"] or 5)
        exp_needed = level * level * 10
        if exp < exp_needed:
            return False, f"Needs {exp_needed - exp} more EXP to level up."
        species = await self.pokemon_get_species(row["species_id"])
        if not species:
            return False, "Species data missing."
        new_level = level + 1
        new_max_hp = self._pokemon_calc_hp(int(species["hp_base"]), new_level)
        new_exp = exp - exp_needed
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, max_hp=?, current_hp=?, experience=? WHERE id=?",
            (new_level, new_max_hp, new_max_hp, new_exp, pokemon_id),
        )
        return True, f"Level up! Now level {new_level}!"

    async def pokemon_get_wild_spawns_per_day(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, int(v))
        except (TypeError, ValueError):
            return 24

    # ---- Pokemon service ----
    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            ((nick or "").strip(), (channel or "").strip()),
        )
        return row is not None

    async def pokemon_trainer_create(
        self,
        nick: str,
        channel: str,
        *,
        species_ids: list[int],
        starter_items: list[tuple[str, int]] | None = None,
    ) -> None:
        now = int(time.time())
        nick = (nick or "").strip()
        channel = (channel or "").strip()
        if not nick or not channel:
            return
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick, channel, now),
        )
        for slot, sid in enumerate(species_ids[:6], start=1):
            sp = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (sid,))
            base_hp = int(sp["hp_base"]) if sp else 20
            max_hp = max(10, int(base_hp * 0.5) + int(5 * 1.5))
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(
                    trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts
                ) VALUES(?,?,?,5,?,?,?,?)""",
                (nick, channel, sid, max_hp, max_hp, slot, now),
            )
        for item_id, qty in starter_items or [
            ("potion", 5),
            ("revive", 3),
            ("pokeball", 10),
        ]:
            await self.execute(
                """INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity)
                VALUES(?,?,?,?) ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET
                quantity=quantity+excluded.quantity""",
                (nick, channel, item_id, qty),
            )

    async def pokemon_species_get_random(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?", (int(limit),)
        )
        return list(rows) if rows else []

    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_get_setting_spawns_per_day(self) -> int:
        row = await self.fetchone(
            "SELECT value FROM settings WHERE key='pokemon_wild_spawns_per_day'", ()
        )
        try:
            return int(row[0]) if row else 24
        except (TypeError, ValueError):
            return 24

    async def pokemon_wild_spawns_count_24h(self, channel: str) -> int:
        since = int(time.time()) - 86400
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts>=?",
            ((channel or "").strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_wild_spawn_active(self, channel: str) -> dict | None:
        now = int(time.time())
        row = await self.fetchone(
            """SELECT w.*, s.name, s.type1, s.type2 FROM pokemon_wild_spawns w
            JOIN pokemon_species s ON s.id=w.species_id
            WHERE w.channel=? AND w.expires_ts>? AND w.captured_by IS NULL""",
            ((channel or "").strip(), now),
        )
        return dict(row) if row else None

    async def pokemon_wild_spawn_create(
        self, channel: str, species_id: int, level: int, duration_seconds: int = 600
    ) -> bool:
        now = int(time.time())
        expires = now + int(duration_seconds)
        try:
            await self.execute(
                """INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
                VALUES(?,?,?,?,?)""",
                ((channel or "").strip(), int(species_id), int(level), now, expires),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    async def pokemon_wild_spawn_capture(self, spawn_id: int, trainer_nick: str) -> bool:
        row = await self.fetchone(
            "SELECT channel, species_id, level FROM pokemon_wild_spawns WHERE id=? AND captured_by IS NULL",
            (int(spawn_id),),
        )
        if not row:
            return False
        channel = row["channel"]
        species_id = row["species_id"]
        level = int(row["level"])
        nick = (trainer_nick or "").strip()
        sp = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (species_id,))
        base_hp = int(sp["hp_base"]) if sp else 20
        max_hp = max(10, int(base_hp * 0.5) + int(level * 1.5))

        async with self._lock:
            cur = self._conn.execute(
                "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=? AND captured_by IS NULL",
                (nick, spawn_id),
            )
            if cur.rowcount != 1:
                return False
            slot = 1
            existing = self._conn.execute(
                "SELECT MAX(slot) FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
                (nick, channel),
            ).fetchone()
            if existing and existing[0] is not None:
                slot = min(6, int(existing[0]) + 1)
            now = int(time.time())
            self._conn.execute(
                """INSERT INTO pokemon_trainer_pokemon(
                    trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts
                ) VALUES(?,?,?,?,?,?,?,?)""",
                (nick, channel, species_id, level, max_hp, max_hp, slot, now),
            )
        return True

    async def pokemon_wild_spawn_delete_expired(self, now_ts: int | None = None) -> int:
        now = int(now_ts or time.time())
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM pokemon_wild_spawns WHERE expires_ts<=? OR captured_by IS NOT NULL",
                (now,),
            )
            return int(cur.rowcount)

    async def pokemon_trainer_list_pokemon(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT p.*, s.name as species_name, s.type1, s.type2 FROM pokemon_trainer_pokemon p
            JOIN pokemon_species s ON s.id=p.species_id
            WHERE p.trainer_nick=? AND p.channel=? ORDER BY p.slot""",
            ((nick or "").strip(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_list_items(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT ti.item_id, ti.quantity, i.name, i.item_type, i.effect_value
            FROM pokemon_trainer_items ti
            JOIN pokemon_items i ON i.id=ti.item_id
            WHERE ti.trainer_nick=? AND ti.channel=? AND ti.quantity>0
            ORDER BY i.item_type, i.name""",
            ((nick or "").strip(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_use_item(
        self, nick: str, channel: str, item_id: str, *, pokemon_id: int | None = None
    ) -> tuple[bool, str]:
        nick = (nick or "").strip()
        channel = (channel or "").strip()
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (item_id,))
        if not item:
            return False, "Unknown item."
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick, channel, item_id),
        )
        if not row or int(row[0] or 0) < 1:
            return False, "You don't have any."

        itype = item["item_type"]
        if itype == "heal":
            if not pokemon_id:
                return False, "Specify which Pokemon to heal: !heal <slot>"
            pkmn = await self.fetchone(
                "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
                (pokemon_id, nick, channel),
            )
            if not pkmn:
                return False, "Pokemon not found."
            if pkmn["is_fainted"]:
                return False, "Use a Revive on fainted Pokemon."
            heal = min(int(item["effect_value"]), int(pkmn["max_hp"]) - int(pkmn["current_hp"]))
            if heal <= 0:
                return False, "Already at full HP."
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=current_hp+? WHERE id=?",
                (heal, pokemon_id),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (nick, channel, item_id),
            )
            return True, f"Healed for {heal} HP."
        if itype == "revive":
            if not pokemon_id:
                return False, "Specify which Pokemon to revive: !revive <slot>"
            pkmn = await self.fetchone(
                "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
                (pokemon_id, nick, channel),
            )
            if not pkmn:
                return False, "Pokemon not found."
            if not pkmn["is_fainted"]:
                return False, "Pokemon is not fainted."
            pct = int(item["effect_value"])  # 50 or 100
            hp = max(1, int(pkmn["max_hp"]) * pct // 100)
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
                (hp, pokemon_id),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (nick, channel, item_id),
            )
            return True, f"Revived to {hp} HP."
        return False, "You can't use that here."

    async def pokemon_trainer_levelup(self, nick: str, channel: str, pokemon_id: int) -> tuple[bool, str]:
        pkmn = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
            (pokemon_id, nick.strip(), channel.strip()),
        )
        if not pkmn:
            return False, "Pokemon not found."
        level = int(pkmn["level"] or 5)
        xp_needed = level * 10
        xp = int(pkmn["experience"] or 0)
        if xp < xp_needed:
            return False, f"Needs {xp_needed - xp} more XP (level {level} requires {xp_needed} XP)."
        sp = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (pkmn["species_id"],))
        base_hp = int(sp["hp_base"]) if sp else 20
        new_level = level + 1
        new_max_hp = max(10, int(base_hp * 0.5) + int(new_level * 1.5))
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, experience=0, max_hp=?, current_hp=? WHERE id=?",
            (new_level, new_max_hp, new_max_hp, pokemon_id),
        )
        return True, f"Leveled up to {new_level}!"

    async def pokemon_trainer_add_xp(self, nick: str, channel: str, pokemon_id: int, amount: int) -> None:
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET experience=experience+? WHERE id=? AND trainer_nick=? AND channel=?",
            (int(amount), pokemon_id, nick.strip(), channel.strip()),
        )

    async def pokemon_list_channels_with_spawns(self) -> list[str]:
        rows = await self.fetchall(
            "SELECT channel FROM service_enablement WHERE service='pokemon' AND enabled=1", ()
        )
        chans = [r[0] for r in rows] if rows else []
        return [c for c in chans if c and c.startswith("#")]

    # ---- Pokemon ----
    async def pokemon_get_wild_spawns_per_day(self) -> int:
        val = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(100, int(val)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_wild_spawns_per_day(self, n: int) -> None:
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, min(100, n))))

    async def pokemon_count_spawns_in_last_24h(self, channel: str) -> int:
        since = int(time.time()) - (24 * 3600)
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            (channel.strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_get_active_wild_spawn(self, channel: str):
        now = int(time.time())
        return await self.fetchone(
            """
            SELECT * FROM pokemon_wild_spawns
            WHERE channel=? AND captured_by IS NULL AND expires_ts > ?
            LIMIT 1
            """,
            (channel.strip(), now),
        )

    async def pokemon_insert_wild_spawn(
        self, channel: str, species_id: int, level: int, duration_seconds: int
    ) -> int | None:
        now = int(time.time())
        expires = now + duration_seconds
        try:
            await self.execute(
                "INSERT INTO pokemon_wild_spawns(channel,species_id,level,appeared_ts,expires_ts) VALUES(?,?,?,?,?)",
                (channel.strip(), int(species_id), int(level), now, expires),
            )
            row = await self.fetchone("SELECT last_insert_rowid()", ())
            return int(row[0]) if row else None
        except sqlite3.IntegrityError:
            return None

    async def pokemon_capture_wild_spawn(self, spawn_id: int, trainer_nick: str) -> bool:
        async with self._lock:
            cur = self._conn.execute(
                "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=? AND captured_by IS NULL",
                (trainer_nick.strip(), int(spawn_id)),
            )
            return cur.rowcount > 0

    async def pokemon_delete_wild_spawn(self, spawn_id: int) -> None:
        await self.execute("DELETE FROM pokemon_wild_spawns WHERE id=?", (int(spawn_id),))

    async def pokemon_prune_expired_wild_spawns(self, now_ts: int | None = None) -> int:
        now = int(now_ts or time.time())
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM pokemon_wild_spawns WHERE expires_ts <= ? OR captured_by IS NOT NULL",
                (now,),
            )
            return int(cur.rowcount or 0)

    async def pokemon_is_trainer(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return row is not None

    async def pokemon_create_trainer(self, nick: str, channel: str) -> bool:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        if not nick_l or not chan:
            return False
        now = int(time.time())
        try:
            await self.execute(
                "INSERT INTO pokemon_trainers(nick,channel,created_ts) VALUES(?,?,?)",
                (nick_l, chan, now),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    async def pokemon_get_random_species(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows) if rows else []

    async def pokemon_add_trainer_pokemon(
        self,
        trainer_nick: str,
        channel: str,
        species_id: int,
        *,
        level: int = 5,
        slot: int = 1,
    ) -> int | None:
        nick_l = (trainer_nick or "").strip().lower()
        chan = (channel or "").strip()
        species = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (int(species_id),))
        if not species:
            return None
        hp_base = int(species[0] or 50)
        max_hp = max(10, int(hp_base * 0.5 + level * 2))
        now = int(time.time())
        await self.execute(
            """
            INSERT INTO pokemon_trainer_pokemon(
                trainer_nick, channel, species_id, level, current_hp, max_hp, experience, slot, created_ts
            )
            VALUES(?,?,?,?,?,?,0,?,?)
            """,
            (nick_l, chan, int(species_id), int(level), max_hp, max_hp, int(slot), now),
        )
        row = await self.fetchone("SELECT last_insert_rowid()", ())
        return int(row[0]) if row else None

    async def pokemon_add_trainer_starter_items(self, trainer_nick: str, channel: str) -> None:
        nick_l = (trainer_nick or "").strip().lower()
        chan = (channel or "").strip()
        if not nick_l or not chan:
            return
        starter_items = [("potion", 5), ("revive", 2), ("pokeball", 10)]
        for item_id, qty in starter_items:
            await self.execute(
                """
                INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity) VALUES(?,?,?,?)
                ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+excluded.quantity
                """,
                (nick_l, chan, item_id, qty),
            )

    async def pokemon_list_trainer_pokemon(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """
            SELECT p.*, s.name as species_name, s.type1, s.type2
            FROM pokemon_trainer_pokemon p
            JOIN pokemon_species s ON p.species_id = s.id
            WHERE p.trainer_nick=? AND p.channel=?
            ORDER BY p.slot, p.id
            """,
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_get_trainer_pokemon(
        self, nick: str, channel: str, slot: int | None = None, pokemon_id: int | None = None
    ):
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        if pokemon_id is not None:
            return await self.fetchone(
                """
                SELECT p.*, s.name as species_name, s.type1, s.type2, s.hp_base
                FROM pokemon_trainer_pokemon p
                JOIN pokemon_species s ON p.species_id = s.id
                WHERE p.trainer_nick=? AND p.channel=? AND p.id=?
                """,
                (nick_l, chan, int(pokemon_id)),
            )
        if slot is not None:
            return await self.fetchone(
                """
                SELECT p.*, s.name as species_name, s.type1, s.type2, s.hp_base
                FROM pokemon_trainer_pokemon p
                JOIN pokemon_species s ON p.species_id = s.id
                WHERE p.trainer_nick=? AND p.channel=? AND p.slot=?
                """,
                (nick_l, chan, int(slot)),
            )
        return None

    async def pokemon_heal_pokemon(self, nick: str, channel: str, pokemon_id: int, amount: int) -> bool:
        async with self._lock:
            cur = self._conn.execute(
                """
                UPDATE pokemon_trainer_pokemon
                SET current_hp = MIN(max_hp, current_hp + ?)
                WHERE trainer_nick=? AND channel=? AND id=? AND is_fainted=0
                """,
                (int(amount), (nick or "").strip().lower(), (channel or "").strip(), int(pokemon_id)),
            )
            return cur.rowcount > 0

    async def pokemon_revive_pokemon(
        self, nick: str, channel: str, pokemon_id: int, percent: int = 50
    ) -> bool:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        if percent <= 0:
            percent = 50
        if percent > 100:
            percent = 100
        async with self._lock:
            cur = self._conn.execute(
                """
                UPDATE pokemon_trainer_pokemon
                SET is_fainted=0, current_hp = (max_hp * ? / 100)
                WHERE trainer_nick=? AND channel=? AND id=? AND is_fainted=1
                """,
                (percent, nick_l, chan, int(pokemon_id)),
            )
            return cur.rowcount > 0

    async def pokemon_level_up(self, nick: str, channel: str, pokemon_id: int) -> bool:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        pokemon = await self.fetchone(
            "SELECT level, species_id FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? AND id=?",
            (nick_l, chan, int(pokemon_id)),
        )
        if not pokemon:
            return False
        level = int(pokemon[0])
        species_id = int(pokemon[1])
        species = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (species_id,))
        hp_base = int(species[0] or 50) if species else 50
        new_level = level + 1
        new_max_hp = max(10, int(hp_base * 0.5 + new_level * 2))
        async with self._lock:
            cur = self._conn.execute(
                """
                UPDATE pokemon_trainer_pokemon
                SET level=?, max_hp=?, current_hp=?
                WHERE trainer_nick=? AND channel=? AND id=?
                """,
                (new_level, new_max_hp, new_max_hp, nick_l, chan, int(pokemon_id)),
            )
            return cur.rowcount > 0

    async def pokemon_get_trainer_item(self, nick: str, channel: str, item_id: str):
        return await self.fetchone(
            "SELECT * FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            ((nick or "").strip().lower(), (channel or "").strip(), (item_id or "").strip()),
        )

    async def pokemon_list_trainer_items(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """
            SELECT t.item_id, t.quantity, i.name, i.item_type, i.effect_value
            FROM pokemon_trainer_items t
            JOIN pokemon_items i ON t.item_id = i.id
            WHERE t.trainer_nick=? AND t.channel=? AND t.quantity > 0
            ORDER BY i.item_type, i.name
            """,
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_use_item(
        self, nick: str, channel: str, item_id: str, pokemon_id: int | None = None
    ) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        inv = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, chan, item_id),
        )
        if not inv or int(inv[0] or 0) < 1:
            return False, "You don't have that item."
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (item_id,))
        if not item:
            return False, "Unknown item."
        item_type = (item["item_type"] or "").strip()
        effect_value = int(item["effect_value"] or 0)
        if item_type == "heal":
            if pokemon_id is None:
                return False, "Specify which Pokemon: !heal <slot|id>"
            ok = await self.pokemon_heal_pokemon(nick_l, chan, pokemon_id, effect_value)
            if not ok:
                return False, "That Pokemon is fainted or doesn't exist."
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=? AND quantity>=1",
                (nick_l, chan, item_id),
            )
            return True, f"Healed for {effect_value} HP!"
        if item_type == "revive":
            if pokemon_id is None:
                return False, "Specify which Pokemon: !revive <slot|id>"
            ok = await self.pokemon_revive_pokemon(nick_l, chan, pokemon_id, effect_value)
            if not ok:
                return False, "That Pokemon isn't fainted or doesn't exist."
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=? AND quantity>=1",
                (nick_l, chan, item_id),
            )
            return True, f"Revived to {effect_value}% HP!"
        return False, "That item cannot be used that way."

    async def pokemon_count_species(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_get_species(self, species_id: int):
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))

    async def pokemon_get_species_by_name(self, name: str):
        return await self.fetchone(
            "SELECT * FROM pokemon_species WHERE LOWER(name)=?",
            ((name or "").strip().lower(),),
        )

    # ---- Pokemon service ----
    async def pokemon_trainer_exists(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            ((nick or "").strip(), (channel or "").strip()),
        )
        return row is not None

    async def pokemon_trainer_create(self, nick: str, channel: str) -> None:
        nick = (nick or "").strip()
        channel = (channel or "").strip()
        if not nick or not channel:
            return
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick, channel, now),
        )
        # Give starter items
        starter_items = [("potion", 5), ("revive", 3), ("pokeball", 10)]
        for item_id, qty in starter_items:
            await self.execute(
                "INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity) VALUES(?,?,?,?) "
                "ON CONFLICT(trainer_nick, channel, item_id) DO UPDATE SET quantity=quantity+excluded.quantity",
                (nick, channel, item_id, qty),
            )

    async def pokemon_trainer_add_starter(self, nick: str, channel: str, species_id: int) -> None:
        nick = (nick or "").strip()
        channel = (channel or "").strip()
        if not nick or not channel:
            return
        now = int(time.time())
        # Get next slot
        row = await self.fetchone(
            "SELECT COALESCE(MAX(slot), 0) + 1 FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
            (nick, channel),
        )
        slot = int(row[0]) if row else 1
        # Base HP formula: level 5 -> ~20 HP for typical starter
        species = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (species_id,))
        hp_base = int(species["hp_base"]) if species else 50
        max_hp = max(10, int(hp_base * 0.4) + (slot * 2))
        await self.execute(
            """
            INSERT INTO pokemon_trainer_pokemon(
                trainer_nick, channel, species_id, nickname, level, current_hp, max_hp, experience, is_fainted, slot, created_ts
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (nick, channel, species_id, None, 5, max_hp, max_hp, 0, 0, min(slot, 6), now),
        )

    async def pokemon_species_get_random(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows) if rows else []

    async def pokemon_species_get(self, species_id: int) -> sqlite3.Row | None:
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))

    async def pokemon_trainer_get_pokemon(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """
            SELECT p.*, s.name as species_name, s.type1, s.type2, s.pokedex_number
            FROM pokemon_trainer_pokemon p
            JOIN pokemon_species s ON p.species_id = s.id
            WHERE p.trainer_nick=? AND p.channel=?
            ORDER BY p.slot, p.id
            """,
            ((nick or "").strip(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_get_items(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """
            SELECT ti.item_id, ti.quantity, i.name, i.item_type, i.effect_value
            FROM pokemon_trainer_items ti
            JOIN pokemon_items i ON ti.item_id = i.id
            WHERE ti.trainer_nick=? AND ti.channel=? AND ti.quantity > 0
            ORDER BY i.name
            """,
            ((nick or "").strip(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_trainer_use_item(
        self, nick: str, channel: str, item_id: str, target_id: int | None = None
    ) -> tuple[bool, str]:
        nick = (nick or "").strip()
        channel = (channel or "").strip()
        item_id = (item_id or "").strip().lower().replace(" ", "_")
        if not nick or not channel or not item_id:
            return False, "Invalid parameters."
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (item_id,))
        if not item:
            return False, f"Unknown item: {item_id}"
        inv = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick, channel, item_id),
        )
        if not inv or int(inv["quantity"] or 0) <= 0:
            return False, "You don't have that item."
        if item["item_type"] == "heal" and target_id:
            poke = await self.fetchone(
                "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
                (target_id, nick, channel),
            )
            if not poke:
                return False, "Pokemon not found."
            if poke["is_fainted"]:
                return False, "Cannot heal a fainted Pokemon. Use a Revive."
            heal = min(int(item["effect_value"]), int(poke["max_hp"]) - int(poke["current_hp"]))
            if heal <= 0:
                return False, "Pokemon is already at full HP."
            new_hp = int(poke["current_hp"]) + heal
            await self.execute("UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE id=?", (new_hp, target_id))
        elif item["item_type"] == "revive" and target_id:
            poke = await self.fetchone(
                "SELECT * FROM pokemon_trainer_pokemon WHERE id=? AND trainer_nick=? AND channel=?",
                (target_id, nick, channel),
            )
            if not poke:
                return False, "Pokemon not found."
            if not poke["is_fainted"]:
                return False, "Pokemon is not fainted."
            percent = int(item["effect_value"])
            max_hp = int(poke["max_hp"])
            new_hp = max(1, int(max_hp * percent / 100))
            await self.execute(
                "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE id=?",
                (new_hp, target_id),
            )
        else:
            return False, "That item can't be used here."
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick, channel, item_id),
        )
        return True, "OK"

    async def pokemon_trainer_level_up(self, nick: str, channel: str, poke_id: int) -> tuple[bool, str]:
        nick = (nick or "").strip()
        channel = (channel or "").strip()
        poke = await self.fetchone(
            "SELECT p.*, s.hp_base FROM pokemon_trainer_pokemon p JOIN pokemon_species s ON p.species_id=s.id WHERE p.id=? AND p.trainer_nick=? AND p.channel=?",
            (poke_id, nick, channel),
        )
        if not poke:
            return False, "Pokemon not found."
        level = int(poke["level"])
        hp_base = int(poke["hp_base"])
        new_level = level + 1
        new_max_hp = max(10, int(hp_base * (0.4 + 0.1 * new_level)) + new_level * 2)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET level=?, max_hp=?, current_hp=? WHERE id=?",
            (new_level, new_max_hp, new_max_hp, poke_id),
        )
        return True, f"Level {new_level}!"

    async def pokemon_wild_get(self, channel: str) -> sqlite3.Row | None:
        now = int(time.time())
        return await self.fetchone(
            "SELECT * FROM pokemon_wild_spawns WHERE channel=? AND expires_ts > ? AND captured_by IS NULL",
            ((channel or "").strip(), now),
        )

    async def pokemon_wild_spawn(self, channel: str, species_id: int, level: int, duration_s: int = 600) -> bool:
        channel = (channel or "").strip()
        if not channel:
            return False
        now = int(time.time())
        expires = now + duration_s
        try:
            await self.execute(
                "INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts) VALUES(?,?,?,?,?)",
                (channel, species_id, level, now, expires),
            )
            return True
        except Exception:
            return False

    async def pokemon_wild_spawns_count_24h(self, channel: str) -> int:
        since = int(time.time()) - 86400
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            ((channel or "").strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_wild_capture(self, spawn_id: int, nick: str) -> tuple[bool, str]:
        spawn = await self.fetchone("SELECT * FROM pokemon_wild_spawns WHERE id=? AND captured_by IS NULL", (spawn_id,))
        if not spawn:
            return False, "That Pokemon is no longer available."
        now = int(time.time())
        if spawn["expires_ts"] <= now:
            return False, "That Pokemon fled!"
        channel = str(spawn["channel"])
        species_id = int(spawn["species_id"])
        level = int(spawn["level"] or 5)
        nick = (nick or "").strip()
        if not nick:
            return False, "Invalid nick."
        # Create trainer if needed
        if not await self.pokemon_trainer_exists(nick, channel):
            await self.pokemon_trainer_create(nick, channel)
        # Check ball
        inv = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick, channel, "pokeball"),
        )
        if not inv or int(inv["quantity"] or 0) <= 0:
            inv = await self.fetchone(
                "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id IN ('great_ball','ultra_ball')",
                (nick, channel),
            )
            if not inv or int(inv["quantity"] or 0) <= 0:
                return False, "You need a Poké Ball to capture! Message me privately for items."
        # Capture chance (simplified)
        species = await self.fetchone("SELECT capture_rate FROM pokemon_species WHERE id=?", (species_id,))
        rate = int(species["capture_rate"]) if species else 255
        import random
        roll = random.randint(1, 255)
        if roll > rate:
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id='pokeball'",
                (nick, channel),
            )
            return False, "The Pokemon broke free!"
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?", (nick, spawn_id),
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id='pokeball'",
            (nick, channel),
        )
        # Add to party
        row = await self.fetchone(
            "SELECT COALESCE(MAX(slot), 0) + 1 FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
            (nick, channel),
        )
        slot = min(int(row[0]) if row else 1, 6)
        species_row = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (species_id,))
        hp_base = int(species_row["hp_base"]) if species_row else 50
        max_hp = max(10, int(hp_base * 0.4) + level * 2)
        now_ts = int(time.time())
        await self.execute(
            """
            INSERT INTO pokemon_trainer_pokemon(trainer_nick, channel, species_id, level, current_hp, max_hp, is_fainted, slot, created_ts)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (nick, channel, species_id, level, max_hp, max_hp, 0, slot, now_ts),
        )
        return True, "Caught!"

    async def pokemon_wild_expire_old(self, now_ts: int | None = None) -> int:
        now = now_ts or int(time.time())
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM pokemon_wild_spawns WHERE expires_ts <= ? OR captured_by IS NOT NULL",
                (now,),
            )
            return int(cur.rowcount or 0)

    async def pokemon_get_spawns_per_day_setting(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(100, int(v)))
        except Exception:
            return 24

    async def pokemon_set_spawns_per_day(self, n: int) -> None:
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, min(100, int(n)))))

    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    # ---- Pokemon service ----
    async def pokemon_get_wild_spawns_per_day(self) -> int:
        v = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(100, int(v)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_wild_spawns_per_day(self, n: int) -> None:
        now = int(time.time())
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, min(100, n))))
        await self.execute(
            "UPDATE settings SET updated_ts=? WHERE key='pokemon_wild_spawns_per_day'",
            (now,),
        )

    async def pokemon_count_spawns_last_24h(self, channel: str) -> int:
        since = int(time.time()) - (24 * 3600)
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts>=?",
            (channel.strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_get_random_species(self, limit: int = 1) -> list:
        rows = await self.fetchall(
            "SELECT * FROM pokemon_species ORDER BY RANDOM() LIMIT ?",
            (int(limit),),
        )
        return list(rows) if rows else []

    async def pokemon_get_species_by_id(self, species_id: int):
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (int(species_id),))

    async def pokemon_is_trainer(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return row is not None

    async def pokemon_create_trainer(self, nick: str, channel: str, *, starter_species_ids: list[int]) -> None:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        if not nick_l or not chan:
            return
        now = int(time.time())
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick_l, chan, now),
        )
        for slot, sid in enumerate(starter_species_ids[:6], 1):
            species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (sid,))
            if not species:
                continue
            max_hp = max(10, species["hp_base"] + species["level"] if "level" in species.keys() else species["hp_base"])
            # Starter Pokemon: level 5, full HP
            base_hp = int(species["hp_base"]) if species["hp_base"] else 50
            level = 5
            max_hp = max(10, int(base_hp * 2 * level / 100) + level + 10)
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(
                    trainer_nick, channel, species_id, level, current_hp, max_hp,
                    experience, is_fainted, slot, created_ts
                ) VALUES(?,?,?,?,?,?,0,0,?,?)""",
                (nick_l, chan, int(sid), level, max_hp, max_hp, slot, now),
            )
        starter_items = [
            ("potion", 5),
            ("revive", 3),
            ("pokeball", 10),
        ]
        for item_id, qty in starter_items:
            await self.execute(
                """INSERT INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity)
                   VALUES(?,?,?,?) ON CONFLICT(trainer_nick, channel, item_id)
                   DO UPDATE SET quantity=quantity+excluded.quantity""",
                (nick_l, chan, item_id, qty),
            )

    async def pokemon_get_trainer_pokemon(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT p.*, s.name as species_name, s.type1, s.type2, s.pokedex_number
               FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON p.species_id = s.id
               WHERE p.trainer_nick=? AND p.channel=? ORDER BY p.slot""",
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_get_trainer_items(self, nick: str, channel: str) -> list:
        rows = await self.fetchall(
            """SELECT i.id, i.name, i.item_type, i.effect_value, t.quantity
               FROM pokemon_trainer_items t
               JOIN pokemon_items i ON t.item_id = i.id
               WHERE t.trainer_nick=? AND t.channel=? AND t.quantity>0
               ORDER BY i.item_type, i.name""",
            ((nick or "").strip().lower(), (channel or "").strip()),
        )
        return list(rows) if rows else []

    async def pokemon_get_active_wild_spawn(self, channel: str):
        now = int(time.time())
        return await self.fetchone(
            """SELECT w.*, s.name as species_name, s.type1, s.type2, s.capture_rate
               FROM pokemon_wild_spawns w
               JOIN pokemon_species s ON w.species_id = s.id
               WHERE w.channel=? AND w.expires_ts>? AND w.captured_by IS NULL""",
            (channel.strip(), now),
        )

    async def pokemon_spawn_wild(self, channel: str, species_id: int, level: int, duration_seconds: int = 900) -> int | None:
        now = int(time.time())
        expires = now + duration_seconds
        try:
            await self.execute(
                """INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
                   VALUES(?,?,?,?,?)""",
                (channel.strip(), int(species_id), int(level), now, expires),
            )
            row = await self.fetchone("SELECT last_insert_rowid()", ())
            return int(row[0]) if row else None
        except Exception:
            return None

    async def pokemon_capture_wild(self, spawn_id: int, trainer_nick: str) -> bool:
        now = int(time.time())
        spawn = await self.fetchone(
            "SELECT * FROM pokemon_wild_spawns WHERE id=? AND captured_by IS NULL AND expires_ts>?",
            (int(spawn_id), now),
        )
        if not spawn:
            return False
        channel = str(spawn["channel"])
        species_id = int(spawn["species_id"])
        level = int(spawn["level"])
        nick_l = (trainer_nick or "").strip().lower()
        species = await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (species_id,))
        if not species:
            return False
        base_hp = int(species["hp_base"]) if species["hp_base"] else 50
        max_hp = max(10, int(base_hp * 2 * level / 100) + level + 10)
        slot_row = await self.fetchone(
            "SELECT COALESCE(MAX(slot),0)+1 FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
            (nick_l, channel),
        )
        slot = int(slot_row[0]) if slot_row and slot_row[0] else 1
        if slot > 6:
            return False
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=?", (nick_l, int(spawn_id))
        )
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon(
                trainer_nick, channel, species_id, level, current_hp, max_hp,
                experience, is_fainted, slot, created_ts
            ) VALUES(?,?,?,?,?,?,0,0,?,?)""",
            (nick_l, channel, species_id, level, max_hp, max_hp, slot, now),
        )
        return True

    async def pokemon_use_item_heal(self, nick: str, channel: str, item_id: str, pokemon_slot: int) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        inv = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, chan, item_id),
        )
        if not inv or int(inv["quantity"] or 0) < 1:
            return False, "You don't have that item."
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=? AND item_type='heal'", (item_id,))
        if not item:
            return False, "That item cannot heal."
        heal = int(item["effect_value"] or 0)
        pokemon = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? AND slot=?",
            (nick_l, chan, int(pokemon_slot)),
        )
        if not pokemon:
            return False, "No Pokemon in that slot."
        if int(pokemon["is_fainted"] or 0):
            return False, "Use a Revive on fainted Pokemon."
        cur_hp = int(pokemon["current_hp"] or 0)
        max_hp = int(pokemon["max_hp"] or 1)
        if cur_hp >= max_hp:
            return False, "Pokemon is already at full HP."
        new_hp = min(max_hp, cur_hp + heal)
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=? WHERE trainer_nick=? AND channel=? AND slot=?",
            (new_hp, nick_l, chan, pokemon_slot),
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, chan, item_id),
        )
        return True, f"Healed {new_hp - cur_hp} HP! ({new_hp}/{max_hp})"

    async def pokemon_use_item_revive(self, nick: str, channel: str, item_id: str, pokemon_slot: int) -> tuple[bool, str]:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        inv = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, chan, item_id),
        )
        if not inv or int(inv["quantity"] or 0) < 1:
            return False, "You don't have that item."
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=? AND item_type='revive'", (item_id,))
        if not item:
            return False, "That item cannot revive."
        revive_pct = int(item["effect_value"] or 50)
        pokemon = await self.fetchone(
            "SELECT * FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=? AND slot=?",
            (nick_l, chan, int(pokemon_slot)),
        )
        if not pokemon:
            return False, "No Pokemon in that slot."
        if not int(pokemon["is_fainted"] or 0):
            return False, "Pokemon is not fainted."
        max_hp = int(pokemon["max_hp"] or 1)
        new_hp = max(1, int(max_hp * revive_pct / 100))
        await self.execute(
            "UPDATE pokemon_trainer_pokemon SET current_hp=?, is_fainted=0 WHERE trainer_nick=? AND channel=? AND slot=?",
            (new_hp, nick_l, chan, pokemon_slot),
        )
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, chan, item_id),
        )
        return True, f"Revived! HP: {new_hp}/{max_hp}"

    async def pokemon_expire_old_spawns(self) -> int:
        now = int(time.time())
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM pokemon_wild_spawns WHERE expires_ts<=? OR captured_by IS NOT NULL",
                (now,),
            )
            return int(cur.rowcount or 0)

    async def pokemon_list_channels_with_active_spawns(self) -> list[str]:
        now = int(time.time())
        rows = await self.fetchall(
            "SELECT DISTINCT channel FROM pokemon_wild_spawns WHERE expires_ts>? AND captured_by IS NULL",
            (now,),
        )
        return [str(r[0]) for r in rows] if rows else []

    async def pokemon_get_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_consume_pokeball(self, nick: str, channel: str, item_id: str) -> bool:
        nick_l = (nick or "").strip().lower()
        chan = (channel or "").strip()
        inv = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, chan, item_id),
        )
        if not inv or int(inv["quantity"] or 0) < 1:
            return False
        await self.execute(
            "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick_l, chan, item_id),
        )
        return True

    async def pokemon_get_item_ball_modifier(self, item_id: str) -> float:
        item = await self.fetchone("SELECT effect_value FROM pokemon_items WHERE id=? AND item_type='ball'", (item_id,))
        if not item:
            return 1.0
        return float(item["effect_value"] or 255) / 255.0

    # ---- Pokemon service ----
    async def pokemon_get_wild_spawns_per_day(self) -> int:
        val = await self.get_setting("pokemon_wild_spawns_per_day", "24")
        try:
            return max(1, min(1000, int(val)))
        except (TypeError, ValueError):
            return 24

    async def pokemon_set_wild_spawns_per_day(self, n: int) -> None:
        await self.set_setting("pokemon_wild_spawns_per_day", str(max(1, min(1000, n))))

    async def pokemon_count_spawns_in_last_24h(self, channel: str) -> int:
        since = int(time.time()) - (24 * 3600)
        row = await self.fetchone(
            "SELECT COUNT(*) FROM pokemon_wild_spawns WHERE channel=? AND appeared_ts >= ?",
            (channel.strip(), since),
        )
        return int(row[0]) if row else 0

    async def pokemon_is_trainer(self, nick: str, channel: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM pokemon_trainers WHERE nick=? AND channel=?",
            (nick.strip(), channel.strip()),
        )
        return row is not None

    async def pokemon_create_trainer(self, nick: str, channel: str, *, species_ids: list[int]) -> None:
        now = int(time.time())
        nick = nick.strip()
        channel = channel.strip()
        await self.execute(
            "INSERT OR IGNORE INTO pokemon_trainers(nick, channel, created_ts) VALUES(?,?,?)",
            (nick, channel, now),
        )
        for slot, species_id in enumerate(species_ids[:6], 1):
            species = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (species_id,))
            if not species:
                continue
            max_hp = max(10, (species[0] or 50) + (5 * 5))
            await self.execute(
                """INSERT INTO pokemon_trainer_pokemon(
                    trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts
                ) VALUES(?,?,?,5,?,?,?,?)""",
                (nick, channel, species_id, max_hp, max_hp, slot, now),
            )
        for item_id, qty in [("potion", 5), ("revive", 2), ("pokeball", 10)]:
            await self.execute(
                """INSERT OR IGNORE INTO pokemon_trainer_items(trainer_nick, channel, item_id, quantity)
                VALUES(?,?,?,?)""",
                (nick, channel, item_id, 0),
            )
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity+? WHERE trainer_nick=? AND channel=? AND item_id=?",
                (qty, nick, channel, item_id),
            )

    async def pokemon_get_random_species(self, limit: int = 1) -> list[int]:
        rows = await self.fetchall(
            "SELECT id FROM pokemon_species ORDER BY RANDOM() LIMIT ?", (int(limit),)
        )
        return [int(r[0]) for r in rows] if rows else []

    async def pokemon_species_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) FROM pokemon_species", ())
        return int(row[0]) if row else 0

    async def pokemon_get_wild_spawn(self, channel: str) -> sqlite3.Row | None:
        return await self.fetchone(
            """SELECT w.*, s.name, s.type1, s.type2
               FROM pokemon_wild_spawns w
               JOIN pokemon_species s ON s.id = w.species_id
               WHERE w.channel=? AND w.captured_by IS NULL AND w.expires_ts > ?""",
            (channel.strip(), int(time.time())),
        )

    async def pokemon_create_wild_spawn(self, channel: str, species_id: int, level: int, duration_s: int) -> bool:
        now = int(time.time())
        expires = now + duration_s
        try:
            await self.execute(
                """INSERT INTO pokemon_wild_spawns(channel, species_id, level, appeared_ts, expires_ts)
                VALUES(?,?,?,?,?)""",
                (channel.strip(), species_id, level, now, expires),
            )
            return True
        except Exception:
            return False

    async def pokemon_capture_wild(self, channel: str, nick: str) -> sqlite3.Row | None:
        spawn = await self.pokemon_get_wild_spawn(channel)
        if not spawn:
            return None
        await self.execute(
            "UPDATE pokemon_wild_spawns SET captured_by=? WHERE id=? AND channel=? AND captured_by IS NULL",
            (nick.strip(), int(spawn["id"]), channel.strip()),
        )
        return spawn

    async def pokemon_add_captured_to_trainer(self, nick: str, channel: str, species_id: int, level: int) -> int:
        now = int(time.time())
        nick = nick.strip()
        channel = channel.strip()
        species = await self.fetchone("SELECT hp_base FROM pokemon_species WHERE id=?", (species_id,))
        max_hp = max(10, (species[0] or 50) + (level * 5)) if species else 10 + level * 5
        row = await self.fetchone(
            "SELECT COALESCE(MAX(slot),0) FROM pokemon_trainer_pokemon WHERE trainer_nick=? AND channel=?",
            (nick, channel),
        )
        slot = int(row[0]) + 1 if row and row[0] else 1
        if slot > 6:
            return 0
        await self.execute(
            """INSERT INTO pokemon_trainer_pokemon(
                trainer_nick, channel, species_id, level, current_hp, max_hp, slot, created_ts
            ) VALUES(?,?,?,?,?,?,?,?)""",
            (nick, channel, species_id, level, max_hp, max_hp, slot, now),
        )
        row = await self.fetchone("SELECT last_insert_rowid()", ())
        return int(row[0]) if row else 0

    async def pokemon_expire_old_spawns(self, now_ts: int | None = None) -> int:
        now = int(now_ts or time.time())
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM pokemon_wild_spawns WHERE expires_ts <= ? OR captured_by IS NOT NULL",
                (now,),
            )
            return int(cur.rowcount or 0)

    async def pokemon_list_trainer_pokemon(self, nick: str, channel: str) -> list[sqlite3.Row]:
        return list(await self.fetchall(
            """SELECT p.*, s.name as species_name, s.type1, s.type2
               FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON s.id = p.species_id
               WHERE p.trainer_nick=? AND p.channel=? ORDER BY p.slot""",
            (nick.strip(), channel.strip()),
        ))

    async def pokemon_get_trainer_pokemon(self, nick: str, channel: str, slot: int) -> sqlite3.Row | None:
        return await self.fetchone(
            """SELECT p.*, s.name as species_name, s.type1, s.type2
               FROM pokemon_trainer_pokemon p
               JOIN pokemon_species s ON s.id = p.species_id
               WHERE p.trainer_nick=? AND p.channel=? AND p.slot=?""",
            (nick.strip(), channel.strip(), int(slot)),
        )

    async def pokemon_heal(self, nick: str, channel: str, slot: int, amount: int) -> bool:
        await self.execute(
            """UPDATE pokemon_trainer_pokemon
               SET current_hp = MIN(current_hp + ?, max_hp), is_fainted = 0
               WHERE trainer_nick=? AND channel=? AND slot=?""",
            (int(amount), nick.strip(), channel.strip(), int(slot)),
        )
        return True

    async def pokemon_revive(self, nick: str, channel: str, slot: int, percent: int = 50) -> bool:
        await self.execute(
            """UPDATE pokemon_trainer_pokemon
               SET current_hp = (max_hp * ? / 100), is_fainted = 0
               WHERE trainer_nick=? AND channel=? AND slot=?""",
            (int(percent), nick.strip(), channel.strip(), int(slot)),
        )
        return True

    async def pokemon_level_up(self, nick: str, channel: str, slot: int) -> bool:
        row = await self.pokemon_get_trainer_pokemon(nick, channel, slot)
        if not row:
            return False
        new_level = int(row["level"] or 5) + 1
        new_max_hp = max(10, int(row["max_hp"] or 20) + 5)
        await self.execute(
            """UPDATE pokemon_trainer_pokemon
               SET level=?, max_hp=?, current_hp=? WHERE trainer_nick=? AND channel=? AND slot=?""",
            (new_level, new_max_hp, new_max_hp, nick.strip(), channel.strip(), int(slot)),
        )
        return True

    async def pokemon_list_trainer_items(self, nick: str, channel: str) -> list[sqlite3.Row]:
        return list(await self.fetchall(
            """SELECT ti.*, i.name, i.item_type, i.effect_value
               FROM pokemon_trainer_items ti
               JOIN pokemon_items i ON i.id = ti.item_id
               WHERE ti.trainer_nick=? AND ti.channel=? AND ti.quantity > 0
               ORDER BY i.item_type, i.name""",
            (nick.strip(), channel.strip()),
        ))

    async def pokemon_use_item(self, nick: str, channel: str, item_id: str, slot: int | None) -> tuple[bool, str]:
        nick = nick.strip()
        channel = channel.strip()
        row = await self.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=?",
            (nick, channel, item_id),
        )
        if not row or int(row[0] or 0) < 1:
            return False, "You don't have that item."
        item = await self.fetchone("SELECT * FROM pokemon_items WHERE id=?", (item_id,))
        if not item:
            return False, "Invalid item."
        item_type = str(item["item_type"] or "")
        if item_type == "heal":
            if slot is None:
                return False, "Specify a Pokemon slot: !pkmn use potion 1"
            pkmn = await self.pokemon_get_trainer_pokemon(nick, channel, slot)
            if not pkmn:
                return False, f"No Pokemon in slot {slot}."
            if pkmn["is_fainted"]:
                return False, "Use a Revive on fainted Pokemon."
            amount = int(item["effect_value"] or 20)
            await self.pokemon_heal(nick, channel, slot, amount)
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (nick, channel, item_id),
            )
            return True, f"Healed {amount} HP!"
        if item_type == "revive":
            if slot is None:
                return False, "Specify a Pokemon slot: !pkmn use revive 1"
            pkmn = await self.pokemon_get_trainer_pokemon(nick, channel, slot)
            if not pkmn:
                return False, f"No Pokemon in slot {slot}."
            if not pkmn["is_fainted"]:
                return False, "Pokemon is not fainted."
            percent = int(item["effect_value"] or 50)
            await self.pokemon_revive(nick, channel, slot, percent)
            await self.execute(
                "UPDATE pokemon_trainer_items SET quantity=quantity-1 WHERE trainer_nick=? AND channel=? AND item_id=?",
                (nick, channel, item_id),
            )
            return True, f"Revived to {percent}% HP!"
        return False, "That item can't be used here."

    async def pokemon_channels_with_active_spawns(self) -> list[str]:
        now = int(time.time())
        rows = await self.fetchall(
            "SELECT DISTINCT channel FROM pokemon_wild_spawns WHERE captured_by IS NULL AND expires_ts > ?",
            (now,),
        )
        return [str(r[0]) for r in rows] if rows else []

    async def pokemon_list_enabled_channels(self) -> list[str]:
        rows = await self.fetchall(
            "SELECT channel FROM service_enablement WHERE service='pokemon' AND enabled=1",
            (),
        )
        return [str(r[0]) for r in rows] if rows else []

    async def pokemon_resolve_channel_for_trainer(self, nick: str) -> str | None:
        rows = await self.fetchall(
            "SELECT channel FROM pokemon_trainers WHERE nick=? ORDER BY created_ts DESC LIMIT 5",
            (nick.strip(),),
        )
        return str(rows[0]["channel"]) if rows else None

    async def pokemon_get_species(self, species_id: int) -> sqlite3.Row | None:
        return await self.fetchone("SELECT * FROM pokemon_species WHERE id=?", (species_id,))