import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from services.chatdb import ChatDB, DBConfig


def _now() -> int:
    return int(time.time())


def _j(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def _jload(s: str, default):
    try:
        return json.loads(s) if s else default
    except Exception:
        return default


@dataclass
class StoreConfig:
    db_path: str = "/var/lib/leobot/db/leobot.db"


class Store:
    """
    Standard storage API for Leobot services.

    Rules:
      - DB is authoritative.
      - Legacy JSON files are imported ONCE if DB tables are empty.
      - No service should write its own random JSON state files after migration.
    """

    def __init__(self, db_path: str):
        self.db = ChatDB(DBConfig(db_path))

    # ---------------------------
    # Greetings
    # ---------------------------

    async def greet_rule_count(self) -> int:
        row = await self.db.fetchone("SELECT COUNT(*) FROM greet_rules")
        return int(row[0]) if row else 0

    async def greet_list_rules(self) -> list[dict]:
        rows = await self.db.fetchall(
            "SELECT id, priority, enabled, match_json, greetings_json, updated_ts "
            "FROM greet_rules ORDER BY priority DESC, id ASC"
        )
        out = []
        for (rid, pr, en, mjs, gjs, uts) in rows:
            out.append(
                {
                    "id": rid,
                    "priority": int(pr),
                    "enabled": bool(en),
                    "match": _jload(mjs, {}),
                    "greetings": _jload(gjs, []),
                    "updated_ts": int(uts),
                }
            )
        return out

    async def greet_upsert_rule(self, *, rid: str, priority: int, enabled: bool, match: dict, greetings: list[str]) -> None:
        await self.db.execute(
            "INSERT INTO greet_rules(id, priority, enabled, match_json, greetings_json, updated_ts) "
            "VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "priority=excluded.priority, enabled=excluded.enabled, match_json=excluded.match_json, "
            "greetings_json=excluded.greetings_json, updated_ts=excluded.updated_ts",
            (rid, int(priority), 1 if enabled else 0, _j(match or {}), _j(greetings or []), _now()),
        )

    async def greet_import_from_legacy_file(self, path: Path) -> int:
        """
        Imports greetings.json into greet_rules if greet_rules is empty.
        Returns number of imported rules.
        """
        if await self.greet_rule_count() > 0:
            return 0
        if not path.exists():
            return 0

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return 0

        rules = data.get("rules") if isinstance(data, dict) else None
        if not isinstance(rules, list) or not rules:
            return 0

        imported = 0
        for r in rules:
            if not isinstance(r, dict):
                continue
            rid = str(r.get("id") or "").strip() or f"rule{imported+1}"
            pr = int(r.get("priority") or 0)
            match = r.get("match") if isinstance(r.get("match"), dict) else {}
            greets = r.get("greetings")
            if not isinstance(greets, list):
                greets = []
            greets = [str(x) for x in greets if str(x).strip()]
            await self.greet_upsert_rule(rid=rid, priority=pr, enabled=True, match=match, greetings=greets)
            imported += 1

        return imported

    # ---------------------------
    # Wiki watchlist
    # ---------------------------

    async def wiki_get_lang(self) -> str:
        row = await self.db.fetchone("SELECT v FROM wiki_settings WHERE k='lang'")
        return str(row[0]).strip().lower() if row and row[0] else "en"

    async def wiki_set_lang(self, lang: str) -> None:
        lang = (lang or "en").strip().lower()
        await self.db.execute(
            "INSERT INTO wiki_settings(k, v, updated_ts) VALUES('lang', ?, ?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_ts=excluded.updated_ts",
            (lang, _now()),
        )

    async def wiki_list_pages(self) -> list[str]:
        rows = await self.db.fetchall("SELECT title FROM wiki_watch ORDER BY title COLLATE NOCASE ASC")
        return [str(r[0]) for r in rows if r and r[0]]

    async def wiki_add_page(self, title: str) -> bool:
        title = (title or "").strip()
        if not title:
            return False
        try:
            await self.db.execute(
                "INSERT INTO wiki_watch(title, lang, created_ts) VALUES(?,?,?)",
                (title, await self.wiki_get_lang(), _now()),
            )
            return True
        except Exception:
            # likely UNIQUE violation
            return False

    async def wiki_del_page(self, title: str) -> bool:
        title = (title or "").strip()
        if not title:
            return False
        before = await self.db.fetchone("SELECT COUNT(*) FROM wiki_watch WHERE lower(title)=lower(?)", (title,))
        await self.db.execute("DELETE FROM wiki_watch WHERE lower(title)=lower(?)", (title,))
        after = await self.db.fetchone("SELECT COUNT(*) FROM wiki_watch WHERE lower(title)=lower(?)", (title,))
        b = int(before[0]) if before else 0
        a = int(after[0]) if after else 0
        return b > 0 and a == 0

    async def wiki_import_from_legacy_file(self, path: Path) -> int:
        """
        Imports wiki_watch.json into wiki tables if wiki_watch empty.
        """
        existing = await self.db.fetchone("SELECT COUNT(*) FROM wiki_watch")
        if existing and int(existing[0]) > 0:
            return 0
        if not path.exists():
            return 0

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return 0

        if isinstance(data, dict):
            lang = str(data.get("lang") or "en").strip().lower()
            pages = data.get("pages") or []
        else:
            return 0

        if not isinstance(pages, list):
            return 0

        await self.wiki_set_lang(lang)

        imported = 0
        for p in pages:
            t = str(p).strip()
            if not t:
                continue
            ok = await self.wiki_add_page(t)
            if ok:
                imported += 1
        return imported

    # ---------------------------
    # Weather watches
    # ---------------------------

    async def weather_get_lang(self) -> str:
        row = await self.db.fetchone("SELECT v FROM weather_settings WHERE k='lang'")
        return str(row[0]).strip().lower() if row and row[0] else "en"

    async def weather_set_lang(self, lang: str) -> None:
        lang = (lang or "en").strip().lower()
        await self.db.execute(
            "INSERT INTO weather_settings(k, v, updated_ts) VALUES('lang', ?, ?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_ts=excluded.updated_ts",
            (lang, _now()),
        )

    async def weather_list_watches(self) -> list[dict]:
        rows = await self.db.fetchall(
            "SELECT city, duration_hours, types_json, interval_minutes, created_ts, expires_ts "
            "FROM weather_watches ORDER BY city COLLATE NOCASE ASC"
        )
        out = []
        for (city, dur, types_json, interval, created, expires) in rows:
            out.append(
                {
                    "city": str(city),
                    "duration_hours": int(dur),
                    "types": _jload(types_json, []),
                    "interval_minutes": int(interval),
                    "created_ts": int(created),
                    "expires_ts": int(expires),
                }
            )
        return out

    async def weather_upsert_watch(self, *, city: str, duration_hours: int, types: list[str], interval_minutes: int) -> None:
        city = (city or "").strip()
        now = _now()
        expires = now + int(duration_hours) * 3600
        await self.db.execute(
            "INSERT INTO weather_watches(city, duration_hours, types_json, interval_minutes, created_ts, expires_ts) "
            "VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(city) DO UPDATE SET "
            "duration_hours=excluded.duration_hours, types_json=excluded.types_json, "
            "interval_minutes=excluded.interval_minutes, created_ts=excluded.created_ts, expires_ts=excluded.expires_ts",
            (city, int(duration_hours), _j(types or []), int(interval_minutes), now, expires),
        )

    async def weather_del_watch(self, city: str) -> bool:
        city = (city or "").strip()
        if not city:
            return False
        before = await self.db.fetchone("SELECT COUNT(*) FROM weather_watches WHERE lower(city)=lower(?)", (city,))
        await self.db.execute("DELETE FROM weather_watches WHERE lower(city)=lower(?)", (city,))
        b = int(before[0]) if before else 0
        return b > 0

    async def weather_import_from_legacy_file(self, path: Path) -> int:
        """
        Imports weather_watch.json into weather tables if weather_watches empty.
        """
        existing = await self.db.fetchone("SELECT COUNT(*) FROM weather_watches")
        if existing and int(existing[0]) > 0:
            return 0
        if not path.exists():
            return 0

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return 0

        if not isinstance(data, dict):
            return 0

        await self.weather_set_lang(str(data.get("lang") or "en").strip().lower())

        watches = data.get("watches") or []
        if not isinstance(watches, list):
            return 0

        imported = 0
        for w in watches:
            if not isinstance(w, dict):
                continue
            city = str(w.get("city") or "").strip()
            if not city:
                continue
            dur = int(w.get("duration_hours") or 0)
            if dur <= 0:
                continue
            types = w.get("types")
            if not isinstance(types, list):
                types = []
            types = [str(x) for x in types if str(x).strip()]
            interval = int(w.get("interval_minutes") or 15)
            await self.weather_upsert_watch(city=city, duration_hours=dur, types=types, interval_minutes=interval)
            imported += 1

        return imported

    # ---------------------------
    # ACL daily auth
    # ---------------------------

    async def acl_get_authed_until(self, identity_key: str) -> Optional[int]:
        identity_key = (identity_key or "").strip().lower()
        if not identity_key:
            return None
        row = await self.db.fetchone("SELECT authed_until_ts FROM acl_auth WHERE identity_key=?", (identity_key,))
        if not row:
            return None
        try:
            return int(row[0])
        except Exception:
            return None

    async def acl_set_auth(self, *, identity_key: str, role: str, authed_until_ts: int) -> None:
        identity_key = (identity_key or "").strip().lower()
        role = (role or "").strip().lower()
        now = _now()
        await self.db.execute(
            "INSERT INTO acl_auth(identity_key, role, authed_until_ts, authed_ts) VALUES(?,?,?,?) "
            "ON CONFLICT(identity_key) DO UPDATE SET role=excluded.role, authed_until_ts=excluded.authed_until_ts, authed_ts=excluded.authed_ts",
            (identity_key, role, int(authed_until_ts), now),
        )

    async def acl_prune_expired(self) -> None:
        now = _now()
        await self.db.execute("DELETE FROM acl_auth WHERE authed_until_ts < ?", (now,))

    # ---------------------------
    # News (RSS)
    # ---------------------------

    async def news_get_setting(self, k: str, default: str = "") -> str:
        k = (k or "").strip()
        if not k:
            return default
        row = await self.db.fetchone("SELECT v FROM news_settings WHERE k=?", (k,))
        return str(row[0]) if row and row[0] is not None else default

    async def news_set_setting(self, k: str, v: str) -> None:
        k = (k or "").strip()
        if not k:
            return
        await self.db.execute(
            "INSERT INTO news_settings(k, v, updated_ts) VALUES(?,?,?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_ts=excluded.updated_ts",
            (k, str(v), _now()),
        )

    async def news_get_int(self, k: str, default: int) -> int:
        try:
            return int(await self.news_get_setting(k, str(default)))
        except Exception:
            return int(default)

    async def news_get_float(self, k: str, default: float) -> float:
        try:
            return float(await self.news_get_setting(k, str(default)))
        except Exception:
            return float(default)

    async def news_list_sources(self, *, include_disabled: bool = True) -> list[dict]:
        if include_disabled:
            rows = await self.db.fetchall(
                "SELECT source_id, name, enabled, created_ts, updated_ts FROM news_sources ORDER BY source_id ASC"
            )
        else:
            rows = await self.db.fetchall(
                "SELECT source_id, name, enabled, created_ts, updated_ts FROM news_sources WHERE enabled=1 ORDER BY source_id ASC"
            )

        out: list[dict] = []
        for sid, name, enabled, created, updated in rows:
            cats = await self.db.fetchall(
                "SELECT category, url FROM news_source_categories WHERE source_id=? ORDER BY category ASC",
                (sid,),
            )
            out.append(
                {
                    "id": str(sid),
                    "name": str(name),
                    "enabled": bool(int(enabled)),
                    "categories": {str(c): str(u) for (c, u) in cats},
                    "created_ts": int(created),
                    "updated_ts": int(updated),
                }
            )
        return out

    async def news_upsert_source(self, *, source_id: str, name: str, categories: dict, enabled: bool = True) -> None:
        sid = (source_id or "").strip().lower()
        if not sid:
            return
        now = _now()
        await self.db.execute(
            "INSERT INTO news_sources(source_id, name, enabled, created_ts, updated_ts) VALUES(?,?,?,?,?) "
            "ON CONFLICT(source_id) DO UPDATE SET name=excluded.name, enabled=excluded.enabled, updated_ts=excluded.updated_ts",
            (sid, (name or sid).strip(), 1 if enabled else 0, now, now),
        )

        # Replace categories atomically
        await self.db.execute("DELETE FROM news_source_categories WHERE source_id=?", (sid,))
        rows: list[tuple] = []
        if isinstance(categories, dict):
            for cat, url in categories.items():
                c = str(cat).strip().lower()
                u = str(url).strip()
                if c and u:
                    rows.append((sid, c, u))
        if rows:
            await self.db.executemany(
                "INSERT INTO news_source_categories(source_id, category, url) VALUES(?,?,?)",
                rows,
            )

    async def news_delete_source(self, source_id: str) -> bool:
        sid = (source_id or "").strip().lower()
        if not sid:
            return False
        before = await self.db.fetchone("SELECT COUNT(*) FROM news_sources WHERE source_id=?", (sid,))
        await self.db.execute("DELETE FROM news_sources WHERE source_id=?", (sid,))
        return int(before[0]) > 0 if before else False

    async def news_get_last_posted(self, *, target: str, source_id: str, category: str, limit: int) -> Optional[int]:
        row = await self.db.fetchone(
            "SELECT posted_ts FROM news_last_posted WHERE target=? AND source_id=? AND category=? AND limit=?",
            ((target or "").strip(), (source_id or "").strip().lower(), (category or "").strip().lower(), int(limit)),
        )
        if not row:
            return None
        try:
            return int(row[0])
        except Exception:
            return None

    async def news_set_last_posted(self, *, target: str, source_id: str, category: str, limit: int, posted_ts: int) -> None:
        await self.db.execute(
            "INSERT INTO news_last_posted(target, source_id, category, limit, posted_ts) VALUES(?,?,?,?,?) "
            "ON CONFLICT(target, source_id, category, limit) DO UPDATE SET posted_ts=excluded.posted_ts",
            ((target or "").strip(), (source_id or "").strip().lower(), (category or "").strip().lower(), int(limit), int(posted_ts)),
        )

    async def news_import_from_legacy_config(self, news_cfg: dict) -> int:
        """Imports config.json news section into DB if there are no sources yet.

        Returns number of sources imported.
        """
        existing = await self.db.fetchone("SELECT COUNT(*) FROM news_sources")
        if existing and int(existing[0]) > 0:
            return 0

        if not isinstance(news_cfg, dict):
            return 0

        # settings
        defaults = {
            "default_limit": str(news_cfg.get("default_limit", 10)),
            "max_limit": str(news_cfg.get("max_limit", 10)),
            "cache_ttl_seconds": str(news_cfg.get("cache_ttl_seconds", 3600)),
            "cooldown_seconds": str(news_cfg.get("cooldown_seconds", 120)),
            "line_delay_seconds": str(news_cfg.get("line_delay_seconds", 1.2)),
            "selection_timeout_seconds": str(news_cfg.get("selection_timeout_seconds", 60)),
        }
        for k, v in defaults.items():
            await self.news_set_setting(k, v)

        imported = 0
        srcs = news_cfg.get("sources")
        if isinstance(srcs, list):
            for s in srcs:
                if not isinstance(s, dict):
                    continue
                sid = str(s.get("id") or "").strip().lower()
                name = str(s.get("name") or sid).strip()
                cats = s.get("categories") if isinstance(s.get("categories"), dict) else {}
                if not sid or not cats:
                    continue
                await self.news_upsert_source(source_id=sid, name=name, categories=cats, enabled=True)
                imported += 1

        return imported
