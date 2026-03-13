"""
Blizzard module for LeoBot.

Exposes World of Warcraft and Diablo III data via Blizzard APIs.
Commands follow: !<game> <entity> [action] [arguments]

WoW: !wow char|guild|realm|item|auction|pvp ...
D3:  !d3 profile|hero|item|leaderboard ...

Aliases: !wc, !wg, !wr, !wi, !wa, !wpvp (WoW) | !dp, !dh, !di, !dlb (D3)

API Reference (official Blizzard documentation):
- OAuth: https://develop.battle.net/documentation/guides/using-oauth/client-credentials-flow
- WoW Profile API: https://develop.battle.net/documentation/api-reference/world-of-warcraft-profile-api
- WoW Game Data API: https://develop.battle.net/documentation/api-reference/world-of-warcraft-game-data-api
- Diablo III: https://develop.battle.net/documentation/diablo-3
- Regionality: https://develop.battle.net/documentation/guides/regionality-and-apis
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

UA = "LeonidasIRCbot/2.0 (https://hairyoctopus.net; admin: Ged)"

log = logging.getLogger("leobot.blizzard")

# WoW quality IDs for display (numeric and string from API)
WOW_QUALITY = {
    0: "Poor", 1: "Common", 2: "Uncommon", 3: "Rare", 4: "Epic",
    5: "Legendary", 6: "Artifact", 7: "Heirloom", 8: "WoW Token",
    "POOR": "Poor", "COMMON": "Common", "UNCOMMON": "Uncommon",
    "RARE": "Rare", "EPIC": "Epic", "LEGENDARY": "Legendary",
    "ARTIFACT": "Artifact", "HEIRLOOM": "Heirloom",
}

# D3 item quality (displayColor from API)
D3_QUALITY = {
    0: "Normal", 1: "Magic", 2: "Rare", 3: "Legendary", 4: "Set",
    "gray": "Normal", "white": "Normal", "blue": "Magic", "yellow": "Rare",
    "orange": "Legendary", "green": "Set",
}

# PvP bracket slugs
PVP_BRACKETS = {"2v2": "2v2", "3v3": "3v3", "shuffle": "shuffle", "ratedbg": "rbg"}


def _norm_space(s: str) -> str:
    return " ".join((s or "").strip().split())


def _realm_slug(name: str) -> str:
    """Convert realm name to API slug (lowercase, spaces to hyphens)."""
    return (name or "").strip().lower().replace(" ", "-").replace("'", "")


def _battletag_slug(bt: str) -> str:
    """Convert Battletag to URL-safe format (Foo#1234 -> Foo-1234)."""
    return (bt or "").strip().replace("#", "-")


def _safe_get(obj: Any, key: str, default: Any = None) -> Any:
    """Get key from obj if it's a dict, else return default."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return default


def _safe_name(obj: Any) -> str:
    """Extract display name from API object (may be dict with 'name' or primitive)."""
    if isinstance(obj, dict):
        return str(obj.get("name") or obj.get("name_en_US") or "?")
    return str(obj) if obj is not None else "?"


def _safe_value(obj: Any) -> int | float:
    """Extract numeric value from API object (may be dict with 'value' or primitive)."""
    if isinstance(obj, dict):
        v = obj.get("value")
        if isinstance(v, (int, float)):
            return v
        return 0
    return int(obj) if isinstance(obj, (int, float)) else 0


def _http_get(
    url: str,
    *,
    headers: dict | None = None,
    timeout: int = 12,
) -> tuple[int, bytes]:
    h = dict(headers or {})
    h.setdefault("User-Agent", UA)
    h.setdefault("Accept", "application/json")
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def _http_post(
    url: str,
    data: bytes,
    *,
    headers: dict | None = None,
    timeout: int = 12,
) -> tuple[int, bytes]:
    h = dict(headers or {})
    h.setdefault("User-Agent", UA)
    h.setdefault("Content-Type", "application/x-www-form-urlencoded")
    req = urllib.request.Request(url, data=data, method="POST", headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


async def _run_sync(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))


@dataclass(slots=True)
class _Token:
    access_token: str
    expires_in: int
    token_type: str


class BlizzardClient:
    """Blizzard API client with OAuth token management."""

    def __init__(self, client_id: str, client_secret: str, region: str = "eu"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.region = (region or "eu").lower()
        self._token: _Token | None = None
        self._token_expires: float = 0
        self._lock = asyncio.Lock()

    def _oauth_url(self) -> str:
        # Official: https://oauth.battle.net/token (region-agnostic, per Blizzard docs)
        return "https://oauth.battle.net/token"

    def _api_base(self) -> str:
        # Per regionality guide: https://{region}.api.blizzard.com
        return f"https://{self.region}.api.blizzard.com"

    async def _ensure_token(self) -> str:
        async with self._lock:
            if self._token and time.time() < self._token_expires - 60:
                return self._token.access_token

            creds = base64.b64encode(
                f"{self.client_id}:{self.client_secret}".encode()
            ).decode()
            body = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode()
            headers = {"Authorization": f"Basic {creds}"}

            status, raw = await _run_sync(
                _http_post,
                self._oauth_url(),
                body,
                headers=headers,
                timeout=10,
            )
            if status != 200:
                raise RuntimeError(f"Blizzard OAuth failed: {status}")

            data = json.loads(raw.decode("utf-8", errors="replace"))
            self._token = _Token(
                access_token=str(data.get("access_token", "")),
                expires_in=int(data.get("expires_in", 86400)),
                token_type=str(data.get("token_type", "Bearer")),
            )
            self._token_expires = time.time() + self._token.expires_in
            return self._token.access_token

    async def get(self, path: str, params: dict | None = None) -> dict:
        """GET from Blizzard API with auth."""
        token = await self._ensure_token()
        base = self._api_base()
        url = f"{base}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        headers = {"Authorization": f"Bearer {token}"}
        status, raw = await _run_sync(_http_get, url, headers=headers, timeout=15)
        if status != 200:
            try:
                err = json.loads(raw.decode("utf-8", errors="replace"))
                detail = err.get("detail", str(raw[:200]))
            except Exception:
                detail = str(raw[:200])
            raise RuntimeError(f"Blizzard API error {status}: {detail}")
        return json.loads(raw.decode("utf-8", errors="replace"))

    async def get_optional(self, path: str, params: dict | None = None) -> tuple[int, dict | None]:
        """GET from Blizzard API; returns (status, data) without raising on non-200."""
        token = await self._ensure_token()
        base = self._api_base()
        url = f"{base}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        headers = {"Authorization": f"Bearer {token}"}
        status, raw = await _run_sync(_http_get, url, headers=headers, timeout=15)
        if status != 200:
            return status, None
        return status, json.loads(raw.decode("utf-8", errors="replace"))


class BlizzardService:
    """
    Blizzard game data service for WoW and Diablo III.

    Commands:
      WoW: !wow char|guild|realm|item|auction|pvp ...
      D3:  !d3 profile|hero|item|leaderboard ...
    """

    service_id = "blizzard"

    def __init__(self, cfg: dict):
        self.cfg = cfg or {}
        self.client_id = (self.cfg.get("client_id") or "").strip()
        self.client_secret = (self.cfg.get("client_secret") or "").strip()
        self.region = (self.cfg.get("region") or "eu").lower()
        self.locale = (self.cfg.get("locale") or "en_US").strip()
        # EU realm names are indexed under en_GB; US under en_US (Blizzard regionality)
        self._realm_name_locale = (
            "en_GB" if (self.region == "eu" and self.locale == "en_US") else self.locale
        ).replace("-", "_")
        self.cache_ttl = int(self.cfg.get("cache_ttl_seconds", 300))
        self.cooldown_s = int(self.cfg.get("cooldown_seconds", 3))
        self._cooldown: dict[tuple[str, str], float] = {}
        self._client: BlizzardClient | None = None

    def _client_or_none(self) -> BlizzardClient | None:
        if self.client_id and self.client_secret:
            if self._client is None:
                self._client = BlizzardClient(
                    self.client_id, self.client_secret, self.region
                )
            return self._client
        return None

    def _cooldown_ok(self, target: str, key: str) -> bool:
        now = time.time()
        k = (target, key)
        until = self._cooldown.get(k, 0)
        if now < until:
            return False
        self._cooldown[k] = now + self.cooldown_s
        return True

    async def _err(self, bot, ev, msg: str) -> None:
        await bot.privmsg(ev.target, f"{ev.nick}: {msg}")

    async def _no_config(self, bot, ev) -> None:
        await self._err(
            bot,
            ev,
            "Blizzard API not configured. Add client_id and client_secret to config.",
        )

    # -------------------------------------------------------------------------
    # WoW: Character
    # -------------------------------------------------------------------------
    async def _wow_char(self, bot, ev, realm: str, char: str, action: str = "") -> None:
        client = self._client_or_none()
        if not client:
            await self._no_config(bot, ev)
            return
        realm_slug = _realm_slug(realm)
        char_slug = (char or "").strip().lower()

        try:
            if action == "gear":
                path = f"/profile/wow/character/{realm_slug}/{char_slug}/equipment"
                data = await client.get(path, {"namespace": f"profile-{self.region}", "locale": self.locale})
                items = data.get("equipped_items", [])
                lines = []
                for it in items[:12]:
                    slot = _safe_name(it.get("slot"))
                    name = (it.get("name", "") or "?")
                    ilvl = _safe_value(it.get("level"))
                    qobj = it.get("quality")
                    qt = _safe_get(qobj, "type") if isinstance(qobj, dict) else qobj
                    qual = WOW_QUALITY.get(qt, "") if qt is not None else ""
                    lines.append(f"{slot}: {name}" + (f" (i{ilvl})" if ilvl else "") + (f" [{qual}]" if qual else ""))
                msg = f"WoW gear {char}@{realm_slug}: " + " | ".join(lines[:6])
                if len(lines) > 6:
                    msg += " | " + " | ".join(lines[6:12])
                await bot.privmsg(ev.target, msg[:400])
                return

            if action == "stats":
                path = f"/profile/wow/character/{realm_slug}/{char_slug}/statistics"
                data = await client.get(path, {"namespace": f"profile-{self.region}", "locale": self.locale})
                stats_data = data.get("character_statistics") or {}
                cats = stats_data.get("categories", []) if isinstance(stats_data, dict) else []
                parts = []
                for cat in cats[:3]:
                    subcats = cat.get("sub_categories", [])
                    for sc in subcats[:2]:
                        stats = sc.get("statistics", [])[:2]
                        for st in stats:
                            parts.append(f"{st.get('name','?')}: {st.get('value',0)}")
                msg = f"WoW stats {char}@{realm_slug}: " + " | ".join(parts[:8])
                await bot.privmsg(ev.target, msg)
                return

            if action == "profs":
                path = f"/profile/wow/character/{realm_slug}/{char_slug}/professions"
                data = await client.get(path, {"namespace": f"profile-{self.region}", "locale": self.locale})
                profs = []
                prof_data = data.get("professions") or {}
                for p in (prof_data.get("primaries", []) if isinstance(prof_data, dict) else [])[:5]:
                    profs.append(f"{_safe_name(p.get('profession'))} {p.get('skill_tier',0)}")
                msg = f"WoW profs {char}@{realm_slug}: " + ", ".join(profs) if profs else "No professions"
                await bot.privmsg(ev.target, msg)
                return

            if action == "reps":
                path = f"/profile/wow/character/{realm_slug}/{char_slug}/reputations"
                data = await client.get(path, {"namespace": f"profile-{self.region}", "locale": self.locale})
                reps = data.get("reputations", [])[:6]
                parts = [f"{_safe_name(r.get('faction'))}: {_safe_name(r.get('standing'))}" for r in reps]
                msg = f"WoW reps {char}@{realm_slug}: " + " | ".join(parts) if parts else "No reputations"
                await bot.privmsg(ev.target, msg)
                return

            if action == "mounts":
                path = f"/profile/wow/character/{realm_slug}/{char_slug}/collections/mounts"
                data = await client.get(path, {"namespace": f"profile-{self.region}", "locale": self.locale})
                total = len(data.get("mounts", []))
                msg = f"WoW mounts {char}@{realm_slug}: {total} mounts collected"
                await bot.privmsg(ev.target, msg)
                return

            if action == "pets":
                path = f"/profile/wow/character/{realm_slug}/{char_slug}/collections/pets"
                data = await client.get(path, {"namespace": f"profile-{self.region}", "locale": self.locale})
                total = len(data.get("pets", []))
                msg = f"WoW pets {char}@{realm_slug}: {total} pets collected"
                await bot.privmsg(ev.target, msg)
                return

            if action == "achieve":
                path = f"/profile/wow/character/{realm_slug}/{char_slug}/achievements/statistics"
                data = await client.get(path, {"namespace": f"profile-{self.region}", "locale": self.locale})
                cats = data.get("categories", [])[:2]
                parts = []
                for c in cats:
                    for s in (c.get("statistics", []) or [])[:2]:
                        parts.append(f"{s.get('name','?')}: {s.get('quantity',0)}")
                msg = f"WoW achieve {char}@{realm_slug}: " + " | ".join(parts[:6]) if parts else "No achievements"
                await bot.privmsg(ev.target, msg)
                return

            if action == "pvp":
                path = f"/profile/wow/character/{realm_slug}/{char_slug}/pvp-summary"
                data = await client.get(path, {"namespace": f"profile-{self.region}", "locale": self.locale})
                brackets = data.get("honorable_kills", 0)
                honor = data.get("honor_level", 0)
                msg = f"WoW PvP {char}@{realm_slug}: Honor {honor}, Honorable kills {brackets}"
                await bot.privmsg(ev.target, msg)
                return

            # Default: character summary
            path = f"/profile/wow/character/{realm_slug}/{char_slug}"
            data = await client.get(path, {"namespace": f"profile-{self.region}", "locale": self.locale})
            level = data.get("level", 0)
            cls = _safe_name(data.get("character_class"))
            race = _safe_name(data.get("race"))
            faction = _safe_name(data.get("faction"))
            ach_pts = data.get("achievement_points", 0)
            eq_ilvl = _safe_value(data.get("equipped_item_level"))
            last_login = data.get("last_login_timestamp")
            last_str = ""
            if last_login:
                try:
                    from datetime import datetime
                    ts = int(last_login) / 1000
                    last_str = f", last login {datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')}"
                except Exception:
                    pass
            msg = f"WoW {char}@{realm_slug}: L{level} {cls} {race} ({faction}) | iLvl {eq_ilvl} | {ach_pts} achieve pts{last_str}"
            await bot.privmsg(ev.target, msg)

        except RuntimeError as e:
            await self._err(bot, ev, str(e))
        except Exception as e:
            log.exception("WoW char")
            await self._err(bot, ev, f"API error: {e}")

    # -------------------------------------------------------------------------
    # WoW: Guild
    # -------------------------------------------------------------------------
    async def _wow_guild(self, bot, ev, realm: str, guild: str, action: str = "") -> None:
        client = self._client_or_none()
        if not client:
            await self._no_config(bot, ev)
            return
        realm_slug = _realm_slug(realm)
        guild_slug = _realm_slug(guild)

        try:
            if action == "roster":
                path = f"/profile/wow/guild/{realm_slug}/{guild_slug}/roster"
                data = await client.get(path, {"namespace": f"profile-{self.region}", "locale": self.locale})
                members = data.get("members", [])[:10]
                parts = []
                for m in members:
                    char_obj = m.get("character")
                    char = _safe_name(char_obj) if isinstance(char_obj, dict) else "?"
                    level = char_obj.get("level", 0) if isinstance(char_obj, dict) else 0
                    parts.append(f"{char}(L{level})")
                msg = f"WoW guild {guild}@{realm_slug} roster (top 10): " + ", ".join(parts) if parts else "No members"
                await bot.privmsg(ev.target, msg)
                return

            if action == "achieve":
                path = f"/profile/wow/guild/{realm_slug}/{guild_slug}/achievements"
                data = await client.get(path, {"namespace": f"profile-{self.region}", "locale": self.locale})
                comp = data.get("achievements", [])[:5]
                parts = [str(c.get("completed_timestamp", "?"))[:10] for c in comp]
                msg = f"WoW guild {guild}@{realm_slug} recent achieves: " + ", ".join(parts) if parts else "None"
                await bot.privmsg(ev.target, msg)
                return

            # Default: guild summary
            path = f"/profile/wow/guild/{realm_slug}/{guild_slug}"
            data = await client.get(path, {"namespace": f"profile-{self.region}", "locale": self.locale})
            faction = _safe_name(data.get("faction"))
            realm_name = _safe_name(data.get("realm"))
            members = data.get("member_count", 0)
            ach_pts = data.get("achievement_points", 0)
            msg = f"WoW guild {guild}@{realm_name}: {faction} | {members} members | {ach_pts} achieve pts"
            await bot.privmsg(ev.target, msg)

        except RuntimeError as e:
            await self._err(bot, ev, str(e))
        except Exception as e:
            log.exception("WoW guild")
            await self._err(bot, ev, f"API error: {e}")

    # -------------------------------------------------------------------------
    # WoW: Realm resolution (shared by realm display and auction)
    # Per Blizzard docs: https://develop.battle.net/documentation/world-of-warcraft/guides/search
    # Use Search API: GET /data/wow/search/connected-realm?realms.name.<locale>=<name>
    # For EU, realm names are indexed under en_GB (not en_US). Respect region+locale.
    # -------------------------------------------------------------------------
    def _realm_search_locale(self) -> str:
        """Locale for realm name search. EU uses en_GB; US uses en_US."""
        if self.region == "eu" and self.locale == "en_US":
            return "en_GB"
        return self.locale

    async def _resolve_realm(
        self, client, realm: str, *, debug: bool = False
    ) -> tuple[dict | None, str | None, str | None]:
        """
        Resolve realm name/slug to (connected_realm_data, connected_realm_id).
        Returns (None, None, debug_msg) on failure; debug_msg is set when debug=True.
        """
        realm_display = _norm_space(realm).strip()
        realm_slug = _realm_slug(realm)
        if not realm_display and not realm_slug:
            return None, None, "debug: empty realm input" if debug else None

        search_locale = self._realm_search_locale()
        name_param = f"realms.name.{search_locale}"
        search_terms = list(dict.fromkeys([realm_display, realm_slug]))
        search_terms = [t for t in search_terms if t]
        debug_parts: list[str] = []

        for term in search_terms:
            params = {
                "namespace": f"dynamic-{self.region}",
                "locale": self.locale,
                name_param: term,
            }
            status, search_data = await client.get_optional(
                "/data/wow/search/connected-realm", params
            )
            if debug:
                debug_parts.append(f"search {name_param}={term!r} -> status={status}")
                if search_data is not None:
                    top_keys = list(search_data.keys())[:10] if isinstance(search_data, dict) else []
                    debug_parts.append(f"response keys={top_keys}")
                    results = search_data.get("results", []) if isinstance(search_data, dict) else []
                    debug_parts.append(f"results count={len(results)}")
                    if results and isinstance(results[0], dict):
                        debug_parts.append(f"first result keys={list(results[0].keys())[:10]}")
            if status == 200 and search_data:
                break
        else:
            return None, None, (" | ".join(debug_parts) if debug else None)

        results = search_data.get("results", [])
        if not results:
            return None, None, (" | ".join(debug_parts) + f" | results empty" if debug else None)

        first = results[0]
        conn_id: str | None = None

        key = first.get("key") if isinstance(first, dict) else None
        if isinstance(key, dict):
            href = key.get("href", "")
            if href:
                conn_id = href.rstrip("/").split("/")[-1]
        if not conn_id and isinstance(first, dict):
            data = first.get("data", {})
            if isinstance(data, dict):
                conn_id = str(data.get("id", "")) if data.get("id") is not None else None

        if debug:
            debug_parts.append(f"extracted conn_id={conn_id!r}")

        if not conn_id or not conn_id.isdigit():
            return None, None, (" | ".join(debug_parts) if debug else None)

        fetch_params = {"namespace": f"dynamic-{self.region}", "locale": self.locale}
        status2, conn_data = await client.get_optional(
            f"/data/wow/connected-realm/{conn_id}", fetch_params
        )
        if debug and not conn_data:
            debug_parts.append(f"connected-realm/{conn_id} -> status={status2}")

        if not conn_data:
            return None, None, (" | ".join(debug_parts) if debug else None)

        return (conn_data, conn_id, None)

    # -------------------------------------------------------------------------
    # WoW: Realm
    # -------------------------------------------------------------------------
    async def _wow_realm(self, bot, ev, args: list[str]) -> None:
        client = self._client_or_none()
        if not client:
            await self._no_config(bot, ev)
            return

        try:
            if args and args[0].lower() == "list":
                path = "/data/wow/realm/index"
                data = await client.get(path, {"namespace": f"dynamic-{self.region}", "locale": self.locale})
                realms = data.get("realms", [])[:15]
                names = [_safe_name(r.get("name")) for r in realms]
                msg = f"WoW realms ({self.region}): " + ", ".join(names)
                if len(data.get("realms", [])) > 15:
                    msg += f" (+{len(data['realms'])-15} more)"
                await bot.privmsg(ev.target, msg)
                return

            realm = _norm_space(" ".join(args)) if args else ""
            if not realm:
                await self._err(bot, ev, "Usage: !wow realm <realm> | !wow realm list")
                return

            data, conn_id, debug_msg = await self._resolve_realm(client, realm, debug=True)
            if not data:
                msg = f"Realm '{realm}' not found"
                if debug_msg:
                    msg += f" | {debug_msg[:280]}"
                await self._err(bot, ev, msg)
                return

            # Connected realm has "realms" array; status/population/region may be on each realm
            realms_in = data.get("realms", [])
            if realms_in:
                name = ", ".join(_safe_name(r.get("name")) for r in realms_in[:3])
                pop = _safe_name(data.get("population"))
                status_obj = data.get("status")
                region = _safe_name(data.get("region"))
                for r in realms_in:
                    if not isinstance(r, dict):
                        continue
                    if not pop or pop == "?":
                        pop = _safe_name(r.get("population"))
                    if status_obj is None:
                        status_obj = r.get("status")
                    if not region or region == "?":
                        region = _safe_name(r.get("region"))
            else:
                name = data.get("name", "?")
                if isinstance(name, dict):
                    name = _safe_name(name)
                pop = _safe_name(data.get("population"))
                status_obj = data.get("status")
                region = _safe_name(data.get("region"))
            # Status: {type: "UP"}, {name: "UP"}, or boolean
            if isinstance(status_obj, dict):
                stype = status_obj.get("type") or status_obj.get("name")
            else:
                stype = status_obj
            status_str = "Online" if (
                stype is True or (stype and str(stype).upper() == "UP")
            ) else "Offline"
            msg = f"WoW realm {name}: {region} | {pop} | {status_str}"
            await bot.privmsg(ev.target, msg)

        except RuntimeError as e:
            await self._err(bot, ev, str(e))
        except Exception as e:
            log.exception("WoW realm")
            await self._err(bot, ev, f"API error: {e}")

    # -------------------------------------------------------------------------
    # WoW: Item
    # -------------------------------------------------------------------------
    async def _wow_item(self, bot, ev, args: list[str]) -> None:
        client = self._client_or_none()
        if not client:
            await self._no_config(bot, ev)
            return

        try:
            if not args:
                await self._err(bot, ev, "Usage: !wow item <name> | !wow item id <id> | !wow item search <text>")
                return

            if args[0].lower() == "id" and len(args) >= 2:
                item_id = args[1]
                if not item_id.isdigit():
                    await self._err(bot, ev, "Item ID must be numeric")
                    return
                path = f"/data/wow/item/{item_id}"
                data = await client.get(path, {"namespace": f"static-{self.region}", "locale": self.locale})
                name = data.get("name", "?")
                ilvl = data.get("level", 0) or data.get("item_level", 0)
                slot = (data.get("inventory_type", {}) or {}).get("name", "?")
                qobj = data.get("quality") or {}
                qual = WOW_QUALITY.get(qobj.get("type", qobj), "?")
                itype = (data.get("item_subclass", {}) or data.get("item_class", {}) or {}).get("name", "?")
                msg = f"WoW item: {name} | iLvl {ilvl} | {slot} | {qual} | {itype}"
                await bot.privmsg(ev.target, msg)
                return

            if args[0].lower() == "search" and len(args) >= 2:
                query = _norm_space(" ".join(args[1:]))
                path = "/data/wow/search/item"
                params = {
                    "namespace": f"static-{self.region}",
                    "locale": self.locale,
                    "name.en_US": query,
                    "orderby": "id",
                    "_page": 1,
                }
                data = await client.get(path, params)
                results = data.get("results", [])[:8]
                if not results:
                    await self._err(bot, ev, f"No items found for '{query}'")
                    return
                parts = []
                for r in results:
                    item = r.get("data", r)
                    name = item.get("name", "?")
                    iid = item.get("id", "?")
                    parts.append(f"{name} (id:{iid})")
                msg = f"WoW item search '{query}': " + " | ".join(parts)
                await bot.privmsg(ev.target, msg[:400])
                return

            # By name - search first
            query = _norm_space(" ".join(args))
            path = "/data/wow/search/item"
            params = {
                "namespace": f"static-{self.region}",
                "locale": self.locale,
                "name.en_US": query,
                "orderby": "id",
                "_page": 1,
            }
            data = await client.get(path, params)
            results = data.get("results", [])
            if not results:
                await self._err(bot, ev, f"No items found for '{query}'")
                return
            if len(results) > 1:
                parts = [f"{r.get('data',r).get('name','?')} (id:{r.get('data',r).get('id','?')})" for r in results[:6]]
                msg = f"WoW item '{query}' - multiple matches: " + " | ".join(parts)
                await bot.privmsg(ev.target, msg)
                return
            item = results[0].get("data", results[0])
            item_id = item.get("id")
            path = f"/data/wow/item/{item_id}"
            data = await client.get(path, {"namespace": f"static-{self.region}", "locale": self.locale})
            name = data.get("name", "?")
            ilvl = data.get("level", 0) or data.get("item_level", 0)
            slot = (data.get("inventory_type", {}) or {}).get("name", "?")
            qobj = data.get("quality") or {}
            qual = WOW_QUALITY.get(qobj.get("type", qobj), "?")
            itype = (data.get("item_subclass", {}) or data.get("item_class", {}) or {}).get("name", "?")
            msg = f"WoW item: {name} | iLvl {ilvl} | {slot} | {qual} | {itype}"
            await bot.privmsg(ev.target, msg)

        except RuntimeError as e:
            await self._err(bot, ev, str(e))
        except Exception as e:
            log.exception("WoW item")
            await self._err(bot, ev, f"API error: {e}")

    # -------------------------------------------------------------------------
    # WoW: Auction
    # -------------------------------------------------------------------------
    async def _wow_auction(self, bot, ev, realm: str, args: list[str]) -> None:
        client = self._client_or_none()
        if not client:
            await self._no_config(bot, ev)
            return

        try:
            _, conn_id, debug_msg = await self._resolve_realm(client, realm, debug=True)
            if not conn_id:
                msg = f"Realm '{realm}' not found or could not resolve connected realm"
                if debug_msg:
                    msg += f" | {debug_msg[:280]}"
                await self._err(bot, ev, msg)
                return

            # Auction API: returns auction houses; each has files with URLs to auction data
            path = f"/data/wow/connected-realm/{conn_id}/auctions"
            data = await client.get(path, {"namespace": f"dynamic-{self.region}", "locale": self.locale})
            files = data.get("files", [])
            if not files:
                await self._err(bot, ev, "No auction data available for realm")
                return
            file_url = files[0].get("url")
            if not file_url:
                await self._err(bot, ev, "Auction data URL not found")
                return
            token = await client._ensure_token()
            status, raw = await _run_sync(
                _http_get,
                file_url,
                headers={"Authorization": f"Bearer {token}", "User-Agent": UA, "Accept": "application/json"},
                timeout=45,
            )
            if status != 200:
                await self._err(bot, ev, f"Failed to fetch auction data: {status}")
                return
            auction_data = json.loads(raw.decode("utf-8", errors="replace"))
            listings = auction_data.get("auctions", [])

            if args and args[0].lower() == "item" and len(args) >= 2 and args[1].isdigit():
                item_id = int(args[1])
                matches = [a for a in listings if (a.get("item", {}) or {}).get("id") == item_id]
            else:
                item_name = _norm_space(" ".join(args)).lower() if args else ""
                if not item_name:
                    await self._err(bot, ev, "Usage: !wow auction <realm> <item name> | !wow auction item <realm> <id>")
                    return
                # Auctions have item.id, not name - resolve name to ID via search
                try:
                    search_path = "/data/wow/search/item"
                    search_data = await client.get(search_path, {
                        "namespace": f"static-{self.region}",
                        "locale": self.locale,
                        "name.en_US": item_name,
                        "_page": 1,
                    })
                    results = search_data.get("results", [])
                    if not results:
                        await self._err(bot, ev, f"No items found for '{item_name}'")
                        return
                    item_id = results[0].get("data", results[0]).get("id")
                    matches = [a for a in listings if (a.get("item", {}) or {}).get("id") == item_id]
                except Exception:
                    matches = []

            if not matches:
                await self._err(bot, ev, "No auctions found")
                return

            buyouts = [a.get("unit_price", 0) or a.get("buyout", 0) for a in matches if a.get("unit_price") or a.get("buyout")]
            qty = sum(a.get("quantity", 1) for a in matches)
            if buyouts:
                min_buy = min(buyouts)
                avg_buy = sum(buyouts) // len(buyouts)
                gold_min = min_buy / 10000
                gold_avg = avg_buy / 10000
                msg = f"WoW auction {_realm_slug(realm)}: {qty} available | min {gold_min:.1f}g | avg {gold_avg:.1f}g"
            else:
                msg = f"WoW auction {_realm_slug(realm)}: {qty} available (no buyout data)"
            await bot.privmsg(ev.target, msg)

        except RuntimeError as e:
            await self._err(bot, ev, str(e))
        except Exception as e:
            log.exception("WoW auction")
            await self._err(bot, ev, f"API error: {e}")

    # -------------------------------------------------------------------------
    # WoW: PvP
    # -------------------------------------------------------------------------
    async def _wow_pvp(self, bot, ev, args: list[str]) -> None:
        client = self._client_or_none()
        if not client:
            await self._no_config(bot, ev)
            return

        try:
            if not args:
                await self._err(bot, ev, "Usage: !wow pvp <realm> <char> | !wow pvp ladder <bracket> | !wow pvp rank <bracket> <char>")
                return

            if args[0].lower() == "ladder" and len(args) >= 2:
                bracket = args[1].lower()
                slug = PVP_BRACKETS.get(bracket, bracket)
                # PvP season index: https://develop.battle.net/documentation/api-reference/world-of-warcraft-game-data-api
                path = "/data/wow/pvp-season/index"
                seasons = await client.get(path, {"namespace": f"dynamic-{self.region}", "locale": self.locale})
                curr = seasons.get("current_season") or {}
                season_id = curr.get("id") if isinstance(curr, dict) else None
                if not season_id and isinstance(curr, dict):
                    href = (curr.get("key") or {}).get("href", "") if isinstance(curr.get("key"), dict) else curr.get("href", "")
                    if href:
                        season_id = href.rstrip("/").split("/")[-1]
                if not season_id:
                    await self._err(bot, ev, "Could not get current PvP season")
                    return
                path = f"/data/wow/pvp-season/{season_id}/pvp-leaderboard/{slug}"
                data = await client.get(path, {"namespace": f"dynamic-{self.region}", "locale": self.locale})
                entries = data.get("entries", [])[:5]
                parts = []
                for e in entries:
                    char = (e.get("character", {}) or {}).get("name", "?")
                    rating = e.get("rating", 0)
                    rank = e.get("rank", 0)
                    parts.append(f"#{rank} {char} ({rating})")
                msg = f"WoW PvP {slug} ladder: " + " | ".join(parts) if parts else "No entries"
                await bot.privmsg(ev.target, msg)
                return

            if args[0].lower() == "rank" and len(args) >= 3:
                bracket = args[1].lower()
                char_name = args[2]
                slug = PVP_BRACKETS.get(bracket, bracket)
                # PvP season index: https://develop.battle.net/documentation/api-reference/world-of-warcraft-game-data-api
                path = "/data/wow/pvp-season/index"
                seasons = await client.get(path, {"namespace": f"dynamic-{self.region}", "locale": self.locale})
                curr = seasons.get("current_season") or {}
                season_id = curr.get("id") if isinstance(curr, dict) else None
                if not season_id and isinstance(curr, dict):
                    href = (curr.get("key") or {}).get("href", "") if isinstance(curr.get("key"), dict) else curr.get("href", "")
                    if href:
                        season_id = href.rstrip("/").split("/")[-1]
                if not season_id:
                    await self._err(bot, ev, "Could not get current PvP season")
                    return
                path = f"/data/wow/pvp-season/{season_id}/pvp-leaderboard/{slug}"
                data = await client.get(path, {"namespace": f"dynamic-{self.region}", "locale": self.locale})
                entries = data.get("entries", [])
                char_lower = char_name.lower()
                for e in entries:
                    c = (e.get("character", {}) or {})
                    if (c.get("name") or "").lower() == char_lower:
                        msg = f"WoW PvP {slug}: {char_name} rank #{e.get('rank',0)} rating {e.get('rating',0)}"
                        await bot.privmsg(ev.target, msg)
                        return
                await self._err(bot, ev, f"{char_name} not found in {slug} ladder")
                return

            # Character PvP summary
            if len(args) >= 2:
                realm_slug = _realm_slug(args[0])
                char_slug = args[1].lower()
                path = f"/profile/wow/character/{realm_slug}/{char_slug}/pvp-summary"
                data = await client.get(path, {"namespace": f"profile-{self.region}", "locale": self.locale})
                honor = data.get("honor_level", 0)
                kills = data.get("honorable_kills", 0)
                msg = f"WoW PvP {char_slug}@{realm_slug}: Honor {honor}, Honorable kills {kills}"
                await bot.privmsg(ev.target, msg)
            else:
                await self._err(bot, ev, "Usage: !wow pvp <realm> <char> | !wow pvp ladder <bracket> | !wow pvp rank <bracket> <char>")

        except RuntimeError as e:
            await self._err(bot, ev, str(e))
        except Exception as e:
            log.exception("WoW pvp")
            await self._err(bot, ev, f"API error: {e}")

    # -------------------------------------------------------------------------
    # D3: Profile
    # -------------------------------------------------------------------------
    async def _d3_profile(self, bot, ev, battletag: str, action: str = "") -> None:
        client = self._client_or_none()
        if not client:
            await self._no_config(bot, ev)
            return
        bt_slug = _battletag_slug(battletag)

        try:
            if action == "heroes":
                path = f"/d3/profile/{bt_slug}/"
                data = await client.get(path, {"locale": self.locale})
                heroes = data.get("heroes", [])[:10]
                parts = []
                for h in heroes:
                    name = h.get("name", "?")
                    cls = h.get("class", "?")
                    lvl = h.get("level", 0)
                    hardcore = "HC" if h.get("hardcore") else ""
                    seasonal = "S" if h.get("seasonal") else ""
                    parts.append(f"{name} {cls} L{lvl} {hardcore}{seasonal}".strip())
                msg = f"D3 profile {battletag} heroes: " + " | ".join(parts) if parts else "No heroes"
                await bot.privmsg(ev.target, msg)
                return

            if action == "seasonal":
                path = f"/d3/profile/{bt_slug}/"
                data = await client.get(path, {"locale": self.locale})
                paragon = data.get("paragonLevel", 0)
                seasonal_paragon = data.get("paragonLevelSeason", 0)
                season = data.get("seasonalProfiles", {})
                msg = f"D3 profile {battletag} seasonal: paragon {paragon} (seasonal {seasonal_paragon})"
                await bot.privmsg(ev.target, msg)
                return

            # Default: profile summary
            path = f"/d3/profile/{bt_slug}/"
            data = await client.get(path, {"locale": self.locale})
            paragon = data.get("paragonLevel", 0)
            seasonal_paragon = data.get("paragonLevelSeason", 0)
            heroes = data.get("heroes", [])
            hc = sum(1 for h in heroes if h.get("hardcore"))
            msg = f"D3 profile {battletag}: paragon {paragon} (seasonal {seasonal_paragon}) | {len(heroes)} heroes ({hc} HC)"
            await bot.privmsg(ev.target, msg)

        except RuntimeError as e:
            await self._err(bot, ev, str(e))
        except Exception as e:
            log.exception("D3 profile")
            await self._err(bot, ev, f"API error: {e}")

    # -------------------------------------------------------------------------
    # D3: Hero
    # -------------------------------------------------------------------------
    async def _d3_hero(self, bot, ev, battletag: str, hero_id: str, action: str = "") -> None:
        client = self._client_or_none()
        if not client:
            await self._no_config(bot, ev)
            return
        bt_slug = _battletag_slug(battletag)

        try:
            path = f"/d3/profile/{bt_slug}/hero/{hero_id}"
            data = await client.get(path, {"locale": self.locale})

            if action == "gear":
                items = data.get("items", {})
                parts = []
                for slot, it in list(items.items())[:8]:
                    if isinstance(it, dict) and it.get("name"):
                        name = it.get("name", "?")
                        qual = D3_QUALITY.get(it.get("displayColor", ""), "")
                        parts.append(f"{slot}: {name} [{qual}]")
                msg = f"D3 hero {hero_id} gear: " + " | ".join(parts) if parts else "No gear"
                await bot.privmsg(ev.target, msg)
                return

            if action == "stats":
                stats = data.get("stats", {})
                life = stats.get("life", 0)
                dmg = stats.get("damage", 0)
                tough = stats.get("toughness", 0)
                msg = f"D3 hero {hero_id} stats: life {life} | damage {dmg} | toughness {tough}"
                await bot.privmsg(ev.target, msg)
                return

            if action == "skills":
                skills = data.get("skills", {})
                active = skills.get("active", [])[:6]
                passive = skills.get("passive", [])[:4]
                a_parts = [s.get("skill", {}).get("name", "?") for s in active if s.get("skill")]
                p_parts = [s.get("skill", {}).get("name", "?") for s in passive if s.get("skill")]
                msg = f"D3 hero {hero_id} skills: active " + ", ".join(a_parts) + " | passive " + ", ".join(p_parts)
                await bot.privmsg(ev.target, msg[:400])
                return

            # Default: hero summary
            name = data.get("name", "?")
            cls = data.get("class", "?")
            level = data.get("level", 0)
            paragon = data.get("paragonLevel", 0)
            hc = "HC " if data.get("hardcore") else ""
            seasonal = "Seasonal " if data.get("seasonal") else ""
            stats = data.get("stats", {})
            dmg = stats.get("damage", 0)
            tough = stats.get("toughness", 0)
            rec = stats.get("recovery", 0)
            msg = f"D3 hero {name}: {hc}{seasonal}{cls} L{level} P{paragon} | dmg {dmg} tough {tough} rec {rec}"
            await bot.privmsg(ev.target, msg)

        except RuntimeError as e:
            await self._err(bot, ev, str(e))
        except Exception as e:
            log.exception("D3 hero")
            await self._err(bot, ev, f"API error: {e}")

    # -------------------------------------------------------------------------
    # D3: Item
    # -------------------------------------------------------------------------
    async def _d3_item(self, bot, ev, args: list[str]) -> None:
        client = self._client_or_none()
        if not client:
            await self._no_config(bot, ev)
            return

        try:
            if not args:
                await self._err(bot, ev, "Usage: !d3 item <name> | !d3 item id <id>")
                return

            if args[0].lower() == "id" and len(args) >= 2:
                item_id = args[1]
                if not item_id.replace("-", "").isdigit():
                    await self._err(bot, ev, "Item ID must be numeric")
                    return
                path = f"/d3/data/item/{item_id}"
                data = await client.get(path, {"locale": self.locale})
                name = data.get("name", "?")
                itype = data.get("typeName", "?")
                qual = D3_QUALITY.get(data.get("displayColor", ""), "?")
                effect = ""
                if data.get("randomAffixes"):
                    ra = data.get("randomAffixes", [])
                    if ra and isinstance(ra[0], dict):
                        effect = ra[0].get("oneOf", [{}])[0].get("text", "")[:80] if ra[0].get("oneOf") else ""
                    elif data.get("attributes", {}).get("primary"):
                        effect = str(data["attributes"]["primary"][0].get("text", ""))[:80]
                msg = f"D3 item: {name} | {itype} | {qual}"
                if effect:
                    msg += f" | {effect}"
                await bot.privmsg(ev.target, msg[:400])
                return

            # By name - D3 doesn't have item search, so we'd need to use a different approach
            # For now, require id for exact lookup
            await self._err(bot, ev, "Use !d3 item id <id> for item lookup. Name search not available.")

        except RuntimeError as e:
            await self._err(bot, ev, str(e))
        except Exception as e:
            log.exception("D3 item")
            await self._err(bot, ev, f"API error: {e}")

    # -------------------------------------------------------------------------
    # D3: Leaderboard
    # -------------------------------------------------------------------------
    async def _d3_leaderboard(self, bot, ev, args: list[str]) -> None:
        client = self._client_or_none()
        if not client:
            await self._no_config(bot, ev)
            return

        try:
            if not args or len(args) < 4:
                await self._err(bot, ev, "Usage: !d3 leaderboard <region> <season> <class> <mode> | !d3 leaderboard rank <region> <season> <class> <rank>")
                return

            if args[0].lower() == "rank" and len(args) >= 5:
                region = args[1].lower()
                season = args[2]
                cls = args[3].lower()
                rank = args[4]
                if not rank.isdigit():
                    await self._err(bot, ev, "Rank must be numeric")
                    return
                lb_id = f"rift-{cls}"
                path = f"/data/d3/season/{season}/leaderboard/{lb_id}"
                data = await client.get(path, {"locale": self.locale})
                entries = data.get("row", [])
                r = int(rank)
                if 0 < r <= len(entries):
                    row = entries[r - 1]
                    player = row.get("player", [{}])[0] if row.get("player") else {}
                    name = (player.get("data", [{}])[0].get("string", "?") if isinstance(player.get("data"), list) else "?")
                    msg = f"D3 leaderboard #{r}: {name}"
                    await bot.privmsg(ev.target, msg)
                else:
                    await self._err(bot, ev, f"Rank {rank} not found")
                return

            region = args[0].lower()
            season = args[1]
            cls = args[2].lower()
            mode = args[3].lower() if len(args) > 3 else "solo"
            lb_id = f"rift-{cls}" if mode == "solo" else f"rift-{mode}-{cls}"
            path = f"/data/d3/season/{season}/leaderboard/{lb_id}"
            data = await client.get(path, {"locale": self.locale})
            entries = data.get("row", [])[:5]
            parts = []
            for i, row in enumerate(entries):
                player = row.get("player", [{}])[0] if row.get("player") else {}
                name = (player.get("data", [{}])[0].get("string", "?") if isinstance(player.get("data"), list) else "?")
                parts.append(f"#{i+1} {name}")
            msg = f"D3 leaderboard {cls} {region} S{season}: " + " | ".join(parts) if parts else "No entries"
            await bot.privmsg(ev.target, msg)

        except RuntimeError as e:
            await self._err(bot, ev, str(e))
        except Exception as e:
            log.exception("D3 leaderboard")
            await self._err(bot, ev, f"API error: {e}")

    # -------------------------------------------------------------------------
    # Command routing
    # -------------------------------------------------------------------------
    def _resolve_alias(self, cmd: str) -> tuple[str, list[str]]:
        """Resolve alias to full command. Returns (game_entity, remaining_args)."""
        aliases = {
            "wc": ("wow", "char"),
            "wg": ("wow", "guild"),
            "wr": ("wow", "realm"),
            "wi": ("wow", "item"),
            "wa": ("wow", "auction"),
            "wpvp": ("wow", "pvp"),
            "dp": ("d3", "profile"),
            "dh": ("d3", "hero"),
            "di": ("d3", "item"),
            "dlb": ("d3", "leaderboard"),
        }
        if cmd in aliases:
            g, e = aliases[cmd]
            return f"{g} {e}", []
        return "", []

    async def on_privmsg(self, bot, ev) -> None:
        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()
        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return

        parts = cmdline.split()
        cmd = parts[0].lower()

        # Check if it's an alias
        alias_expanded, _ = self._resolve_alias(cmd)
        if alias_expanded:
            # Expand: !wc argent-dawn Thrall -> wow char argent-dawn Thrall
            parts = alias_expanded.split() + parts[1:]
            cmd = parts[0].lower() if parts else ""

        if cmd not in ("wow", "d3"):
            return

        if not ev.is_private and not self._cooldown_ok(ev.target, "blizzard"):
            await bot.privmsg(ev.target, f"{ev.nick}: slow down.")
            return

        rest = parts[1:]

        if cmd == "wow":
            if not rest:
                await self._err(bot, ev, "Usage: !wow char|guild|realm|item|auction|pvp ...")
                return
            entity = rest[0].lower()
            args = rest[1:]

            if entity == "char":
                if len(args) >= 2:
                    actions = ("gear", "stats", "profs", "reps", "mounts", "pets", "achieve", "pvp")
                    action = args[0].lower() if args[0].lower() in actions else ""
                    if action and len(args) >= 3:
                        await self._wow_char(bot, ev, args[1], args[2], action)
                    elif not action:
                        await self._wow_char(bot, ev, args[0], args[1])
                    else:
                        await self._err(bot, ev, f"Usage: !wow char {action} <realm> <character>")
                else:
                    await self._err(bot, ev, "Usage: !wow char <realm> <character> [gear|stats|profs|reps|mounts|pets|achieve|pvp]")
                return

            if entity == "guild":
                if len(args) >= 2:
                    action = args[0].lower() if args[0].lower() in ("roster", "achieve") else ""
                    if action and len(args) >= 3:
                        await self._wow_guild(bot, ev, args[1], args[2], action)
                    elif not action:
                        await self._wow_guild(bot, ev, args[0], args[1])
                    else:
                        await self._err(bot, ev, f"Usage: !wow guild {action} <realm> <guild>")
                else:
                    await self._err(bot, ev, "Usage: !wow guild <realm> <guild> [roster|achieve]")
                return

            if entity == "realm":
                await self._wow_realm(bot, ev, args)
                return

            if entity == "item":
                await self._wow_item(bot, ev, args)
                return

            if entity == "auction":
                if len(args) >= 1:
                    await self._wow_auction(bot, ev, args[0], args[1:])
                else:
                    await self._err(bot, ev, "Usage: !wow auction <realm> <item name> | !wow auction item <realm> <id>")
                return

            if entity == "pvp":
                await self._wow_pvp(bot, ev, args)
                return

            await self._err(bot, ev, "Usage: !wow char|guild|realm|item|auction|pvp ...")

        elif cmd == "d3":
            if not rest:
                await self._err(bot, ev, "Usage: !d3 profile|hero|item|leaderboard ...")
                return
            entity = rest[0].lower()
            args = rest[1:]

            if entity == "profile":
                if args:
                    action = args[0].lower() if args[0].lower() in ("seasonal", "heroes") else ""
                    if action and len(args) >= 2:
                        await self._d3_profile(bot, ev, args[1], action)
                    elif not action:
                        await self._d3_profile(bot, ev, args[0])
                    else:
                        await self._err(bot, ev, f"Usage: !d3 profile {action} <battletag>")
                else:
                    await self._err(bot, ev, "Usage: !d3 profile <battletag> [seasonal|heroes]")
                return

            if entity == "hero":
                if len(args) >= 2:
                    action = args[0].lower() if args[0].lower() in ("gear", "stats", "skills") and len(args) >= 3 else ""
                    if action:
                        await self._d3_hero(bot, ev, args[1], args[2], action)
                    else:
                        await self._d3_hero(bot, ev, args[0], args[1])
                else:
                    await self._err(bot, ev, "Usage: !d3 hero <battletag> <hero-id> [gear|stats|skills]")
                return

            if entity == "item":
                await self._d3_item(bot, ev, args)
                return

            if entity == "leaderboard":
                await self._d3_leaderboard(bot, ev, args)
                return

            await self._err(bot, ev, "Usage: !d3 profile|hero|item|leaderboard ...")


def setup(bot):
    cfg = bot.cfg.get("blizzard", {}) if isinstance(getattr(bot, "cfg", None), dict) else {}
    svc = BlizzardService(cfg)

    if hasattr(bot, "register_command"):
        # WoW commands
        bot.register_command("wow", min_role="guest", mutating=False, help="WoW game data. !wow char|guild|realm|item|auction|pvp ...", category="Blizzard", service_id="blizzard")
        bot.register_command("wc", min_role="guest", mutating=False, help="Alias: !wow char", category="Blizzard", service_id="blizzard")
        bot.register_command("wg", min_role="guest", mutating=False, help="Alias: !wow guild", category="Blizzard", service_id="blizzard")
        bot.register_command("wr", min_role="guest", mutating=False, help="Alias: !wow realm", category="Blizzard", service_id="blizzard")
        bot.register_command("wi", min_role="guest", mutating=False, help="Alias: !wow item", category="Blizzard", service_id="blizzard")
        bot.register_command("wa", min_role="guest", mutating=False, help="Alias: !wow auction", category="Blizzard", service_id="blizzard")
        bot.register_command("wpvp", min_role="guest", mutating=False, help="Alias: !wow pvp", category="Blizzard", service_id="blizzard")
        # D3 commands
        bot.register_command("d3", min_role="guest", mutating=False, help="Diablo III game data. !d3 profile|hero|item|leaderboard ...", category="Blizzard", service_id="blizzard")
        bot.register_command("dp", min_role="guest", mutating=False, help="Alias: !d3 profile", category="Blizzard", service_id="blizzard")
        bot.register_command("dh", min_role="guest", mutating=False, help="Alias: !d3 hero", category="Blizzard", service_id="blizzard")
        bot.register_command("di", min_role="guest", mutating=False, help="Alias: !d3 item", category="Blizzard", service_id="blizzard")
        bot.register_command("dlb", min_role="guest", mutating=False, help="Alias: !d3 leaderboard", category="Blizzard", service_id="blizzard")

    return svc
