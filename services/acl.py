import fnmatch
import hashlib
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

from services.store import Store

LONDON_TZ = ZoneInfo("Europe/London") if ZoneInfo else None

ROLE_ORDER = {
    "guest": 0,
    "user": 1,
    "contributor": 2,
    "admin": 3,
}


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _identity_key(ev) -> str:
    # Stable identity: user@host if present, otherwise nick
    if getattr(ev, "user", None) and getattr(ev, "host", None):
        return f"{ev.user}@{ev.host}".lower()
    return (ev.nick or "").lower()


def _next_local_midnight_epoch() -> int:
    if LONDON_TZ is None:
        return int(time.time() + 24 * 3600)

    now = datetime.now(tz=LONDON_TZ)
    tomorrow = (now + timedelta(days=1)).date()
    midnight = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 0, 0, tzinfo=LONDON_TZ)
    return int(midnight.timestamp())


@dataclass
class CommandPolicy:
    min_role: str
    mutating: bool = False


class ACLService:
    """
    Bot-wide access control with optional daily password re-auth for mutating commands.
    Persist auth in sqlite (acl_auth).
    """

    def __init__(self, bot):
        self.bot = bot
        self.policies: Dict[str, CommandPolicy] = {}

        db_path = "/var/lib/leobot/db/leobot.db"
        if isinstance(getattr(bot, "cfg", None), dict):
            db_path = bot.cfg.get("chatdb", {}).get("db_path", db_path)
        self.store = Store(db_path)

        # in-memory cache: identity_key -> auth_until_epoch
        self.auth_until: Dict[str, int] = {}
        self._init_done = False

        self.register("auth", min_role="guest", mutating=False)
        self.register("whoami", min_role="guest", mutating=False)

    async def _init_once(self) -> None:
        if self._init_done:
            return
        await self.store.acl_prune_expired()
        self._init_done = True

    def register(self, command: str, min_role: str = "user", mutating: bool = False, help: str = "", category: str = "General", **_ignored) -> None:
        command = command.lower().lstrip("!")
        if min_role not in ROLE_ORDER:
            raise ValueError(f"Unknown role: {min_role}")
        self.policies[command] = CommandPolicy(min_role=min_role, mutating=mutating)

        if hasattr(self.bot, "register_command"):
            self.bot.register_command(command, min_role=min_role, mutating=mutating, help=help or "", category=category or "General")

    def _cfg(self) -> dict:
        return self.bot.cfg.get("acl", {}) if isinstance(self.bot.cfg, dict) else {}

    def role_for_event(self, ev) -> str:
        return self.role_for(ev)

    def role_for(self, ev) -> str:
        cfg = self._cfg()
        if self._match_any(cfg.get("admins", []), ev):
            return "admin"
        if self._match_any(cfg.get("contributors", []), ev):
            return "contributor"
        if self._match_any(cfg.get("users", []), ev):
            return "user"
        return "guest"

    def _match_any(self, entries, ev) -> bool:
        if not isinstance(entries, list):
            return False

        full = None
        if getattr(ev, "user", None) and getattr(ev, "host", None):
            full = f"{ev.nick}!{ev.user}@{ev.host}"
        nick_only = ev.nick or ""

        for e in entries:
            if isinstance(e, str):
                if full and fnmatch.fnmatchcase(full, e):
                    return True
                if fnmatch.fnmatchcase(nick_only, e):
                    return True
            elif isinstance(e, dict):
                m = str(e.get("mask", ""))
                if not m:
                    continue
                if full and fnmatch.fnmatchcase(full, m):
                    return True
                if fnmatch.fnmatchcase(nick_only, m):
                    return True
        return False

    def _pass_sha256_for(self, ev, role: str) -> Optional[str]:
        cfg = self._cfg()
        key = "admins" if role == "admin" else "contributors"
        entries = cfg.get(key, [])
        if not isinstance(entries, list):
            return None

        full = None
        if getattr(ev, "user", None) and getattr(ev, "host", None):
            full = f"{ev.nick}!{ev.user}@{ev.host}"

        for e in entries:
            if not isinstance(e, dict):
                continue
            m = str(e.get("mask", ""))
            p = str(e.get("pass_sha256", "")).strip().lower()
            if not m or not p:
                continue
            if full and fnmatch.fnmatchcase(full, m):
                return p
            if fnmatch.fnmatchcase(ev.nick or "", m):
                return p
        return None

    async def _is_authed_today(self, identity_key: str) -> bool:
        now = int(time.time())
        cached = self.auth_until.get(identity_key)
        if cached and now < cached:
            return True

        until = await self.store.acl_get_authed_until(identity_key)
        if until and now < until:
            self.auth_until[identity_key] = int(until)
            return True

        return False

    async def require_auth(self, bot, ev, role: str) -> None:
        if not ev.is_private:
            await bot.privmsg(ev.target, f"{ev.nick}: check your PMs to authenticate.")
        await bot.privmsg(ev.nick, "Auth required for changes.\nReply here with: !auth <password>")

    async def precheck(self, bot, ev) -> bool:
        await self._init_once()

        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()
        if not text.startswith(prefix):
            return True

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return True

        parts = [p.lower().lstrip("!") for p in cmdline.split() if p.strip()]
        if not parts:
            return True

        cmd = parts[0]
        best = None
        for i in range(len(parts), 0, -1):
            cand = " ".join(parts[:i])
            if cand in self.policies:
                best = cand
                break
        if best is not None:
            cmd = best

        if parts[0] in ("auth", "whoami"):
            return True

        policy = self.policies.get(cmd)
        if policy is None:
            return True

        role = self.role_for(ev)
        if ROLE_ORDER[role] < ROLE_ORDER[policy.min_role]:
            await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires {policy.min_role}).")
            return False

        if policy.mutating and role in ("admin", "contributor"):
            ident = _identity_key(ev)
            if not await self._is_authed_today(ident):
                await self.require_auth(bot, ev, role)
                return False

        return True

    async def on_privmsg(self, bot, ev) -> None:
        await self._init_once()

        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()
        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return

        parts = cmdline.split(maxsplit=1)
        cmd = parts[0].lower().lstrip("!")
        arg = parts[1].strip() if len(parts) == 2 else ""

        if cmd == "whoami":
            role = self.role_for(ev)
            ident = _identity_key(ev)
            authed = await self._is_authed_today(ident)
            await bot.privmsg(ev.target, f"{ev.nick}: role={role}, daily_auth={'yes' if authed else 'no'}")
            return

        if cmd == "auth":
            if not ev.is_private:
                await bot.privmsg(ev.target, f"{ev.nick}: use !auth only in PM.")
                return

            role = self.role_for(ev)
            if role not in ("admin", "contributor"):
                await bot.privmsg(ev.nick, "You are not an admin/contributor.")
                return

            if not arg:
                await bot.privmsg(ev.nick, "Usage: !auth <password>")
                return

            stored = self._pass_sha256_for(ev, role)
            if not stored:
                await bot.privmsg(ev.nick, "No password configured for your mask.")
                return

            if _sha256_hex(arg) != stored:
                await bot.privmsg(ev.nick, "Auth failed.")
                return

            ident = _identity_key(ev)
            until = _next_local_midnight_epoch()
            self.auth_until[ident] = until
            await self.store.acl_set_auth(identity_key=ident, role=role, authed_until_ts=until)

            await bot.privmsg(ev.nick, "Auth OK. You’re cleared for changes until next midnight.")
            return


def setup(bot):
    svc = ACLService(bot)
    bot.acl = svc

    svc.register("8ball", min_role="guest", mutating=False)
    svc.register("eightball", min_role="guest", mutating=False)

    svc.register("news", min_role="user", mutating=False)
    svc.register("headlines", min_role="user", mutating=False)

    return svc
