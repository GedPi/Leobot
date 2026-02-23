import fnmatch
import hashlib
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

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
    # Stable-ish identity: user@host if present, otherwise nick
    if ev.user and ev.host:
        return f"{ev.user}@{ev.host}".lower()
    return (ev.nick or "").lower()


def _next_local_midnight_epoch() -> int:
    # “Once a day” -> valid until next midnight in Europe/London
    if LONDON_TZ is None:
        # fallback: 24h
        return int(time.time() + 24 * 3600)

    now = datetime.now(tz=LONDON_TZ)
    tomorrow = (now + timedelta(days=1)).date()
    midnight = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 0, 0, tzinfo=LONDON_TZ)
    return int(midnight.timestamp())


@dataclass
class CommandPolicy:
    min_role: str          # guest/user/contributor/admin
    mutating: bool = False # if True, requires daily auth for admin/contributor


class ACLService:
    """
    Bot-wide access control with optional 'daily password re-auth' for mutating commands.
    """

    def __init__(self, bot):
        self.bot = bot

        # command -> policy
        self.policies: Dict[str, CommandPolicy] = {}

        # identity_key -> auth_until_epoch
        self.auth_until: Dict[str, int] = {}

        # identity_key -> role (cached per message is fine, but keep simple)
        # no persistent storage by default (can add later)

        # “pending auth” isn’t strictly required; we just accept !auth in PM any time

        # register built-ins
        self.register("auth", min_role="guest", mutating=False)
        self.register("whoami", min_role="guest", mutating=False)

    # ---------- policy registration ----------

    def register(self, command: str, min_role: str = "user", mutating: bool = False, help: str = "", category: str = "General", **_ignored) -> None:
        command = command.lower().lstrip("!")
        if min_role not in ROLE_ORDER:
            raise ValueError(f"Unknown role: {min_role}")
        self.policies[command] = CommandPolicy(min_role=min_role, mutating=mutating)

        if hasattr(self.bot, 'register_command'):
            self.bot.register_command(command, min_role=min_role, mutating=mutating, help=help or '', category=category or 'General')
    # ---------- role resolution ----------

    def _cfg(self) -> dict:
        return self.bot.cfg.get("acl", {}) if isinstance(self.bot.cfg, dict) else {}

    def role_for_event(self, ev) -> str:
        return self.role_for(ev)

    def role_for(self, ev) -> str:
        cfg = self._cfg()

        # Match lists in descending privilege order
        if self._match_any(cfg.get("admins", []), ev):
            return "admin"
        if self._match_any(cfg.get("contributors", []), ev):
            return "contributor"
        if self._match_any(cfg.get("users", []), ev):
            return "user"

        return "guest"

    def _match_any(self, entries, ev) -> bool:
        """
        entries supports:
          - string masks like "*!*@example.com"
          - objects like {"mask": "*!*@example.com", "pass_sha256": "..."} for admins/contributors
        """
        if not isinstance(entries, list):
            return False

        # Construct mask forms
        full = None
        if ev.user and ev.host:
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

    # ---------- daily auth ----------

    def _pass_sha256_for(self, ev, role: str) -> Optional[str]:
        cfg = self._cfg()
        key = "admins" if role == "admin" else "contributors"
        entries = cfg.get(key, [])
        if not isinstance(entries, list):
            return None

        full = None
        if ev.user and ev.host:
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

    def is_authed_today(self, identity_key: str) -> bool:
        until = self.auth_until.get(identity_key)
        return bool(until and time.time() < until)

    async def require_auth(self, bot, ev, role: str) -> None:
        # Ask privately for auth; also nudge in channel if needed
        if not ev.is_private:
            await bot.privmsg(ev.target, f"{ev.nick}: check your PMs to authenticate.")
        await bot.privmsg(ev.nick, "Auth required for changes. Reply here with: !auth <password>")

    # ---------- bot-wide precheck ----------

    async def precheck(self, bot, ev) -> bool:
        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()
        if not text.startswith(prefix):
            return True  # not a command

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return True

        cmd = cmdline.split()[0].lower().lstrip("!")

        # Always allow auth/whoami to reach us
        if cmd in ("auth", "whoami"):
            return True

        policy = self.policies.get(cmd)

        # Unknown commands: let services decide (don’t block)
        if policy is None:
            return True

        role = self.role_for(ev)
        if ROLE_ORDER[role] < ROLE_ORDER[policy.min_role]:
            await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires {policy.min_role}).")
            return False

        # Mutating commands: require daily auth for admin/contributor
        if policy.mutating and role in ("admin", "contributor"):
            ident = _identity_key(ev)
            if not self.is_authed_today(ident):
                await self.require_auth(bot, ev, role)
                return False

        return True

    # ---------- command handling ----------

    async def on_privmsg(self, bot, ev) -> None:
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
            authed = self.is_authed_today(ident)
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
            self.auth_until[ident] = _next_local_midnight_epoch()
            await bot.privmsg(ev.nick, "Auth OK. You’re cleared for changes until next midnight.")
            return


def setup(bot):
    svc = ACLService(bot)

    # Expose on bot so other services can register policies
    bot.acl = svc

    # Register policies for existing commands (bot-wide).
    # eightball: safe for guests
    svc.register("8ball", min_role="guest", mutating=False)
    svc.register("eightball", min_role="guest", mutating=False)

    # news: allow users to read news; later we’ll add mutating subcommands
    svc.register("news", min_role="user", mutating=False)
    svc.register("headlines", min_role="user", mutating=False)

    # future examples:
    # svc.register("newsaddsource", min_role="contributor", mutating=True)

    return svc
