from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from fnmatch import fnmatch

from system.types import Event, Role

log = logging.getLogger("leobot.acl")

ROLE_ORDER = {"guest": 0, "user": 1, "contributor": 2, "admin": 3}


def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _identity_key(ev: Event) -> str:
    # Stable-ish key. Prefer user@host.
    user = (ev.user or "").strip()
    host = (ev.host or "").strip()
    if user and host:
        return f"{user}@{host}".lower()
    # fall back to nick
    return (ev.nick or "").strip().lower()


def _utc_midnight_next(now: int | None = None) -> int:
    t = int(now or time.time())
    # UTC midnight boundary
    return t - (t % 86400) + 86400


@dataclass(slots=True)
class ACLConfig:
    admins: list[dict]
    contributors: list[dict]
    users: list[str]
    guest_allowed_cmds: set[str]


class ACL:
    def __init__(self, store, cfg: dict):
        self.store = store
        acl = cfg.get("acl", {}) if isinstance(cfg, dict) else {}
        self.cfg = ACLConfig(
            admins=list(acl.get("admins") or []),
            contributors=list(acl.get("contributors") or []),
            users=list(acl.get("users") or []),
            guest_allowed_cmds=set((acl.get("guest_allowed") or {}).get("commands") or []),
        )

    def _mask_role(self, ev: Event) -> Role:
        hostmask = ""
        if ev.nick and ev.user and ev.host:
            hostmask = f"{ev.nick}!{ev.user}@{ev.host}"
        userhost = f"{ev.user}@{ev.host}" if ev.user and ev.host else ""
        host = ev.host or ""

        # user (no auth) via masks
        for pat in self.cfg.users:
            pat = (pat or "").strip()
            if not pat:
                continue
            if hostmask and fnmatch(hostmask, pat):
                return "user"
            if userhost and fnmatch(userhost, pat):
                return "user"
            if host and fnmatch(host, pat):
                return "user"

        return "guest"

    async def session_role(self, ev: Event) -> Role | None:
        row = await self.store.get_acl_session(_identity_key(ev))
        if not row:
            return None
        role, until_ts = str(row[0]), int(row[1])
        if int(time.time()) >= until_ts:
            return None
        return role

    async def effective_role(self, ev: Event) -> Role:
        base = self._mask_role(ev)
        sess = await self.session_role(ev)
        if sess and ROLE_ORDER.get(sess, 0) > ROLE_ORDER.get(base, 0):
            return sess
        return base

    async def handle_core(self, bot, ev: Event) -> bool:
        # !auth <password>
        prefix = bot.cfg.get("command_prefix", "!")
        txt = (ev.text or "").strip()
        if not txt.startswith(prefix):
            return False

        cmdline = txt[len(prefix):].strip()
        if not cmdline:
            return False

        parts = cmdline.split()
        cmd = parts[0].lower()

        if cmd == "whoami":
            role = await self.effective_role(ev)
            await bot.privmsg(ev.target, f"{ev.nick}: role={role} identity={_identity_key(ev)}")
            return True

        if cmd != "auth":
            return False

        if len(parts) < 2:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !auth <password>")
            return True

        password = parts[1]
        digest = _sha256(password)

        # Highest match wins
        granted: Role | None = None
        for item in self.cfg.admins:
            if fnmatch(f"{ev.nick}!{ev.user}@{ev.host}", item.get("mask", "")) and item.get("pass_sha256") == digest:
                granted = "admin"
                break
        if granted is None:
            for item in self.cfg.contributors:
                if fnmatch(f"{ev.nick}!{ev.user}@{ev.host}", item.get("mask", "")) and item.get("pass_sha256") == digest:
                    granted = "contributor"
                    break

        if granted is None:
            await bot.privmsg(ev.target, f"{ev.nick}: auth failed")
            return True

        until = _utc_midnight_next()
        await self.store.set_acl_session(_identity_key(ev), granted, until)
        await bot.privmsg(ev.target, f"{ev.nick}: authenticated as {granted} until UTC midnight")
        return True

    async def precheck(self, bot, ev: Event) -> bool:
        # Only gate commands.
        prefix = bot.cfg.get("command_prefix", "!")
        txt = (ev.text or "").strip()
        if not txt.startswith(prefix):
            return True

        cmdline = txt[len(prefix):].strip().lower()
        if not cmdline:
            return True

        # support subcommands registered like "service enable"
        # choose longest matching registered command
        parts = cmdline.split()
        cands = []
        for i in range(len(parts), 0, -1):
            cands.append(" ".join(parts[:i]))

        cmd = None
        info = None
        for c in cands:
            if c in bot.commands:
                cmd = c
                info = bot.commands[c]
                break

        if cmd is None:
            return True

        # allow auth/whoami/help regardless
        if cmd in ("auth", "whoami", "help", "commands"):
            return True

        role = await self.effective_role(ev)
        min_role = info["min_role"]
        if ROLE_ORDER.get(role, 0) < ROLE_ORDER.get(min_role, 0):
            await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires {min_role}).")
            return False

        # guest allowlist (if caller is guest)
        if role == "guest" and cmd not in self.cfg.guest_allowed_cmds:
            await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires user).")
            return False

        return True

    async def prune(self) -> None:
        n = await self.store.prune_acl_sessions()
        if n:
            log.info("Pruned %d expired ACL sessions", n)
