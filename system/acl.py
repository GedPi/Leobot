from __future__ import annotations

import hashlib
import logging
import time
import asyncio
import re
from dataclasses import dataclass
from fnmatch import fnmatch

from system.types import Event, Role

log = logging.getLogger("leobot.acl")

ROLE_ORDER = {"guest": 0, "user": 1, "contributor": 2, "admin": 3}

# Anope NickServ STATUS reply typically includes: "<nick> <status> [account]"
# We parse by scanning for "<nick> <0-3>" tokens.
_STATUS_PAIR_RE = re.compile(r"^(\S+)\s+([0-3])(?:\s+\S+)?\s*$", re.IGNORECASE)


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


def _norm_role(r: str | None) -> Role:
    rr = (r or "").strip().lower()
    if rr in ("guest", "user", "contributor", "admin"):
        return rr  # type: ignore[return-value]
    return "guest"


@dataclass(slots=True)
class ACLConfig:
    # Legacy config-based ACL remains supported (no feature loss)
    admins: list[dict]
    contributors: list[dict]
    users: list[str]
    guest_allowed_cmds: set[str]

    # New: bootstrap master (optional)
    master: str


class ACL:
    """
    Backwards-compatible ACL + DB-backed future path.

    Keeps existing:
      - !auth <password> (mask + pass_sha256 from config.json)
      - sessions stored in sqlite (acl_sessions)
      - !whoami
      - guest allowlist
      - precheck() contract used by Dispatcher

    Adds (without breaking old model):
      - DB identities table: acl_identities (nick->role)
      - DB command overrides: acl_command_perms (command->min_role)
      - Optional bootstrap admin from config: acl.master
      - Optional NickServ STATUS verification helper (Anope)
    """

    def __init__(self, store, cfg: dict):
        self.store = store
        acl = cfg.get("acl", {}) if isinstance(cfg, dict) else {}
        self.cfg = ACLConfig(
            admins=list(acl.get("admins") or []),
            contributors=list(acl.get("contributors") or []),
            users=list(acl.get("users") or []),
            guest_allowed_cmds=set((acl.get("guest_allowed") or {}).get("commands") or []),
            master=str(acl.get("master") or "").strip(),
        )

        # NickServ STATUS pending futures (nick_lower -> Future[int])
        self._ns_pending: dict[str, asyncio.Future] = {}

    # ---------------- legacy mask-based role ----------------

    def _mask_role(self, ev: Event) -> Role:
        hostmask = ""
        if ev.nick and ev.user and ev.host:
            hostmask = f"{ev.nick}!{ev.user}@{ev.host}"
        userhost = f"{ev.user}@{ev.host}" if ev.user and ev.host else ""
        host = ev.host or ""

        # Admin masks
        for item in self.cfg.admins:
            mask = str(item.get("mask") or "")
            if mask and (fnmatch(hostmask, mask) or fnmatch(userhost, mask) or fnmatch(host, mask)):
                return "admin"

        # Contributor masks
        for item in self.cfg.contributors:
            mask = str(item.get("mask") or "")
            if mask and (fnmatch(hostmask, mask) or fnmatch(userhost, mask) or fnmatch(host, mask)):
                return "contributor"

        # Plain users list (nick match)
        n = (ev.nick or "").strip()
        if n and n in self.cfg.users:
            return "user"

        return "guest"

    # ---------------- sessions (existing) ----------------

    async def session_role(self, ev: Event) -> Role | None:
        row = await self.store.get_acl_session(_identity_key(ev))
        if not row:
            return None
        role, until_ts = row[0], int(row[1] or 0)
        if until_ts and int(time.time()) >= until_ts:
            try:
                await self.store.clear_acl_session(_identity_key(ev))
            except Exception:
                pass
            return None
        return _norm_role(str(role))

    # ---------------- DB schema + bootstrap ----------------

    async def on_ready(self, bot) -> None:
        # Create DB tables if missing (safe)
        await self.store.execute(
            """
            CREATE TABLE IF NOT EXISTS acl_identities (
              ident TEXT PRIMARY KEY,
              role TEXT NOT NULL,
              created_ts INTEGER NOT NULL
            )
            """
        )
        await self.store.execute(
            """
            CREATE TABLE IF NOT EXISTS acl_command_perms (
              command TEXT PRIMARY KEY,
              min_role TEXT NOT NULL,
              updated_ts INTEGER NOT NULL
            )
            """
        )
        await self.store.execute("CREATE INDEX IF NOT EXISTS idx_acl_role ON acl_identities(role)")

        # Bootstrap master admin if DB has no admins yet
        if self.cfg.master:
            row = await self.store.fetchone("SELECT COUNT(*) FROM acl_identities WHERE role='admin'")
            admin_count = int(row[0]) if row else 0
            if admin_count == 0:
                await self.store.execute(
                    "INSERT OR REPLACE INTO acl_identities(ident, role, created_ts) VALUES(?,?,?)",
                    (self.cfg.master.lower(), "admin", int(time.time())),
                )
                await bot.privmsg(self.cfg.master, "ACL: bootstrapped you as admin (master).")

    async def db_role(self, ev: Event) -> Role | None:
        nick = (ev.nick or "").strip().lower()
        if not nick:
            return None
        row = await self.store.fetchone("SELECT role FROM acl_identities WHERE ident=?", (nick,))
        if not row:
            return None
        return _norm_role(str(row[0]))

    async def db_min_role_for_cmd(self, cmd: str) -> Role | None:
        c = (cmd or "").strip().lower()
        if not c:
            return None
        row = await self.store.fetchone("SELECT min_role FROM acl_command_perms WHERE command=?", (c,))
        if not row:
            return None
        return _norm_role(str(row[0]))

    # ---------------- effective role (non-breaking) ----------------

    async def effective_role(self, ev: Event) -> Role:
        base = self._mask_role(ev)
        sess = await self.session_role(ev)
        db = await self.db_role(ev)

        # highest wins
        best = base
        for r in (sess, db):
            if r and ROLE_ORDER.get(r, 0) > ROLE_ORDER.get(best, 0):
                best = r
        return best

    # ---------------- NickServ STATUS (Anope) ----------------

    async def nickserv_status(self, bot, nick: str, timeout: float = 3.0) -> int | None:
        """
        Ask NickServ for STATUS <nick>. Returns 0..3, or None on timeout.
        Requires Dispatcher to forward on_notice to core handler (done in dispatcher.py replacement).
        """
        n = (nick or "").strip()
        if not n:
            return None
        key = n.lower()

        fut = self._ns_pending.get(key)
        if fut and not fut.done():
            try:
                return int(await asyncio.wait_for(fut, timeout=timeout))
            except Exception:
                return None

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._ns_pending[key] = fut

        try:
            await bot.privmsg("NickServ", f"STATUS {n}")
        except Exception:
            self._ns_pending.pop(key, None)
            return None

        try:
            val = await asyncio.wait_for(fut, timeout=timeout)
            return int(val)
        except Exception:
            return None
        finally:
            self._ns_pending.pop(key, None)

    async def on_notice(self, bot, ev: Event) -> None:
        # Consume NickServ replies
        if (ev.nick or "").strip().lower() != "nickserv":
            return
        self._consume_status(ev.text or "")

    def _consume_status(self, text: str) -> bool:
        txt = (text or "").strip()
        if not txt:
            return False

        # Whole-line match first
        m = _STATUS_PAIR_RE.match(txt)
        if m:
            nick = m.group(1)
            status = int(m.group(2))
            fut = self._ns_pending.get(nick.lower())
            if fut and not fut.done():
                fut.set_result(status)
                return True
            return False

        # Token scan for "<nick> <0-3>"
        parts = txt.split()
        for i in range(len(parts) - 1):
            if parts[i + 1] in ("0", "1", "2", "3"):
                nick = parts[i]
                status = int(parts[i + 1])
                fut = self._ns_pending.get(nick.lower())
                if fut and not fut.done():
                    fut.set_result(status)
                    return True
        return False

    # ---------------- core commands (existing + future-safe) ----------------

    async def handle_core(self, bot, ev: Event) -> bool:
        # !auth <password>, !whoami
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

    # ---------------- dispatcher gate ----------------

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

        if cmd is None or info is None:
            return True

        # allow auth/whoami/help regardless
        if cmd in ("auth", "whoami", "help", "commands"):
            return True

        role = await self.effective_role(ev)

        # DB override wins if present
        db_min = await self.db_min_role_for_cmd(cmd)
        min_role = db_min if db_min is not None else info["min_role"]

        if ROLE_ORDER.get(role, 0) < ROLE_ORDER.get(min_role, 0):
            await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires {min_role}).")
            return False

        # guest allowlist (if caller is guest)
        if role == "guest" and cmd not in self.cfg.guest_allowed_cmds:
            await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires user).")
            return False

        return True

    async def prune(self) -> None:
        # Keep compatibility; store in your zip does not implement prune_acl_sessions()
        fn = getattr(self.store, "prune_acl_sessions", None)
        if callable(fn):
            try:
                n = await fn()
                if n:
                    log.info("Pruned %d expired ACL sessions", n)
            except Exception:
                pass
        return