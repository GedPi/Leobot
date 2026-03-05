from __future__ import annotations

# Access control: roles (guest/user/contributor/admin), identity from event, NickServ STATUS cache,
# !auth/!whoami/!acl commands and precheck/is_allowed for command gating via Store.

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Optional

from system.types import Event, Role

log = logging.getLogger("leobot.acl")

ROLE_ORDER = {"guest": 0, "user": 1, "contributor": 2, "admin": 3}
_STATUS_DIGITS = {"0", "1", "2", "3"}
NICKSERV_STATUS_TIMEOUT = 8.0
NICKSERV_STATUS_GRACE = 0.75
NICKSERV_STATUS_GRACE_POLL = 0.05
NICKSERV_STATUS_CACHE_TTL = 60


def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


# Builds a stable key from event: user@host when present, else nick (lowercased).
def _identity_key(ev: Event) -> str:
    user = (ev.user or "").strip()
    host = (ev.host or "").strip()
    if user and host:
        return f"{user}@{host}".lower()
    return (ev.nick or "").strip().lower()


# Public alias for canonical principal key (sessions and identity); same as _identity_key(ev).
def principal_from_event(ev: Event) -> str:
    return _identity_key(ev)


# Returns next UTC midnight as Unix timestamp; used for session expiry after !auth.
def _utc_midnight_next(now: int | None = None) -> int:
    t = int(now or time.time())
    return t - (t % 86400) + 86400


# Normalizes a string to a valid role (guest|user|contributor|admin); plural forms map to singular.
def _norm_role(s: str | None) -> Role:
    r = (s or "").strip().lower()
    if r in ("guest", "user", "contributor", "admin"):
        return r  # type: ignore[return-value]
    if r == "users":
        return "user"
    if r == "contributors":
        return "contributor"
    return "guest"


# Lowercases and strips leading "!" from a command string.
def _norm_cmd(s: str) -> str:
    return (s or "").strip().lower().lstrip("!")


# Lowercases and strips punctuation from a token; used when parsing NickServ STATUS lines.
def _clean_token(tok: str) -> str:
    t = (tok or "").strip().lower()
    return t.strip(",:;.!?()[]{}<>\"'")


# Holds parsed acl config: admins/contributors (mask+pass), users masks, guest_allowed commands, master nick.
@dataclass(slots=True)
class ACLConfig:
    admins: list[dict]
    contributors: list[dict]
    users: list[str]
    guest_allowed_cmds: set[str]
    master: str


# Core ACL: role resolution (mask/session/DB), NickServ STATUS, !auth/!whoami/!acl, precheck and is_allowed.
# help.py uses effective_role(ev); dispatcher calls precheck(bot, ev) before dispatching commands.
class ACL:
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
        self._ns_pending: dict[str, asyncio.Future] = {}
        self._ns_cache: dict[str, tuple[int, int]] = {}
        self._schema_ready = False
        self._commands_registered = False
        self._bootstrapped_master = False
        self._bot: Optional[object] = None

    def _bind_bot(self, bot) -> None:
        if self._bot is None:
            self._bot = bot

    # Creates acl_identities and acl_command_perms tables if missing; idempotent.
    async def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
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
        self._schema_ready = True

    # If config has master and no admin exists in DB, adds master nick as admin and notifies them.
    async def _bootstrap_master(self, bot) -> None:
        if self._bootstrapped_master:
            return
        if not self.cfg.master:
            self._bootstrapped_master = True
            return

        await self._ensure_schema()

        admin_count = await self.store.acl_count_admins()
        if admin_count == 0:
            await self.store.acl_set_identity_role(self.cfg.master, "admin")
            try:
                await bot.privmsg(self.cfg.master, "ACL: bootstrapped you as admin (master).")
            except Exception:
                pass

        self._bootstrapped_master = True

    # Registers !acl and subcommands in bot.commands once; used so help and precheck see them.
    def _ensure_commands_registered(self, bot) -> None:
        if self._commands_registered:
            return

        try:
            bot.register_command(
                "acl",
                min_role="admin",
                mutating=True,
                help="ACL management. Usage: !acl <adduser|deluser|usrlist|addserv|delserv|servlist> ...",
                category="System",
            )
            bot.register_command(
                "acl adduser",
                min_role="admin",
                mutating=True,
                help="Add user (DB). Usage: !acl adduser <nick> <admin|contributor|user>",
                category="System",
            )
            bot.register_command(
                "acl deluser",
                min_role="admin",
                mutating=True,
                help="Delete user (DB). Usage: !acl deluser <nick> <group>",
                category="System",
            )
            bot.register_command(
                "acl usrlist",
                min_role="admin",
                mutating=False,
                help="List users in group (DB). Usage: !acl usrlist <admin|contributor|user>",
                category="System",
            )
            bot.register_command(
                "acl addserv",
                min_role="admin",
                mutating=True,
                help="Set command min role override (DB). Usage: !acl addserv <service> <guest|user|contributor|admin>",
                category="System",
            )
            bot.register_command(
                "acl delserv",
                min_role="admin",
                mutating=True,
                help="Delete command override (DB). Usage: !acl delserv <service> <group>",
                category="System",
            )
            bot.register_command(
                "acl servlist",
                min_role="admin",
                mutating=False,
                help="List command overrides (DB). Usage: !acl servlist",
                category="System",
            )
        except Exception:
            pass

        self._commands_registered = True

    # Binds bot, registers ACL commands, ensures schema and bootstraps master; called before first use.
    async def _lazy_init(self, bot) -> None:
        self._bind_bot(bot)
        self._ensure_commands_registered(bot)
        await self._ensure_schema()
        await self._bootstrap_master(bot)

    # Returns "user" if event hostmask/userhost/host matches any config users pattern, else "guest".
    def _mask_role(self, ev: Event) -> Role:
        hostmask = ""
        if ev.nick and ev.user and ev.host:
            hostmask = f"{ev.nick}!{ev.user}@{ev.host}"
        userhost = f"{ev.user}@{ev.host}" if ev.user and ev.host else ""
        host = ev.host or ""

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

    # Returns role from acl_sessions for principal(ev) if session exists and not expired, else None.
    async def session_role(self, ev: Event) -> Role | None:
        row = await self.store.get_acl_session(principal_from_event(ev))
        if not row:
            return None
        role, until_ts = str(row[0]), int(row[1])
        if int(time.time()) >= until_ts:
            return None
        return _norm_role(role)

    # Returns role from acl_identities for normalized nick, or None.
    async def db_role(self, nick: str) -> Role | None:
        await self._ensure_schema()
        n = (nick or "").strip().lower()
        if not n:
            return None
        role = await self.store.acl_get_identity_role(n)
        return _norm_role(role) if role else None

    # Highest role among mask, session and DB; used by help and precheck.
    async def effective_role(self, ev: Event) -> Role:
        if self._bot is not None:
            try:
                self._ensure_commands_registered(self._bot)
            except Exception:
                pass

        base = self._mask_role(ev)
        sess = await self.session_role(ev)
        db = await self.db_role(ev.nick or "")

        best = base
        for r in (sess, db):
            if r and ROLE_ORDER.get(r, 0) > ROLE_ORDER.get(best, 0):
                best = r
        return best

    def _cache_set(self, nick: str, status: int) -> None:
        self._ns_cache[nick.lower()] = (int(status), int(time.time()))

    def _cache_get(self, nick: str) -> int | None:
        key = (nick or "").strip().lower()
        if not key:
            return None
        val = self._ns_cache.get(key)
        if not val:
            return None
        status, ts = val
        if int(time.time()) - int(ts) > NICKSERV_STATUS_CACHE_TTL:
            return None
        return int(status)

    # Parses NickServ STATUS reply from text: finds nick + digit, updates cache and completes pending future; returns True if consumed.
    def _consume_status_line(self, text: str) -> bool:
        txt = (text or "").strip()
        if not txt:
            return False

        raw_parts = txt.split()
        if len(raw_parts) < 2:
            return False

        parts = [_clean_token(p) for p in raw_parts]

        for i in range(len(parts) - 1):
            if parts[i + 1] in _STATUS_DIGITS:
                nick_tok = parts[i]
                st = int(parts[i + 1])
                if nick_tok:
                    self._cache_set(nick_tok, st)
                    fut = self._ns_pending.get(nick_tok)
                    if fut and not fut.done():
                        fut.set_result(st)
                return True

        for i in range(len(parts) - 2):
            if parts[i] == "status" and parts[i + 2] in _STATUS_DIGITS:
                nick_tok = parts[i + 1]
                st = int(parts[i + 2])
                if nick_tok:
                    self._cache_set(nick_tok, st)
                    fut = self._ns_pending.get(nick_tok)
                    if fut and not fut.done():
                        fut.set_result(st)
                return True

        for pending_nick, fut in list(self._ns_pending.items()):
            if not fut or fut.done():
                continue
            if pending_nick in parts:
                idxs = [i for i, p in enumerate(parts) if p == pending_nick]
                for idx in idxs:
                    window = parts[idx + 1 : idx + 7]
                    for w in window:
                        if w in _STATUS_DIGITS:
                            st = int(w)
                            self._cache_set(pending_nick, st)
                            fut.set_result(st)
                            return True

        return False

    # Sends STATUS to NickServ for nick, waits for reply (or cache), returns status digit or None; uses grace window on timeout.
    async def nickserv_status(self, bot, nick: str, timeout: float = NICKSERV_STATUS_TIMEOUT) -> int | None:
        n = (nick or "").strip()
        if not n:
            return None
        key = n.lower()
        cached = self._cache_get(key)
        if cached is not None:
            return cached
        fut = self._ns_pending.get(key)
        if fut and not fut.done():
            try:
                return int(await asyncio.wait_for(fut, timeout=timeout))
            except Exception:
                end = time.time() + NICKSERV_STATUS_GRACE
                while time.time() < end:
                    c = self._cache_get(key)
                    if c is not None:
                        return c
                    await asyncio.sleep(NICKSERV_STATUS_GRACE_POLL)
                return self._cache_get(key)

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
            end = time.time() + NICKSERV_STATUS_GRACE
            while time.time() < end:
                c = self._cache_get(key)
                if c is not None:
                    return c
                await asyncio.sleep(NICKSERV_STATUS_GRACE_POLL)
            return self._cache_get(key)
        finally:
            self._ns_pending.pop(key, None)

    # If event is from NickServ, tries to parse STATUS from text and update cache.
    async def _maybe_consume_nickserv_reply(self, ev: Event) -> None:
        if (ev.nick or "").strip().lower() != "nickserv":
            return
        self._consume_status_line(ev.text or "")

    # Hook for NOTICE: only handles NickServ; parses STATUS and updates cache.
    async def on_notice(self, bot, ev: Event) -> None:
        if (ev.nick or "").strip().lower() != "nickserv":
            return
        self._consume_status_line(ev.text or "")

    # Returns True only if nick has NickServ STATUS >= 3 (identified); sends error to reply_target otherwise.
    async def _require_identified(self, bot, nick: str, reply_target: str) -> bool:
        st = await self.nickserv_status(bot, nick)
        if st is None:
            await bot.privmsg(reply_target, f"{nick}: NickServ STATUS not verified (no reply/parse).")
            return False
        if st < 3:
            await bot.privmsg(reply_target, f"{nick}: not identified with NickServ (STATUS={st}). Identify first.")
            return False
        return True

    # Handles !whoami, !acl (adduser/deluser/usrlist/addserv/delserv/servlist) and !auth; returns True if handled.
    async def handle_core(self, bot, ev: Event) -> bool:
        await self._lazy_init(bot)
        await self._maybe_consume_nickserv_reply(ev)

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
            await bot.privmsg(ev.target, f"{ev.nick}: role={role} identity={principal_from_event(ev)}")
            return True

        if cmd == "acl":
            role = await self.effective_role(ev)
            if ROLE_ORDER.get(role, 0) < ROLE_ORDER["admin"]:
                await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires admin).")
                return True

            if len(parts) < 2:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl adduser|deluser|usrlist|addserv|delserv|servlist ...")
                return True

            sub = parts[1].lower()

            if sub not in ("usrlist", "servlist"):
                ok = await self._require_identified(bot, ev.nick or "", ev.target)
                if not ok:
                    return True

            if sub == "adduser":
                if len(parts) != 4:
                    await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl adduser <nick> <admin|contributor|user>")
                    return True
                nn = parts[2].strip()
                rr = _norm_role(parts[3])
                if rr == "guest":
                    await bot.privmsg(ev.target, f"{ev.nick}: group must be admin|contributor|user")
                    return True

                ok = await self._require_identified(bot, nn, ev.target)
                if not ok:
                    await bot.privmsg(ev.target, f"{ev.nick}: refusing to add {nn} because they are not identified.")
                    return True

                await self.store.acl_set_identity_role(nn, rr)
                await bot.privmsg(ev.target, f"ACL: added user {nn} -> {rr}.")
                return True

            if sub == "deluser":
                if len(parts) < 3:
                    await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl deluser <nick> <group>")
                    return True
                nn = parts[2].strip().lower()
                await self.store.acl_del_identity(nn)
                await bot.privmsg(ev.target, f"ACL: removed user {parts[2]}.")
                return True

            if sub == "usrlist":
                if len(parts) != 3:
                    await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl usrlist <admin|contributor|user>")
                    return True
                rr = _norm_role(parts[2])
                if rr == "guest":
                    await bot.privmsg(ev.target, f"{ev.nick}: group must be admin|contributor|user")
                    return True
                users = await self.store.acl_list_identities(rr)
                if not users:
                    await bot.privmsg(ev.target, f"ACL: {rr} users: (none)")
                else:
                    show = users[:30]
                    extra = "" if len(users) <= 30 else f" (+{len(users)-30} more)"
                    await bot.privmsg(ev.target, f"ACL: {rr} users: " + ", ".join(show) + extra)
                return True

            if sub == "addserv":
                if len(parts) != 4:
                    await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl addserv <service> <guest|user|contributor|admin>")
                    return True
                svc = _norm_cmd(parts[2])
                rr = _norm_role(parts[3])
                if parts[3].strip().lower() == "guest":
                    rr = "guest"
                await self.store.acl_set_command_min_role(svc, rr)
                await bot.privmsg(ev.target, f"ACL: service '{svc}' now requires {rr}.")
                return True

            if sub == "delserv":
                if len(parts) < 3:
                    await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl delserv <service> <group>")
                    return True
                svc = _norm_cmd(parts[2])
                await self.store.acl_del_command_min_role(svc)
                await bot.privmsg(ev.target, f"ACL: service '{svc}' override removed.")
                return True

            if sub == "servlist":
                pairs_raw = await self.store.acl_list_command_perms()
                if not pairs_raw:
                    await bot.privmsg(ev.target, "ACL: no service overrides set.")
                    return True
                pairs = [f"{c}->{r}" for c, r in pairs_raw]
                show = pairs[:25]
                extra = "" if len(pairs) <= 25 else f" (+{len(pairs)-25} more)"
                await bot.privmsg(ev.target, "ACL: overrides: " + " | ".join(show) + extra)
                return True

            await bot.privmsg(ev.target, f"{ev.nick}: unknown subcommand. Use: adduser, deluser, usrlist, addserv, delserv, servlist")
            return True

        if cmd != "auth":
            return False

        if len(parts) < 2:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !auth <password>")
            return True

        password = parts[1]
        digest = _sha256(password)

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
        await self.store.set_acl_session(principal_from_event(ev), granted, until)
        await bot.privmsg(ev.target, f"{ev.nick}: authenticated as {granted} until UTC midnight")
        return True

    # Resolves effective role and min_role (DB command override -> channel policy -> default); returns True if role >= min_role and guest whitelist satisfied.
    async def is_allowed(self, bot, ev: Event, cmd: str, info: dict) -> bool:
        role = await self.effective_role(ev)
        min_role = await self.store.acl_get_command_min_role(cmd)
        if min_role is None and info.get("service_id") and info.get("capability") and ev.channel:
            policy = await self.store.acl_get_policy(ev.channel, info["service_id"], info["capability"])
            if policy is not None:
                min_role = policy
        if min_role is None:
            min_role = _norm_role(str(info.get("min_role") or "user"))

        min_role = _norm_role(min_role)

        if ROLE_ORDER.get(role, 0) < ROLE_ORDER.get(min_role, 0):
            return False
        if role == "guest" and cmd not in self.cfg.guest_allowed_cmds:
            return False
        return True

    # Runs before command dispatch: parses command, skips auth/whoami/help/commands, calls is_allowed; sends denial message and returns False if not allowed.
    async def precheck(self, bot, ev: Event) -> bool:
        await self._lazy_init(bot)

        # Consume NickServ PRIVMSG replies (NOTICE handled via on_notice)
        await self._maybe_consume_nickserv_reply(ev)

        prefix = bot.cfg.get("command_prefix", "!")
        txt = (ev.text or "").strip()
        if not txt.startswith(prefix):
            return True

        cmdline = txt[len(prefix):].strip().lower()
        if not cmdline:
            return True

        parts = cmdline.split()
        cands = [" ".join(parts[:i]) for i in range(len(parts), 0, -1)]

        cmd = None
        info = None
        for c in cands:
            if c in bot.commands:
                cmd = c
                info = bot.commands[c]
                break

        if cmd is None:
            return True

        if cmd in ("auth", "whoami", "help", "commands"):
            return True

        allowed = await self.is_allowed(bot, ev, cmd, info)
        if not allowed:
            role = await self.effective_role(ev)
            min_role = await self.store.acl_get_command_min_role(cmd)
            if min_role is None and info.get("service_id") and info.get("capability") and ev.channel:
                min_role = await self.store.acl_get_policy(ev.channel, info["service_id"], info["capability"])
            if min_role is None:
                min_role = _norm_role(str(info.get("min_role") or "user"))
            if role == "guest" and cmd not in self.cfg.guest_allowed_cmds:
                await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires user).")
            else:
                await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires {min_role}).")
            return False

        return True

    # Deletes expired rows from acl_sessions; call periodically (e.g. from scheduler).
    async def prune(self) -> None:
        n = await self.store.prune_acl_sessions()
        if n:
            log.info("Pruned %d expired ACL sessions", n)