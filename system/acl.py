from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass
from fnmatch import fnmatch
import re
from typing import Optional

from system.types import Event, Role

log = logging.getLogger("leobot.acl")

ROLE_ORDER = {"guest": 0, "user": 1, "contributor": 2, "admin": 3}

# Accept numeric status 0..3 in common NickServ STATUS response formats.
_STATUS_DIGITS = {"0", "1", "2", "3"}


def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _identity_key(ev: Event) -> str:
    user = (ev.user or "").strip()
    host = (ev.host or "").strip()
    if user and host:
        return f"{user}@{host}".lower()
    return (ev.nick or "").strip().lower()


def _utc_midnight_next(now: int | None = None) -> int:
    t = int(now or time.time())
    return t - (t % 86400) + 86400


def _norm_role(s: str | None) -> Role:
    r = (s or "").strip().lower()
    if r in ("guest", "user", "contributor", "admin"):
        return r  # type: ignore[return-value]
    # tolerate plurals/aliases
    if r == "users":
        return "user"
    if r == "contributors":
        return "contributor"
    return "guest"


def _norm_cmd(s: str) -> str:
    return (s or "").strip().lower().lstrip("!")


def _clean_token(tok: str) -> str:
    """
    Normalize IRC-ish tokens for matching:
      - lower-case
      - strip common punctuation that appears in NickServ output: "Ged:" "Ged," etc
    """
    t = (tok or "").strip().lower()
    # strip leading/trailing punctuation often seen in STATUS output
    t = t.strip(",:;.!?()[]{}<>\"'")
    return t


@dataclass(slots=True)
class ACLConfig:
    admins: list[dict]
    contributors: list[dict]
    users: list[str]
    guest_allowed_cmds: set[str]
    master: str


class ACL:
    """
    Backwards compatible ACL (keeps !auth / session / guest allowlist) + DB ACL management commands.

    IMPORTANT architectural detail:
      - ACL is a CORE handler. Your bot only calls on_ready() for SERVICES.
      - Therefore, schema creation + command registration must happen lazily from precheck()/handle_core().

    CRITICAL API CONTRACTS (used elsewhere):
      - help.py calls: await bot.acl.effective_role(ev)
      - dispatcher.py calls: await bot.acl.precheck(bot, ev)
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

        # Lazy one-time init flags
        self._schema_ready = False
        self._commands_registered = False
        self._bootstrapped_master = False

        # Store bot reference for places that only receive `ev` (e.g. help.py -> effective_role(ev))
        self._bot: Optional[object] = None

    def _bind_bot(self, bot) -> None:
        if self._bot is None:
            self._bot = bot

    # ---------------- lazy init ----------------

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

    async def _bootstrap_master(self, bot) -> None:
        if self._bootstrapped_master:
            return
        if not self.cfg.master:
            self._bootstrapped_master = True
            return

        await self._ensure_schema()

        row = await self.store.fetchone("SELECT COUNT(*) FROM acl_identities WHERE role='admin'")
        admin_count = int(row[0]) if row else 0
        if admin_count == 0:
            now = int(time.time())
            await self.store.execute(
                "INSERT OR REPLACE INTO acl_identities(ident, role, created_ts) VALUES(?,?,?)",
                (self.cfg.master.lower(), "admin", now),
            )
            try:
                await bot.privmsg(self.cfg.master, "ACL: bootstrapped you as admin (master).")
            except Exception:
                pass

        self._bootstrapped_master = True

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

    async def _lazy_init(self, bot) -> None:
        """
        Perform all lazy init tasks that require `bot`.
        This is safe to call repeatedly.
        """
        self._bind_bot(bot)
        self._ensure_commands_registered(bot)
        await self._ensure_schema()
        await self._bootstrap_master(bot)

    # ---------------- legacy mask role (unchanged) ----------------

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
        return _norm_role(role)

    async def db_role(self, nick: str) -> Role | None:
        await self._ensure_schema()
        n = (nick or "").strip().lower()
        if not n:
            return None
        row = await self.store.fetchone("SELECT role FROM acl_identities WHERE ident=?", (n,))
        if not row:
            return None
        return _norm_role(str(row[0]))

    async def effective_role(self, ev: Event) -> Role:
        """
        Signature MUST remain `effective_role(ev)` (help.py depends on this).
        If we have a bot bound, we ensure command registration + bootstrap has occurred.
        """
        if self._bot is not None:
            try:
                # best effort: ensure help visibility & bootstrap for callers that only hit help first
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

    # ---------------- NickServ STATUS parsing ----------------

    def _consume_status_line(self, text: str) -> bool:
        """
        Extract status for any pending nick from arbitrary NickServ text.
        We only accept numeric 0..3.
        """
        txt = (text or "").strip()
        if not txt:
            return False

        raw_parts = txt.split()
        if len(raw_parts) < 2:
            return False

        parts = [_clean_token(p) for p in raw_parts]

        # Pattern 1: "<nick> <digit>"
        for i in range(len(parts) - 1):
            if parts[i + 1] in _STATUS_DIGITS:
                nick_tok = parts[i]
                fut = self._ns_pending.get(nick_tok)
                if fut and not fut.done():
                    fut.set_result(int(parts[i + 1]))
                    return True

        # Pattern 2: "status <nick> <digit>"
        for i in range(len(parts) - 2):
            if parts[i] == "status" and parts[i + 2] in _STATUS_DIGITS:
                nick_tok = parts[i + 1]
                fut = self._ns_pending.get(nick_tok)
                if fut and not fut.done():
                    fut.set_result(int(parts[i + 2]))
                    return True

        # Pattern 3: nick appears, then within next 6 tokens a digit (handles "status for nick: 3")
        for pending_nick, fut in list(self._ns_pending.items()):
            if not fut or fut.done():
                continue
            if pending_nick in parts:
                idxs = [i for i, p in enumerate(parts) if p == pending_nick]
                for idx in idxs:
                    window = parts[idx + 1 : idx + 7]
                    for w in window:
                        if w in _STATUS_DIGITS:
                            fut.set_result(int(w))
                            return True

        return False

    async def nickserv_status(self, bot, nick: str, timeout: float = 3.0) -> int | None:
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

    async def _maybe_consume_nickserv_reply(self, ev: Event) -> None:
        if (ev.nick or "").strip().lower() != "nickserv":
            return
        self._consume_status_line(ev.text or "")

    async def on_notice(self, bot, ev: Event) -> None:
        # If your services ever reply via NOTICE, we still accept it.
        if (ev.nick or "").strip().lower() != "nickserv":
            return
        self._consume_status_line(ev.text or "")

    async def _require_identified(self, bot, nick: str, reply_target: str) -> bool:
        st = await self.nickserv_status(bot, nick)
        if st is None:
            await bot.privmsg(reply_target, f"{nick}: NickServ STATUS not verified (no reply/parse).")
            return False
        if st < 3:
            await bot.privmsg(reply_target, f"{nick}: not identified with NickServ (STATUS={st}). Identify first.")
            return False
        return True

    # ---------------- core commands ----------------

    async def handle_core(self, bot, ev: Event) -> bool:
        await self._lazy_init(bot)

        # Always try to consume NickServ replies (they are PRIVMSG in your network)
        await self._maybe_consume_nickserv_reply(ev)

        prefix = bot.cfg.get("command_prefix", "!")
        txt = (ev.text or "").strip()
        if not txt.startswith(prefix):
            return False

        cmdline = txt[len(prefix) :].strip()
        if not cmdline:
            return False

        parts = cmdline.split()
        cmd = parts[0].lower()

        if cmd == "whoami":
            role = await self.effective_role(ev)
            await bot.privmsg(ev.target, f"{ev.nick}: role={role} identity={_identity_key(ev)}")
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

            # Require caller to be identified for mutations (strict)
            if sub not in ("usrlist", "servlist"):
                ok = await self._require_identified(bot, ev.nick or "", ev.target)
                if not ok:
                    return True

            # (schema already ensured by _lazy_init)

            if sub == "adduser":
                if len(parts) != 4:
                    await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl adduser <nick> <admin|contributor|user>")
                    return True
                nn = parts[2].strip()
                rr = _norm_role(parts[3])
                if rr == "guest":
                    await bot.privmsg(ev.target, f"{ev.nick}: group must be admin|contributor|user")
                    return True

                # strict: target must be identified too
                ok = await self._require_identified(bot, nn, ev.target)
                if not ok:
                    await bot.privmsg(ev.target, f"{ev.nick}: refusing to add {nn} because they are not identified.")
                    return True

                await self.store.execute(
                    "INSERT OR REPLACE INTO acl_identities(ident, role, created_ts) VALUES(?,?,?)",
                    (nn.lower(), rr, int(time.time())),
                )
                await bot.privmsg(ev.target, f"ACL: added user {nn} -> {rr}.")
                return True

            if sub == "deluser":
                if len(parts) < 3:
                    await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl deluser <nick> <group>")
                    return True
                nn = parts[2].strip().lower()
                await self.store.execute("DELETE FROM acl_identities WHERE ident=?", (nn,))
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
                rows = await self.store.fetchall("SELECT ident FROM acl_identities WHERE role=? ORDER BY ident ASC", (rr,))
                users = [str(r[0]) for r in rows] if rows else []
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
                await self.store.execute(
                    "INSERT OR REPLACE INTO acl_command_perms(command, min_role, updated_ts) VALUES(?,?,?)",
                    (svc, rr, int(time.time())),
                )
                await bot.privmsg(ev.target, f"ACL: service '{svc}' now requires {rr}.")
                return True

            if sub == "delserv":
                if len(parts) < 3:
                    await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl delserv <service> <group>")
                    return True
                svc = _norm_cmd(parts[2])
                await self.store.execute("DELETE FROM acl_command_perms WHERE command=?", (svc,))
                await bot.privmsg(ev.target, f"ACL: service '{svc}' override removed.")
                return True

            if sub == "servlist":
                rows = await self.store.fetchall(
                    "SELECT command, min_role FROM acl_command_perms ORDER BY min_role DESC, command ASC",
                    (),
                )
                if not rows:
                    await bot.privmsg(ev.target, "ACL: no service overrides set.")
                    return True
                pairs = [f"{str(r[0])}->{str(r[1])}" for r in rows]
                show = pairs[:25]
                extra = "" if len(pairs) <= 25 else f" (+{len(pairs)-25} more)"
                await bot.privmsg(ev.target, "ACL: overrides: " + " | ".join(show) + extra)
                return True

            await bot.privmsg(ev.target, f"{ev.nick}: unknown subcommand. Use: adduser, deluser, usrlist, addserv, delserv, servlist")
            return True

        # Existing !auth behaviour remains (unchanged)
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
        await self.store.set_acl_session(_identity_key(ev), granted, until)
        await bot.privmsg(ev.target, f"{ev.nick}: authenticated as {granted} until UTC midnight")
        return True

    async def precheck(self, bot, ev: Event) -> bool:
        await self._lazy_init(bot)

        # Consume NickServ replies (dispatcher calls precheck for ALL PRIVMSG)
        await self._maybe_consume_nickserv_reply(ev)

        prefix = bot.cfg.get("command_prefix", "!")
        txt = (ev.text or "").strip()
        if not txt.startswith(prefix):
            return True

        cmdline = txt[len(prefix) :].strip().lower()
        if not cmdline:
            return True

        # choose longest matching registered command ("service enable" etc)
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

        role = await self.effective_role(ev)

        row = await self.store.fetchone("SELECT min_role FROM acl_command_perms WHERE command=?", (cmd,))
        if row:
            min_role = _norm_role(str(row[0]))
        else:
            min_role = _norm_role(str(info["min_role"]))

        if ROLE_ORDER.get(role, 0) < ROLE_ORDER.get(min_role, 0):
            await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires {min_role}).")
            return False

        if role == "guest" and cmd not in self.cfg.guest_allowed_cmds:
            await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires user).")
            return False

        return True

    async def prune(self) -> None:
        n = await self.store.prune_acl_sessions()
        if n:
            log.info("Pruned %d expired ACL sessions", n)