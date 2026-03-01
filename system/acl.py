from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass
from fnmatch import fnmatch
import re

from system.types import Event, Role

log = logging.getLogger("leobot.acl")

ROLE_ORDER = {"guest": 0, "user": 1, "contributor": 2, "admin": 3}

# Anope NickServ STATUS commonly yields a line containing: "<nick> <0-3> [account]"
# We parse by scanning tokens for "<nick> <0|1|2|3>" anywhere in the line.
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


def _norm_role(s: str | None) -> Role:
    r = (s or "").strip().lower()
    if r in ("guest", "user", "contributor", "admin"):
        return r  # type: ignore[return-value]
    # tolerate plurals/aliases
    if r in ("users",):
        return "user"
    if r in ("contributors",):
        return "contributor"
    return "guest"


def _norm_cmd(s: str) -> str:
    return (s or "").strip().lower().lstrip("!")


@dataclass(slots=True)
class ACLConfig:
    admins: list[dict]
    contributors: list[dict]
    users: list[str]
    guest_allowed_cmds: set[str]
    master: str


class ACL:
    """
    Backwards compatible ACL (keeps !auth / mask-based behaviour) + DB-backed ACL management.

    New DB tables:
      - acl_identities(ident, role, created_ts)            -- nick -> role
      - acl_command_perms(command, min_role, updated_ts)  -- command override per role

    New commands:
      - !acl adduser <nick> <admin|contributor|user>
      - !acl deluser <nick> <group>
      - !acl usrlist <admin|contributor|user>
      - !acl addserv <service> <guest|user|contributor|admin>
      - !acl delserv <service> <group>
      - !acl servlist

    Nick authenticity:
      - For mutating !acl operations, require NickServ STATUS == 3 for the caller.
      - For adduser, also require STATUS == 3 for the target nick (strict: “genuine user”).
      - Consumes NickServ replies delivered as NOTICE or PRIVMSG (network-dependent).
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

    # ---------------- schema/bootstrap ----------------

    async def on_ready(self, bot) -> None:
        # Create tables if they don't exist (safe)
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

        # Register ACL commands for help/commands listing (core system)
        # NOTE: bot.register_command expects bare command keys (no leading '!').
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
                help="Add user to group (DB). Usage: !acl adduser <nick> <admin|contributor|user>",
                category="System",
            )
            bot.register_command(
                "acl deluser",
                min_role="admin",
                mutating=True,
                help="Remove user from group (DB). Usage: !acl deluser <nick> <group>",
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
                help="Set command min role (DB override). Usage: !acl addserv <service> <guest|user|contributor|admin>",
                category="System",
            )
            bot.register_command(
                "acl delserv",
                min_role="admin",
                mutating=True,
                help="Remove command override. Usage: !acl delserv <service> <group>",
                category="System",
            )
            bot.register_command(
                "acl servlist",
                min_role="admin",
                mutating=False,
                help="List command overrides. Usage: !acl servlist",
                category="System",
            )
        except Exception:
            # Never fail startup because of help registration.
            pass

        # Bootstrap master from config as DB admin if DB has no admins yet
        if self.cfg.master:
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
        n = (nick or "").strip().lower()
        if not n:
            return None
        row = await self.store.fetchone("SELECT role FROM acl_identities WHERE ident=?", (n,))
        if not row:
            return None
        return _norm_role(str(row[0]))

    async def effective_role(self, ev: Event) -> Role:
        base = self._mask_role(ev)
        sess = await self.session_role(ev)

        # DB identity role (nick-based)
        db = await self.db_role(ev.nick or "")

        # highest wins
        best = base
        for r in (sess, db):
            if r and ROLE_ORDER.get(r, 0) > ROLE_ORDER.get(best, 0):
                best = r
        return best

    # ---------------- NickServ STATUS (Anope) ----------------

    async def nickserv_status(self, bot, nick: str, timeout: float = 3.0) -> int | None:
        """
        Ask NickServ STATUS <nick>. Returns 0..3 or None.
        Consumes replies received as NOTICE or PRIVMSG.
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

    def _consume_status_line(self, text: str) -> bool:
        txt = (text or "").strip()
        if not txt:
            return False

        # whole-line match
        m = _STATUS_PAIR_RE.match(txt)
        if m:
            nn = m.group(1)
            st = int(m.group(2))
            fut = self._ns_pending.get(nn.lower())
            if fut and not fut.done():
                fut.set_result(st)
                return True
            return False

        # token scan for "<nick> <0-3>"
        parts = txt.split()
        for i in range(len(parts) - 1):
            if parts[i + 1] in ("0", "1", "2", "3"):
                nn = parts[i]
                st = int(parts[i + 1])
                fut = self._ns_pending.get(nn.lower())
                if fut and not fut.done():
                    fut.set_result(st)
                    return True
        return False

    async def on_notice(self, bot, ev: Event) -> None:
        if (ev.nick or "").strip().lower() != "nickserv":
            return
        self._consume_status_line(ev.text or "")

    # IMPORTANT: We also consume PRIVMSG replies from NickServ, because you confirmed
    # NickServ replies as /msg when you /msg it.
    async def _maybe_consume_nickserv_privmsg(self, ev: Event) -> None:
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
        # Consume NickServ PRIVMSG replies even if they are not commands
        await self._maybe_consume_nickserv_privmsg(ev)

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

        # ---------------- !acl ... ----------------
        if cmd == "acl":
            caller_role = await self.effective_role(ev)
            if ROLE_ORDER.get(caller_role, 0) < ROLE_ORDER["admin"]:
                await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires admin).")
                return True

            if len(parts) < 2:
                await bot.privmsg(
                    ev.target,
                    f"{ev.nick}: usage: !acl adduser|deluser|usrlist|addserv|delserv|servlist ...",
                )
                return True

            sub = parts[1].lower()

            # Require the CALLER to be identified for mutating operations
            if sub not in ("usrlist", "servlist"):
                ok = await self._require_identified(bot, ev.nick or "", ev.target)
                if not ok:
                    return True

            # !acl adduser <nick> <group>
            if sub == "adduser":
                if len(parts) != 4:
                    await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl adduser <nick> <admin|contributor|user>")
                    return True
                nn = parts[2].strip()
                rr = _norm_role(parts[3])
                if rr == "guest":
                    await bot.privmsg(ev.target, f"{ev.nick}: group must be admin|contributor|user")
                    return True

                # Strict target authenticity: target must be STATUS=3 now
                ok = await self._require_identified(bot, nn, ev.target)
                if not ok:
                    await bot.privmsg(ev.target, f"{ev.nick}: refusing to add {nn} because they are not identified.")
                    return True

                now = int(time.time())
                await self.store.execute(
                    "INSERT OR REPLACE INTO acl_identities(ident, role, created_ts) VALUES(?,?,?)",
                    (nn.lower(), rr, now),
                )
                await bot.privmsg(ev.target, f"ACL: added user {nn} -> {rr}.")
                return True

            # !acl deluser <nick> <group>
            if sub == "deluser":
                if len(parts) < 3:
                    await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl deluser <nick> <group>")
                    return True
                nn = parts[2].strip().lower()
                await self.store.execute("DELETE FROM acl_identities WHERE ident=?", (nn,))
                await bot.privmsg(ev.target, f"ACL: removed user {parts[2]}.")
                return True

            # !acl usrlist <group>
            if sub == "usrlist":
                if len(parts) != 3:
                    await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl usrlist <admin|contributor|user>")
                    return True
                rr = _norm_role(parts[2])
                if rr == "guest":
                    await bot.privmsg(ev.target, f"{ev.nick}: group must be admin|contributor|user")
                    return True
                rows = await self.store.fetchall(
                    "SELECT ident FROM acl_identities WHERE role=? ORDER BY ident ASC",
                    (rr,),
                )
                users = [str(r[0]) for r in rows] if rows else []
                if not users:
                    await bot.privmsg(ev.target, f"ACL: {rr} users: (none)")
                else:
                    show = users[:30]
                    extra = "" if len(users) <= 30 else f" (+{len(users)-30} more)"
                    await bot.privmsg(ev.target, f"ACL: {rr} users: " + ", ".join(show) + extra)
                return True

            # !acl addserv <service> <group>
            if sub == "addserv":
                if len(parts) != 4:
                    await bot.privmsg(
                        ev.target,
                        f"{ev.nick}: usage: !acl addserv <service> <guest|user|contributor|admin>",
                    )
                    return True
                svc = _norm_cmd(parts[2])
                rr = _norm_role(parts[3])

                # allow explicit guest
                if parts[3].strip().lower() == "guest":
                    rr = "guest"

                now = int(time.time())
                await self.store.execute(
                    "INSERT OR REPLACE INTO acl_command_perms(command, min_role, updated_ts) VALUES(?,?,?)",
                    (svc, rr, now),
                )
                await bot.privmsg(ev.target, f"ACL: service '{svc}' now requires {rr}.")
                return True

            # !acl delserv <service> <group>
            if sub == "delserv":
                if len(parts) < 3:
                    await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl delserv <service> <group>")
                    return True
                svc = _norm_cmd(parts[2])
                await self.store.execute("DELETE FROM acl_command_perms WHERE command=?", (svc,))
                await bot.privmsg(ev.target, f"ACL: service '{svc}' override removed.")
                return True

            # !acl servlist
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

        # ---------------- !auth <password> (unchanged) ----------------
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
        # Also consume NickServ PRIVMSG replies here (dispatcher calls precheck for all PRIVMSG)
        await self._maybe_consume_nickserv_privmsg(ev)

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

        # DB override wins if present
        db_min = await self.store.fetchone("SELECT min_role FROM acl_command_perms WHERE command=?", (cmd,))
        if db_min:
            min_role = _norm_role(str(db_min[0]))
        else:
            min_role = _norm_role(str(info["min_role"]))

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