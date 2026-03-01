from __future__ import annotations

import logging
import time
import asyncio
import re
from dataclasses import dataclass

from system.types import Event, Role

log = logging.getLogger("leobot.acl")

ROLE_ORDER = {"guest": 0, "user": 1, "contributor": 2, "admin": 3}

# Anope NickServ STATUS reply is typically: "<nick> <status> [account]"
# status commonly: 0..3 where 3 == identified.
STATUS_TOKEN_RE = re.compile(r"^(\S+)\s+([0-3])(?:\s+\S+)?\s*$", re.IGNORECASE)


def _now() -> int:
    return int(time.time())


def _norm_role(r: str | None) -> Role:
    r2 = (r or "").strip().lower()
    if r2 in ("admin", "contributor", "user", "guest"):
        return r2  # type: ignore[return-value]
    return "guest"


def _utc_midnight_next(now: int | None = None) -> int:
    t = int(now or time.time())
    return t - (t % 86400) + 86400


@dataclass(slots=True)
class ACLConfig:
    # Only bootstrap + guest allowlist live in config now.
    master: str
    guest_allowed_cmds: set[str]


class ACL:
    """
    DB-backed ACL compatible with existing bot/dispatcher expectations.

    Required by Dispatcher:
      - handle_core(bot, ev) -> bool
      - precheck(bot, ev) -> bool

    Optional but used here:
      - on_ready(bot): create tables + bootstrap master
      - on_notice(bot, ev): parse NickServ STATUS replies (Anope)
    """

    def __init__(self, store, cfg: dict):
        self.store = store
        acl = cfg.get("acl", {}) if isinstance(cfg, dict) else {}
        self.cfg = ACLConfig(
            master=str(acl.get("master") or "").strip(),
            guest_allowed_cmds=set((acl.get("guest_allowed") or {}).get("commands") or []),
        )

        # NickServ STATUS pending futures: nick_lower -> Future[int]
        self._status_pending: dict[str, asyncio.Future] = {}

    # ---------------- DB bootstrapping ----------------

    async def on_ready(self, bot) -> None:
        # Create tables if missing (safe)
        await self._ensure_schema()

        # Bootstrap: if no admins exist and cfg.master is set, insert it as admin.
        if self.cfg.master:
            row = await self.store.fetchone("SELECT COUNT(*) FROM acl_identities WHERE role='admin'")
            admin_count = int(row[0]) if row else 0
            if admin_count == 0:
                await self.store.execute(
                    "INSERT OR REPLACE INTO acl_identities(ident, role, created_ts) VALUES(?,?,?)",
                    (self.cfg.master.lower(), "admin", _now()),
                )
                await bot.privmsg(self.cfg.master, "ACL: bootstrapped you as admin (master). Use !acl to manage ACL.")

    async def _ensure_schema(self) -> None:
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

    # ---------------- Role / perms lookups ----------------

    async def db_role(self, nick: str) -> Role:
        n = (nick or "").strip().lower()
        if not n:
            return "guest"
        row = await self.store.fetchone("SELECT role FROM acl_identities WHERE ident=?", (n,))
        if not row:
            return "guest"
        return _norm_role(str(row[0]))

    async def db_min_role_for_cmd(self, cmd: str) -> Role | None:
        c = (cmd or "").strip().lower()
        if not c:
            return None
        row = await self.store.fetchone("SELECT min_role FROM acl_command_perms WHERE command=?", (c,))
        if not row:
            return None
        return _norm_role(str(row[0]))

    # ---------------- NickServ STATUS (Anope) ----------------

    async def nickserv_status(self, bot, nick: str, timeout: float = 3.0) -> int | None:
        """
        Query NickServ STATUS <nick>.
        Returns 0..3, or None if unknown/timeout.

        IMPORTANT: This relies on bot.py dispatching NOTICE into on_notice,
        because Anope usually replies via NOTICE.
        """
        n = (nick or "").strip()
        if not n:
            return None

        key = n.lower()
        fut = self._status_pending.get(key)
        if fut and not fut.done():
            try:
                return int(await asyncio.wait_for(fut, timeout=timeout))
            except Exception:
                return None

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._status_pending[key] = fut

        try:
            await bot.privmsg("NickServ", f"STATUS {n}")
        except Exception:
            self._status_pending.pop(key, None)
            return None

        try:
            val = await asyncio.wait_for(fut, timeout=timeout)
            return int(val)
        except Exception:
            return None
        finally:
            self._status_pending.pop(key, None)

    async def on_notice(self, bot, ev: Event) -> None:
        # Accept NickServ replies via NOTICE
        if (ev.nick or "").strip().lower() != "nickserv":
            return
        self._consume_status_reply(ev.text or "")

    def _consume_status_reply(self, text: str) -> bool:
        """
        Parse an Anope STATUS reply. Common formats include:
          "<nick> <status> <account>"
        Sometimes with additional prefix text; we scan tokens for "<nick> <0-3>".
        """
        txt = (text or "").strip()
        if not txt:
            return False

        # First attempt: whole-line match
        m = STATUS_TOKEN_RE.match(txt)
        if m:
            nick = m.group(1)
            status = int(m.group(2))
            fut = self._status_pending.get(nick.lower())
            if fut and not fut.done():
                fut.set_result(status)
                return True
            return False

        # Fallback: scan tokens for "<nick> <0-3>"
        parts = txt.split()
        for i in range(len(parts) - 1):
            if parts[i+1] in ("0", "1", "2", "3"):
                nick = parts[i]
                status = int(parts[i+1])
                fut = self._status_pending.get(nick.lower())
                if fut and not fut.done():
                    fut.set_result(status)
                    return True

        return False

    async def require_identified(self, bot, nick: str, reply_target: str) -> bool:
        """
        Strict: require NickServ STATUS == 3 (identified) for ACL mutations.
        """
        st = await self.nickserv_status(bot, nick)
        if st is None:
            await bot.privmsg(reply_target, f"{nick}: cannot verify NickServ STATUS (no usable reply).")
            return False
        if st < 3:
            await bot.privmsg(reply_target, f"{nick}: not identified with NickServ (STATUS={st}). Identify first.")
            return False
        return True

    # ---------------- Core handler ----------------

    async def handle_core(self, bot, ev: Event) -> bool:
        """
        Handles:
          - !whoami
          - !acl ...
        """
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
            role = await self.db_role(ev.nick)
            await bot.privmsg(ev.target, f"{ev.nick}: role={role}")
            return True

        if cmd != "acl":
            return False

        # Admin-only
        caller_role = await self.db_role(ev.nick)
        if ROLE_ORDER.get(caller_role, 0) < ROLE_ORDER["admin"]:
            await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires admin).")
            return True

        if len(parts) < 2:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl adduser|deluser|usrlist|addserv|delserv|servlist ...")
            return True

        sub = parts[1].lower()

        # Require the *caller* to be identified for mutating subcommands
        if sub not in ("usrlist", "servlist"):
            ok = await self.require_identified(bot, ev.nick, ev.target)
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

            # Strict target validation: require target is identified *right now*.
            ok = await self.require_identified(bot, nn, ev.target)
            if not ok:
                await bot.privmsg(ev.target, f"{ev.nick}: refusing to add {nn} because they are not identified.")
                return True

            await self.store.execute(
                "INSERT OR REPLACE INTO acl_identities(ident, role, created_ts) VALUES(?,?,?)",
                (nn.lower(), rr, _now()),
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
            users = [r[0] for r in rows] if rows else []
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
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl addserv <service> <guest|user|contributor|admin>")
                return True
            svc = parts[2].strip().lower()
            rr = _norm_role(parts[3])

            # allow explicit guest
            if parts[3].strip().lower() == "guest":
                rr = "guest"

            await self.store.execute(
                "INSERT OR REPLACE INTO acl_command_perms(command, min_role, updated_ts) VALUES(?,?,?)",
                (svc, rr, _now()),
            )
            await bot.privmsg(ev.target, f"ACL: service '{svc}' now requires {rr}.")
            return True

        # !acl delserv <service> <group>
        if sub == "delserv":
            if len(parts) < 3:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl delserv <service> <group>")
                return True
            svc = parts[2].strip().lower()
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
            pairs = [f"{r[0]}->{r[1]}" for r in rows]
            show = pairs[:25]
            extra = "" if len(pairs) <= 25 else f" (+{len(pairs)-25} more)"
            await bot.privmsg(ev.target, "ACL: overrides: " + " | ".join(show) + extra)
            return True

        await bot.privmsg(ev.target, f"{ev.nick}: unknown subcommand. Use: adduser, deluser, usrlist, addserv, delserv, servlist")
        return True

    # ---------------- Dispatcher precheck ----------------

    async def precheck(self, bot, ev: Event) -> bool:
        """
        This is called by Dispatcher for every PRIVMSG command.
        Must not crash. Must return True/False.
        """
        prefix = bot.cfg.get("command_prefix", "!")
        txt = (ev.text or "").strip()
        if not txt.startswith(prefix):
            return True

        cmdline = txt[len(prefix):].strip().lower()
        if not cmdline:
            return True

        # longest match command key (supports "service enable" style commands)
        parts = cmdline.split()
        cands = [" ".join(parts[:i]) for i in range(len(parts), 0, -1)]

        cmd = None
        info = None
        for c in cands:
            if c in bot.commands:
                cmd = c
                info = bot.commands[c]
                break

        if cmd is None or info is None:
            return True

        # Always allow these core commands
        if cmd in ("help", "commands", "whoami", "acl"):
            return True

        role = await self.db_role(ev.nick)

        # DB override wins if present
        db_min = await self.db_min_role_for_cmd(cmd)
        min_role = db_min if db_min is not None else info["min_role"]

        if ROLE_ORDER.get(role, 0) < ROLE_ORDER.get(min_role, 0):
            await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires {min_role}).")
            return False

        # guest allowlist (optional, still supported)
        if role == "guest" and self.cfg.guest_allowed_cmds and cmd not in self.cfg.guest_allowed_cmds:
            await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires user).")
            return False

        return True

    async def prune(self) -> None:
        """
        Kept for compatibility. No sessions in this design.
        """
        return