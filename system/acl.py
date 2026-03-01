# system/acl.py
import asyncio
import re
import time
from typing import Dict, Optional, Tuple, List

# NOTE:
# - ACC is not reliably available on Anope.
# - Anope provides NickServ STATUS via module ns_status.
# - Services may reply via NOTICE or PRIVMSG depending on anope config (useprivmsg).

ROLE_ORDER = {"guest": 0, "users": 1, "contributors": 2, "admin": 3}
VALID_ROLES = set(ROLE_ORDER.keys())

# Anope STATUS reply format (documented): "nickname status-code account"
# status-code meaning commonly:
# 0: no such user online or nick not registered
# 1: user not recognized
# 2: recognized via access list
# 3: identified (password/cert)
STATUS_RE = re.compile(r"^\s*(\S+)\s+([0-3])\s*(?:\S+)?\s*$", re.IGNORECASE)

def _now() -> int:
    return int(time.time())

def _norm_role(r: str) -> str:
    r = (r or "").strip().lower()
    if r == "user":
        r = "users"
    if r == "contributor":
        r = "contributors"
    return r if r in VALID_ROLES else "guest"

def _norm_cmd(c: str) -> str:
    return (c or "").strip().lower().lstrip("!")

class ACL:
    """
    DB-backed ACL service.

    Config:
      - acl.master: bootstrap nick added as admin if there are no admins in DB yet.

    Commands:
      - !acl adduser <nick> <admin|contributors|users>
      - !acl deluser <nick> <group>           (group accepted but not required)
      - !acl usrlist <group>
      - !acl addserv <service> <group>        (service = command key)
      - !acl delserv <service> <group>        (group accepted but not required)
      - !acl servlist
      - !whoami
    """

    def __init__(self, bot, store):
        self.bot = bot
        self.store = store

        # NickServ status pending: nick_lower -> future(int status)
        self._status_pending: Dict[str, asyncio.Future] = {}

        # cache for service perms from DB: cmd -> (min_role, ts)
        self._serv_cache: Dict[str, Tuple[Optional[str], float]] = {}
        self._serv_cache_ttl = 30.0

    # ---------------- lifecycle ----------------

    async def init_db(self) -> None:
        # Create tables if not present (safe)
        await self.store.ensure_acl_schema()

        # Bootstrap master
        master = ""
        if isinstance(self.bot.cfg, dict):
            master = str((self.bot.cfg.get("acl", {}) if isinstance(self.bot.cfg.get("acl", {}), dict) else {}).get("master", "")).strip()

        if master:
            admins = await self.store.acl_count_admins()
            if admins == 0:
                await self.store.acl_set_user(master, "admin")
                await self.bot.privmsg(master, "ACL: bootstrapped you as admin (master). Use !acl to manage ACL.")

    # ---------------- NickServ STATUS ----------------

    async def nickserv_status(self, nick: str, timeout: float = 3.0) -> Optional[int]:
        """
        Query NickServ STATUS for a nick.

        Returns:
          0..3 or None on timeout/unknown.

        Important:
          Services may reply via NOTICE or PRIVMSG. We accept both.
        """
        nick = (nick or "").strip()
        if not nick:
            return None

        key = nick.lower()
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
            await self.bot.privmsg("NickServ", f"STATUS {nick}")
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

    def _consume_status_reply(self, text: str) -> bool:
        """
        Parses a possible STATUS reply, completes the corresponding future if pending.
        Returns True if consumed.
        """
        txt = (text or "").strip()
        if not txt:
            return False

        # Some networks prefix replies with "STATUS" or other words; attempt to locate
        # a "<nick> <digit>" pattern anywhere in the string.
        # First try the simple whole-line format:
        m = STATUS_RE.match(txt)
        if not m:
            # fallback: find last occurrence of "nick digit"
            parts = txt.split()
            if len(parts) >= 2 and parts[-2].strip() and parts[-1].isdigit():
                # not good enough; require digit 0-3 in position 2
                pass
            # Try scanning tokens:
            for i in range(len(parts) - 1):
                if parts[i] and parts[i+1] in ("0", "1", "2", "3"):
                    nick = parts[i]
                    status = int(parts[i+1])
                    fut = self._status_pending.get(nick.lower())
                    if fut and not fut.done():
                        fut.set_result(status)
                        return True
            return False

        nick = m.group(1)
        status = int(m.group(2))
        fut = self._status_pending.get(nick.lower())
        if fut and not fut.done():
            fut.set_result(status)
            return True
        return False

    async def on_notice(self, ev) -> None:
        # NickServ may reply via NOTICE
        if (ev.nick or "").strip().lower() != "nickserv":
            return
        self._consume_status_reply(ev.text)

    async def on_privmsg(self, ev) -> None:
        # NickServ may reply via PRIVMSG if useprivmsg is enabled in Anope.
        if (ev.nick or "").strip().lower() == "nickserv":
            if self._consume_status_reply(ev.text):
                return

        # Handle commands
        await self.handle_command(ev)

    # ---------------- ACL logic ----------------

    async def role_for(self, nick: str) -> str:
        nick = (nick or "").strip().lower()
        if not nick:
            return "guest"
        role = await self.store.acl_get_user_role(nick)
        return _norm_role(role) if role else "guest"

    async def min_role_for_service(self, cmd: str) -> Optional[str]:
        cmd = _norm_cmd(cmd)
        if not cmd:
            return None

        cached = self._serv_cache.get(cmd)
        if cached and (time.time() - cached[1]) < self._serv_cache_ttl:
            return cached[0]

        mr = await self.store.acl_get_service_min_role(cmd)
        mr = _norm_role(mr) if mr else None
        self._serv_cache[cmd] = (mr, time.time())
        return mr

    async def require_identified(self, nick: str, target: str) -> bool:
        """
        Strict authenticity requirement:
          - must be STATUS==3 (identified)
        """
        status = await self.nickserv_status(nick)
        if status is None:
            await self.bot.privmsg(target, f"{nick}: cannot verify NickServ STATUS (no reply). Check ns_status module + NOTICE/PRIVMSG handling.")
            return False
        if status < 3:
            await self.bot.privmsg(target, f"{nick}: not identified with NickServ (STATUS={status}). Identify first.")
            return False
        return True

    # ---------------- command handling ----------------

    async def handle_command(self, ev) -> None:
        prefix = self.bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()
        if not text.startswith(prefix):
            return

        parts = text[len(prefix):].strip().split()
        if not parts:
            return

        cmd = _norm_cmd(parts[0])

        if cmd == "whoami":
            role = await self.role_for(ev.nick)
            status = await self.nickserv_status(ev.nick)
            s = "unknown" if status is None else str(status)
            await self.bot.privmsg(ev.target, f"{ev.nick}: role={role}, NickServ_STATUS={s} (3=identified)")
            return

        if cmd != "acl":
            return

        # Permission: ACL command itself requires admin role in DB
        caller_role = await self.role_for(ev.nick)
        if ROLE_ORDER[caller_role] < ROLE_ORDER["admin"]:
            await self.bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires admin).")
            return

        # Authenticity for mutating operations: must be identified
        # (All !acl subcommands are mutating except list commands; keep it simple/strict.)
        if len(parts) >= 2 and parts[1].lower() not in ("usrlist", "servlist"):
            if not await self.require_identified(ev.nick, ev.target):
                return

        if len(parts) < 2:
            await self.bot.privmsg(ev.target, f"{ev.nick}: usage: !acl adduser|deluser|usrlist|addserv|delserv|servlist ...")
            return

        sub = parts[1].lower()

        # !acl adduser <nick> <group>
        if sub == "adduser":
            if len(parts) != 4:
                await self.bot.privmsg(ev.target, f"{ev.nick}: usage: !acl adduser <nick> <admin|contributors|users>")
                return
            nn = parts[2].strip()
            rr = _norm_role(parts[3])

            if rr == "guest":
                await self.bot.privmsg(ev.target, f"{ev.nick}: group must be admin|contributors|users")
                return

            # Strict target verification: ensure the target is identified (genuine user)
            ok = await self.require_identified(nn, ev.target)
            if not ok:
                await self.bot.privmsg(ev.target, f"{ev.nick}: refusing to add {nn} because they are not currently identified.")
                return

            await self.store.acl_set_user(nn, rr)
            await self.bot.privmsg(ev.target, f"ACL: added user {nn} -> {rr}.")
            return

        # !acl deluser <nick> <group>
        if sub == "deluser":
            if len(parts) < 3:
                await self.bot.privmsg(ev.target, f"{ev.nick}: usage: !acl deluser <nick> <group>")
                return
            nn = parts[2].strip()
            await self.store.acl_del_user(nn)
            await self.bot.privmsg(ev.target, f"ACL: removed user {nn}.")
            return

        # !acl usrlist <group>
        if sub == "usrlist":
            if len(parts) != 3:
                await self.bot.privmsg(ev.target, f"{ev.nick}: usage: !acl usrlist <admin|contributors|users>")
                return
            rr = _norm_role(parts[2])
            if rr == "guest":
                await self.bot.privmsg(ev.target, f"{ev.nick}: group must be admin|contributors|users")
                return
            users = await self.store.acl_list_users(rr)
            if not users:
                await self.bot.privmsg(ev.target, f"ACL: {rr} users: (none)")
                return
            show = users[:30]
            extra = "" if len(users) <= 30 else f" (+{len(users)-30} more)"
            await self.bot.privmsg(ev.target, f"ACL: {rr} users: " + ", ".join(show) + extra)
            return

        # !acl addserv <service> <group>
        if sub == "addserv":
            if len(parts) != 4:
                await self.bot.privmsg(ev.target, f"{ev.nick}: usage: !acl addserv <service> <guest|users|contributors|admin>")
                return
            svc = _norm_cmd(parts[2])
            rr = _norm_role(parts[3])
            if parts[3].lower() == "guest":
                rr = "guest"
            if rr not in VALID_ROLES:
                await self.bot.privmsg(ev.target, f"{ev.nick}: group must be guest|users|contributors|admin")
                return
            await self.store.acl_set_service(svc, rr)
            self._serv_cache.pop(svc, None)
            await self.bot.privmsg(ev.target, f"ACL: service '{svc}' now requires {rr}.")
            return

        # !acl delserv <service> <group>
        if sub == "delserv":
            if len(parts) < 3:
                await self.bot.privmsg(ev.target, f"{ev.nick}: usage: !acl delserv <service> <group>")
                return
            svc = _norm_cmd(parts[2])
            await self.store.acl_del_service(svc)
            self._serv_cache.pop(svc, None)
            await self.bot.privmsg(ev.target, f"ACL: service '{svc}' override removed.")
            return

        # !acl servlist
        if sub == "servlist":
            rows = await self.store.acl_list_services()
            if not rows:
                await self.bot.privmsg(ev.target, "ACL: no service overrides set.")
                return
            pairs = [f"{cmd}->{role}" for (cmd, role) in rows]
            show = pairs[:25]
            extra = "" if len(pairs) <= 25 else f" (+{len(pairs)-25} more)"
            await self.bot.privmsg(ev.target, "ACL: overrides: " + " | ".join(show) + extra)
            return

        await self.bot.privmsg(ev.target, f"{ev.nick}: unknown subcommand. Use: adduser, deluser, usrlist, addserv, delserv, servlist")
        return