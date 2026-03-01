from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass

from system.types import Event, Role

log = logging.getLogger("leobot.acl")

ROLE_ORDER = {"guest": 0, "user": 1, "contributor": 2, "admin": 3}
VALID_ROLES = set(ROLE_ORDER.keys())

# Anope-style: "ACC <nick> <level>" (0..3)
ACC_RE = re.compile(r"^\s*ACC\s+(\S+)\s+([0-3])\s*$", re.IGNORECASE)


def _now() -> int:
    return int(time.time())


def _norm_role(role: str) -> Role:
    r = (role or "").strip().lower()
    return r if r in VALID_ROLES else "guest"


def _norm_nick(nick: str) -> str:
    return (nick or "").strip().lower()


def _norm_cmd(cmd: str) -> str:
    # stored exactly as bot.commands keys (lowercase, no prefix)
    return (cmd or "").strip().lower().lstrip("!")


@dataclass(slots=True)
class ACLConfig:
    master: str


class ACL:
    """DB-backed ACL.

    Design goals:
      - config.json only bootstraps a single "master" identity
      - user/group membership is stored in sqlite
      - per-command minimum-role overrides stored in sqlite
      - mutating admin/contributor operations require NickServ identify (Anope ACC>=2)

    Identity model (v1): nick-based (lowercased nick). This is deliberate simplicity.
    Upgrade path: switch to NickServ account-based identities later.
    """

    def __init__(self, store, cfg: dict):
        self.store = store
        acl_cfg = cfg.get("acl", {}) if isinstance(cfg, dict) else {}
        self.cfg = ACLConfig(master=str(acl_cfg.get("master") or "").strip())

        # Pending NickServ ACC queries: nick -> Future[int]
        self._acc_wait: dict[str, asyncio.Future] = {}

        # Short cache for per-command overrides
        self._perm_cache: dict[str, tuple[str, float]] = {}
        self._perm_cache_ttl = 15.0

    # -------------------------
    # Bootstrap
    # -------------------------
    async def on_ready(self, bot) -> None:
        """Ensure a master admin exists on first boot."""
        master = _norm_nick(self.cfg.master)
        if not master:
            return

        try:
            n_admin = await self.store.acl_count_admins()
        except Exception:
            log.exception("Failed to query admin count")
            return

        if n_admin > 0:
            return

        try:
            await self.store.acl_set_identity_role(master, "admin")
        except Exception:
            log.exception("Failed to bootstrap master admin")
            return

        try:
            await bot.privmsg(self.cfg.master, "ACL: bootstrapped you as admin (master). Use !acl to manage users/services.")
        except Exception:
            pass

    # -------------------------
    # NickServ ACC
    # -------------------------
    async def _nickserv_acc(self, bot, nick: str, timeout: float = 3.0) -> int | None:
        """Return NickServ ACC level (0..3) or None if unknown/timeout."""
        n = (nick or "").strip()
        if not n:
            return None

        key = _norm_nick(n)
        fut = self._acc_wait.get(key)
        if fut and not fut.done():
            try:
                return int(await asyncio.wait_for(fut, timeout=timeout))
            except Exception:
                return None

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._acc_wait[key] = fut

        try:
            await bot.privmsg("NickServ", f"ACC {n}")
        except Exception:
            self._acc_wait.pop(key, None)
            return None

        try:
            v = await asyncio.wait_for(fut, timeout=timeout)
            return int(v)
        except Exception:
            return None
        finally:
            self._acc_wait.pop(key, None)

    async def on_notice(self, bot, ev: Event) -> None:
        """Capture NickServ ACC replies."""
        if _norm_nick(ev.nick) != "nickserv":
            return
        txt = (ev.text or "").strip()
        m = ACC_RE.match(txt)
        if not m:
            return
        nick = _norm_nick(m.group(1))
        acc = int(m.group(2))

        fut = self._acc_wait.get(nick)
        if fut and not fut.done():
            fut.set_result(acc)

    async def _require_identified(self, bot, ev: Event, *, why: str) -> bool:
        """Require the caller to be identified (ACC>=2)."""
        acc = await self._nickserv_acc(bot, ev.nick)
        if acc is None:
            await bot.privmsg(ev.target, f"{ev.nick}: cannot verify NickServ status (ACC unknown). {why}")
            return False
        if acc < 2:
            await bot.privmsg(ev.target, f"{ev.nick}: not identified with NickServ (ACC={acc}). {why}")
            return False
        return True

    # -------------------------
    # Roles
    # -------------------------
    async def role_for(self, ev: Event) -> Role:
        """Return DB role; default is 'user' (not 'guest')."""
        ident = _norm_nick(ev.nick)
        if not ident:
            return "guest"
        try:
            r = await self.store.acl_get_identity_role(ident)
        except Exception:
            log.exception("Failed to read identity role")
            r = None
        if not r:
            return "user"
        return _norm_role(str(r))

    # -------------------------
    # Command permission overrides
    # -------------------------
    async def _db_min_role_for_cmd(self, cmd_key: str) -> Role | None:
        cmd_key = _norm_cmd(cmd_key)
        if not cmd_key:
            return None

        cached = self._perm_cache.get(cmd_key)
        if cached and (time.time() - cached[1]) < self._perm_cache_ttl:
            return _norm_role(cached[0]) if cached[0] else None

        try:
            r = await self.store.acl_get_command_min_role(cmd_key)
        except Exception:
            log.exception("Failed to read command override")
            r = None

        self._perm_cache[cmd_key] = ((str(r) if r else ""), time.time())
        return _norm_role(str(r)) if r else None

    # -------------------------
    # Core handler
    # -------------------------
    async def handle_core(self, bot, ev: Event) -> bool:
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
            role = await self.role_for(ev)
            acc = await self._nickserv_acc(bot, ev.nick)
            acc_s = "unknown" if acc is None else str(acc)
            await bot.privmsg(ev.target, f"{ev.nick}: role={role} NickServ_ACC={acc_s} (>=2 means identified)")
            return True

        if cmd == "auth":
            await bot.privmsg(ev.target, f"{ev.nick}: !auth is deprecated. Use NickServ IDENTIFY; ACL uses NickServ ACC for mutations.")
            return True

        if cmd != "acl":
            return False

        # Only admins can run !acl (and must be identified)
        role = await self.role_for(ev)
        if ROLE_ORDER.get(role, 0) < ROLE_ORDER["admin"]:
            await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires admin).")
            return True

        if not await self._require_identified(bot, ev, why="Identify first to run ACL mutations."):
            return True

        if len(parts) < 2:
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: usage: !acl adduser|deluser|usrlist|addserv|delserv|servlist ...",
            )
            return True

        sub = parts[1].lower()

        # !acl adduser <nick> <group>
        if sub == "adduser":
            if len(parts) != 4:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl adduser <nick> <admin|contributor|user>")
                return True

            target_nick = parts[2].strip()
            group = _norm_role(parts[3])
            if group not in ("admin", "contributor", "user"):
                await bot.privmsg(ev.target, f"{ev.nick}: group must be admin|contributor|user")
                return True

            # Enforce genuine user: require the target nick to be registered+identified *right now*.
            acc = await self._nickserv_acc(bot, target_nick)
            if acc is None:
                await bot.privmsg(ev.target, f"{ev.nick}: cannot verify NickServ ACC for {target_nick} (timeout). Not adding.")
                return True
            if acc < 2:
                await bot.privmsg(
                    ev.target,
                    f"{ev.nick}: {target_nick} is not identified with NickServ (ACC={acc}). Ask them to IDENTIFY, then retry.",
                )
                return True

            await self.store.acl_set_identity_role(target_nick, group)
            await bot.privmsg(ev.target, f"ACL: added {target_nick} -> {group} (NickServ ACC={acc}).")
            return True

        # !acl deluser <nick> <group>
        if sub == "deluser":
            if len(parts) < 3:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl deluser <nick> <group>")
                return True
            target_nick = parts[2].strip()
            await self.store.acl_del_identity(target_nick)
            await bot.privmsg(ev.target, f"ACL: removed user {target_nick}.")
            return True

        # !acl usrlist <group>
        if sub == "usrlist":
            if len(parts) != 3:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl usrlist <admin|contributor|user>")
                return True
            group = _norm_role(parts[2])
            if group not in ("admin", "contributor", "user"):
                await bot.privmsg(ev.target, f"{ev.nick}: group must be admin|contributor|user")
                return True
            users = await self.store.acl_list_identities(group)
            if not users:
                await bot.privmsg(ev.target, f"ACL: {group} users: (none)")
            else:
                show = users[:30]
                extra = "" if len(users) <= 30 else f" (+{len(users) - 30} more)"
                await bot.privmsg(ev.target, f"ACL: {group} users: " + ", ".join(show) + extra)
            return True

        # !acl addserv <service> <group>
        if sub == "addserv":
            if len(parts) != 4:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl addserv <command> <guest|user|contributor|admin>")
                return True

            cmd_key = _norm_cmd(parts[2])
            group = _norm_role(parts[3])
            if group not in ("guest", "user", "contributor", "admin"):
                await bot.privmsg(ev.target, f"{ev.nick}: group must be guest|user|contributor|admin")
                return True

            # Optional sanity: warn if command is not registered
            if cmd_key not in bot.commands:
                await bot.privmsg(ev.target, f"ACL: warning: command '{cmd_key}' is not currently registered. Override will still be stored.")

            await self.store.acl_set_command_min_role(cmd_key, group)
            self._perm_cache.pop(cmd_key, None)
            await bot.privmsg(ev.target, f"ACL: command '{cmd_key}' now requires {group}.")
            return True

        # !acl delserv <service> <group>
        if sub == "delserv":
            if len(parts) < 3:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !acl delserv <command> <group>")
                return True
            cmd_key = _norm_cmd(parts[2])
            await self.store.acl_del_command_min_role(cmd_key)
            self._perm_cache.pop(cmd_key, None)
            await bot.privmsg(ev.target, f"ACL: command '{cmd_key}' override removed (reverts to defaults).")
            return True

        # !acl servlist
        if sub == "servlist":
            rows = await self.store.acl_list_command_perms()
            if not rows:
                await bot.privmsg(ev.target, "ACL: no command overrides set.")
                return True

            pairs = [f"{c}->{r}" for (c, r) in rows]
            show = pairs[:25]
            extra = "" if len(pairs) <= 25 else f" (+{len(pairs) - 25} more)"
            await bot.privmsg(ev.target, "ACL: overrides: " + " | ".join(show) + extra)
            return True

        await bot.privmsg(ev.target, f"{ev.nick}: unknown subcommand. Use: adduser, deluser, usrlist, addserv, delserv, servlist")
        return True

    # -------------------------
    # Precheck gate
    # -------------------------
    async def precheck(self, bot, ev: Event) -> bool:
        prefix = bot.cfg.get("command_prefix", "!")
        txt = (ev.text or "").strip()
        if not txt.startswith(prefix):
            return True

        cmdline = txt[len(prefix) :].strip().lower()
        if not cmdline:
            return True

        # Choose longest matching registered command (supports subcommands like "service enable")
        parts = cmdline.split()
        cands = [" ".join(parts[:i]) for i in range(len(parts), 0, -1)]

        cmd_key = None
        info = None
        for c in cands:
            if c in bot.commands:
                cmd_key = c
                info = bot.commands[c]
                break

        if cmd_key is None or info is None:
            return True

        # Always allow help/commands/whoami (so you can recover)
        if cmd_key in ("help", "commands", "whoami"):
            return True

        role = await self.role_for(ev)

        # Effective minimum role = DB override if present else registered default
        db_min = await self._db_min_role_for_cmd(cmd_key)
        min_role = db_min if db_min else _norm_role(info.get("min_role", "user"))

        if ROLE_ORDER.get(role, 0) < ROLE_ORDER.get(min_role, 0):
            await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires {min_role}).")
            return False

        # If command is marked mutating and caller is elevated, require NickServ identify.
        # This blocks hostile nick impersonation for privileged changes.
        if bool(info.get("mutating")) and role in ("admin", "contributor"):
            if not await self._require_identified(bot, ev, why="Identify first to run mutating commands."):
                return False

        return True

    async def prune(self) -> None:
        # Keep this for continuity (old session model). Harmless.
        try:
            n = await self.store.prune_acl_sessions()
            if n:
                log.info("Pruned %d expired ACL sessions", n)
        except Exception:
            pass
