#!/opt/leobot/venv/bin/python
import asyncio
import importlib
import json
import logging
import signal
import ssl
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from services.chatdb import ChatDB, DBConfig


CONFIG_PATH = Path("/etc/leobot/config.json")
LOG_PATH = Path("/var/log/leobot/bot.log")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH),
            logging.StreamHandler(),
        ],
    )


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Missing config at {CONFIG_PATH}. Create it before starting the service."
        )
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    required = ["server", "port", "nick", "user", "realname", "channels", "services"]
    for k in required:
        if k not in cfg:
            raise ValueError(f"Config missing required key: {k}")

    if not isinstance(cfg["channels"], list) or not cfg["channels"]:
        raise ValueError("Config 'channels' must be a non-empty list (e.g. ['#test']).")

    if not isinstance(cfg["services"], list) or not cfg["services"]:
        raise ValueError("Config 'services' must be a non-empty list (e.g. ['services.acl', 'services.help']).")

    cfg.setdefault("use_tls", True)
    cfg.setdefault("verify_tls", True)
    cfg.setdefault("password", None)  # server PASS
    cfg.setdefault("nickserv_password", None)
    cfg.setdefault("command_prefix", "!")
    cfg.setdefault("reconnect_min_seconds", 2)
    cfg.setdefault("reconnect_max_seconds", 60)
    return cfg


@dataclass
class Event:
    nick: str
    user: str | None
    host: str | None
    target: str            # where bot should reply (channel or PM nick)
    channel: str | None    # channel if applicable
    text: str | None
    is_private: bool
    raw: str
    cmd: str
    params: list[str]
    old_nick: str | None = None
    new_nick: str | None = None
    victim: str | None = None
    kicker: str | None = None


def _parse_prefix(prefix: str) -> tuple[str, Optional[str], Optional[str]]:
    # nick!user@host
    if "!" in prefix and "@" in prefix:
        nick, rest = prefix.split("!", 1)
        user, host = rest.split("@", 1)
        return nick, user, host
    return prefix, None, None


# ----------------------------
# Command router
# ----------------------------

ROLE_ORDER = {"guest": 0, "user": 1, "contributor": 2, "admin": 3}


def _role_rank(role: str) -> int:
    return ROLE_ORDER.get((role or "guest").lower(), 0)


def _can_see(user_role: str, cmd_min_role: str) -> bool:
    return _role_rank(user_role) >= _role_rank(cmd_min_role)


@dataclass
class CommandArgs:
    tokens: list[str]
    rest: str


@dataclass
class CommandNode:
    name: str
    parent: "CommandNode | None" = None
    children: dict[str, "CommandNode"] = field(default_factory=dict)

    # metadata
    help: str = ""
    category: str = "General"
    min_role: str = "guest"
    mutating: bool = False
    aliases: list[str] = field(default_factory=list)

    # whether this node was explicitly registered (vs created as a parent placeholder)
    registered: bool = False

    # handler: async (bot, ev, args) -> None
    handler: object | None = None

    @property
    def path_tokens(self) -> list[str]:
        toks: list[str] = []
        cur: CommandNode | None = self
        while cur is not None and cur.parent is not None:
            toks.append(cur.name)
            cur = cur.parent
        return list(reversed(toks))

    @property
    def path(self) -> str:
        return " ".join(self.path_tokens).strip()


class CommandRouter:
    def __init__(self, bot: "IRCBot"):
        self.bot = bot
        self.root = CommandNode(name="")  # synthetic root node
        # index: normalized command path -> node
        self._index: dict[str, CommandNode] = {}

    def _norm(self, s: str) -> str:
        return (s or "").strip().lower().lstrip("!")

    def register(
        self,
        cmd: str,
        *,
        min_role: str = "user",
        mutating: bool = False,
        help: str = "",
        category: str = "General",
        aliases: list[str] | None = None,
        handler=None,
    ) -> CommandNode:
        path = self._norm(cmd)
        if not path:
            return self.root

        toks = path.split()
        node = self.root
        for t in toks:
            t = self._norm(t)
            if not t:
                continue
            node = node.children.setdefault(t, CommandNode(name=t, parent=node))

        node.registered = True
        node.min_role = (min_role or "guest").lower()
        node.mutating = bool(mutating)
        node.help = (help or "").strip()
        node.category = (category or "General").strip() or "General"
        if aliases:
            node.aliases = [self._norm(a) for a in aliases if self._norm(a)]
        if handler is not None:
            node.handler = handler

        # index canonical path
        key = self._norm(node.path)
        if key:
            self._index[key] = node

        # index aliases as root-level single-token aliases
        for a in node.aliases:
            if " " not in a:
                self._index[a] = node

        return node

    def _parse(self, text: str, prefix: str) -> tuple[list[str], str] | None:
        raw = (text or "").strip()
        if not raw.startswith(prefix):
            return None
        cmdline = raw[len(prefix):].strip()
        if not cmdline:
            return None
        toks = cmdline.split()
        return toks, cmdline

    def match(self, text: str, *, prefix: str) -> tuple[CommandNode | None, CommandArgs | None]:
        parsed = self._parse(text, prefix)
        if parsed is None:
            return None, None
        toks, _cmdline = parsed

        node = self.root
        consumed: list[str] = []

        for t in toks:
            nt = self._norm(t)
            nxt = node.children.get(nt)

            if nxt is None:
                # root alias support for single-token aliases (e.g. !commands -> help)
                if node is self.root and nt in self._index and self._index[nt].parent is self.root:
                    node = self._index[nt]
                    consumed = node.path_tokens[:]
                    continue
                break

            node = nxt
            consumed.append(nt)

        if node is self.root:
            return None, None

        rest_tokens = toks[len(consumed):]
        rest = " ".join(rest_tokens).strip() if rest_tokens else ""
        return node, CommandArgs(tokens=rest_tokens, rest=rest)

    def _role_for(self, bot: "IRCBot", ev: "Event") -> str:
        role = "guest"
        if getattr(bot, "acl", None) is not None:
            try:
                role = bot.acl.role_for_event(ev)
            except Exception:
                role = "guest"
        return (role or "guest").lower()

    async def dispatch(self, bot: "IRCBot", ev: "Event") -> bool:
        """
        Strict router (migration-safe):
          - Only intercept commands that have a router handler.
          - Commands registered only as metadata (handler=None) fall through to services.
        """
        prefix = bot.cfg.get("command_prefix", "!")
        node, args = self.match(ev.text or "", prefix=prefix)
        if not node or not args:
            return False

        # Migration-safe strictness: only router-owned commands are intercepted.
        if node.handler is None:
            return False

        # Enforce permissions here (help already hides, but execution must enforce too).
        role = self._role_for(bot, ev)
        if not _can_see(role, node.min_role):
            await bot.privmsg(ev.target, f"{ev.nick}: not allowed (requires {node.min_role}).")
            return True

        # Mutating enforcement is already handled by ACL precheck in IRCBot.dispatch().
        await node.handler(bot, ev, args)
        return True

    def iter_nodes(self) -> list[CommandNode]:
        out: list[CommandNode] = []
        stack = [self.root]
        while stack:
            n = stack.pop()
            for ch in n.children.values():
                stack.append(ch)
            if n is not self.root:
                out.append(n)
        return out

    def registered_commands(self) -> list[CommandNode]:
        return [n for n in self.iter_nodes() if n.registered]

    def _visible_commands(self, role: str) -> list[CommandNode]:
        return [n for n in self.registered_commands() if _can_see(role, n.min_role)]

    # ---------- Help rendering ----------

    async def cmd_help(self, bot: "IRCBot", ev: "Event", args: CommandArgs) -> None:
        role = self._role_for(bot, ev)
        prefix = bot.cfg.get("command_prefix", "!")

        # !help -> list commands grouped by category
        if not args.tokens:
            nodes = self._visible_commands(role)
            if not nodes:
                await bot.privmsg(ev.target, f"{ev.nick}: no commands available for your role.")
                return

            grouped: dict[str, list[str]] = {}
            for n in nodes:
                cat = n.category or "General"
                grouped.setdefault(cat, []).append(n.path)

            out_target = ev.target
            if sum(len(v) for v in grouped.values()) > 12:
                out_target = ev.nick

            await bot.privmsg(
                out_target,
                f"{ev.nick}: commands (role: {role}). Use {prefix}help <command> for details. Also: {prefix}help categories",
            )
            for cat in sorted(grouped.keys(), key=lambda s: s.lower()):
                cmds = " ".join(f"{prefix}{c}" for c in sorted(grouped[cat], key=lambda s: s.lower()))
                await bot.privmsg(out_target, f"{cat}: {cmds}")
            return

        # !help categories
        if len(args.tokens) == 1 and args.tokens[0].lower() == "categories":
            await self.cmd_help_categories(bot, ev, args)
            return

        # !help <command path...>
        query = " ".join([self._norm(t) for t in args.tokens]).strip()

        node = self._index.get(query)
        if node is None:
            # fallback: walk tree
            toks = query.split()
            cur: CommandNode | None = self.root
            for t in toks:
                if cur is None:
                    break
                nxt = cur.children.get(t)
                if nxt is None:
                    cur = None
                    break
                cur = nxt
            node = cur if cur is not None and cur is not self.root else None

        if node is None or not node.registered:
            await bot.privmsg(ev.target, f"{ev.nick}: unknown command '{args.tokens[0]}'. Try {prefix}help")
            return

        if not _can_see(role, node.min_role):
            await bot.privmsg(ev.target, f"{ev.nick}: you don't have access to '{prefix}{node.path}'.")
            return

        help_text = node.help or "(no help text yet)"
        hdr = f"{prefix}{node.path} — {help_text} (min role: {node.min_role}{', mutating' if node.mutating else ''})"
        await bot.privmsg(ev.target, hdr)

        # visible subcommands
        subs: list[CommandNode] = []
        for ch in node.children.values():
            if not ch.registered:
                continue
            if _can_see(role, ch.min_role):
                subs.append(ch)

        if subs:
            subs_sorted = sorted(subs, key=lambda n: n.name.lower())
            await bot.privmsg(ev.target, "Subcommands:")
            for s in subs_sorted[:20]:
                s_help = s.help or "(no help text yet)"
                await bot.privmsg(
                    ev.target,
                    f"  {prefix}{s.path} — {s_help} (min role: {s.min_role}{', mutating' if s.mutating else ''})",
                )
            if len(subs_sorted) > 20:
                await bot.privmsg(ev.target, f"  (+{len(subs_sorted) - 20} more)")
        return

    async def cmd_help_categories(self, bot: "IRCBot", ev: "Event", args: CommandArgs) -> None:
        role = self._role_for(bot, ev)
        nodes = self._visible_commands(role)
        cats = sorted({(n.category or "General") for n in nodes}, key=lambda s: s.lower())
        if not cats:
            await bot.privmsg(ev.target, f"{ev.nick}: no categories available.")
            return
        await bot.privmsg(ev.target, f"{ev.nick}: categories: " + ", ".join(cats))


class IRCBot:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.log = logging.getLogger("leobot")
        self.commands: dict[str, dict] = {}
        self.services: list[object] = []
        self.service_map: dict[str, object] = {}
        self.core_services: set[str] = {"acl", "help", "control"}
        self.router = CommandRouter(self)

        # Control-plane DB (single source of truth)
        db_path = None
        try:
            db_path = (cfg.get("chatdb") or {}).get("db_path")
        except Exception:
            db_path = None
        if not db_path:
            db_path = "/var/lib/leobot/db/leobot.db"
        self.db = ChatDB(DBConfig(str(db_path)))

        # Built-in router commands
        self.router.register(
            "help",
            min_role="guest",
            mutating=False,
            help="Show available commands. Usage: !help [command] | !help categories",
            category="General",
            aliases=["commands"],
            handler=self.router.cmd_help,
        )
        self.router.register(
            "help categories",
            min_role="guest",
            mutating=False,
            help="List command categories.",
            category="General",
            handler=self.router.cmd_help_categories,
        )

        # Control-plane commands (always available; not toggleable)
        self.router.register(
            "services",
            min_role="user",
            mutating=False,
            help="List service enablement for the current channel. Usage: !services",
            category="General",
            handler=self.cmd_services,
        )
        self.router.register(
            "service status",
            min_role="user",
            mutating=False,
            help="Show service status for a channel. Usage: !service status [#channel]",
            category="General",
            handler=self.cmd_service_status,
        )
        self.router.register(
            "service enable",
            min_role="admin",
            mutating=True,
            help="Enable a service in a channel. Usage: !service enable <service> [#channel]",
            category="General",
            handler=self.cmd_service_enable,
        )
        self.router.register(
            "service disable",
            min_role="admin",
            mutating=True,
            help="Disable a service in a channel. Usage: !service disable <service> [#channel]",
            category="General",
            handler=self.cmd_service_disable,
        )

        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.stop_event = asyncio.Event()

        # ACL service will set this in its setup()
        self.acl = None

    # -----------------------------
    # Control-plane: service toggles
    # -----------------------------

    def _norm_service(self, s: str) -> str:
        return (s or "").strip().lower()

    async def cmd_services(self, bot: "IRCBot", ev: "Event", args: CommandArgs) -> None:
        if not ev.channel:
            await self.privmsg(ev.target, f"{ev.nick}: use this in a channel.")
            return
        await self.db.ensure_channel(ev.channel)
        # ensure all known services exist in catalog
        for svc in sorted(self.service_map.keys()):
            await self.db.ensure_service(svc)

        rows = await self.db.list_service_status_for_channel(ev.channel)
        rows = [(s, True if s in self.core_services else en) for (s, en) in rows]
        if not rows:
            await self.privmsg(ev.target, f"{ev.nick}: no services registered.")
            return
        on = [s for s, en in rows if en]
        off = [s for s, en in rows if not en]
        await self.privmsg(ev.target, f"{ev.nick}: services in {ev.channel} — ON: {', '.join(on) if on else '(none)'} | OFF: {', '.join(off) if off else '(none)'}")

    async def cmd_service_status(self, bot: "IRCBot", ev: "Event", args: CommandArgs) -> None:
        channel = ev.channel
        if args.tokens:
            if args.tokens[0].startswith("#"):
                channel = args.tokens[0]
        if not channel:
            await self.privmsg(ev.target, f"{ev.nick}: provide a channel, e.g. !service status #chan")
            return
        await self.db.ensure_channel(channel)
        for svc in sorted(self.service_map.keys()):
            await self.db.ensure_service(svc)
        rows = await self.db.list_service_status_for_channel(channel)
        rows = [(s, True if s in self.core_services else en) for (s, en) in rows]
        on = [s for s, en in rows if en]
        off = [s for s, en in rows if not en]
        await self.privmsg(ev.target, f"{ev.nick}: {channel} — ON: {', '.join(on) if on else '(none)'} | OFF: {', '.join(off) if off else '(none)'}")

    async def _set_service(self, ev: "Event", service: str, channel: str | None, enabled: bool) -> None:
        svc = self._norm_service(service)
        if svc in self.core_services:
            await self.privmsg(ev.target, f"{ev.nick}: '{svc}' is core and cannot be toggled.")
            return
        if svc not in self.service_map:
            await self.privmsg(ev.target, f"{ev.nick}: unknown service '{svc}'.")
            return
        ch = channel or ev.channel
        if not ch:
            await self.privmsg(ev.target, f"{ev.nick}: provide a channel, e.g. !service {'enable' if enabled else 'disable'} {svc} #chan")
            return
        await self.db.set_service_channel_enabled(svc, ch, enabled, updated_by=ev.nick)

        # optional lifecycle hook
        inst = self.service_map.get(svc)
        hook_name = "on_service_enabled" if enabled else "on_service_disabled"
        fn = getattr(inst, hook_name, None)
        if callable(fn):
            try:
                await fn(self, ch)
            except Exception:
                self.log.exception("Service %s %s hook failed", svc, hook_name)

        await self.privmsg(ev.target, f"{ev.nick}: {svc} {'ENABLED' if enabled else 'DISABLED'} in {ch}.")

    async def cmd_service_enable(self, bot: "IRCBot", ev: "Event", args: CommandArgs) -> None:
        if not args.tokens:
            await self.privmsg(ev.target, f"{ev.nick}: usage: !service enable <service> [#channel]")
            return
        service = args.tokens[0]
        channel = args.tokens[1] if len(args.tokens) > 1 and args.tokens[1].startswith("#") else None
        await self._set_service(ev, service, channel, True)

    async def cmd_service_disable(self, bot: "IRCBot", ev: "Event", args: CommandArgs) -> None:
        if not args.tokens:
            await self.privmsg(ev.target, f"{ev.nick}: usage: !service disable <service> [#channel]")
            return
        service = args.tokens[0]
        channel = args.tokens[1] if len(args.tokens) > 1 and args.tokens[1].startswith("#") else None
        await self._set_service(ev, service, channel, False)

    def register_command(
        self,
        cmd: str,
        *,
        min_role: str = "user",
        mutating: bool = False,
        help: str = "",
        category: str = "General",
    ) -> None:
        c = (cmd or "").strip().lower().lstrip("!")
        if not c:
            return
        self.commands[c] = {
            "min_role": min_role,
            "mutating": bool(mutating),
            "help": (help or "").strip(),
            "category": (category or "General").strip(),
        }

        # Keep router metadata in sync (handler remains service-owned unless explicitly set)
        try:
            if getattr(self, "router", None) is not None:
                self.router.register(c, min_role=min_role, mutating=mutating, help=help, category=category)
        except Exception:
            self.log.exception("Router register failed for %s", c)

    async def send_raw(self, line: str) -> None:
        assert self.writer is not None
        wire = (line + "\r\n").encode("utf-8", errors="ignore")
        self.writer.write(wire)
        await self.writer.drain()
        logging.info(">> %s", line)

    async def close_connection(self) -> None:
        try:
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()
        except Exception:
            pass
        finally:
            self.reader = None
            self.writer = None

    async def privmsg(self, target: str, msg: str) -> None:
        for chunk in self._chunk_message(msg, limit=380):
            await self.send_raw(f"PRIVMSG {target} :{chunk}")

    def _chunk_message(self, msg: str, limit: int = 380):
        msg = msg.replace("\r", " ").replace("\n", " ")
        while len(msg) > limit:
            yield msg[:limit]
            msg = msg[limit:]
        yield msg

    def load_services(self) -> None:
        self.services = []
        self.service_map = {}
        for modname in self.cfg.get("services", []):
            self.log.info("Loading service: %s", modname)
            mod = importlib.import_module(modname)
            if not hasattr(mod, "setup"):
                raise RuntimeError(f"Service module {modname} has no setup(bot)")
            svc = mod.setup(self)
            if svc is not None:
                self.services.append(svc)
                key = (modname.split(".")[-1] or modname).strip().lower()
                setattr(svc, "_service_name", key)
                self.service_map[key] = svc

        # Catalog rows will be ensured from the async run() context.

    async def connect(self) -> None:
        server = self.cfg["server"]
        port = int(self.cfg["port"])
        use_tls = bool(self.cfg.get("use_tls", True))

        ssl_ctx = None
        if use_tls:
            ssl_ctx = ssl.create_default_context()
            if not self.cfg.get("verify_tls", True):
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE

        logging.info("Connecting to %s:%s TLS=%s", server, port, use_tls)
        self.reader, self.writer = await asyncio.open_connection(server, port, ssl=ssl_ctx)

        if self.cfg.get("password"):
            await self.send_raw(f"PASS {self.cfg['password']}")

        await self.send_raw(f"NICK {self.cfg['nick']}")
        await self.send_raw(f"USER {self.cfg['user']} 0 * :{self.cfg['realname']}")

    async def run(self) -> None:
        # Load services before connecting so they can register commands
        self.load_services()

        # Ensure control-plane catalog entries exist
        for ch in self.cfg.get("channels", []):
            await self.db.ensure_channel(ch)
        for svc in self.service_map.keys():
            await self.db.ensure_service(svc)
        await self.connect()

        assert self.reader is not None
        while not self.stop_event.is_set():
            line_b = await self.reader.readline()
            if not line_b:
                raise ConnectionError("Disconnected (EOF).")
            line = line_b.decode("utf-8", errors="ignore").rstrip("\r\n")
            logging.info("<< %s", line)
            await self.handle_line(line)

    async def handle_line(self, line: str) -> None:
        if line.startswith("PING "):
            token = line.split(" ", 1)[1]
            await self.send_raw(f"PONG {token}")
            return

        prefix = ""
        rest = line
        if rest.startswith(":"):
            prefix, rest = rest[1:].split(" ", 1)

        trailing = None
        if " :" in rest:
            head, trailing = rest.split(" :", 1)
            parts = head.split()
        else:
            parts = rest.split()

        if trailing is not None:
            parts.append(trailing)

        if not parts:
            return

        cmd = parts[0]
        params = parts[1:]

        # Welcome
        if cmd == "001":
            for chan in self.cfg["channels"]:
                await self.send_raw(f"JOIN {chan}")
                await asyncio.sleep(0.7)
            if self.cfg.get("nickserv_password"):
                await self.privmsg("NickServ", f"IDENTIFY {self.cfg['nickserv_password']}")
            # notify services
            for svc in self.services:
                svc_name = getattr(svc, "_service_name", None)
                if svc_name and svc_name not in self.core_services:
                    try:
                        if not await self.db.is_service_enabled_any(str(svc_name)):
                            continue
                    except Exception:
                        # fail open on DB issues
                        pass
                fn = getattr(svc, "on_ready", None)
                if callable(fn):
                    try:
                        await fn(self)
                    except Exception:
                        self.log.exception("Service error in on_ready (%s)", type(svc).__name__)
            return

        nick, user, host = ("", None, None)
        if prefix:
            nick, user, host = _parse_prefix(prefix)

        # Dispatch events
        if cmd == "PRIVMSG" and len(params) >= 2:
            target = params[0]
            text = params[1]
            is_private = target.lower() == self.cfg["nick"].lower()
            reply_target = nick if is_private else target
            channel = None if is_private else target

            ev = Event(
                nick=nick,
                user=user,
                host=host,
                target=reply_target,
                channel=channel,
                text=text,
                is_private=is_private,
                raw=line,
                cmd=cmd,
                params=params,
            )
            await self.dispatch("on_privmsg", ev)
            return

        if cmd == "JOIN" and params:
            channel = params[0]
            ev = Event(nick=nick, user=user, host=host, target=channel, channel=channel, text=None, is_private=False, raw=line, cmd=cmd, params=params)
            await self.dispatch("on_join", ev)
            return

        if cmd == "PART" and params:
            channel = params[0]
            ev = Event(nick=nick, user=user, host=host, target=channel, channel=channel, text=params[1] if len(params) > 1 else None, is_private=False, raw=line, cmd=cmd, params=params)
            await self.dispatch("on_part", ev)
            return

        if cmd == "QUIT":
            ev = Event(nick=nick, user=user, host=host, target=nick, channel=None, text=params[0] if params else None, is_private=True, raw=line, cmd=cmd, params=params)
            await self.dispatch("on_quit", ev)
            return

        if cmd == "NICK" and params:
            new_nick = params[0]
            ev = Event(nick=new_nick, user=user, host=host, target=new_nick, channel=None, text=None, is_private=True, raw=line, cmd=cmd, params=params, old_nick=nick, new_nick=new_nick)
            await self.dispatch("on_nick", ev)
            return

        if cmd == "KICK" and len(params) >= 2:
            channel = params[0]
            victim = params[1]
            ev = Event(nick=nick, user=user, host=host, target=channel, channel=channel, text=params[2] if len(params) > 2 else None, is_private=False, raw=line, cmd=cmd, params=params, victim=victim, kicker=nick)
            await self.dispatch("on_kick", ev)
            return

    async def dispatch(self, hook: str, ev: Event) -> None:
        # ACL precheck (if installed)
        if getattr(self, "acl", None) is not None and hook == "on_privmsg":
            try:
                ok = await self.acl.precheck(self, ev)
                if not ok:
                    return
            except Exception:
                self.log.exception("ACL precheck error")
                # fail open

        # Router dispatch (strict router is migration-safe by only intercepting commands with handlers)
        if hook == "on_privmsg" and getattr(self, "router", None) is not None:
            try:
                handled = await self.router.dispatch(self, ev)
                if handled:
                    return
            except Exception:
                self.log.exception("Router dispatch error")
                # fail open

        for svc in self.services:
            svc_name = getattr(svc, "_service_name", None)
            # Option 2: if disabled, it's disabled — suppress ALL hooks for that service in that channel.
            if ev.channel and svc_name and str(svc_name) not in self.core_services:
                try:
                    if not await self.db.is_service_enabled(str(svc_name), ev.channel):
                        continue
                except Exception:
                    # fail open on DB issues
                    pass
            fn = getattr(svc, hook, None)
            if callable(fn):
                try:
                    await fn(self, ev)
                except Exception:
                    self.log.exception("Service error in %s (%s)", hook, type(svc).__name__)

    async def shutdown(self) -> None:
        self.stop_event.set()
        try:
            if self.writer:
                await self.send_raw("QUIT :Shutting down")
                self.writer.close()
                await self.writer.wait_closed()
        except Exception:
            pass


async def main():
    setup_logging()
    cfg = load_config()
    bot = IRCBot(cfg)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(bot.shutdown()))

    backoff = int(cfg["reconnect_min_seconds"])
    backoff_max = int(cfg["reconnect_max_seconds"])

    while True:
        try:
            await bot.run()
            backoff = int(cfg["reconnect_min_seconds"])
        except Exception as e:
            logging.exception("Bot crashed/disconnected: %s", e)

        await bot.shutdown()
        await bot.close_connection()
        logging.info("Reconnecting in %ss...", backoff)
        await asyncio.sleep(backoff)
        bot.stop_event = asyncio.Event()
        backoff = min(backoff * 2, backoff_max)


if __name__ == "__main__":
    asyncio.run(main())