#!/opt/leobot/venv/bin/python
# Leobot entrypoint: loads config and system modules, wires Store/Dispatcher/ACL/Help/ServiceCtl,
# connects to IRC and runs a reconnect loop with graceful shutdown on SIGTERM/SIGINT/SIGUSR1.

from __future__ import annotations

import asyncio
import importlib
import logging
import signal
import sys
from pathlib import Path
from typing import Any, Optional

_BASE_DIR = str(Path(__file__).resolve().parent)
if _BASE_DIR not in sys.path:
    sys.path.insert(0, _BASE_DIR)

from system.acl import ACL
from system.config import ConfigError, load_config
from system.dispatcher import Dispatcher
from system.help import Help
from system.irc_client import IRCClient
from system.irc_parse import parse_line, parse_prefix
from system.logging_setup import setup_logging
from system.scheduler import Scheduler
from system.servicectl import ServiceCtl
from system.store import Store
from system.types import Event

log = logging.getLogger("leobot")


# Holds config, Store, Dispatcher, ACL, Help, ServiceCtl and command registry.
# Validates that Store exposes required methods at init, then registers core commands.
class Bot:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.store = Store(cfg.get("db_path", "./data/leonidas.db"))

        REQUIRED_STORE_METHODS = [
            "get_acl_session",
            "set_acl_session",
            "clear_acl_session",
            "news_list_categories",
            "news_set_category",
            "greet_select_target",
            "greet_pick_greeting",
        ]

        for m in REQUIRED_STORE_METHODS:
            if not hasattr(self.store, m):
                raise RuntimeError(f"Store missing required method: {m}")

        self.dispatcher = Dispatcher(self)
        self.scheduler = Scheduler()
        self.irc: Optional[IRCClient] = None

        self.commands: dict[str, dict[str, Any]] = {}

        self.acl = ACL(self.store, cfg)
        self.help = Help()
        self.servicectl = ServiceCtl()

        self.dispatcher.add_core_handler(self.acl)
        self.dispatcher.add_core_handler(self.help)
        self.dispatcher.add_core_handler(self.servicectl)

        self._register_core_commands()

        self._services: list[Any] = []

        self.stop_event = asyncio.Event()
        self.exit_requested = False
        self.quit_message = "Shutting down"
        self._shutdown_once = False

    # Registers a command in the bot command table with min_role, help, category and optional
    # service_id/capability for per-channel policy lookup. Normalizes cmd to lowercase and strips "!".
    def register_command(
        self,
        cmd: str,
        *,
        min_role: str = "user",
        mutating: bool = False,
        help: str = "",
        category: str = "General",
        service_id: str | None = None,
        capability: str | None = None,
    ) -> None:
        c = (cmd or "").strip().lower().lstrip("!")
        if not c:
            return
        self.commands[c] = {
            "min_role": (min_role or "user").strip(),
            "mutating": bool(mutating),
            "help": (help or "").strip(),
            "category": (category or "General").strip(),
            "service_id": (service_id or "").strip() or None,
            "capability": (capability or "").strip() or None,
        }

    # Sends a raw IRC line; requires irc client to be connected.
    async def send_raw(self, line: str) -> None:
        if not self.irc:
            raise RuntimeError("IRC client not initialized")
        await self.irc.send_raw(line)

    # Sends a PRIVMSG to target (channel or nick); requires irc client to be connected.
    async def privmsg(self, target: str, msg: str) -> None:
        if not self.irc:
            raise RuntimeError("IRC client not initialized")
        await self.irc.privmsg(target, msg)

    # Registers help, commands, auth, whoami and delegates service-control commands to ServiceCtl.
    # Core handlers (Help, ACL) use this registry for permission and help text.
    def _register_core_commands(self) -> None:
        self.register_command(
            "help",
            min_role="guest",
            mutating=False,
            help="Show help for a command. Usage: !help <command>",
            category="System",
        )
        self.register_command(
            "commands",
            min_role="guest",
            mutating=False,
            help="List available commands for your role. Usage: !commands",
            category="System",
        )
        self.register_command(
            "auth",
            min_role="guest",
            mutating=True,
            help="Authenticate to a higher role. Usage: !auth <password>",
            category="System",
        )
        self.register_command(
            "whoami",
            min_role="guest",
            mutating=False,
            help="Show your effective role/identity. Usage: !whoami",
            category="System",
        )
        self.servicectl.register_commands(self)

    # Resolves a service name to a Python module: tries services.<name> and system.<name>,
    # and if the name contains a dot, also the opposite prefix. Raises ModuleNotFoundError if none load.
    def _import_service_module(self, name: str):
        name = (name or "").strip()
        if not name:
            raise ModuleNotFoundError("empty service module name")

        candidates: list[str] = []
        if "." in name:
            candidates.append(name)
            if name.startswith("services."):
                candidates.append("system." + name.split(".", 1)[1])
            elif name.startswith("system."):
                candidates.append("services." + name.split(".", 1)[1])
        else:
            candidates.append(f"services.{name}")
            candidates.append(f"system.{name}")

        last: Exception | None = None
        for modname in candidates:
            try:
                return importlib.import_module(modname)
            except ModuleNotFoundError as e:
                last = e
        assert last is not None
        raise last

    # Loads addons from config "services" list: imports each module, calls setup(bot), assigns a unique
    # service_id (from attribute or module name) and adds the instance to the dispatcher; skips duplicates and modules without setup().
    def load_services(self) -> None:
        self.dispatcher.services.clear()
        self._services.clear()
        seen_ids: set[str] = set()

        for name in (self.cfg.get("services", []) or []):
            log.info("Loading service: %s", name)
            mod = self._import_service_module(str(name))
            setup_fn = getattr(mod, "setup", None)
            if not callable(setup_fn):
                log.warning("Skipping %s (no setup(bot))", getattr(mod, "__name__", str(mod)))
                continue
            svc = setup_fn(self)
            if svc is None:
                continue
            sid = getattr(svc, "service_id", None)
            if not sid or not str(sid).strip():
                sid = (getattr(mod, "__name__", "") or "").split(".")[-1] or str(name)
                setattr(svc, "service_id", sid)
            sid = str(sid).strip().lower()
            if sid in seen_ids:
                log.error("Duplicate service_id %r (service %s); skipping.", sid, name)
                continue
            seen_ids.add(sid)
            self._services.append(svc)
            self.dispatcher.add_service(svc)

    # Creates the IRC client from config and opens the connection; used at startup and after reconnect.
    async def connect(self) -> None:
        self.irc = IRCClient(self.cfg, self.on_line)
        await self.irc.connect()

    # Parses each IRC line, handles 001 (JOIN channels, identify to NickServ, start scheduler, on_ready),
    # then builds an Event and dispatches by command (PRIVMSG, NOTICE, JOIN, PART, QUIT, NICK, KICK, MODE, TOPIC).
    async def on_line(self, line: str) -> None:
        pl = parse_line(line)
        if not pl:
            return

        cmd = pl.cmd
        params = pl.params

        if cmd == "001":
            for chan in self.cfg.get("channels", []):
                await self.send_raw(f"JOIN {chan}")
                await asyncio.sleep(0.7)

            if self.cfg.get("nickserv_password"):
                await self.privmsg("NickServ", f"IDENTIFY {self.cfg['nickserv_password']}")

            try:
                await self.scheduler.start()
            except Exception:
                log.exception("Scheduler start failed")

            for svc in list(self._services):
                fn = getattr(svc, "on_ready", None)
                if callable(fn):
                    try:
                        await fn(self)
                    except Exception:
                        log.exception("Service error in on_ready (%s)", type(svc).__name__)
            return

        prefix = pl.prefix
        nick, user, host = ("", None, None)
        if prefix:
            nick, user, host = parse_prefix(prefix)

        if cmd == "PRIVMSG" and len(params) >= 2:
            target = params[0]
            text = params[1]

            channel = target if target.startswith("#") else None
            is_private = (target.lower() == (self.cfg.get("nick", "").lower()))

            reply_target = nick if is_private else (channel or target)

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
            await self.dispatcher.dispatch("on_privmsg", ev)
            return

        if cmd == "NOTICE" and len(params) >= 2:
            target = params[0]
            text = params[1]

            channel = target if target.startswith("#") else None
            is_private = (target.lower() == (self.cfg.get("nick", "").lower()))
            reply_target = nick if is_private else (channel or target)

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
            await self.dispatcher.dispatch("on_notice", ev)
            return

        if cmd == "JOIN" and params:
            channel = params[0]
            ev = Event(
                nick=nick,
                user=user,
                host=host,
                target=channel,
                channel=channel,
                text=None,
                is_private=False,
                raw=line,
                cmd=cmd,
                params=params,
            )
            await self.dispatcher.dispatch("on_join", ev)
            return

        if cmd == "PART" and params:
            channel = params[0]
            ev = Event(
                nick=nick,
                user=user,
                host=host,
                target=channel,
                channel=channel,
                text=params[1] if len(params) > 1 else None,
                is_private=False,
                raw=line,
                cmd=cmd,
                params=params,
            )
            await self.dispatcher.dispatch("on_part", ev)
            return

        if cmd == "QUIT":
            ev = Event(
                nick=nick,
                user=user,
                host=host,
                target=nick,
                channel=None,
                text=params[0] if params else None,
                is_private=True,
                raw=line,
                cmd=cmd,
                params=params,
            )
            await self.dispatcher.dispatch("on_quit", ev)
            return

        if cmd == "NICK" and params:
            new_nick = params[0]
            ev = Event(
                nick=new_nick,
                user=user,
                host=host,
                target=new_nick,
                channel=None,
                text=None,
                is_private=True,
                raw=line,
                cmd=cmd,
                params=params,
                old_nick=nick,
                new_nick=new_nick,
            )
            await self.dispatcher.dispatch("on_nick", ev)
            return

        if cmd == "KICK" and len(params) >= 2:
            channel = params[0]
            victim = params[1]
            ev = Event(
                nick=nick,
                user=user,
                host=host,
                target=channel,
                channel=channel,
                text=params[2] if len(params) > 2 else None,
                is_private=False,
                raw=line,
                cmd=cmd,
                params=params,
                victim=victim,
                kicker=nick,
            )
            await self.dispatcher.dispatch("on_kick", ev)
            return

        if cmd == "MODE" and len(params) >= 2:
            target = params[0]
            mode_str = params[1]
            mode_args = params[2:] if len(params) > 2 else []
            detail = mode_str
            if mode_args:
                detail = detail + " " + " ".join(mode_args)

            channel = target if target.startswith("#") else None
            ev = Event(
                nick=nick,
                user=user,
                host=host,
                target=target,
                channel=channel,
                text=detail,
                is_private=False if channel else True,
                raw=line,
                cmd=cmd,
                params=params,
            )
            await self.dispatcher.dispatch("on_mode", ev)
            return

        if cmd == "TOPIC" and len(params) >= 2:
            channel = params[0]
            topic = params[1]
            ev = Event(
                nick=nick,
                user=user,
                host=host,
                target=channel,
                channel=channel,
                text=topic,
                is_private=False,
                raw=line,
                cmd=cmd,
                params=params,
            )
            await self.dispatcher.dispatch("on_topic", ev)
            return

    # Sets exit flag and quit message then triggers shutdown asynchronously; safe to call from signal handlers.
    def request_exit(self, quit_message: str) -> None:
        self.exit_requested = True
        self.quit_message = (quit_message or "Shutting down").strip() or "Shutting down"
        self.stop_event.set()
        asyncio.create_task(self.shutdown(self.quit_message))

    # Stops scheduler, sends QUIT to IRC, closes socket and store; runs only once per bot instance.
    async def shutdown(self, quit_message: Optional[str] = None) -> None:
        if self._shutdown_once:
            return
        self._shutdown_once = True

        msg = (quit_message or self.quit_message or "Shutting down").strip() or "Shutting down"
        if len(msg) > 220:
            msg = msg[:220]

        try:
            await self.scheduler.stop()
        except Exception:
            pass

        try:
            if self.irc and self.irc.writer:
                try:
                    await self.irc.send_raw(f"QUIT :{msg}")
                except Exception:
                    pass
                await self.irc.close()
        except Exception:
            pass

        try:
            await self.store.close()
        except Exception:
            pass


# Loads config, sets up logging, builds Bot, loads services, connects to IRC and runs the main loop
# with signal handlers and exponential backoff reconnect on disconnect until exit is requested.
async def main() -> None:
    cfg = load_config()
    setup_logging(cfg.get("log_path"))

    bot = Bot(cfg)
    bot.load_services()
    await bot.connect()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: bot.request_exit("Shot in the head by God"))
    loop.add_signal_handler(signal.SIGTERM, lambda: bot.request_exit("Shot in the head by God"))
    if hasattr(signal, "SIGUSR1"):
        loop.add_signal_handler(signal.SIGUSR1, lambda: bot.request_exit("I'll be back, even stronger"))

    backoff = int(cfg.get("reconnect_min_seconds", 2))
    backoff_max = int(cfg.get("reconnect_max_seconds", 60))

    while True:
        try:
            assert bot.irc is not None
            await bot.irc.run(bot.stop_event)
            backoff = int(cfg.get("reconnect_min_seconds", 2))
        except Exception as e:
            if not bot.exit_requested:
                bot.quit_message = f"Internal fault: {type(e).__name__}"
            log.exception("Bot crashed/disconnected: %s", e)

        await bot.shutdown(bot.quit_message)

        if bot.exit_requested:
            break

        log.info("Reconnecting in %ss...", backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, backoff_max)
        bot = Bot(cfg)
        bot.load_services()
        await bot.connect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except ConfigError as e:
        logging.basicConfig(level=logging.INFO)
        logging.error(str(e))
        raise
        