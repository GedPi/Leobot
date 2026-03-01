#!/opt/leobot/venv/bin/python
"""Leobot entrypoint.

This file orchestrates the refactored system/* modules:
  - system.config        (./config/config.json)
  - system.logging_setup (file+stdout logging)
  - system.store         (sqlite + migrations)
  - system.acl/help/servicectl core handlers
  - system.dispatcher    (hook dispatch to services)
  - system.irc_client    (socket + PING/PONG)

Change implemented here:
  - Graceful SIGTERM/SIGINT and SIGUSR1 handling with IRC QUIT messages.
  - Store interface validation at startup (prevents runtime contract drift).

This is designed to match the repo structure you uploaded (services/* + system/*).
"""

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


class Bot:
    def __init__(self, cfg: dict):
        self.cfg = cfg

        # Persistence
        self.store = Store(cfg.get("db_path", "./data/leonidas.db"))

        # ---- STORE CONTRACT VALIDATION (added) ----
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
        # -------------------------------------------

        # Core subsystems
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

    # ----- rest of file unchanged below -----

    # -----------------------------
    # Public helpers used by services
    # -----------------------------
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
            "min_role": (min_role or "user").strip(),
            "mutating": bool(mutating),
            "help": (help or "").strip(),
            "category": (category or "General").strip(),
        }

    async def send_raw(self, line: str) -> None:
        if not self.irc:
            raise RuntimeError("IRC client not initialized")
        await self.irc.send_raw(line)

    async def privmsg(self, target: str, msg: str) -> None:
        if not self.irc:
            raise RuntimeError("IRC client not initialized")
        await self.irc.privmsg(target, msg)

    # -----------------------------
    # Startup
    # -----------------------------
    def _register_core_commands(self) -> None:
        # help/commands live in system.help (core handler), but we list them here.
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
        # ACL core commands
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
        # Service control commands
        self.servicectl.register_commands(self)

    def _import_service_module(self, name: str):
        """Import service module with a tolerant fallback between services.* and system.*.

        Only modules providing setup(bot) are treated as services.
        If core modules appear in cfg['services'] by mistake, we skip them.
        """
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

    def load_services(self) -> None:
        # Reset dispatcher services list each time we (re)load.
        self.dispatcher.services.clear()
        self._services.clear()

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
            self._services.append(svc)
            self.dispatcher.add_service(svc)

    async def connect(self) -> None:
        self.irc = IRCClient(self.cfg, self.on_line)
        await self.irc.connect()

    # -----------------------------
    # IRC line handling
    # -----------------------------
    async def on_line(self, line: str) -> None:
        pl = parse_line(line)
        if not pl:
            return

        cmd = pl.cmd
        params = pl.params

        # Welcome
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

        # PRIVMSG
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

        # NOTICE (useful for posterity; same shape as PRIVMSG)
        if cmd == "NOTICE" and len(params) >= 2:
            target = params[0]
            text = params[1]

            channel = target if target.startswith("#") else None
            is_private = (target.lower() == (self.cfg.get("nick", "").lower()))

            # Reply target: for PM notices, reply to nick; for channel notices, reply to channel; otherwise reply nowhere (keep as target)
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

        # JOIN
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

        # PART
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

        # QUIT
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

        # NICK
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

        # KICK
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

        # MODE (captures bans, ops, etc.)
        # MODE <target> <modes> [args...]
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

        # TOPIC
        # TOPIC <channel> :<topic>
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

    # -----------------------------
    # Shutdown
    # -----------------------------
    def request_exit(self, quit_message: str) -> None:
        """Signal-safe request to exit."""
        self.exit_requested = True
        self.quit_message = (quit_message or "Shutting down").strip() or "Shutting down"
        self.stop_event.set()
        asyncio.create_task(self.shutdown(self.quit_message))

    async def shutdown(self, quit_message: Optional[str] = None) -> None:
        """Gracefully QUIT, stop scheduler, close DB. Idempotent."""
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

        # Send QUIT and close socket to break any pending readline()
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


async def main() -> None:
    cfg = load_config()
    setup_logging(cfg.get("log_path"))

    bot = Bot(cfg)
    bot.load_services()
    await bot.connect()

    loop = asyncio.get_running_loop()

    # Graceful signals
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

        # New session (fresh Bot avoids stale dispatcher/services/scheduler state)
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