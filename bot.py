#!/opt/leobot/venv/bin/python
"""Leonidas IRC Bot entrypoint.

This file wires together:
- config loading
- SQLite store
- core handlers (ACL/Help/ServiceCtl)
- pluggable services under ./services
- IRC client loop
- graceful shutdown for SIGTERM/SIGINT (stop) and SIGUSR1 (restart)

Notes:
- Repo layout has *core* modules under ./system and *services* under ./services.
- Services are generally created via module-level setup(bot) -> service instance.
- Dispatcher API is dispatcher.dispatch(hook, ev).
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import signal
import sys
from pathlib import Path
from typing import Any, Optional

# Ensure local packages (./system, ./services) are importable when launched by systemd.
_BASE_DIR = str(Path(__file__).resolve().parent)
if _BASE_DIR not in sys.path:
    sys.path.insert(0, _BASE_DIR)

from system.acl import ACL
from system.config import load_config
from system.dispatcher import Dispatcher
from system.help import Help
from system.irc_client import IRCClient
from system.irc_parse import parse_line, parse_prefix
from system.logging_setup import setup_logging
from system.servicectl import ServiceCtl
from system.store import Store
from system.types import Event

log = logging.getLogger("leobot")


def _import_module_with_fallback(name: str):
    """Import a module referenced in config.

    Accepts:
      - "greet"            -> services.greet then system.greet
      - "services.greet"   -> services.greet then system.greet
      - "system.acl"       -> system.acl then services.acl

    The fallback is there to tolerate older configs, but *core* modules (system.*)
    are not expected to be loaded as channel services.
    """
    name = (name or "").strip()
    if not name:
        raise ModuleNotFoundError("empty module name")

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


class Bot:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.commands: dict[str, dict[str, Any]] = {}

        # Runtime wiring
        self.store = Store(cfg.get("db_path", "./data/leonidas.db"))
        self.acl = ACL(self.store, cfg)
        self.dispatcher = Dispatcher(self)

        # Core handlers run before services (auth/help/servicectl)
        self.dispatcher.add_core_handler(self.acl)
        self.dispatcher.add_core_handler(Help())
        self.dispatcher.add_core_handler(ServiceCtl())

        self.irc: IRCClient | None = None

        # Shutdown semantics
        self._shutdown_once = False
        self.exit_requested = False
        self.quit_message = "Shutting down"

    # ---- command registry (used by ACL/help) ----
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

    # ---- IRC send helpers ----
    async def send_raw(self, line: str) -> None:
        if not self.irc:
            return
        await self.irc.send_raw(line)

    async def privmsg(self, target: str, msg: str) -> None:
        if not self.irc:
            return
        await self.irc.privmsg(target, msg)

    # ---- service loading ----
    def load_services(self) -> None:
        # Clear existing list on dispatcher
        self.dispatcher.services = []

        for raw in (self.cfg.get("services") or []):
            name = str(raw)
            log.info("Loading service: %s", name)
            mod = _import_module_with_fallback(name)

            # Service modules are expected to provide setup(bot)->service
            setup_fn = getattr(mod, "setup", None)
            if callable(setup_fn):
                svc = setup_fn(self)
                if svc is not None:
                    self.dispatcher.add_service(svc)
                continue

            # If config references a core module (e.g. system.acl), do NOT crash.
            # Just log and skip: core functionality is already wired separately.
            log.warning("Module %s has no setup(bot); skipping", mod.__name__)

    # ---- lifecycle ----
    async def shutdown(self, quit_message: Optional[str] = None) -> None:
        """Best-effort graceful QUIT + stop flag. Idempotent."""
        if self._shutdown_once:
            return
        self._shutdown_once = True

        msg = (quit_message or self.quit_message or "Shutting down").strip() or "Shutting down"
        if len(msg) > 220:
            msg = msg[:220]

        try:
            if self.irc and self.irc.writer:
                await self.irc.send_raw(f"QUIT :{msg}")
        except Exception:
            pass


async def handle_line(bot: Bot, line: str) -> None:
    pl = parse_line(line)
    if not pl:
        return

    # Welcome => join channels + NickServ identify
    if pl.cmd == "001":
        for chan in bot.cfg.get("channels", []):
            await bot.send_raw(f"JOIN {chan}")
            await asyncio.sleep(0.7)
        if bot.cfg.get("nickserv_password"):
            await bot.privmsg("NickServ", f"IDENTIFY {bot.cfg['nickserv_password']}")

        # Optional on_ready hook for services
        for svc in bot.dispatcher.services:
            fn = getattr(svc, "on_ready", None)
            if callable(fn):
                try:
                    await fn(bot)
                except Exception:
                    logging.getLogger("leobot.dispatch").exception(
                        "Service error in on_ready (%s)", type(svc).__name__
                    )
        return

    nick, user, host = ("", None, None)
    if pl.prefix:
        nick, user, host = parse_prefix(pl.prefix)

    cmd = pl.cmd
    params = pl.params

    # PRIVMSG
    if cmd == "PRIVMSG" and len(params) >= 2:
        target = params[0]
        text = params[1]
        is_private = target.lower() == bot.cfg.get("nick", "").lower()
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
        await bot.dispatcher.dispatch("on_privmsg", ev)
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
        await bot.dispatcher.dispatch("on_join", ev)
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
        await bot.dispatcher.dispatch("on_part", ev)
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
        await bot.dispatcher.dispatch("on_quit", ev)
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
        await bot.dispatcher.dispatch("on_nick", ev)
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
        await bot.dispatcher.dispatch("on_kick", ev)
        return


async def main() -> None:
    cfg = load_config()  # defaults to ./config/config.json
    setup_logging(cfg.get("log_path"))

    bot = Bot(cfg)
    bot.load_services()

    stop_event = asyncio.Event()

    async def request_exit(msg: str) -> None:
        bot.exit_requested = True
        bot.quit_message = msg
        stop_event.set()
        await bot.shutdown(msg)

    loop = asyncio.get_running_loop()

    # SIGTERM/SIGINT => stop
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(request_exit("Shot in the head by God")))
    loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(request_exit("Shot in the head by God")))

    # SIGUSR1 => restart (systemd Restart=always will bring it back)
    if hasattr(signal, "SIGUSR1"):
        loop.add_signal_handler(signal.SIGUSR1, lambda: asyncio.create_task(request_exit("I'll be back, even stronger")))

    backoff = int(cfg.get("reconnect_min_seconds", 2))
    backoff_max = int(cfg.get("reconnect_max_seconds", 60))

    while True:
        irc = IRCClient(cfg, on_line=lambda line: handle_line(bot, line))
        bot.irc = irc

        try:
            await irc.connect()
            await irc.run(stop_event)
            backoff = int(cfg.get("reconnect_min_seconds", 2))
        except Exception as e:
            if not bot.exit_requested:
                bot.quit_message = f"Internal fault: {type(e).__name__}"
            logging.getLogger().exception("Bot crashed/disconnected: %s", e)

        # Ensure QUIT if possible
        try:
            await bot.shutdown(bot.quit_message)
        finally:
            await irc.close()

        if bot.exit_requested:
            break

        logging.getLogger("leobot").info("Reconnecting in %ss...", backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, backoff_max)

        # reset loop state
        stop_event = asyncio.Event()
        bot._shutdown_once = False


if __name__ == "__main__":
    asyncio.run(main())
