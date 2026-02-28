#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import importlib
import logging
import signal
import time
from dataclasses import dataclass
from typing import Any, Optional

from system.acl import ACL
from system.config import load_config
from system.dispatcher import Dispatcher
from system.help import Help
from system.irc_client import IRCClient
from system.irc_parse import parse_line, parse_prefix
from system.logging_setup import setup_logging
from system.scheduler import Scheduler
from system.servicectl import ServiceCtl
from system.store import Store
from system.types import Event


def _safe_quit_msg(msg: str, *, fallback: str = "Bye") -> str:
    # Keep it short-ish: servers differ, but avoiding huge quit lines is sensible.
    m = (msg or "").strip()
    if not m:
        m = fallback
    return m[:180]


def _internal_fault_quit(exc: BaseException) -> str:
    # Intentionally terse; avoid leaking internals in channel logs.
    return _safe_quit_msg(f"Internal fault: {type(exc).__name__}")


def _import_service(name: str):
    """Import a service module with fallback between services.* and system.*.

    Accepts:
      - "greet" -> services.greet, then system.greet
      - "services.greet" -> as-is, then system.greet
      - "system.acl" -> as-is
    """
    name = (name or "").strip()
    if not name:
        raise ModuleNotFoundError("empty service name")

    candidates: list[str] = []

    if "." in name:
        candidates.append(name)
        if name.startswith("services."):
            candidates.append("system." + name.split(".", 1)[1])
    else:
        candidates.append(f"services.{name}")
        candidates.append(f"system.{name}")
        candidates.append(name)

    last: Exception | None = None
    for modname in candidates:
        try:
            return importlib.import_module(modname)
        except ModuleNotFoundError as e:
            last = e
            continue
    assert last is not None
    raise last


def _instantiate_service(mod, bot):
    """Create a service instance from a module.

    Supported module patterns:
      1) setup(bot) -> returns service instance (preferred / legacy)
      2) First class named *Service -> instantiated.

    Non-service modules (e.g. system.acl) are ignored (return None).
    """
    setup_fn = getattr(mod, "setup", None)
    if callable(setup_fn):
        return setup_fn(bot)

    # Try to locate a *Service class
    service_cls = None
    for attr in dir(mod):
        if not attr.endswith("Service"):
            continue
        obj = getattr(mod, attr, None)
        if isinstance(obj, type):
            service_cls = obj
            break

    if service_cls is None:
        return None

    # Prefer (bot, cfg) if the ctor accepts it; otherwise (bot).
    # We avoid inspect.signature to keep it simple/robust.
    try:
        return service_cls(bot, bot.cfg)
    except TypeError:
        return service_cls(bot)


@dataclass
class Bot:
    cfg: dict
    store: Store
    scheduler: Scheduler
    dispatcher: Dispatcher
    irc: IRCClient
    acl: ACL

    commands: dict[str, dict]

    async def send_raw(self, line: str) -> None:
        await self.irc.send_raw(line)

    async def privmsg(self, target: str, msg: str) -> None:
        await self.irc.privmsg(target, msg)

    async def quit(self, msg: str) -> None:
        """Best-effort IRC QUIT with message, if connected."""
        try:
            if self.irc and self.irc.writer:
                await self.send_raw(f"QUIT :{_safe_quit_msg(msg)}")
        except Exception:
            pass

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


async def main() -> None:
    cfg = load_config()
    setup_logging(cfg.get("log_path"))
    log = logging.getLogger("leobot")

    store = Store(cfg.get("db_path", "./data/leonidas.db"))
    scheduler = Scheduler()

    # Bot skeleton first (dispatcher + irc need callbacks)
    commands: dict[str, dict] = {}

    stop_event = asyncio.Event()
    shutdown_quit: dict[str, str] = {"msg": ""}  # mutable for signal handlers
    exit_after_disconnect: dict[str, bool] = {"value": False}

    async def on_line(line: str) -> None:
        await handle_line(bot, line)

    irc = IRCClient(cfg, on_line=on_line)

    bot = Bot(
        cfg=cfg,
        store=store,
        scheduler=scheduler,
        dispatcher=Dispatcher(None),
        irc=irc,
        acl=ACL(store, cfg),
        commands=commands,
    )

    bot.dispatcher.bot = bot

    # Core handlers
    help_core = Help()
    svc_ctl = ServiceCtl()
    svc_ctl.register_commands(bot)

    bot.register_command("help", min_role="guest", mutating=False, help="Show help. Usage: !help [command]", category="System")
    bot.register_command("commands", min_role="guest", mutating=False, help="List commands.", category="System")
    bot.register_command("auth", min_role="guest", mutating=True, help="Authenticate for higher roles. Usage: !auth <password>", category="System")
    bot.register_command("whoami", min_role="guest", mutating=False, help="Show your effective role.", category="System")

    bot.dispatcher.add_core_handler(bot.acl)
    bot.dispatcher.add_core_handler(help_core)
    bot.dispatcher.add_core_handler(svc_ctl)

    # Load services (robust to services.* vs system.* and to modules without setup()).
    for raw in cfg.get("services", []):
        name = str(raw)
        log.info("Loading service: %s", name)
        try:
            mod = _import_service(name)
        except Exception as e:
            log.error("Failed to import service %s: %s", name, e)
            continue

        try:
            svc = _instantiate_service(mod, bot)
        except Exception as e:
            log.error("Failed to initialize service %s (%s): %s", name, getattr(mod, "__name__", "?"), e)
            continue

        if svc is None:
            # Likely a core/system module, not a channel service.
            log.warning("Skipping non-service module: %s", getattr(mod, "__name__", name))
            continue

        bot.dispatcher.add_service(svc)

    # Scheduler core jobs
    scheduler.register_interval("acl_prune", 300, bot.acl.prune, jitter_seconds=5, run_on_start=True)

    # Signal handling
    loop = asyncio.get_running_loop()

    def _request_stop(msg: str, *, exit_loop: bool = True) -> None:
        shutdown_quit["msg"] = msg
        exit_after_disconnect["value"] = exit_loop
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: _request_stop("Shot in the head by God"))
        except NotImplementedError:
            pass

    # SIGUSR1 = "restart" request (systemd Restart=always will bring it back)
    if hasattr(signal, "SIGUSR1"):
        try:
            loop.add_signal_handler(signal.SIGUSR1, lambda: _request_stop("I'll be back, even stronger"))
        except NotImplementedError:
            pass

    backoff = int(cfg.get("reconnect_min_seconds", 2))
    backoff_max = int(cfg.get("reconnect_max_seconds", 60))

    while True:
        try:
            await irc.connect()

            # Wait for welcome (001) handled in handle_line
            await scheduler.start()
            await irc.run(stop_event)
        except Exception as e:
            logging.exception("Bot crashed/disconnected: %s", e)

            # If we were connected, try to QUIT with a fault reason (best-effort).
            try:
                await bot.quit(_internal_fault_quit(e))
            except Exception:
                pass
        finally:
            # If stop/restart requested, try to QUIT with the configured message.
            if stop_event.is_set() and shutdown_quit["msg"]:
                try:
                    await bot.quit(shutdown_quit["msg"])
                except Exception:
                    pass

            try:
                await scheduler.stop()
            except Exception:
                pass
            await irc.close()

        # If this was an explicit stop/restart, exit the loop and end the process.
        if exit_after_disconnect["value"]:
            break

        # Otherwise, reconnect loop unless a stop was requested without exit flag.
        if stop_event.is_set():
            break

        log.info("Reconnecting in %ss...", backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, backoff_max)

    try:
        await scheduler.stop()
    except Exception:
        pass
    await store.close()


async def handle_line(bot: Bot, line: str) -> None:
    p = parse_line(line)
    if p is None:
        return

    prefix = p.prefix
    cmd = p.cmd
    params = p.params

    # Welcome
    if cmd == "001":
        for chan in bot.cfg["channels"]:
            await bot.send_raw(f"JOIN {chan}")
            await asyncio.sleep(0.7)
        if bot.cfg.get("nickserv_password"):
            await bot.privmsg("NickServ", f"IDENTIFY {bot.cfg['nickserv_password']}")

        # Notify services
        for svc in bot.dispatcher.services:
            fn = getattr(svc, "on_ready", None)
            if callable(fn):
                try:
                    await fn(bot)
                except Exception:
                    logging.getLogger("leobot").exception("Service error in on_ready (%s)", type(svc).__name__)
        return

    nick, user, host = ("", None, None)
    if prefix:
        nick, user, host = parse_prefix(prefix)

    if cmd == "PRIVMSG" and len(params) >= 2:
        target = params[0]
        text = params[1]
        is_private = target.lower() == bot.cfg["nick"].lower()
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
        await bot.dispatcher.dispatch_privmsg(ev)
        return

    if cmd == "JOIN" and params:
        channel = params[0]
        if channel.startswith(":"):
            channel = channel[1:]
        ev = Event(
            nick=nick,
            user=user,
            host=host,
            target=channel,
            channel=channel,
            text="",
            is_private=False,
            raw=line,
            cmd=cmd,
            params=params,
        )
        await bot.dispatcher.dispatch_join(ev)
        return

    if cmd == "PART" and params:
        channel = params[0]
        ev = Event(
            nick=nick,
            user=user,
            host=host,
            target=channel,
            channel=channel,
            text="",
            is_private=False,
            raw=line,
            cmd=cmd,
            params=params,
        )
        await bot.dispatcher.dispatch_part(ev)
        return

    if cmd == "KICK" and len(params) >= 2:
        channel = params[0]
        kicked = params[1]
        reason = params[2] if len(params) >= 3 else ""
        ev = Event(
            nick=nick,
            user=user,
            host=host,
            target=channel,
            channel=channel,
            text=reason,
            is_private=False,
            raw=line,
            cmd=cmd,
            params=params,
        )
        await bot.dispatcher.dispatch_kick(ev, kicked)
        return


if __name__ == "__main__":
    asyncio.run(main())
