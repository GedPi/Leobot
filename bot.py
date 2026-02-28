#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import importlib
import logging
import signal
import time
from dataclasses import dataclass
from typing import Any, Optional

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
from system.acl import ACL


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

    # Load services
    for modname in cfg.get("services", []):
        log.info("Loading service: %s", modname)
        mod = importlib.import_module(modname)
        if not hasattr(mod, "setup"):
            raise RuntimeError(f"Service module {modname} has no setup(bot)")
        svc = mod.setup(bot)
        if svc is not None:
            bot.dispatcher.add_service(svc)

    # Scheduler core jobs
    scheduler.register_interval("acl_prune", 300, bot.acl.prune, jitter_seconds=5, run_on_start=True)

    # shutdown handler
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: stop_event.set())
        except NotImplementedError:
            pass

    backoff = int(cfg.get("reconnect_min_seconds", 2))
    backoff_max = int(cfg.get("reconnect_max_seconds", 60))

    while not stop_event.is_set():
        try:
            await irc.connect()

            # Wait for welcome (001) handled in handle_line
            await scheduler.start()
            await irc.run(stop_event)
        except Exception as e:
            logging.exception("Bot crashed/disconnected: %s", e)
        finally:
            try:
                await scheduler.stop()
            except Exception:
                pass
            await irc.close()

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
        await bot.dispatcher.dispatch("on_privmsg", ev)
        return

    if cmd == "JOIN" and params:
        channel = params[0]
        ev = Event(nick=nick, user=user, host=host, target=channel, channel=channel, text=None, is_private=False, raw=line, cmd=cmd, params=params)
        await bot.dispatcher.dispatch("on_join", ev)
        return

    if cmd == "PART" and params:
        channel = params[0]
        ev = Event(nick=nick, user=user, host=host, target=channel, channel=channel, text=params[1] if len(params) > 1 else None, is_private=False, raw=line, cmd=cmd, params=params)
        await bot.dispatcher.dispatch("on_part", ev)
        return

    if cmd == "QUIT":
        ev = Event(nick=nick, user=user, host=host, target=nick, channel=None, text=params[0] if params else None, is_private=True, raw=line, cmd=cmd, params=params)
        await bot.dispatcher.dispatch("on_quit", ev)
        return

    if cmd == "NICK" and params:
        new_nick = params[0]
        ev = Event(nick=new_nick, user=user, host=host, target=new_nick, channel=None, text=None, is_private=True, raw=line, cmd=cmd, params=params, old_nick=nick, new_nick=new_nick)
        await bot.dispatcher.dispatch("on_nick", ev)
        return

    if cmd == "KICK" and len(params) >= 2:
        channel = params[0]
        victim = params[1]
        ev = Event(nick=nick, user=user, host=host, target=channel, channel=channel, text=params[2] if len(params) > 2 else None, is_private=False, raw=line, cmd=cmd, params=params, victim=victim, kicker=nick)
        await bot.dispatcher.dispatch("on_kick", ev)
        return


if __name__ == "__main__":
    asyncio.run(main())
