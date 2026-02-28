from __future__ import annotations

import logging
from typing import Any

from system.types import Event

log = logging.getLogger("leobot.dispatch")


class Dispatcher:
    def __init__(self, bot):
        self.bot = bot
        self.services: list[Any] = []
        self.core_handlers: list[Any] = []  # objects with handle_core(bot, ev)->bool

    def add_service(self, svc: Any) -> None:
        self.services.append(svc)

    def add_core_handler(self, h: Any) -> None:
        self.core_handlers.append(h)

    async def dispatch(self, hook: str, ev: Event) -> None:
        # ACL precheck for PRIVMSG commands
        if hook == "on_privmsg" and getattr(self.bot, "acl", None) is not None:
            ok = await self.bot.acl.precheck(self.bot, ev)
            if not ok:
                return

            # core handlers first (auth/help/service ctl etc)
            for h in self.core_handlers:
                fn = getattr(h, "handle_core", None)
                if callable(fn):
                    try:
                        handled = await fn(self.bot, ev)
                        if handled:
                            return
                    except Exception:
                        log.exception("Core handler error (%s)", type(h).__name__)

        for svc in self.services:
            fn = getattr(svc, hook, None)
            if not callable(fn):
                continue

            # Global gating: services are disabled by default per channel.
            # Service objects may set `service_id`; otherwise derive from module name.
            if ev.channel and hook in ("on_privmsg", "on_join", "on_part", "on_quit", "on_kick", "on_nick"):
                sid = getattr(svc, "service_id", None)
                if not sid:
                    sid = type(svc).__module__.split(".")[-1]
                # Skip core-like services: none, help, acl, servicectl are not in services list.
                try:
                    if not await self.bot.store.is_service_enabled(ev.channel, sid):
                        continue
                except Exception:
                    # If DB fails, fail closed (skip)
                    continue

            try:
                await fn(self.bot, ev)
            except Exception:
                log.exception("Service error in %s (%s)", hook, type(svc).__name__)
