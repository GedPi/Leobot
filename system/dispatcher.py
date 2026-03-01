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

    def _find_logging_service(self) -> Any | None:
        for svc in self.services:
            if getattr(svc, "service_id", None) == "logging":
                return svc
        return None

    async def _tee_to_logging(self, hook: str, ev: Event) -> None:
        """
        Ensure canonical logging gets a copy of events even when core handlers
        short-circuit dispatch (commands).
        """
        if hook != "on_privmsg":
            return
        if not ev.channel:
            return  # we don't log PMs by default
        svc = self._find_logging_service()
        if not svc:
            return
        fn = getattr(svc, "on_privmsg", None)
        if not callable(fn):
            return

        # respect per-channel enablement for logging
        try:
            if not await self.bot.store.is_service_enabled(ev.channel, "logging"):
                return
        except Exception:
            return

        try:
            await fn(self.bot, ev)
        except Exception:
            log.exception("Logging tee failed")

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
                            # core handled it; still tee the event to logging for posterity
                            await self._tee_to_logging(hook, ev)
                            return
                    except Exception:
                        log.exception("Core handler error (%s)", type(h).__name__)

        gated_hooks = (
            "on_privmsg",
            "on_notice",
            "on_join",
            "on_part",
            "on_quit",
            "on_kick",
            "on_nick",
            "on_mode",
            "on_topic",
        )

        for svc in self.services:
            fn = getattr(svc, hook, None)
            if not callable(fn):
                continue

            # Per-channel enablement gating (disabled by default)
            if ev.channel and hook in gated_hooks:
                sid = getattr(svc, "service_id", None)
                if not sid:
                    sid = type(svc).__module__.split(".")[-1]
                try:
                    if not await self.bot.store.is_service_enabled(ev.channel, sid):
                        continue
                except Exception:
                    continue

            try:
                await fn(self.bot, ev)
            except Exception:
                log.exception("Service error in %s (%s)", hook, type(svc).__name__)