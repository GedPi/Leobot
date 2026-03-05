from __future__ import annotations

# Routes IRC events to core handlers (ACL, Help, ServiceCtl) then to services; enforces ACL precheck and per-channel service enablement.

import logging
from typing import Any

from system.types import Event

log = logging.getLogger("leobot.dispatch")


# Holds core_handlers and services; dispatch() runs ACL precheck for PRIVMSG, then core handle_core, then each service hook with enablement gating.
class Dispatcher:
    def __init__(self, bot):
        self.bot = bot
        self.services: list[Any] = []
        self.core_handlers: list[Any] = []

    def add_service(self, svc: Any) -> None:
        self.services.append(svc)

    def add_core_handler(self, h: Any) -> None:
        self.core_handlers.append(h)

    # Returns the service instance whose service_id is "logging", or None; used to tee PRIVMSG to logs when a core handler consumes the event.
    def _find_logging_service(self) -> Any | None:
        for svc in self.services:
            if getattr(svc, "service_id", None) == "logging":
                return svc
        return None

    # Sends a copy of the event to the logging service when a core handler has already handled the message, so commands are still logged.
    async def _tee_to_logging(self, hook: str, ev: Event) -> None:
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
        try:
            if not await self.bot.store.is_service_enabled(ev.channel, "logging"):
                return
        except Exception:
            return

        try:
            await fn(self.bot, ev)
        except Exception:
            log.exception("Logging tee failed")

    # For on_privmsg runs ACL precheck then core handle_core; for on_notice runs core on_notice; then invokes each service hook for the event, skipping services disabled for ev.channel.
    async def dispatch(self, hook: str, ev: Event) -> None:
        if hook == "on_privmsg" and getattr(self.bot, "acl", None) is not None:
            ok = await self.bot.acl.precheck(self.bot, ev)
            if not ok:
                return

            for h in self.core_handlers:
                fn = getattr(h, "handle_core", None)
                if callable(fn):
                    try:
                        handled = await fn(self.bot, ev)
                        if handled:
                            await self._tee_to_logging(hook, ev)
                            return
                    except Exception:
                        log.exception("Core handler error (%s)", type(h).__name__)

        if hook == "on_notice":
            for h in self.core_handlers:
                fn = getattr(h, "on_notice", None)
                if callable(fn):
                    try:
                        await fn(self.bot, ev)
                    except Exception:
                        log.exception("Core handler error in on_notice (%s)", type(h).__name__)

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