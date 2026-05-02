from __future__ import annotations

import logging
import random
import time

from system.acl import ROLE_ORDER

log = logging.getLogger("leobot.lover")


class LoverService:
    service_id = "lover"
    PUBLIC_COOLDOWN_SECONDS = 30 * 60

    def __init__(self, bot):
        self.bot = bot

    async def _require_admin_pm(self, bot, ev) -> bool:
        if not ev.is_private:
            await bot.reply(ev, f"{ev.nick}: use this command in a private message to me.")
            return False
        role = await bot.acl.effective_role(ev)
        if ROLE_ORDER.get(role, 0) < ROLE_ORDER.get("admin", 0):
            await bot.reply(ev, f"{ev.nick}: requires admin role.")
            return False
        return True

    async def _send_love_messages(
        self,
        bot,
        *,
        nick: str,
        channel: str,
        today: str,
        enforce_public_cooldown: bool = True,
    ) -> tuple[bool, bool, int]:
        min_v, max_v = await bot.store.lover_get_min_max()
        _ = min_v  # min is used by scheduler target selection; max gates hard send count.
        sent_public = False
        sent_private = False

        count = await bot.store.lover_daily_count_get(nick, today)
        if count >= max_v:
            return sent_public, sent_private, count

        line = await bot.store.lover_line_get_random()
        if not line:
            return sent_public, sent_private, count

        remaining = max_v - count

        if remaining > 0:
            public_ok = True
            if enforce_public_cooldown:
                public_ok = await bot.store.lover_public_cooldown_ready(
                    channel, self.PUBLIC_COOLDOWN_SECONDS
                )
            if public_ok:
                await bot.privmsg(channel, f"@{nick} {line}")
                await bot.store.lover_public_cooldown_mark_now(channel)
                await bot.store.lover_daily_count_increment(nick, today)
                count += 1
                remaining -= 1
                sent_public = True

        if remaining > 0:
            await bot.privmsg(nick, line)
            await bot.store.lover_daily_count_increment(nick, today)
            count += 1
            sent_private = True

        return sent_public, sent_private, count

    async def on_privmsg(self, bot, ev) -> None:
        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()
        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix) :].strip()
        if not cmdline:
            return

        parts = cmdline.split()
        cmd = (parts[0] or "").lower()
        if cmd not in ("love", "unlove", "lover"):
            return

        if not await self._require_admin_pm(bot, ev):
            return

        if cmd in ("love", "unlove"):
            if len(parts) < 3:
                await bot.reply(ev, f"{ev.nick}: usage: !{cmd} <nick> <#channel>")
                return

            nick = (parts[1] or "").strip()
            channel = (parts[2] or "").strip()
            if not nick or not channel.startswith("#"):
                await bot.reply(ev, f"{ev.nick}: usage: !{cmd} <nick> <#channel>")
                return

            if cmd == "unlove":
                exists = await bot.store.lover_target_exists(nick, channel)
                if not exists:
                    await bot.reply(ev, f"{ev.nick}: no lover target for {nick} in {channel}.")
                    return
                await bot.store.lover_target_set_enabled(nick, channel, False)
                await bot.reply(ev, f"{ev.nick}: removed {nick} from lover targets in {channel}.")
                return

            await bot.store.lover_target_upsert(
                nick,
                channel,
                enabled=True,
                created_by=ev.nick,
            )
            today = time.strftime("%Y-%m-%d", time.gmtime())
            sent_public, sent_private, count = await self._send_love_messages(
                bot,
                nick=nick,
                channel=channel,
                today=today,
                enforce_public_cooldown=True,
            )
            if sent_public and sent_private:
                await bot.privmsg(
                    ev.target,
                    f"{ev.nick}: lover target set for {nick} in {channel}. First messages sent (today count={count}).",
                )
            elif sent_public or sent_private:
                await bot.privmsg(
                    ev.target,
                    f"{ev.nick}: lover target set for {nick} in {channel}. First message sent (today count={count}).",
                )
            else:
                await bot.privmsg(
                    ev.target,
                    f"{ev.nick}: lover target set for {nick} in {channel}, but no message sent (no lines loaded or daily max reached).",
                )
            return

        # !lover ...
        sub = (parts[1] or "").lower() if len(parts) >= 2 else ""

        if sub in ("on", "off", "status"):
            if len(parts) < 3:
                await bot.reply(ev, f"{ev.nick}: usage: !lover {sub} <#channel>")
                return
            channel = (parts[2] or "").strip()
            if not channel.startswith("#"):
                await bot.reply(ev, f"{ev.nick}: usage: !lover {sub} <#channel>")
                return

            if sub == "on":
                await bot.store.lover_enablement_set(channel, True, updated_by=ev.nick)
                await bot.reply(ev, f"{ev.nick}: lover enabled in {channel}.")
                return
            if sub == "off":
                await bot.store.lover_enablement_set(channel, False, updated_by=ev.nick)
                await bot.reply(ev, f"{ev.nick}: lover disabled in {channel}.")
                return

            enabled = await bot.store.lover_enablement_is_enabled(channel)
            min_v, max_v = await bot.store.lover_get_min_max()
            lines = await bot.store.lover_line_count_enabled()
            targets = await bot.store.lover_targets_list(channel=channel, enabled_only=True)
            status = "ON" if enabled else "OFF"
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: lover in {channel}: {status}. targets={len(targets)} lines={lines} min/max={min_v}/{max_v}.",
            )
            return

        if sub == "min":
            if len(parts) < 3:
                await bot.reply(ev, f"{ev.nick}: usage: !lover min <1-100>")
                return
            try:
                n = int(parts[2])
                if n < 1 or n > 100:
                    raise ValueError("out of range")
                _, cur_max = await bot.store.lover_get_min_max()
                if n > cur_max:
                    await bot.store.lover_set_min_max(minimum=n, maximum=n)
                else:
                    await bot.store.lover_set_min_max(minimum=n)
                await bot.reply(ev, f"{ev.nick}: lover min per user per day set to {n}.")
            except ValueError:
                await bot.reply(ev, f"{ev.nick}: usage: !lover min <1-100>")
            return

        if sub == "max":
            if len(parts) < 3:
                await bot.reply(ev, f"{ev.nick}: usage: !lover max <1-100>")
                return
            try:
                n = int(parts[2])
                if n < 1 or n > 100:
                    raise ValueError("out of range")
                cur_min, _ = await bot.store.lover_get_min_max()
                if n < cur_min:
                    await bot.store.lover_set_min_max(minimum=n, maximum=n)
                else:
                    await bot.store.lover_set_min_max(maximum=n)
                await bot.reply(ev, f"{ev.nick}: lover max per user per day set to {n}.")
            except ValueError:
                await bot.reply(ev, f"{ev.nick}: usage: !lover max <1-100>")
            return

        if sub == "list":
            channel = (parts[2] or "").strip() if len(parts) >= 3 else ""
            if channel and not channel.startswith("#"):
                await bot.reply(ev, f"{ev.nick}: usage: !lover list [#channel]")
                return
            rows = await bot.store.lover_targets_list(channel=channel or None, enabled_only=True)
            if not rows:
                await bot.reply(ev, f"{ev.nick}: no active lover targets.")
                return
            items = [f"{nick}@{ch}" for nick, ch in rows[:20]]
            extra = "" if len(rows) <= 20 else f" (+{len(rows) - 20} more)"
            scope = channel if channel else "all channels"
            await bot.reply(ev, f"{ev.nick}: lover targets ({scope}): " + ", ".join(items) + extra)
            return

        await bot.privmsg(
            ev.target,
            f"{ev.nick}: usage: !lover <on|off|status> <#channel> | !lover min <n> | !lover max <n> | !lover list [#channel]",
        )

    async def job_auto_lover(self, bot) -> None:
        """Scheduler job: sends pickup lines to one random eligible target."""
        try:
            configured_channels = set(bot.cfg.get("channels", []) or [])
            if not configured_channels:
                return

            enabled_channels = await bot.store.lover_enablement_list_enabled_channels()
            eligible_channels: set[str] = set()
            for channel in enabled_channels:
                if channel not in configured_channels:
                    continue
                if not await bot.store.is_service_enabled(channel, "lover"):
                    continue
                eligible_channels.add(channel)
            if not eligible_channels:
                return

            targets = await bot.store.lover_targets_list(enabled_only=True)
            if not targets:
                return

            today = time.strftime("%Y-%m-%d", time.gmtime())
            min_v, max_v = await bot.store.lover_get_min_max()
            under_min: list[tuple[str, str]] = []
            candidates: list[tuple[str, str]] = []

            for nick, channel in targets:
                if channel not in eligible_channels:
                    continue
                count = await bot.store.lover_daily_count_get(nick, today)
                if count >= max_v:
                    continue
                candidates.append((nick, channel))
                if count < min_v:
                    under_min.append((nick, channel))

            if not candidates:
                return

            pool = under_min if under_min else candidates
            nick, channel = random.choice(pool)
            sent_public, sent_private, _ = await self._send_love_messages(
                bot,
                nick=nick,
                channel=channel,
                today=today,
                enforce_public_cooldown=True,
            )
            if sent_public or sent_private:
                log.debug(
                    "Lover auto sent to %s in %s (public=%s private=%s)",
                    nick,
                    channel,
                    sent_public,
                    sent_private,
                )
        except Exception:
            log.exception("Lover auto job failed")


def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command(
            "love",
            min_role="admin",
            mutating=True,
            help="Add or re-enable a lover target (PM only). Usage: !love <nick> <#channel>",
            category="Fun",
            service_id="lover",
        )
        bot.register_command(
            "unlove",
            min_role="admin",
            mutating=True,
            help="Disable a lover target (PM only). Usage: !unlove <nick> <#channel>",
            category="Fun",
            service_id="lover",
        )
        bot.register_command(
            "lover",
            min_role="admin",
            mutating=False,
            help="Manage lover service (PM only). Usage: !lover <on|off|status|min|max|list> ...",
            category="Fun",
            service_id="lover",
        )
        bot.register_command(
            "lover on",
            min_role="admin",
            mutating=True,
            help="Enable lover for a channel (PM only). Usage: !lover on <#channel>",
            category="Fun",
            service_id="lover",
        )
        bot.register_command(
            "lover off",
            min_role="admin",
            mutating=True,
            help="Disable lover for a channel (PM only). Usage: !lover off <#channel>",
            category="Fun",
            service_id="lover",
        )
        bot.register_command(
            "lover status",
            min_role="admin",
            mutating=False,
            help="Show lover status for a channel (PM only). Usage: !lover status <#channel>",
            category="Fun",
            service_id="lover",
        )
        bot.register_command(
            "lover min",
            min_role="admin",
            mutating=True,
            help="Set lover min hits per user/day (PM only). Usage: !lover min <1-100>",
            category="Fun",
            service_id="lover",
        )
        bot.register_command(
            "lover max",
            min_role="admin",
            mutating=True,
            help="Set lover max hits per user/day (PM only). Usage: !lover max <1-100>",
            category="Fun",
            service_id="lover",
        )
        bot.register_command(
            "lover list",
            min_role="admin",
            mutating=False,
            help="List lover targets (PM only). Usage: !lover list [#channel]",
            category="Fun",
            service_id="lover",
        )

    svc = LoverService(bot)

    if getattr(bot, "scheduler", None) is not None and hasattr(bot.scheduler, "register_interval"):
        base_interval = 30 * 60
        jitter = 20 * 60
        bot.scheduler.register_interval(
            "lover:auto",
            base_interval,
            lambda: svc.job_auto_lover(bot),
            jitter_seconds=jitter,
            run_on_start=False,
        )

    return svc
