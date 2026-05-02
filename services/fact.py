from __future__ import annotations

from system.commands import parse_command

# Fact service: !fact returns a random fact; !fact {category} returns a random fact from that category.
# !fact auto on|off toggles per-channel automatic facts; !fact auto shows status.
# All facts and categories are stored in the database; categories are inferred from distinct category values.

import logging
import random
import time

from system.acl import ROLE_ORDER

log = logging.getLogger("leobot.fact")


class FactService:
    service_id = "fact"

    def __init__(self, bot):
        self.bot = bot

    async def on_privmsg(self, bot, ev) -> None:
        parsed = parse_command(ev.text, bot.cfg.get("command_prefix", "!"))
        if not parsed:
            return
        cmd = parsed["name"]
        parts = [cmd, " ".join(parsed["args"])] if parsed["args"] else [cmd]
        if cmd != "fact":
            return

        rest = (parts[1].strip() if len(parts) > 1 else "").strip()
        tokens = rest.split() if rest else []
        sub = (tokens[0] or "").lower() if tokens else ""

        # !fact auto on | !fact auto off | !fact auto | !fact auto min N | !fact auto max N
        if sub == "auto":
            if not ev.channel:
                await bot.privmsg(ev.target, f"{ev.nick}: Use !fact auto in a channel.")
                return

            role = await bot.acl.effective_role(ev)
            if ROLE_ORDER.get(role, 0) < ROLE_ORDER.get("contributor", 0):
                await bot.privmsg(ev.target, f"{ev.nick}: Requires contributor role to change fact auto.")
                return

            sub2 = (tokens[1] or "").lower() if len(tokens) >= 2 else ""
            arg2 = (tokens[2] or "").strip() if len(tokens) >= 3 else ""

            if sub2 == "on":
                await bot.store.fact_auto_set_enabled(ev.channel, True, updated_by=ev.nick)
                await bot.privmsg(ev.target, f"{ev.nick}: Fact auto enabled in {ev.channel}.")
                return
            if sub2 == "off":
                await bot.store.fact_auto_set_enabled(ev.channel, False, updated_by=ev.nick)
                await bot.privmsg(ev.target, f"{ev.nick}: Fact auto disabled in {ev.channel}.")
                return
            if sub2 == "min" and arg2:
                try:
                    n = int(arg2)
                    if n < 1 or n > 100:
                        raise ValueError("out of range")
                    await bot.store.set_setting("fact_auto_min_per_day", str(n))
                    await bot.privmsg(ev.target, f"{ev.nick}: Fact auto min per day set to {n}.")
                except ValueError:
                    await bot.privmsg(ev.target, f"{ev.nick}: Use !fact auto min <1-100>.")
                return
            if sub2 == "max" and arg2:
                try:
                    n = int(arg2)
                    if n < 1 or n > 100:
                        raise ValueError("out of range")
                    await bot.store.set_setting("fact_auto_max_per_day", str(n))
                    await bot.privmsg(ev.target, f"{ev.nick}: Fact auto max per day set to {n}.")
                except ValueError:
                    await bot.privmsg(ev.target, f"{ev.nick}: Use !fact auto max <1-100>.")
                return

            # !fact auto (show status)
            enabled = await bot.store.fact_auto_is_enabled(ev.channel)
            min_v, max_v = await bot.store.fact_auto_get_min_max()
            today = time.strftime("%Y-%m-%d", time.gmtime())
            count = await bot.store.fact_auto_get_posted_count(ev.channel, today)
            status = "ON" if enabled else "OFF"
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: Fact auto in {ev.channel}: {status}. Today: {count}/{max_v} (min={min_v}, max={max_v}).",
            )
            return

        # Original !fact [category] handling
        category_arg = rest

        if category_arg:
            result = await bot.store.fact_get_random_by_category(category_arg)
            if result is None:
                categories = await bot.store.fact_list_categories()
                if not categories:
                    await bot.privmsg(ev.target, f"{ev.nick}: No facts in the database yet.")
                    return
                cats_str = ", ".join(sorted(categories)[:15])
                if len(categories) > 15:
                    cats_str += f" (+{len(categories) - 15} more)"
                await bot.privmsg(
                    ev.target,
                    f"{ev.nick}: No facts for '{category_arg}'. Categories: {cats_str}",
                )
                return
            category, fact = result
        else:
            result = await bot.store.fact_get_random()
            if result is None:
                await bot.privmsg(ev.target, f"{ev.nick}: No facts in the database yet.")
                return
            category, fact = result

        await bot.privmsg(ev.target, f"{category} fact!: {fact}")

    async def job_auto_fact(self, bot) -> None:
        """Scheduler job: post one auto fact to a random eligible channel."""
        try:
            channels = bot.cfg.get("channels", []) or []
            if not channels:
                return

            enabled = await bot.store.fact_auto_list_enabled_channels()
            eligible = [
                ch for ch in enabled
                if ch in channels and await bot.store.is_service_enabled(ch, "fact")
            ]
            if not eligible:
                return

            today = time.strftime("%Y-%m-%d", time.gmtime())
            min_v, max_v = await bot.store.fact_auto_get_min_max()

            candidates = []
            for ch in eligible:
                count = await bot.store.fact_auto_get_posted_count(ch, today)
                if count < max_v:
                    candidates.append(ch)

            if not candidates:
                return

            channel = random.choice(candidates)
            count = await bot.store.fact_auto_get_posted_count(channel, today)
            if count >= max_v:
                return

            result = await bot.store.fact_get_random()
            if not result:
                return

            _, fact = result
            online = getattr(bot, "get_channel_users", lambda _: [])(channel)
            nick = random.choice(online) if online else "everyone"

            await bot.store.fact_auto_increment_posted(channel, today)
            await bot.privmsg(channel, f"Hey {nick} did you know that {fact}")
            log.debug("Fact auto posted to %s for %s", channel, nick)
        except Exception:
            log.exception("Fact auto job failed")


def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command(
            "fact",
            min_role="guest",
            mutating=False,
            help="Random fact. Usage: !fact [category] — omit category for any, or use e.g. !fact science. !fact auto on|off for automatic facts.",
            category="Info",
        )

    svc = FactService(bot)

    if getattr(bot, "scheduler", None) is not None and hasattr(bot.scheduler, "register_interval"):
        base_interval = 2 * 3600
        jitter = base_interval * 0.5
        bot.scheduler.register_interval(
            "fact:auto",
            base_interval,
            lambda: svc.job_auto_fact(bot),
            jitter_seconds=jitter,
            run_on_start=False,
        )

    return svc
