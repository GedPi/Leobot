from __future__ import annotations

import logging
import time

log = logging.getLogger("leobot.greet")


def _now() -> int:
    return int(time.time())


def _lower(s: str) -> str:
    return (s or "").strip().lower()


def setup(bot):
    return GreetService(bot)


class GreetService:
    """Join greetings (normalized DB schema).

    Data model:
      - greet_targets (match rules)
      - greetings (one greeting per row)

    This service is DISABLED by default. Enable per channel:
      !service enable greet #Channel
    """

    def __init__(self, bot):
        self.bot = bot
        self.cooldown_per_nick = 900
        self.cooldown_per_channel = 3
        self._cool_nick: dict[str, int] = {}
        self._cool_chan: dict[str, int] = {}

        bot.register_command(
            "greet test",
            min_role="contributor",
            mutating=False,
            help="Test greet matching for your current identity (no DB changes).",
            category="Greet",
        )

    def _cooldown_ok(self, nick: str, channel: str) -> bool:
        now = _now()
        nl = _lower(nick)
        cl = _lower(channel)
        if now < self._cool_nick.get(nl, 0):
            return False
        if now < self._cool_chan.get(cl, 0):
            return False
        self._cool_nick[nl] = now + self.cooldown_per_nick
        self._cool_chan[cl] = now + self.cooldown_per_channel
        return True

    async def on_join(self, bot, ev):
        if not ev.channel:
            return
        if not await bot.store.is_service_enabled(ev.channel, "greet"):
            return

        # Avoid greeting ourselves
        if ev.nick.lower() == bot.cfg.get("nick", "").lower():
            return

        if not self._cooldown_ok(ev.nick, ev.channel):
            return

        hostmask = ""
        userhost = ""
        host = ev.host or ""
        if ev.nick and ev.user and ev.host:
            hostmask = f"{ev.nick}!{ev.user}@{ev.host}"
            userhost = f"{ev.user}@{ev.host}"

        target = await bot.store.greet_select_target(
            nick=ev.nick,
            hostmask=hostmask,
            userhost=userhost,
            host=host,
            channel=ev.channel,
        )
        if not target:
            return

        greet = await bot.store.greet_pick_greeting(int(target["id"]))
        if not greet:
            return

        greet = greet.replace("{nick}", ev.nick).replace("{channel}", ev.channel)
        await bot.privmsg(ev.channel, greet)

    async def on_privmsg(self, bot, ev):
        txt = (ev.text or "").strip()
        prefix = bot.cfg.get("command_prefix", "!")
        if not txt.startswith(prefix):
            return

        cmdline = txt[len(prefix):].strip().lower()
        if cmdline != "greet test":
            return

        # test current identity
        hostmask = ""
        userhost = ""
        host = ev.host or ""
        if ev.nick and ev.user and ev.host:
            hostmask = f"{ev.nick}!{ev.user}@{ev.host}"
            userhost = f"{ev.user}@{ev.host}"

        channel = ev.channel or ""
        target = await bot.store.greet_select_target(
            nick=ev.nick,
            hostmask=hostmask,
            userhost=userhost,
            host=host,
            channel=channel or "",
        )
        if not target:
            await bot.privmsg(ev.target, f"{ev.nick}: no greet target matched.")
            return
        greet = await bot.store.greet_pick_greeting(int(target["id"]))
        await bot.privmsg(ev.target, f"{ev.nick}: matched target id={target['id']} priority={target['priority']} greeting={greet!r}")
