"""
Pokemon service for Leobot.

Users in the channel become trainers. New users get 5 random starter Pokémon.
Wild Pokémon spawn periodically; trainers can capture them.
Trainers can heal, revive, level up via channel or PM.

Channel commands: !pokemon, !pkmn, !capture [ball], !battle @user
PM commands: !pokemon team #channel, !pokemon items #channel, !use potion <slot> #channel, etc.

Config: pokemon.wild_spawns_per_day (default 24)
"""

from __future__ import annotations

import logging
import random
import time

from system.acl import ROLE_ORDER

log = logging.getLogger("leobot.pokemon")


def _lower(s: str) -> str:
    return (s or "").strip().lower()


def _norm(s: str) -> str:
    return (s or "").strip()


# Ball modifier for capture rate (lower effective rate = easier catch)
BALL_MODIFIERS = {
    "pokeball": 1.0,
    "great_ball": 1.5,
    "ultra_ball": 2.0,
}


class PokemonService:
    service_id = "pokemon"

    def __init__(self, bot, cfg: dict | None = None):
        self.bot = bot
        self.cfg = cfg or {}
        self.spawn_duration_s = int(self.cfg.get("spawn_duration_seconds", 600))
        self._cooldown: dict[tuple[str, str], float] = {}
        self.cooldown_s = int(self.cfg.get("cooldown_seconds", 2))

    def _cooldown_ok(self, target: str, key: str) -> bool:
        now = time.time()
        k = (target, key)
        until = self._cooldown.get(k, 0)
        if now < until:
            return False
        self._cooldown[k] = now + self.cooldown_s
        return True

    def _resolve_channel(self, ev, rest: str) -> str | None:
        """Resolve channel from message. For PM, channel must be in rest (e.g. #General)."""
        if ev.channel:
            return ev.channel
        # PM: look for #channel in rest
        parts = (rest or "").split()
        for p in parts:
            if p.startswith("#"):
                return p
        return None

    async def on_join(self, bot, ev) -> None:
        """When a user joins, ensure they're a trainer; if new, give 5 starters."""
        if not ev.channel or not str(ev.channel).startswith("#"):
            return
        if not await bot.store.is_service_enabled(ev.channel, "pokemon"):
            return
        if _lower(ev.nick) == _lower(bot.cfg.get("nick", "")):
            return
        created = await bot.store.pokemon_ensure_trainer(ev.nick, ev.channel)
        if created:
            pokemon_count = await bot.store.pokemon_species_count()
            if pokemon_count > 0:
                team = await bot.store.pokemon_trainer_get_pokemon(ev.nick, ev.channel)
                names = [r.get("species_name") or "?" for r in team[:5]]
                await bot.privmsg(
                    ev.channel,
                    f"Welcome {ev.nick}! You're a new Pokémon trainer! You received: {', '.join(names)}",
                )

    async def on_privmsg(self, bot, ev) -> None:
        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()
        if not text.startswith(prefix):
            return
        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return
        parts = cmdline.split()
        cmd = (parts[0] or "").lower()
        if cmd not in ("pokemon", "pkmn", "capture", "use", "battle", "levelup"):
            return

        # PM: pokemon service works for trainers to check team, items, heal, revive
        if ev.is_private:
            await self._handle_pm(bot, ev, cmd, parts[1:])
            return

        # Channel: must have pokemon enabled
        if not ev.channel or not await bot.store.is_service_enabled(ev.channel, "pokemon"):
            return

        if cmd == "levelup":
            await self._handle_levelup(bot, ev, parts[1:])
            return
        if cmd == "levelup":
            await self._handle_levelup(bot, ev, parts[1:])
            return
        if cmd == "levelup":
            await self._handle_levelup(bot, ev, parts[1:])
            return
        if cmd == "capture":
            await self._handle_capture(bot, ev, parts[1:])
            return
        if cmd == "levelup":
            await self._handle_levelup(bot, ev, parts[1:])
            return
        if cmd == "battle":
            await self._handle_battle(bot, ev, parts[1:])
            return
        if cmd in ("pokemon", "pkmn"):
            await self._handle_channel_pokemon(bot, ev, parts[1:])
            return

    async def _handle_pm(self, bot, ev, cmd: str, args: list) -> None:
        """Handle PM: !pkmn team #chan, !pkmn items #chan, !use potion 1 #chan"""
        rest = " ".join(args)
        channel = self._resolve_channel(ev, rest)
        if not channel:
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: Specify a channel. E.g. !pkmn team #General or !use potion 1 #General",
            )
            return
        if not channel.startswith("#"):
            await bot.privmsg(ev.target, f"{ev.nick}: Channel must start with #")
            return

        # Check if they're a trainer in that channel
        if not await bot.store.is_service_enabled(channel, "pokemon"):
            await bot.privmsg(ev.target, f"{ev.nick}: Pokémon isn't enabled in {channel}")
            return

        if cmd == "use":
            await self._handle_use_item_pm(bot, ev, args, channel)
            return
        if cmd == "levelup":
            await self._handle_levelup_pm(bot, ev, args, channel)
            return

        # Ensure trainer exists (might have joined before service was added)
        await bot.store.pokemon_ensure_trainer(ev.nick, channel)
        nick_l = _lower(ev.nick)

        if cmd in ("pokemon", "pkmn"):
            sub = (args[0] or "").lower() if args else "team"
            if sub == "team" or sub == channel:
                sub = "team"
            elif sub in ("items", "item", "inv", "inventory"):
                sub = "items"
            elif sub == "heal":
                await self._show_heal_help(bot, ev)
                return
            elif sub == "revive":
                await self._show_revive_help(bot, ev)
                return

            if sub == "team":
                team = await bot.store.pokemon_trainer_get_pokemon(nick_l, channel)
                if not team:
                    await bot.privmsg(ev.target, f"{ev.nick}: You have no Pokémon in {channel}.")
                    return
                lines = []
                for r in team:
                    name = r.get("nickname") or r.get("species_name") or "?"
                    lvl = r.get("level") or 5
                    hp = r.get("current_hp") or 0
                    mhp = r.get("max_hp") or 1
                    faint = " [FAINTED]" if r.get("is_fainted") else ""
                    slot = r.get("slot") or 0
                    lines.append(f"  {slot}. {name} Lv.{lvl} HP {hp}/{mhp}{faint}")
                await bot.privmsg(
                    ev.target,
                    f"{ev.nick}: Your Pokémon in {channel}:\n" + "\n".join(lines),
                )
            elif sub == "items":
                items = await bot.store.pokemon_trainer_get_items(nick_l, channel)
                if not items:
                    await bot.privmsg(ev.target, f"{ev.nick}: No items in {channel}.")
                    return
                lines = [f"  {r.get('name')} x{r.get('quantity')}" for r in items]
                await bot.privmsg(
                    ev.target,
                    f"{ev.nick}: Items in {channel}:\n" + "\n".join(lines),
                )

    async def _show_heal_help(self, bot, ev) -> None:
        await bot.privmsg(
            ev.target,
            f"{ev.nick}: Use !use potion <slot> #channel to heal. E.g. !use potion 1 #General",
        )

    async def _show_revive_help(self, bot, ev) -> None:
        await bot.privmsg(
            ev.target,
            f"{ev.nick}: Use !use revive <slot> #channel to revive. E.g. !use revive 2 #General",
        )

    async def _handle_levelup_pm(self, bot, ev, args: list, channel: str) -> None:
        if not args:
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: Usage: !levelup <slot> #channel  e.g. !levelup 1 #General",
            )
            return
        try:
            slot = int((args[0] or "").strip())
        except ValueError:
            await bot.privmsg(ev.target, f"{ev.nick}: Slot must be 1-6.")
            return
        if slot < 1 or slot > 6:
            await bot.privmsg(ev.target, f"{ev.nick}: Slot must be 1-6.")
            return
        team = await bot.store.pokemon_trainer_get_pokemon(_lower(ev.nick), channel)
        poke = next((p for p in team if int(p.get("slot") or 0) == slot), None)
        if not poke:
            await bot.privmsg(ev.target, f"{ev.nick}: No Pokémon in slot {slot}.")
            return
        ok, msg = await bot.store.pokemon_level_up(ev.nick, channel, int(poke["id"]))
        await bot.privmsg(ev.target, f"{ev.nick}: {msg}")

    async def _handle_levelup(self, bot, ev, args: list) -> None:
        if not args:
            await bot.privmsg(ev.target, f"{ev.nick}: Usage: !levelup <slot>")
            return
        try:
            slot = int((args[0] or "").strip())
        except ValueError:
            await bot.privmsg(ev.target, f"{ev.nick}: Slot must be 1-6.")
            return
        if slot < 1 or slot > 6:
            await bot.privmsg(ev.target, f"{ev.nick}: Slot must be 1-6.")
            return
        team = await bot.store.pokemon_trainer_get_pokemon(ev.nick, ev.channel)
        poke = next((p for p in team if int(p.get("slot") or 0) == slot), None)
        if not poke:
            await bot.privmsg(ev.target, f"{ev.nick}: No Pokémon in slot {slot}.")
            return
        ok, msg = await bot.store.pokemon_level_up(ev.nick, ev.channel, int(poke["id"]))
        await bot.privmsg(ev.target, f"{ev.nick}: {msg}")

    async def _handle_use_item_pm(self, bot, ev, args: list, channel: str) -> None:
        if len(args) < 3:
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: Usage: !use <item> <slot> #channel  e.g. !use potion 1 #General",
            )
            return
        item_id = (args[0] or "").strip().lower()
        slot_str = (args[1] or "").strip()
        try:
            slot = int(slot_str)
        except ValueError:
            await bot.privmsg(ev.target, f"{ev.nick}: Slot must be a number 1-6.")
            return
        if slot < 1 or slot > 6:
            await bot.privmsg(ev.target, f"{ev.nick}: Slot must be 1-6.")
            return
        team = await bot.store.pokemon_trainer_get_pokemon(_lower(ev.nick), channel)
        poke = next((p for p in team if int(p.get("slot") or 0) == slot), None)
        if not poke:
            await bot.privmsg(ev.target, f"{ev.nick}: No Pokémon in slot {slot}.")
            return
        pokemon_id = int(poke["id"])
        ok, msg = await bot.store.pokemon_trainer_use_item(
            ev.nick, channel, item_id, pokemon_id=pokemon_id
        )
        await bot.privmsg(ev.target, f"{ev.nick}: {msg}")

    async def _handle_channel_pokemon(self, bot, ev, args: list) -> None:
        sub = (args[0] or "").lower() if args else "help"
        if sub == "spawns":
            role = await bot.acl.effective_role(ev)
            if ROLE_ORDER.get(role, 0) < ROLE_ORDER.get("contributor", 0):
                await bot.privmsg(
                    ev.target,
                    f"{ev.nick}: Only contributors can change spawn rate.",
                )
                return
            if len(args) >= 2:
                try:
                    n = int(args[1])
                    if n < 1 or n > 1440:
                        raise ValueError("1-1440")
                    await bot.store.pokemon_set_spawns_per_day(n)
                    await bot.privmsg(
                        ev.target,
                        f"{ev.nick}: Wild spawns per day set to {n}.",
                    )
                except ValueError:
                    await bot.privmsg(
                        ev.target,
                        f"{ev.nick}: Use !pokemon spawns <1-1440>",
                    )
            else:
                n = await bot.store.pokemon_get_spawns_per_day()
                await bot.privmsg(
                    ev.target,
                    f"{ev.nick}: Wild Pokémon spawn limit: {n} per day. Use !pokemon spawns <N> to change.",
                )
            return

        # Ensure trainer
        created = await bot.store.pokemon_ensure_trainer(ev.nick, ev.channel)
        if created:
            team = await bot.store.pokemon_trainer_get_pokemon(ev.nick, ev.channel)
            names = [r.get("species_name") or "?" for r in team[:5]]
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: Welcome! You received: {', '.join(names)}. PM me for team/items/heal/revive.",
            )
            return

        if sub in ("team", "party"):
            team = await bot.store.pokemon_trainer_get_pokemon(ev.nick, ev.channel)
            if not team:
                await bot.privmsg(ev.target, f"{ev.nick}: You have no Pokémon.")
                return
            lines = []
            for r in team[:6]:
                name = r.get("nickname") or r.get("species_name") or "?"
                lvl = r.get("level") or 5
                hp = r.get("current_hp") or 0
                mhp = r.get("max_hp") or 1
                faint = " [FAINTED]" if r.get("is_fainted") else ""
                lines.append(f"{name} Lv.{lvl} {hp}/{mhp}{faint}")
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: {', '.join(lines)}",
            )
        elif sub == "help":
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: !pokemon team | !capture [ball] | !battle @user | !levelup <slot> | PM for items/heal/revive. !pokemon spawns [N]",
            )
        else:
            await bot.privmsg(ev.target, f"{ev.nick}: Unknown. Try !pokemon help")

    async def _handle_capture(self, bot, ev, args: list) -> None:
        if not self._cooldown_ok(ev.target, "capture"):
            return
        ball_id = (args[0] or "pokeball").strip().lower() if args else "pokeball"
        ball_mod = BALL_MODIFIERS.get(ball_id, 1.0)
        await bot.store.pokemon_ensure_trainer(ev.nick, ev.channel)
        has_ball = await bot.store.fetchone(
            "SELECT quantity FROM pokemon_trainer_items WHERE trainer_nick=? AND channel=? AND item_id=? AND quantity>0",
            (_lower(ev.nick), ev.channel, ball_id),
        )
        if not has_ball:
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: You need a {ball_id.replace('_', ' ').title()} to capture. Use !pokemon to check your items.",
            )
            return
        ok, msg = await bot.store.pokemon_wild_spawn_capture(
            ev.channel, ev.nick, ball_modifier=ball_mod
        )
        if ok:
            await bot.store.pokemon_trainer_deduct_item(ev.nick, ev.channel, ball_id, 1)
            await bot.privmsg(ev.target, f"{ev.nick}: Gotcha! {msg} was caught!")
        else:
            await bot.privmsg(ev.target, f"{ev.nick}: {msg}")

    async def _handle_battle(self, bot, ev, args: list) -> None:
        """Simple battle: two trainers, winner by highest total level."""
        if not self._cooldown_ok(ev.target, "battle"):
            return
        target_nick = None
        for a in args:
            a = (a or "").strip()
            if a.startswith("@"):
                target_nick = a[1:].strip()
                break
            elif a and not a.startswith("#"):
                target_nick = a
                break
        if not target_nick:
            await bot.privmsg(ev.target, f"{ev.nick}: Usage: !battle @user")
            return
        nick_l = _lower(ev.nick)
        target_l = _lower(target_nick)
        if nick_l == target_l:
            await bot.privmsg(ev.target, f"{ev.nick}: You can't battle yourself!")
            return
        my_team = await bot.store.pokemon_trainer_get_pokemon(ev.nick, ev.channel)
        their_team = await bot.store.pokemon_trainer_get_pokemon(target_nick, ev.channel)
        if not my_team:
            await bot.privmsg(ev.target, f"{ev.nick}: You have no Pokémon!")
            return
        if not their_team:
            await bot.privmsg(ev.target, f"{ev.nick}: {target_nick} is not a trainer.")
            return
        my_fainted = sum(1 for p in my_team if p.get("is_fainted"))
        their_fainted = sum(1 for p in their_team if p.get("is_fainted"))
        my_able = len(my_team) - my_fainted
        their_able = len(their_team) - their_fainted
        if my_able == 0:
            await bot.privmsg(ev.target, f"{ev.nick}: All your Pokémon have fainted! Heal up first.")
            return
        if their_able == 0:
            await bot.privmsg(ev.target, f"{ev.nick}: {target_nick}'s team is all fainted.")
            return
        my_total = sum(int(p.get("level") or 5) for p in my_team if not p.get("is_fainted"))
        their_total = sum(int(p.get("level") or 5) for p in their_team if not p.get("is_fainted"))
        roll = random.randint(0, my_total + their_total - 1) if (my_total + their_total) > 0 else 0
        if roll < my_total:
            winner, loser = ev.nick, target_nick
        else:
            winner, loser = target_nick, ev.nick
        await bot.privmsg(
            ev.target,
            f"Battle! {ev.nick} vs {target_nick} — {winner} wins!",
        )

    async def job_wild_spawn(self, bot) -> None:
        """Spawn wild Pokémon in eligible channels (respecting daily limit)."""
        try:
            channels = bot.cfg.get("channels", []) or []
            eligible = []
            for ch in channels:
                if ch.startswith("#") and await bot.store.is_service_enabled(ch, "pokemon"):
                    eligible.append(ch)
            if not eligible:
                return
            max_per_day = await bot.store.pokemon_get_spawns_per_day()
            candidates = []
            for ch in eligible:
                count = await bot.store.pokemon_wild_spawn_count_24h(ch)
                if count < max_per_day:
                    candidates.append(ch)
            if not candidates:
                return
            channel = random.choice(candidates)
            existing = await bot.store.pokemon_wild_spawn_get(channel)
            if existing:
                return
            species_list = await bot.store.pokemon_species_get_random(1)
            if not species_list:
                return
            sp = species_list[0]
            level = random.randint(3, 15)
            created = await bot.store.pokemon_wild_spawn_create(
                channel, int(sp["id"]), level=level
            )
            if created:
                name = sp.get("name") or "?"
                t1 = sp.get("type1") or "?"
                t2 = sp.get("type2")
                types = f"{t1}"
                if t2:
                    types += f"/{t2}"
                await bot.privmsg(
                    channel,
                    f"A wild {name} (Lv.{level}, {types}) appeared! Use !capture [ball] to catch it!",
                )
                log.debug("Wild %s spawned in %s", name, channel)
        except Exception:
            log.exception("Pokemon wild spawn job failed")


def setup(bot):
    cfg = bot.cfg.get("pokemon", {})
    svc = PokemonService(bot, cfg)
    if hasattr(bot, "register_command"):
        bot.register_command(
            "pokemon",
            min_role="guest",
            mutating=False,
            help="Pokémon trainer: !pokemon team, !capture, !battle @user. PM for items/heal/revive.",
            category="Games",
            service_id="pokemon",
        )
        bot.register_command(
            "pkmn",
            min_role="guest",
            mutating=False,
            help="Alias for !pokemon",
            category="Games",
            service_id="pokemon",
        )
        bot.register_command(
            "capture",
            min_role="guest",
            mutating=True,
            help="Capture wild Pokémon. !capture [pokeball|great_ball|ultra_ball]",
            category="Games",
            service_id="pokemon",
        )
        bot.register_command(
            "battle",
            min_role="guest",
            mutating=False,
            help="Battle another trainer: !battle @user",
            category="Games",
            service_id="pokemon",
        )
        bot.register_command(
            "levelup",
            min_role="guest",
            mutating=True,
            help="Level up a Pokémon: !levelup <slot>",
            category="Games",
            service_id="pokemon",
        )
    if getattr(bot, "scheduler", None) and hasattr(bot.scheduler, "register_interval"):
        interval = 24 * 3600 // max(1, int(cfg.get("wild_spawns_per_day", 24)))
        interval = max(300, min(86400, interval))
        bot.scheduler.register_interval(
            "pokemon:wild_spawn",
            float(interval),
            lambda: svc.job_wild_spawn(bot),
            jitter_seconds=interval * 0.3,
            run_on_start=False,
        )
    return svc
