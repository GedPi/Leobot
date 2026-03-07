from __future__ import annotations

# Fact service: !fact returns a random fact; !fact {category} returns a random fact from that category.
# All facts and categories are stored in the database; categories are inferred from distinct category values.


class FactService:
    service_id = "fact"

    async def on_privmsg(self, bot, ev) -> None:
        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()

        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix) :].strip()
        if not cmdline:
            return

        parts = cmdline.split(maxsplit=1)
        cmd = (parts[0] or "").lower()
        if cmd != "fact":
            return

        category_arg = (parts[1].strip() if len(parts) > 1 else "").strip()

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


def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command(
            "fact",
            min_role="guest",
            mutating=False,
            help="Random fact. Usage: !fact [category] — omit category for any, or use e.g. !fact science",
            category="Info",
        )
    return FactService()
