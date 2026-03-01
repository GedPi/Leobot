from __future__ import annotations


def setup(bot):
    return MaintenanceService(bot)


class MaintenanceService:
    """Maintenance service.

    Note: pruning/retention is intentionally not implemented yet.
    """

    def __init__(self, bot):
        self.bot = bot

    async def on_privmsg(self, bot, ev):
        return