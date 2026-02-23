from collections import defaultdict

# NOTE:
# Help is now implemented centrally by the CommandRouter in bot.py.
# This module is kept as a compatibility stub so 'services.help' can remain
# in config.json without producing duplicate output.

class HelpService:
    def __init__(self, bot):
        self.bot = bot

    async def on_privmsg(self, bot, ev):
        # Router handles !help / !commands before services are dispatched.
        return

def setup(bot):
    # No registration needed; router provides built-in help.
    return HelpService(bot)