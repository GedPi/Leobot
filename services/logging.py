import time
from typing import Dict, Set

from services.chatdb import ChatDB, DBConfig


PREFIX_ORDER = ["~", "&", "@", "%", "+"]  # owner, admin, op, halfop, voice
MODE_TO_PREFIX = {
    "q": "~",
    "a": "&",
    "o": "@",
    "h": "%",
    "v": "+",
}
PREFIX_TO_MODE = {v: k for k, v in MODE_TO_PREFIX.items()}


def _now() -> int:
    return int(time.time())


def _best_prefix(flags: Set[str]) -> str:
    for p in PREFIX_ORDER:
        if p in flags:
            return p
    return ""


def _parse_action(text: str) -> str | None:
    # CTCP ACTION: "\x01ACTION waves\x01"
    if text.startswith("\x01ACTION ") and text.endswith("\x01"):
        return text[len("\x01ACTION ") : -1].strip()
    return None


class LoggingService:
    """
    Logging service:
      - enabled/disabled per channel via !service enable/disable logging
      - stores a human-readable stream in SQLite (table: channel_log)
    """

    _service_name = "logging"

    def __init__(self, bot, cfg=None):
        cfg = cfg or {}
        db_path = "/var/lib/leobot/db/leobot.db"
        try:
            db_path = (bot.cfg.get("chatdb") or {}).get("db_path", db_path)
        except Exception:
            pass

        self.db = ChatDB(DBConfig(str(db_path)))

        # channel -> nick(lower) -> set(prefix flags {'@','+'...})
        self.chan_flags: Dict[str, Dict[str, Set[str]]] = {}

        # nick(lower) -> set(channels) we believe they are in
        self.nick_channels: Dict[str, Set[str]] = {}

    async def _log(self, channel: str, mode: str, nick: str, message: str) -> None:
        if not channel or not message:
            return
        ts = _now()
        await self.db.execute(
            "INSERT INTO channel_log(ts, channel, mode, nick, message) VALUES(?,?,?,?,?)",
            (ts, channel, mode or "", nick or "", message),
        )

    async def _log_if_enabled(self, bot, channel: str, mode: str, nick: str, message: str) -> None:
        # For hooks where ev.channel is None (QUIT/NICK), we must enforce enablement ourselves.
        try:
            if not await bot.db.is_service_enabled("logging", channel):
                return
        except Exception:
            # Fail open if DB check fails (consistent with rest of bot).
            pass
        await self._log(channel, mode, nick, message)

    def _ensure_chan(self, channel: str) -> None:
        if channel not in self.chan_flags:
            self.chan_flags[channel] = {}

    def _set_prefix(self, channel: str, nick: str, prefix: str) -> None:
        self._ensure_chan(channel)
        key = nick.lower()
        flags = set()
        if prefix in PREFIX_TO_MODE:
            flags.add(prefix)
        self.chan_flags[channel][key] = flags

    def _add_flag(self, channel: str, nick: str, prefix: str) -> None:
        self._ensure_chan(channel)
        key = nick.lower()
        cur = self.chan_flags[channel].get(key, set())
        if prefix:
            cur.add(prefix)
        self.chan_flags[channel][key] = cur

    def _remove_flag(self, channel: str, nick: str, prefix: str) -> None:
        self._ensure_chan(channel)
        key = nick.lower()
        cur = self.chan_flags[channel].get(key, set())
        if prefix and prefix in cur:
            cur.remove(prefix)
        self.chan_flags[channel][key] = cur

    def _get_mode_prefix(self, channel: str, nick: str) -> str:
        try:
            flags = self.chan_flags.get(channel, {}).get(nick.lower(), set())
            return _best_prefix(flags)
        except Exception:
            return ""

    def _track_join(self, channel: str, nick: str) -> None:
        n = nick.lower()
        self.nick_channels.setdefault(n, set()).add(channel)

    def _track_part(self, channel: str, nick: str) -> None:
        n = nick.lower()
        if n in self.nick_channels and channel in self.nick_channels[n]:
            self.nick_channels[n].remove(channel)
            if not self.nick_channels[n]:
                self.nick_channels.pop(n, None)
        # also drop their flags for that channel
        self.chan_flags.get(channel, {}).pop(n, None)

    async def on_ready(self, bot) -> None:
        # Make sure schema exists (ChatDB will run SCHEMA on first use, but this is cheap).
        await self.db.ensure_schema()

    async def on_privmsg(self, bot, ev) -> None:
        if ev.is_private:
            return
        if not ev.channel:
            return

        text = (ev.text or "").strip()
        if not text:
            return

        prefix = self._get_mode_prefix(ev.channel, ev.nick)

        action = _parse_action(text)
        if action is not None:
            # log as user would see it
            await self._log(ev.channel, prefix, ev.nick, f"* {ev.nick} {action}")
        else:
            await self._log(ev.channel, prefix, ev.nick, text)

    async def on_join(self, bot, ev) -> None:
        if not ev.channel:
            return
        self._track_join(ev.channel, ev.nick)

        # When *bot* joins, request NAMES to seed prefixes.
        if ev.nick.lower() == bot.cfg.get("nick", "").lower():
            try:
                await bot.send_raw(f"NAMES {ev.channel}")
            except Exception:
                pass

        await self._log(ev.channel, "", ev.nick, f"*** Joins: {ev.nick}")

    async def on_part(self, bot, ev) -> None:
        if not ev.channel:
            return
        reason = (ev.text or "").strip()
        self._track_part(ev.channel, ev.nick)
        msg = f"*** Parts: {ev.nick}"
        if reason:
            msg += f" ({reason})"
        await self._log(ev.channel, "", ev.nick, msg)

    async def on_kick(self, bot, ev) -> None:
        if not ev.channel:
            return
        victim = ev.victim or ""
        reason = (ev.text or "").strip()
        if victim:
            self._track_part(ev.channel, victim)
        msg = f"*** {victim} was kicked by {ev.kicker or ev.nick}"
        if reason:
            msg += f" ({reason})"
        await self._log(ev.channel, "", ev.nick, msg)

    async def on_nick(self, bot, ev) -> None:
        # Bot dispatches nick changes without channel context; log to channels we believe they're in.
        old = (ev.old_nick or "").strip()
        new = (ev.new_nick or ev.nick or "").strip()
        if not old or not new:
            return

        old_key = old.lower()
        chans = list(self.nick_channels.get(old_key, set()))
        if not chans:
            return

        # Move membership tracking to new nick
        self.nick_channels.setdefault(new.lower(), set()).update(self.nick_channels.get(old_key, set()))
        self.nick_channels.pop(old_key, None)

        # Move flags per channel
        for ch in chans:
            flags_map = self.chan_flags.get(ch, {})
            if old_key in flags_map:
                flags_map[new.lower()] = flags_map.pop(old_key)

        for ch in chans:
            await self._log_if_enabled(bot, ch, "", old, f"*** {old} is now known as {new}")

    async def on_quit(self, bot, ev) -> None:
        # QUIT is also without channel context; log to channels we believe they're in.
        nick = (ev.nick or "").strip()
        if not nick:
            return
        reason = (ev.text or "").strip()
        chans = list(self.nick_channels.get(nick.lower(), set()))
        if not chans:
            return

        msg = f"*** Quits: {nick}"
        if reason:
            msg += f" ({reason})"

        for ch in chans:
            await self._log_if_enabled(bot, ch, "", nick, msg)

        # Clear tracking
        for ch in chans:
            self.chan_flags.get(ch, {}).pop(nick.lower(), None)
        self.nick_channels.pop(nick.lower(), None)

    async def on_topic(self, bot, ev) -> None:
        if not ev.channel:
            return
        topic = (ev.text or "").strip()
        await self._log(ev.channel, "", ev.nick, f"*** {ev.nick} changed the topic to: {topic}")

    async def on_mode(self, bot, ev) -> None:
        if not ev.channel:
            return

        # ev.params: [<channel>, <modestring>, <arg1>, <arg2>...]
        if len(ev.params) < 2:
            return
        mode_str = ev.params[1]
        args = ev.params[2:]

        # Update prefix tracking for +o/-o/+v etc.
        sign = None
        arg_i = 0
        for c in mode_str:
            if c == "+":
                sign = "+"
                continue
            if c == "-":
                sign = "-"
                continue
            if c in MODE_TO_PREFIX:
                if arg_i >= len(args):
                    continue
                target_nick = args[arg_i]
                arg_i += 1
                prefix = MODE_TO_PREFIX[c]
                if sign == "+":
                    self._add_flag(ev.channel, target_nick, prefix)
                    self._track_join(ev.channel, target_nick)  # harmless; ensures mapping exists
                elif sign == "-":
                    self._remove_flag(ev.channel, target_nick, prefix)
                continue
            # Other mode chars may or may not consume args. We don't need them for prefix tracking.

        # Log a human-readable mode line
        tail = " ".join(ev.params[1:]).strip()
        await self._log(ev.channel, "", ev.nick, f"*** {ev.nick} sets mode: {tail}")

    async def on_names(self, bot, ev) -> None:
        # NAMES numeric 353: params contain names with prefixes
        if not ev.channel:
            return
        names = (ev.text or "").strip()
        if not names:
            return

        self._ensure_chan(ev.channel)

        for token in names.split():
            prefix = ""
            nick = token
            if token and token[0] in PREFIX_TO_MODE:
                prefix = token[0]
                nick = token[1:]
            if not nick:
                continue
            self._track_join(ev.channel, nick)
            self._set_prefix(ev.channel, nick, prefix)


def setup(bot):
    # No user-facing commands required; it’s controlled via !service enable/disable logging
    if hasattr(bot, "register_command"):
        bot.register_command(
            "logging",
            min_role="admin",
            mutating=False,
            help="Channel logging service (no direct commands). Enable/disable with !service enable/disable logging.",
            category="Admin",
        )
    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register(
            "logging",
            min_role="admin",
            mutating=False,
            help="Channel logging service (no direct commands). Enable/disable with !service enable/disable logging.",
            category="Admin",
        )

    return LoggingService(bot, (bot.cfg.get("logging") or {}) if isinstance(bot.cfg, dict) else {})