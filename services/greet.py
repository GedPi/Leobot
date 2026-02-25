import json
import random
import time
import logging
log = logging.getLogger("leobot.greet")
from dataclasses import dataclass
from services.store import Store
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

GREETINGS_PATH_DEFAULT = Path("/var/lib/leobot/greetings.json")


def _now() -> int:
    return int(time.time())


def _norm(s: str) -> str:
    return (s or "").strip()


def _lower(s: str) -> str:
    return (s or "").strip().lower()


def _safe_list(x) -> list:
    return x if isinstance(x, list) else []


def _render(tpl: str, *, nick: str, channel: str) -> str:
    # Minimal templating; safe and predictable
    return (
        tpl.replace("{nick}", nick)
        .replace("{channel}", channel)
    )


def _extract_hostmask(ev) -> dict[str, str]:
    """
    Best-effort extraction from whatever your event object provides.
    We support matching against:
      - hostmask: nick!user@host
      - userhost: user@host
      - host: host
    """
    nick = getattr(ev, "nick", "") or ""
    user = getattr(ev, "user", "") or getattr(ev, "ident", "") or ""
    host = getattr(ev, "host", "") or getattr(ev, "hostname", "") or ""

    hostmask = ""
    userhost = ""
    if nick and user and host:
        hostmask = f"{nick}!{user}@{host}"
        userhost = f"{user}@{host}"
    elif user and host:
        userhost = f"{user}@{host}"
    return {
        "nick": nick,
        "user": user,
        "host": host,
        "hostmask": hostmask,
        "userhost": userhost,
    }


@dataclass
class GreetConfig:
    path: Path
    enabled: bool = True
    channel_whitelist: list[str] | None = None
    cooldown_seconds_per_nick: int = 900          # 15 min default
    cooldown_seconds_per_channel: int = 3         # anti-flood for netsplits, etc.
    rules: list[dict[str, Any]] = None


class GreetService:
        self.path = Path((cfg.get("path") or str(GREETINGS_PATH_DEFAULT)))  # legacy import location only

        db_path = "/var/lib/leobot/db/leobot.db"
        if isinstance(getattr(bot, "cfg", None), dict):
            db_path = bot.cfg.get("chatdb", {}).get("db_path", db_path)

        self.store = Store(db_path)
        self._loaded = False

        self._cfg = GreetConfig(path=self.path, rules=[])
        self._cool_nick: dict[str, int] = {}       # nick_lower -> ts_until
        self._cool_chan: dict[str, int] = {}       # channel_lower -> ts_until
    
    async def _load(self, force: bool = False) -> None:
        # DB is authoritative. Legacy JSON is imported ONCE if DB is empty.
        if self._loaded and not force:
            return

        try:
            imported = await self.store.greet_import_from_legacy_file(self.path)
            rules = await self.store.greet_list_rules()

            # For now keep these operational toggles local/defaults (we can move to kv later)
            enabled = True
            whitelist = None
            cd_nick = 900
            cd_chan = 3

            self._cfg = GreetConfig(
                path=self.path,
                enabled=enabled,
                channel_whitelist=whitelist,
                cooldown_seconds_per_nick=max(0, int(cd_nick)),
                cooldown_seconds_per_channel=max(0, int(cd_chan)),
                rules=rules,
            )
            self._loaded = True
            if imported:
                log.info("Greetings imported from legacy file into DB: %s rules", imported)
            log.info("Greetings loaded from DB: %s rules", len(rules))
        except Exception as e:
            log.error("Failed to load greetings from DB: %s: %s", type(e).__name__, e)

    def _channel_allowed(self, channel: str) -> bool:
        wl = self._cfg.channel_whitelist
        if not wl:
            return True
        cl = channel.strip().lower()
        return any(cl == c.strip().lower() for c in wl)

    def _cooldown_ok(self, nick: str, channel: str) -> bool:
        now = _now()
        nl = _lower(nick)
        cl = _lower(channel)

        n_until = self._cool_nick.get(nl, 0)
        if now < n_until:
            return False

        c_until = self._cool_chan.get(cl, 0)
        if now < c_until:
            return False

        self._cool_nick[nl] = now + self._cfg.cooldown_seconds_per_nick
        self._cool_chan[cl] = now + self._cfg.cooldown_seconds_per_channel
        return True

    def _match_rule(self, rule: dict[str, Any], nick: str, hostmask: str, userhost: str, host: str) -> bool:
        match = rule.get("match") or {}
        nicks = [str(x) for x in _safe_list(match.get("nicks"))]
        hosts = [str(x) for x in _safe_list(match.get("hosts"))]

        # Nick match is case-insensitive exact
        if nicks:
            for n in nicks:
                if _lower(n) == _lower(nick):
                    return True

        # Host patterns are wildcard fnmatch against:
        # - full hostmask: nick!user@host
        # - user@host
        # - host
        if hosts:
            candidates = [hostmask, userhost, host]
            candidates = [c for c in candidates if c]
            for pat in hosts:
                pat = pat.strip()
                if not pat:
                    continue
                for c in candidates:
                    if fnmatch(c, pat):
                        return True

        return False

    def _choose_greeting(self, rule: dict[str, Any]) -> str | None:
        greets = _safe_list(rule.get("greetings"))
        greets = [str(g) for g in greets if str(g).strip()]
        if not greets:
            return None
        return random.choice(greets)

    def _select_rule(self, nick: str, hostmask: str, userhost: str, host: str) -> dict[str, Any] | None:
        matches: list[tuple[int, dict[str, Any]]] = []
        for rule in (self._cfg.rules or []):
            if not isinstance(rule, dict):
                continue
            if self._match_rule(rule, nick, hostmask, userhost, host):
                pr = rule.get("priority", 0)
                try:
                    pr = int(pr)
                except Exception:
                    pr = 0
                matches.append((pr, rule))

        if not matches:
            return None

        # Highest priority wins; if tie, first in file wins
        matches.sort(key=lambda x: x[0], reverse=True)
        top_pr = matches[0][0]
        for pr, rule in matches:
            if pr == top_pr:
                return rule
        return matches[0][1]

    async def on_join(self, bot, ev) -> None:
        # Auto-reload on mtime change
        self._load(force=False)

        if not self._cfg.enabled:
            return

        channel = getattr(ev, "channel", None) or getattr(ev, "target", None) or ""
        channel = str(channel)
        if not channel.startswith("#"):
            return
        if not self._channel_allowed(channel):
            return

        hm = _extract_hostmask(ev)
        nick = hm["nick"] or getattr(ev, "nick", "") or ""
        if not nick:
            return

        # cooldown to avoid spam
        if not self._cooldown_ok(nick, channel):
            return

        rule = self._select_rule(nick, hm["hostmask"], hm["userhost"], hm["host"])
        if not rule:
            return

        greet = self._choose_greeting(rule)
        if not greet:
            return

        msg = _render(greet, nick=nick, channel=channel)
        await bot.privmsg(channel, msg)

    async def on_privmsg(self, bot, ev) -> None:
        # Optional operational commands (no edits, just reload/test)

        prefix = bot.cfg.get("command_prefix", "!")
        text = (getattr(ev, "text", "") or "").strip()
        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return

        parts = cmdline.split()
        cmd = parts[0].lower()
        if cmd != "greet":
            return

        # !greet reload
        if len(parts) == 2 and parts[1].lower() == "reload":
            self._load(force=True)
            await bot.privmsg(ev.target, f"{ev.nick}: greetings reloaded ({len(self._cfg.rules or [])} rules).")
            return

        # !greet test <nick> [hostmask]
        # hostmask example: Ged!ged@HairyOctopus.net
        if len(parts) >= 3 and parts[1].lower() == "test":
            test_nick = parts[2]
            hostmask = parts[3] if len(parts) >= 4 else ""
            userhost = ""
            host = ""
            if "@" in hostmask and "!" in hostmask:
                try:
                    userhost = hostmask.split("!", 1)[1]
                    host = userhost.split("@", 1)[1]
                except Exception:
                    pass
            elif "@" in hostmask:
                userhost = hostmask
                try:
                    host = hostmask.split("@", 1)[1]
                except Exception:
                    pass
            else:
                host = hostmask

            rule = self._select_rule(test_nick, hostmask, userhost, host)
            if not rule:
                await bot.privmsg(ev.target, f"{ev.nick}: no rule matched.")
                return
            rid = rule.get("id") or "(no id)"
            await bot.privmsg(ev.target, f"{ev.nick}: matched rule '{rid}'.")
            return

        await bot.privmsg(ev.target, f"{ev.nick}: usage: !greet reload  |  !greet test <nick> [hostmask]")
        return


def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command("greet reload", min_role="contributor", mutating=False, help="Reload greetings.json from disk.", category="Fun")
        bot.register_command("greet test", min_role="contributor", mutating=False, help="Test greeting matching. Usage: !greet test <nick> [hostmask]", category="Fun")
    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register("greet reload", min_role="contributor", mutating=False, help="Reload greetings.json from disk.", category="Fun")
        bot.acl.register("greet test", min_role="contributor", mutating=False, help="Test greeting matching. Usage: !greet test <nick> [hostmask]", category="Fun")

    return GreetService(bot, bot.cfg.get('greet', {}) if isinstance(bot.cfg, dict) else {})
