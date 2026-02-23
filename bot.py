#!/opt/leobot/venv/bin/python
import asyncio
import importlib
import json
import logging
import signal
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


CONFIG_PATH = Path("/etc/leobot/config.json")
LOG_PATH = Path("/var/log/leobot/bot.log")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH),
            logging.StreamHandler(),
        ],
    )


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Missing config at {CONFIG_PATH}. Create it before starting the service."
        )
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    required = ["server", "port", "nick", "user", "realname", "channels", "services"]
    for k in required:
        if k not in cfg:
            raise ValueError(f"Config missing required key: {k}")

    if not isinstance(cfg["channels"], list) or not cfg["channels"]:
        raise ValueError("Config 'channels' must be a non-empty list (e.g. ['#test']).")

    if not isinstance(cfg["services"], list) or not cfg["services"]:
        raise ValueError("Config 'services' must be a non-empty list (e.g. ['services.acl', 'services.help']).")

    cfg.setdefault("use_tls", True)
    cfg.setdefault("verify_tls", True)
    cfg.setdefault("password", None)  # server PASS
    cfg.setdefault("nickserv_password", None)
    cfg.setdefault("command_prefix", "!")
    cfg.setdefault("reconnect_min_seconds", 2)
    cfg.setdefault("reconnect_max_seconds", 60)
    return cfg


@dataclass
class Event:
    nick: str
    user: str | None
    host: str | None
    target: str            # where bot should reply (channel or PM nick)
    channel: str | None    # channel if applicable
    text: str | None
    is_private: bool
    raw: str
    cmd: str
    params: list[str]
    old_nick: str | None = None
    new_nick: str | None = None
    victim: str | None = None
    kicker: str | None = None


def _parse_prefix(prefix: str) -> tuple[str, Optional[str], Optional[str]]:
    # nick!user@host
    if "!" in prefix and "@" in prefix:
        nick, rest = prefix.split("!", 1)
        user, host = rest.split("@", 1)
        return nick, user, host
    return prefix, None, None


class IRCBot:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.log = logging.getLogger("leobot")
        self.commands: dict[str, dict] = {}
        self.services: list[object] = []

        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.stop_event = asyncio.Event()

        # ACL service will set this in its setup()
        self.acl = None

    def register_command(
        self,
        cmd: str,
        *,
        min_role: str = "user",
        mutating: bool = False,
        help: str = "",
        category: str = "General",
    ) -> None:
        c = (cmd or "").strip().lower().lstrip("!")
        if not c:
            return
        self.commands[c] = {
            "min_role": min_role,
            "mutating": bool(mutating),
            "help": (help or "").strip(),
            "category": (category or "General").strip(),
        }

    async def send_raw(self, line: str) -> None:
        assert self.writer is not None
        wire = (line + "\r\n").encode("utf-8", errors="ignore")
        self.writer.write(wire)
        await self.writer.drain()
        logging.info(">> %s", line)

    async def close_connection(self) -> None:
        try:
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()
        except Exception:
            pass
        finally:
            self.reader = None
            self.writer = None

    async def privmsg(self, target: str, msg: str) -> None:
        for chunk in self._chunk_message(msg, limit=380):
            await self.send_raw(f"PRIVMSG {target} :{chunk}")

    def _chunk_message(self, msg: str, limit: int = 380):
        msg = msg.replace("\r", " ").replace("\n", " ")
        while len(msg) > limit:
            yield msg[:limit]
            msg = msg[limit:]
        yield msg

    def load_services(self) -> None:
        self.services = []
        for modname in self.cfg.get("services", []):
            self.log.info("Loading service: %s", modname)
            mod = importlib.import_module(modname)
            if not hasattr(mod, "setup"):
                raise RuntimeError(f"Service module {modname} has no setup(bot)")
            svc = mod.setup(self)
            if svc is not None:
                self.services.append(svc)

    async def connect(self) -> None:
        server = self.cfg["server"]
        port = int(self.cfg["port"])
        use_tls = bool(self.cfg.get("use_tls", True))

        ssl_ctx = None
        if use_tls:
            ssl_ctx = ssl.create_default_context()
            if not self.cfg.get("verify_tls", True):
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE

        logging.info("Connecting to %s:%s TLS=%s", server, port, use_tls)
        self.reader, self.writer = await asyncio.open_connection(server, port, ssl=ssl_ctx)

        if self.cfg.get("password"):
            await self.send_raw(f"PASS {self.cfg['password']}")

        await self.send_raw(f"NICK {self.cfg['nick']}")
        await self.send_raw(f"USER {self.cfg['user']} 0 * :{self.cfg['realname']}")

    async def run(self) -> None:
        # Load services before connecting so they can register commands
        self.load_services()
        await self.connect()

        assert self.reader is not None
        while not self.stop_event.is_set():
            line_b = await self.reader.readline()
            if not line_b:
                raise ConnectionError("Disconnected (EOF).")
            line = line_b.decode("utf-8", errors="ignore").rstrip("\r\n")
            logging.info("<< %s", line)
            await self.handle_line(line)

    async def handle_line(self, line: str) -> None:
        if line.startswith("PING "):
            token = line.split(" ", 1)[1]
            await self.send_raw(f"PONG {token}")
            return

        prefix = ""
        rest = line
        if rest.startswith(":"):
            prefix, rest = rest[1:].split(" ", 1)

        trailing = None
        if " :" in rest:
            head, trailing = rest.split(" :", 1)
            parts = head.split()
        else:
            parts = rest.split()

        if trailing is not None:
            parts.append(trailing)

        if not parts:
            return

        cmd = parts[0]
        params = parts[1:]

        # Welcome
        if cmd == "001":
            for chan in self.cfg["channels"]:
                await self.send_raw(f"JOIN {chan}")
                await asyncio.sleep(0.7)
            if self.cfg.get("nickserv_password"):
                await self.privmsg("NickServ", f"IDENTIFY {self.cfg['nickserv_password']}")
            # notify services
            for svc in self.services:
                fn = getattr(svc, "on_ready", None)
                if callable(fn):
                    try:
                        await fn(self)
                    except Exception:
                        self.log.exception("Service error in on_ready (%s)", type(svc).__name__)
            return

        nick, user, host = ("", None, None)
        if prefix:
            nick, user, host = _parse_prefix(prefix)

        # Dispatch events
        if cmd == "PRIVMSG" and len(params) >= 2:
            target = params[0]
            text = params[1]
            is_private = target.lower() == self.cfg["nick"].lower()
            reply_target = nick if is_private else target
            channel = None if is_private else target

            ev = Event(
                nick=nick,
                user=user,
                host=host,
                target=reply_target,
                channel=channel,
                text=text,
                is_private=is_private,
                raw=line,
                cmd=cmd,
                params=params,
            )
            await self.dispatch("on_privmsg", ev)
            return

        if cmd == "JOIN" and params:
            channel = params[0]
            ev = Event(nick=nick, user=user, host=host, target=channel, channel=channel, text=None, is_private=False, raw=line, cmd=cmd, params=params)
            await self.dispatch("on_join", ev)
            return

        if cmd == "PART" and params:
            channel = params[0]
            ev = Event(nick=nick, user=user, host=host, target=channel, channel=channel, text=params[1] if len(params) > 1 else None, is_private=False, raw=line, cmd=cmd, params=params)
            await self.dispatch("on_part", ev)
            return

        if cmd == "QUIT":
            ev = Event(nick=nick, user=user, host=host, target=nick, channel=None, text=params[0] if params else None, is_private=True, raw=line, cmd=cmd, params=params)
            await self.dispatch("on_quit", ev)
            return

        if cmd == "NICK" and params:
            new_nick = params[0]
            ev = Event(nick=new_nick, user=user, host=host, target=new_nick, channel=None, text=None, is_private=True, raw=line, cmd=cmd, params=params, old_nick=nick, new_nick=new_nick)
            await self.dispatch("on_nick", ev)
            return

        if cmd == "KICK" and len(params) >= 2:
            channel = params[0]
            victim = params[1]
            ev = Event(nick=nick, user=user, host=host, target=channel, channel=channel, text=params[2] if len(params) > 2 else None, is_private=False, raw=line, cmd=cmd, params=params, victim=victim, kicker=nick)
            await self.dispatch("on_kick", ev)
            return

    async def dispatch(self, hook: str, ev: Event) -> None:
        # ACL precheck (if installed)
        if getattr(self, "acl", None) is not None and hook == "on_privmsg":
            try:
                ok = await self.acl.precheck(self, ev)
                if not ok:
                    return
            except Exception:
                self.log.exception("ACL precheck error")
                # fail open

        for svc in self.services:
            fn = getattr(svc, hook, None)
            if callable(fn):
                try:
                    await fn(self, ev)
                except Exception:
                    self.log.exception("Service error in %s (%s)", hook, type(svc).__name__)

    async def shutdown(self) -> None:
        self.stop_event.set()
        try:
            if self.writer:
                await self.send_raw("QUIT :Shutting down")
                self.writer.close()
                await self.writer.wait_closed()
        except Exception:
            pass


async def main():
    setup_logging()
    cfg = load_config()
    bot = IRCBot(cfg)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(bot.shutdown()))

    backoff = int(cfg["reconnect_min_seconds"])
    backoff_max = int(cfg["reconnect_max_seconds"])

    while True:
        try:
            await bot.run()
            backoff = int(cfg["reconnect_min_seconds"])
        except Exception as e:
            logging.exception("Bot crashed/disconnected: %s", e)

        await bot.shutdown()
        await bot.close_connection()
        logging.info("Reconnecting in %ss...", backoff)
        await asyncio.sleep(backoff)
        bot.stop_event = asyncio.Event()
        backoff = min(backoff * 2, backoff_max)


if __name__ == "__main__":
    asyncio.run(main())
