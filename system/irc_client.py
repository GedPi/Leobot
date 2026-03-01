from __future__ import annotations

import asyncio
import logging
import ssl
from typing import Awaitable, Callable, Optional

from system.irc_parse import chunk_message

log = logging.getLogger("leobot.irc")

LineHandler = Callable[[str], Awaitable[None]]


class IRCClient:
    def __init__(self, cfg: dict, on_line: LineHandler):
        self.cfg = cfg
        self.on_line = on_line
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

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

        log.info("Connecting to %s:%s TLS=%s", server, port, use_tls)
        self.reader, self.writer = await asyncio.open_connection(server, port, ssl=ssl_ctx)

        if self.cfg.get("password"):
            await self.send_raw(f"PASS {self.cfg['password']}")

        await self.send_raw(f"NICK {self.cfg['nick']}")
        await self.send_raw(f"USER {self.cfg['user']} 0 * :{self.cfg['realname']}")

    async def close(self) -> None:
        """Best-effort close of the underlying stream."""
        try:
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()
        except Exception:
            # Close is best-effort; TLS shutdown can throw on some servers.
            pass
        finally:
            self.reader = None
            self.writer = None

    async def send_raw(self, line: str) -> None:
        assert self.writer is not None
        wire = (line + "\r\n").encode("utf-8", errors="ignore")
        self.writer.write(wire)
        await self.writer.drain()
        log.info(">> %s", line)

    async def privmsg(self, target: str, msg: str) -> None:
        for chunk in chunk_message(msg, limit=380):
            await self.send_raw(f"PRIVMSG {target} :{chunk}")

    async def run(self, stop_event: asyncio.Event) -> None:
        """Read loop.

        During graceful shutdown, asyncio's TLS transport can raise ssl.SSLError
        (e.g. APPLICATION_DATA_AFTER_CLOSE_NOTIFY). That is not a crash in our
        context; it just means the peer sent data after close_notify. When we are
        stopping, swallow and exit cleanly.
        """
        assert self.reader is not None

        while not stop_event.is_set():
            try:
                line_b = await self.reader.readline()
            except (ssl.SSLError, ConnectionResetError, BrokenPipeError) as e:
                if stop_event.is_set():
                    log.debug("Ignoring TLS/connection error during shutdown: %r", e)
                    break
                raise

            if not line_b:
                # EOF. If we're stopping, treat as clean exit.
                if stop_event.is_set():
                    break
                raise ConnectionError("Disconnected (EOF)")

            line = line_b.decode("utf-8", errors="ignore").rstrip("\r\n")
            log.info("<< %s", line)

            if line.startswith("PING "):
                token = line.split(" ", 1)[1]
                await self.send_raw(f"PONG {token}")
                continue

            await self.on_line(line)