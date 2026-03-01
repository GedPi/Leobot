from __future__ import annotations

import asyncio
import logging
import ssl
from typing import Awaitable, Callable, Optional, Set

from system.irc_parse import chunk_message

log = logging.getLogger("leobot.irc")

LineHandler = Callable[[str], Awaitable[None]]


class IRCClient:
    def __init__(self, cfg: dict, on_line: LineHandler):
        self.cfg = cfg
        self.on_line = on_line
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

        # Track in-flight line handlers so we can apply backpressure and drain on shutdown.
        self._line_tasks: Set[asyncio.Task] = set()
        self._max_inflight = int(self.cfg.get("max_inflight_handlers", 200))

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

    async def _handle_line_task(self, line: str) -> None:
        try:
            await self.on_line(line)
        except Exception:
            # Never let handler exceptions silently disappear.
            log.exception("Unhandled exception in on_line handler for: %r", line)

    def _track_task(self, t: asyncio.Task) -> None:
        self._line_tasks.add(t)

        def _done(_t: asyncio.Task) -> None:
            self._line_tasks.discard(_t)

        t.add_done_callback(_done)

    async def _backpressure(self) -> None:
        """
        Prevent unbounded growth if we get spammed.
        Wait for at least one handler task to finish once we exceed the inflight limit.
        """
        if len(self._line_tasks) < self._max_inflight:
            return
        done, _pending = await asyncio.wait(self._line_tasks, return_when=asyncio.FIRST_COMPLETED)
        # Exceptions are already logged in _handle_line_task.
        _ = done

    async def run(self, stop_event: asyncio.Event) -> None:
        """Read loop.

        IMPORTANT: Do not block socket reads on handler execution. If we await on_line(),
        we can deadlock ourselves (e.g. ACL waits for NickServ NOTICE, but NOTICE can't be
        read until handler returns). Instead, schedule handler tasks and keep reading.
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
                if stop_event.is_set():
                    break
                raise ConnectionError("Disconnected (EOF)")

            line = line_b.decode("utf-8", errors="ignore").rstrip("\r\n")
            log.info("<< %s", line)

            if line.startswith("PING "):
                token = line.split(" ", 1)[1]
                await self.send_raw(f"PONG {token}")
                continue

            # Apply light backpressure under load.
            await self._backpressure()

            # Schedule handler so we keep reading incoming messages (NickServ NOTICE etc).
            t = asyncio.create_task(self._handle_line_task(line))
            self._track_task(t)

        # Drain outstanding tasks on shutdown (best effort).
        if self._line_tasks:
            try:
                await asyncio.wait(self._line_tasks, timeout=2.0)
            except Exception:
                pass