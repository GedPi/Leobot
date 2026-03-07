from __future__ import annotations

# TCP (optionally TLS) connection to IRC server: connect, send_raw/privmsg, run() read loop with backpressure and optional PING/PONG.

import asyncio
import logging
import os
import ssl
from typing import Awaitable, Callable, Optional, Set

from system.irc_parse import chunk_message

log = logging.getLogger("leobot.irc")

LineHandler = Callable[[str], Awaitable[None]]


# Holds config and on_line callback; connect() opens socket and sends NICK/USER; run() reads lines and dispatches until stop_event.
class IRCClient:
    def __init__(self, cfg: dict, on_line: LineHandler):
        self.cfg = cfg
        self.on_line = on_line
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self._line_tasks: Set[asyncio.Task] = set()
        self._max_inflight = int(self.cfg.get("max_inflight_handlers", 200))

    @staticmethod
    def _truthy(val) -> bool:
        if val is None:
            return False
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return val != 0
        s = str(val).strip().lower()
        return s not in ("0", "false", "no", "off", "")

    # Opens connection to server:port (TLS if use_tls), sends PASS/NICK/USER from config.
    async def connect(self) -> None:
        server = self.cfg["server"]
        port = int(self.cfg["port"])
        use_tls = self._truthy(self.cfg.get("use_tls", True))
        verify_tls = self._truthy(self.cfg.get("verify_tls", True))
        # Env override so you can force disable verification without editing config (e.g. in systemd).
        env_verify = os.environ.get("LEOBOT_VERIFY_TLS")
        if env_verify is not None:
            verify_tls = self._truthy(env_verify)

        ssl_ctx = None
        if use_tls:
            ssl_ctx = ssl.create_default_context()
            if not verify_tls:
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE

        log.info("Connecting to %s:%s TLS=%s verify_tls=%s", server, port, use_tls, verify_tls)
        self.reader, self.writer = await asyncio.open_connection(server, port, ssl=ssl_ctx)

        if self.cfg.get("password"):
            await self.send_raw(f"PASS {self.cfg['password']}")

        await self.send_raw(f"NICK {self.cfg['nick']}")
        await self.send_raw(f"USER {self.cfg['user']} 0 * :{self.cfg['realname']}")

    # Closes writer and waits for it; clears reader/writer; best-effort (TLS shutdown may raise).
    async def close(self) -> None:
        try:
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()
        except Exception:
            pass
        finally:
            self.reader = None
            self.writer = None

    # Appends CRLF, encodes to UTF-8 and writes to the stream; drains after.
    async def send_raw(self, line: str) -> None:
        assert self.writer is not None
        wire = (line + "\r\n").encode("utf-8", errors="ignore")
        self.writer.write(wire)
        await self.writer.drain()
        log.info(">> %s", line)

    # Splits message into chunks (380 chars) and sends each as PRIVMSG target.
    async def privmsg(self, target: str, msg: str) -> None:
        for chunk in chunk_message(msg, limit=380):
            await self.send_raw(f"PRIVMSG {target} :{chunk}")

    async def _handle_line_task(self, line: str) -> None:
        try:
            await self.on_line(line)
        except Exception:
            log.exception("Unhandled exception in on_line handler for: %r", line)

    def _track_task(self, t: asyncio.Task) -> None:
        self._line_tasks.add(t)

        def _done(_t: asyncio.Task) -> None:
            self._line_tasks.discard(_t)

        t.add_done_callback(_done)

    # Waits for at least one line handler to finish when inflight count exceeds max; limits task growth.
    async def _backpressure(self) -> None:
        if len(self._line_tasks) < self._max_inflight:
            return
        done, _pending = await asyncio.wait(self._line_tasks, return_when=asyncio.FIRST_COMPLETED)
        _ = done

    # Reads lines from the socket, spawns a task per line for on_line, handles PING with PONG and drains tasks on stop.
    async def run(self, stop_event: asyncio.Event) -> None:
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
            await self._backpressure()
            t = asyncio.create_task(self._handle_line_task(line))
            self._track_task(t)

        if self._line_tasks:
            try:
                await asyncio.wait(self._line_tasks, timeout=2.0)
            except Exception:
                pass