from __future__ import annotations

import asyncio
import os
import re
import shutil
import time
from pathlib import Path
from typing import Iterable, Optional, Tuple


def _fmt_bytes(n: int) -> str:
    units = ["B", "K", "M", "G", "T", "P"]
    v = float(n)
    i = 0
    while v >= 1024 and i < len(units) - 1:
        v /= 1024
        i += 1
    if i == 0:
        return f"{int(v)}{units[i]}"
    return f"{v:.1f}{units[i]}"


def _fmt_age(seconds: int) -> str:
    if seconds < 0:
        seconds = 0
    if seconds < 60:
        return f"{seconds}s"
    m = seconds // 60
    if m < 60:
        return f"{m}m"
    h = m // 60
    if h < 48:
        return f"{h}h"
    d = h // 24
    return f"{d}d"


async def _run_cmd(args: list[str], timeout: int = 8) -> Tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        out, err = await proc.communicate()
        return 124, (out or b"").decode("utf-8", "replace"), (err or b"").decode("utf-8", "replace")
    return int(proc.returncode or 0), (out or b"").decode("utf-8", "replace"), (err or b"").decode("utf-8", "replace")


def _read_uptime_pretty() -> str:
    try:
        with open("/proc/uptime", "r", encoding="utf-8") as f:
            sec = int(float(f.read().split()[0]))
        d, rem = divmod(sec, 86400)
        h, rem = divmod(rem, 3600)
        m, s = divmod(rem, 60)
        if d:
            return f"{d}d {h:02d}:{m:02d}"
        return f"{h:02d}:{m:02d}"
    except Exception:
        return "unknown"


def _read_loadavg() -> str:
    try:
        a, b, c = os.getloadavg()
        return f"{a:.2f}/{b:.2f}/{c:.2f}"
    except Exception:
        return "?/?/?"


def _read_mem() -> Tuple[Optional[int], Optional[int]]:
    # returns (used_bytes, total_bytes)
    try:
        mem_total = None
        mem_avail = None
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    mem_total = int(line.split()[1]) * 1024
                elif line.startswith("MemAvailable:"):
                    mem_avail = int(line.split()[1]) * 1024
        if mem_total is None or mem_avail is None:
            return None, None
        used = mem_total - mem_avail
        return used, mem_total
    except Exception:
        return None, None


def _fs_usage_for_path(path: str) -> Optional[str]:
    """Filesystem usage (df-style) for the filesystem containing path."""
    try:
        usage = shutil.disk_usage(path)
        used_pct = int(round((usage.used / usage.total) * 100.0)) if usage.total else 0
        return f"{path} {used_pct}% ({_fmt_bytes(usage.used)}/{_fmt_bytes(usage.total)})"
    except Exception:
        return None


async def _dir_size_bytes(path: str, timeout: int = 10) -> Optional[int]:
    """Directory size (du-style) in bytes. Uses du if available."""
    # Prefer du because it's fast and accurate (and doesn't require walking python-side).
    if shutil.which("du"):
        rc, out, _ = await _run_cmd(["du", "-sb", path], timeout=timeout)
        if rc == 0 and out.strip():
            # output: "<bytes>\t<path>"
            try:
                return int(out.strip().split()[0])
            except Exception:
                return None
        return None

    # Fallback: walk (slower)
    total = 0
    try:
        for root, dirs, files in os.walk(path):
            for fn in files:
                try:
                    total += os.path.getsize(os.path.join(root, fn))
                except Exception:
                    pass
        return total
    except Exception:
        return None


def _dedupe_fs_lines(lines: list[str]) -> list[str]:
    """Deduplicate identical filesystem totals (common when multiple paths share same mount)."""
    seen = set()
    out = []
    for s in lines:
        # Deduplicate by the "(used/total)" portion
        key = s.split("(", 1)[-1] if "(" in s else s
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
    return out


def _db_size_bytes(db_path: str) -> int:
    base = Path(db_path)
    total = 0
    for p in (base, Path(str(base) + "-wal"), Path(str(base) + "-shm")):
        try:
            total += p.stat().st_size
        except FileNotFoundError:
            pass
        except Exception:
            pass
    return total


class SysMonService:
    """
    Sysmon (new world):
      - live OS reads for snapshot commands
      - optional proactive alerts (poll job) posted to channels where enabled
      - persistent sys_events table for '!events'
    """

    service_id = "sysmon"

    def __init__(self):
        self._cooldown_until: dict[tuple[str, str], float] = {}
        self._poll_started = False

    def _cfg(self, bot) -> dict:
        return (bot.cfg.get("sysmon") or {}) if isinstance(getattr(bot, "cfg", None), dict) else {}

    def _cooldown_ok(self, target: str, cmd: str, seconds: int) -> bool:
        k = (target, cmd)
        now = time.time()
        until = float(self._cooldown_until.get(k, 0.0))
        if now < until:
            return False
        self._cooldown_until[k] = now + float(seconds)
        return True

    async def _log_event(self, bot, *, level: str, source: str, kind: str, message: str) -> None:
        await bot.store.execute(
            "INSERT INTO sys_events(ts,level,source,kind,message) VALUES(?,?,?,?,?)",
            (int(time.time()), level, source, kind, message),
        )

    async def _alert_channels(self, bot) -> list[str]:
        # Post alerts to channels explicitly configured, otherwise none.
        cfg = self._cfg(bot)
        chans = cfg.get("alert_channels") or []
        out = []
        for c in chans:
            c = str(c).strip()
            if c.startswith("#"):
                out.append(c)
        return out

    async def _poll(self, bot) -> None:
        cfg = self._cfg(bot)
        interval = int(cfg.get("poll_seconds", 60))
        if interval <= 0:
            return

        # ---- systemd failed units (new-only) ----
        rc, out, err = await _run_cmd(["systemctl", "--failed", "--no-legend", "--plain"], timeout=8)
        if rc == 0:
            units = []
            for line in (out or "").splitlines():
                line = line.strip()
                if not line:
                    continue
                # format: UNIT LOAD ACTIVE SUB DESCRIPTION...
                unit = line.split()[0]
                if unit:
                    units.append(unit)
            units.sort()
            fp = ",".join(units)
            prev = await bot.store.get_setting("sysmon:failed_units_fp", "")
            if fp != (prev or ""):
                await bot.store.set_setting("sysmon:failed_units_fp", fp)
                if units:
                    msg = f"systemd failed units: {len(units)} — " + ", ".join(units[:12])
                    await self._log_event(bot, level="WARN", source="systemd", kind="failed_units", message=msg)
                    for chan in await self._alert_channels(bot):
                        # Only if sysmon enabled in that channel
                        if await bot.store.is_service_enabled(chan, "sysmon"):
                            await bot.privmsg(chan, f"SYS: {msg}")
        else:
            # don't spam; log once per change
            prev = await bot.store.get_setting("sysmon:systemctl_failed_err", "")
            fp = f"rc={rc}:{(err or out or '')[:120]}"
            if fp != (prev or ""):
                await bot.store.set_setting("sysmon:systemctl_failed_err", fp)
                await self._log_event(bot, level="ERROR", source="sysmon", kind="systemctl_failed", message=fp)

        # ---- watched services (new-only) ----
        watch = cfg.get("watch_services") or []
        watch = [str(x).strip() for x in watch if str(x).strip()]
        if watch:
            bad = []
            for unit in watch:
                rc2, out2, _ = await _run_cmd(["systemctl", "is-active", unit], timeout=6)
                state = (out2 or "").strip()
                if rc2 != 0 or state != "active":
                    bad.append(f"{unit}:{state or 'unknown'}")

            bad.sort()
            fp2 = ",".join(bad)
            prev2 = await bot.store.get_setting("sysmon:watch_bad_fp", "")
            if fp2 != (prev2 or ""):
                await bot.store.set_setting("sysmon:watch_bad_fp", fp2)
                if bad:
                    msg = "service issues: " + " | ".join(bad[:10])
                    await self._log_event(bot, level="WARN", source="systemd", kind="service_down", message=msg)
                    for chan in await self._alert_channels(bot):
                        if await bot.store.is_service_enabled(chan, "sysmon"):
                            await bot.privmsg(chan, f"SYS: {msg}")

        # ---- auth/login failures (best-effort journald) ----
        # This will fail under strict systemd sandboxing unless journal access is allowed.
        # We treat failure as non-fatal and log once per change.
        since_ts = int(time.time()) - max(300, interval * 2)
        prev_since = await bot.store.get_setting("sysmon:auth_since_ts", "")
        try:
            # Keep a simple moving window; you can make this cursor-based later.
            if prev_since and prev_since.isdigit():
                since_ts = int(prev_since)
        except Exception:
            pass

        patterns = [
            r"Failed password",
            r"authentication failure",
            r"Invalid user",
            r"Failed publickey",
        ]
        pat = re.compile("|".join(patterns), re.IGNORECASE)

        rc3, out3, err3 = await _run_cmd(
            ["journalctl", "--no-pager", "--since", f"@{since_ts}", "-u", "sshd.service"],
            timeout=8,
        )
        if rc3 == 0:
            lines = [ln for ln in (out3 or "").splitlines() if pat.search(ln)]
            if lines:
                # fingerprint on the tail to avoid repeats
                tail = lines[-1].strip()
                prev_tail = await bot.store.get_setting("sysmon:last_auth_tail", "")
                if tail != (prev_tail or ""):
                    await bot.store.set_setting("sysmon:last_auth_tail", tail)
                    await bot.store.set_setting("sysmon:auth_since_ts", str(int(time.time()) - 5))
                    msg = f"auth failures detected (sshd): {len(lines)} (latest: {tail[-140:]})"
                    await self._log_event(bot, level="WARN", source="sshd", kind="auth_fail", message=msg)
                    for chan in await self._alert_channels(bot):
                        if await bot.store.is_service_enabled(chan, "sysmon"):
                            await bot.privmsg(chan, f"SYS: {msg}")
            else:
                await bot.store.set_setting("sysmon:auth_since_ts", str(int(time.time()) - 5))
        else:
            fp3 = f"rc={rc3}:{(err3 or out3 or '')[:140]}"
            prev3 = await bot.store.get_setting("sysmon:journal_err", "")
            if fp3 != (prev3 or ""):
                await bot.store.set_setting("sysmon:journal_err", fp3)
                await self._log_event(bot, level="ERROR", source="sysmon", kind="journalctl_err", message=fp3)

    async def _ensure_poll(self, bot) -> None:
        if self._poll_started:
            return
        self._poll_started = True
        cfg = self._cfg(bot)
        interval = int(cfg.get("poll_seconds", 60))
        jitter = int(cfg.get("poll_jitter_seconds", 5))
        if interval > 0:
            bot.scheduler.register_interval(
                "sysmon.poll",
                seconds=interval,
                fn=lambda: self._poll(bot),
                jitter_seconds=jitter,
                run_on_start=True,
            )

    async def on_privmsg(self, bot, ev) -> None:
        await self._ensure_poll(bot)

        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()
        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return

        parts = cmdline.split()
        cmd = parts[0].lower()

        # Subcommands allowed via "!sys <sub>"
        if cmd == "sys" and len(parts) >= 2:
            sub = parts[1].lower()
            if sub in ("uptime", "disk", "updates", "failed", "errors", "services", "events"):
                cmd = sub
                parts = [cmd] + parts[2:]

        if cmd not in ("sys", "uptime", "disk", "updates", "failed", "errors", "services", "events"):
            return

        # mild flood control in channels
        if not ev.is_private:
            if cmd in ("events", "services", "errors", "failed", "updates"):
                if not self._cooldown_ok(ev.target, cmd, seconds=20):
                    await bot.privmsg(ev.target, f"{ev.nick}: already posted recently. Try again in a bit.")
                    return
            else:
                if not self._cooldown_ok(ev.target, cmd, seconds=5):
                    await bot.privmsg(ev.target, f"{ev.nick}: slow down.")
                    return

        cfg = self._cfg(bot)
        disk_paths = cfg.get("disk_paths") or ["/"]
        watch = cfg.get("watch_services") or []

        if cmd == "uptime":
            await bot.privmsg(ev.target, f"Uptime: {_read_uptime_pretty()}")
            return

        if cmd == "disk":
            cfg = self._cfg(bot)
            fs_paths = cfg.get("disk_paths") or ["/"]
            dir_paths = cfg.get("dir_sizes") or ["/opt/leobot"]

            fs_lines = []
            for p in fs_paths:
                line = _fs_usage_for_path(str(p))
                if line:
                    fs_lines.append(line)
            fs_lines = _dedupe_fs_lines(fs_lines)

            dir_lines = []
            for p in dir_paths:
                p = str(p)
                sz = await _dir_size_bytes(p, timeout=12)
                if sz is not None:
                    dir_lines.append(f"{p} {_fmt_bytes(sz)}")

            msg = []
            if fs_lines:
                msg.append("FS: " + " | ".join(fs_lines))
            if dir_lines:
                msg.append("DIR: " + " | ".join(dir_lines))

            if not msg:
                await bot.privmsg(ev.target, "Disk: unavailable")
            else:
                await bot.privmsg(ev.target, " ; ".join(msg))
            return

        if cmd == "updates":
            # Arch: prefer checkupdates (pacman-contrib), fallback pacman -Qu
            rc, out, err = await _run_cmd(["checkupdates"], timeout=10)
            if rc != 0:
                rc, out, err = await _run_cmd(["pacman", "-Qu"], timeout=12)

            if rc == 0:
                lines = [ln for ln in (out or "").splitlines() if ln.strip()]
                await bot.privmsg(ev.target, f"Updates pending: {len(lines)}")
            else:
                await bot.privmsg(ev.target, f"Updates: unavailable ({(err or out or '').strip()[:120]})")
            return

        if cmd == "failed":
            rc, out, err = await _run_cmd(["systemctl", "--failed", "--no-legend", "--plain"], timeout=8)
            if rc != 0:
                await bot.privmsg(ev.target, f"systemd failed: unavailable ({(err or out or '').strip()[:120]})")
                return
            units = []
            for line in (out or "").splitlines():
                line = line.strip()
                if not line:
                    continue
                unit = line.split()[0]
                if unit:
                    units.append(unit)
            if not units:
                await bot.privmsg(ev.target, "systemd failed units: 0")
            else:
                await bot.privmsg(ev.target, f"systemd failed units: {len(units)} — " + ", ".join(units[:12]))
            return

        if cmd == "errors":
            # optional limit: !sys errors 3
            n = 3
            if len(parts) >= 2 and parts[1].isdigit():
                n = max(1, min(10, int(parts[1])))

            # Query last hour errors. Use short format for IRC.
            rc, out, err = await _run_cmd(
                ["journalctl", "--no-pager", "-p", "err", "--since", "-1 hour", "-o", "short"],
                timeout=10,
            )
            if rc != 0:
                await bot.privmsg(ev.target, f"Journal errors: unavailable ({(err or out or '').strip()[:120]})")
                return

            raw_lines = [ln.strip() for ln in (out or "").splitlines() if ln.strip()]

            # journalctl placeholders / noise lines that are not real entries
            noise_prefixes = (
                "-- No entries --",
                "Hint:",
                "warning:",
            )

            lines = []
            for ln in raw_lines:
                if any(ln.startswith(p) for p in noise_prefixes):
                    continue
                lines.append(ln)

            count = len(lines)
            if count == 0:
                await bot.privmsg(ev.target, "Journal errors (last 1h): 0")
                return

            await bot.privmsg(ev.target, f"Journal errors (last 1h): {count}")

            # Show last N errors with some structure.
            # Example short format:
            # "Mar 01 16:12:34 host unit[pid]: message..."
            tail = lines[-n:]
            for ln in tail:
                s = ln.strip()
                # Clip to avoid IRC line spam
                if len(s) > 260:
                    s = s[:259] + "…"
                await bot.privmsg(ev.target, f"ERR: {s}")
                if not ev.is_private:
                    await asyncio.sleep(0.8)

            return

        if cmd == "services":
            wl = [str(x).strip() for x in watch if str(x).strip()]
            if not wl:
                await bot.privmsg(ev.target, "Services: watchlist empty")
                return
            bad = []
            for unit in wl:
                rc, out, _ = await _run_cmd(["systemctl", "is-active", unit], timeout=6)
                state = (out or "").strip()
                if rc != 0 or state != "active":
                    bad.append(f"{unit}:{state or 'unknown'}")
            if bad:
                await bot.privmsg(ev.target, "Services (issues): " + " | ".join(bad[:10]))
            else:
                await bot.privmsg(ev.target, f"Services: all active ({len(wl)} units)")
            return

        if cmd == "events":
            n = 10
            if len(parts) >= 2 and parts[1].isdigit():
                n = max(1, min(50, int(parts[1])))

            rows = await bot.store.fetchall(
                "SELECT ts, level, source, kind, message FROM sys_events ORDER BY ts DESC LIMIT ?",
                (int(n),),
            )
            if not rows:
                await bot.privmsg(ev.target, "Events: none")
                return
            rows = list(reversed(rows))
            await bot.privmsg(ev.target, f"Last {len(rows)} sys events:")
            for r in rows:
                ts = int(r[0])
                iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts))
                await bot.privmsg(ev.target, f"{iso}Z {r[1]} {r[2]}/{r[3]}: {r[4]}")
                if not ev.is_private:
                    await asyncio.sleep(0.8)
            return

        # cmd == "sys" summary
        up = _read_uptime_pretty()
        load_s = _read_loadavg()
        used, total = _read_mem()
        mem_s = "mem ?"
        if used is not None and total is not None:
            mem_s = f"mem {_fmt_bytes(int(used))}/{_fmt_bytes(int(total))}"

        fs_paths = cfg.get("disk_paths") or ["/"]
        fs_lines = []
        for p in fs_paths:
            line = _fs_usage_for_path(str(p))
            if line:
                fs_lines.append(line)
        fs_lines = _dedupe_fs_lines(fs_lines)

        disk_s = "fs ?"
        if fs_lines:
            disk_s = "fs " + " , ".join(fs_lines[:2])

        # Optional dir size (only one, keep summary short)
        dir_paths = cfg.get("dir_sizes") or ["/opt/leobot"]
        leobot_sz = await _dir_size_bytes(str(dir_paths[0]), timeout=8) if dir_paths else None
        if leobot_sz is not None:
            disk_s += f" | dir {Path(str(dir_paths[0])).name}={_fmt_bytes(leobot_sz)}"

        # systemd failed count
        rc, out, err = await _run_cmd(["systemctl", "--failed", "--no-legend", "--plain"], timeout=8)
        failed_n = "?"
        if rc == 0:
            failed_n = str(len([ln for ln in (out or "").splitlines() if ln.strip()]))

        # updates count (quick)
        upd_n = "?"
        rc_u, out_u, _ = await _run_cmd(["checkupdates"], timeout=8)
        if rc_u == 0:
            upd_n = str(len([ln for ln in (out_u or "").splitlines() if ln.strip()]))

        db_size = _fmt_bytes(_db_size_bytes(bot.cfg.get("db_path", "./data/leonidas.db")))

        await bot.privmsg(
            ev.target,
            f"SYS: up {up} | load {load_s} | {mem_s} | {disk_s} | systemd failed={failed_n} | updates={upd_n} | db={db_size}",
        )


def setup(bot):
    # Command registration
    if hasattr(bot, "register_command"):
        bot.register_command("sys", min_role="user", mutating=False,
                             help="Server health summary. Usage: !sys [subcmd]",
                             category="System")
        bot.register_command("uptime", min_role="user", mutating=False,
                             help="Show system uptime. Usage: !uptime",
                             category="System")
        bot.register_command("disk", min_role="user", mutating=False,
                             help="Show disk usage. Usage: !disk",
                             category="System")
        bot.register_command("updates", min_role="user", mutating=False,
                             help="Show pending package updates. Usage: !updates",
                             category="System")
        bot.register_command("failed", min_role="user", mutating=False,
                             help="Show failed systemd units. Usage: !failed",
                             category="System")
        bot.register_command("errors", min_role="user", mutating=False,
                             help="Show recent journal errors count. Usage: !errors",
                             category="System")
        bot.register_command("events", min_role="user", mutating=False,
                             help="Tail sysmon events. Usage: !events [N]",
                             category="System")

    return SysMonService()