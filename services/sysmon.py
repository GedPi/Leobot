import asyncio
import json
import sqlite3
import time
from pathlib import Path

DB_PATH = Path("/var/lib/leobot/db/leobot.db")


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


def _db() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH), timeout=10)


def _load_health_db():
    if not DB_PATH.exists():
        return None, "db not found"
    try:
        conn = _db()
        cur = conn.execute(
            "SELECT payload_json FROM sys_health_snapshots ORDER BY ts DESC LIMIT 1"
        )
        row = cur.fetchone()
        conn.close()
        if not row:
            return None, "no health snapshot in DB (collector not running?)"
        return json.loads(row[0]), None
    except Exception as e:
        return None, f"health DB unreadable: {e}"


def _tail_events_db(n: int) -> list[str]:
    if not DB_PATH.exists():
        return []
    try:
        conn = _db()
        cur = conn.execute(
            "SELECT ts, message FROM sys_events ORDER BY ts DESC LIMIT ?",
            (int(n),),
        )
        rows = cur.fetchall()
        conn.close()
        rows.reverse()
        out = []
        for ts, msg in rows:
            iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(ts)))
            out.append(f"{iso} {msg}")
        return out
    except Exception:
        return []


class SysMonService:
    def __init__(self):
        self._cooldown_until = {}  # (target, cmd) -> epoch

    def _cooldown_ok(self, target: str, cmd: str, seconds: int) -> bool:
        k = (target, cmd)
        now = time.time()
        until = self._cooldown_until.get(k, 0)
        if now < until:
            return False
        self._cooldown_until[k] = now + seconds
        return True

    async def on_privmsg(self, bot, ev) -> None:
        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()
        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return

        parts = cmdline.split()
        cmd = parts[0].lower()

        if cmd not in ("sys", "uptime", "disk", "updates", "failed", "errors", "services", "events"):
            return

        # mild flood control in channels
        if not ev.is_private:
            if cmd in ("events", "services"):
                if not self._cooldown_ok(ev.target, cmd, seconds=20):
                    await bot.privmsg(ev.target, f"{ev.nick}: already posted recently. Try again in a bit.")
                    return
            else:
                if not self._cooldown_ok(ev.target, cmd, seconds=5):
                    await bot.privmsg(ev.target, f"{ev.nick}: slow down.")
                    return

        data, err = _load_health_db()
        if err:
            await bot.privmsg(ev.target, f"{ev.nick}: {err}")
            return

        age = int(time.time() - int(data.get("generated_at_epoch", 0)))
        age_s = f"{age}s" if age < 120 else f"{age//60}m"

        if cmd == "uptime":
            up = (data.get("uptime") or {}).get("pretty") or "unknown"
            await bot.privmsg(ev.target, f"Uptime: {up} (data age {age_s})")
            return

        if cmd == "disk":
            disks = data.get("disks") or []
            if not disks:
                await bot.privmsg(ev.target, f"Disk: unknown (data age {age_s})")
                return
            lines = []
            for d in disks:
                p = d.get("path")
                used_pct = d.get("used_pct")
                used = d.get("used")
                total = d.get("total")
                if p and used_pct is not None and used is not None and total is not None:
                    lines.append(f"{p} {used_pct}% ({_fmt_bytes(int(used))}/{_fmt_bytes(int(total))})")
            await bot.privmsg(ev.target, "Disk: " + " | ".join(lines) + f" (age {age_s})")
            return

        if cmd == "updates":
            upd = data.get("updates") or {}
            count = upd.get("count")
            note = upd.get("note")
            top = upd.get("top") or []
            pending_days = upd.get("pending_days")

            if count is None:
                await bot.privmsg(ev.target, f"Updates: unknown ({note or 'n/a'}) (age {age_s})")
            else:
                msg = f"Updates pending: {count}"
                if isinstance(pending_days, int) and pending_days > 0:
                    msg += f" (pending {pending_days}d)"
                if top:
                    msg += " (top: " + ", ".join(top[:5]) + ")"
                await bot.privmsg(ev.target, msg + f" (age {age_s})")
            return

        if cmd == "failed":
            sysd = data.get("systemd") or {}
            failed = (sysd.get("failed") or {})
            units = failed.get("units") or []
            count = failed.get("count")
            state = sysd.get("state") or "unknown"

            if not units:
                await bot.privmsg(ev.target, f"systemd: state={state}; failed units: 0 (age {age_s})")
            else:
                await bot.privmsg(ev.target, f"systemd: state={state}; failed={count}; {', '.join(units[:10])} (age {age_s})")
            return

        if cmd == "errors":
            j = (data.get("journal") or {}).get("errors") or {}
            since = j.get("since") or "?"
            cnt = j.get("count")
            note = j.get("note")
            top = j.get("top") or []

            if cnt is None:
                await bot.privmsg(ev.target, f"Journal errors since {since}: unknown ({note or 'n/a'}) (age {age_s})")
            else:
                msg = f"Journal errors since {since}: {cnt}"
                if top:
                    top_s = ", ".join(
                        [f"{x.get('source')}={x.get('count')}" for x in top
                         if x.get("source") and x.get("count") is not None]
                    )
                    if top_s:
                        msg += f" (top: {top_s})"
                await bot.privmsg(ev.target, msg + f" (age {age_s})")
            return

        if cmd == "services":
            wl = (data.get("services") or {}).get("watchlist") or []
            if not wl:
                await bot.privmsg(ev.target, f"Services: watchlist empty (age {age_s})")
                return

            bad = []
            good = []
            for s in wl:
                name = s.get("name")
                active = s.get("active")
                sub = s.get("sub")
                if not name:
                    continue
                state = f"{active}/{sub}"
                if active != "active":
                    bad.append(f"{name}:{state}")
                else:
                    good.append(f"{name}:{state}")

            if bad:
                await bot.privmsg(ev.target, "Services (issues): " + " | ".join(bad[:8]) + f" (age {age_s})")
            else:
                await bot.privmsg(ev.target, f"Services: all active ({len(good)} units) (age {age_s})")
            return

        if cmd == "events":
            n = 10
            if len(parts) >= 2 and parts[1].isdigit():
                n = max(1, min(50, int(parts[1])))

            lines = _tail_events_db(n)
            if not lines:
                await bot.privmsg(ev.target, f"Events: none (age {age_s})")
                return

            await bot.privmsg(ev.target, f"Last {len(lines)} events:")
            for ln in lines:
                await bot.privmsg(ev.target, ln)
                if not ev.is_private:
                    await asyncio.sleep(0.8)
            return

        # cmd == "sys"
        up = (data.get("uptime") or {}).get("pretty") or "unknown"
        load = data.get("loadavg") or {}
        load_s = f"{load.get('1m','?')}/{load.get('5m','?')}/{load.get('15m','?')}"
        mem = data.get("memory") or {}
        mem_s = "mem ?"
        if mem.get("mem_total") and mem.get("mem_used") is not None:
            mem_s = f"mem {_fmt_bytes(int(mem['mem_used']))}/{_fmt_bytes(int(mem['mem_total']))}"
        sysd = data.get("systemd") or {}
        state = sysd.get("state") or "unknown"
        failed_n = (sysd.get("failed") or {}).get("count")
        upd = data.get("updates") or {}
        upd_n = upd.get("count")

        await bot.privmsg(
            ev.target,
            f"SYS: up {up} | load {load_s} | {mem_s} | systemd {state} failed={failed_n} | updates={upd_n} | age {age_s}",
        )


def setup(bot):
    # Register commands for help/ACL visibility
    if hasattr(bot, "register_command"):
        bot.register_command("sys", min_role="user", mutating=False,
                             help="Server health summary from DB. Usage: !sys",
                             category="System")
        bot.register_command("uptime", min_role="user", mutating=False,
                             help="Show system uptime (DB). Usage: !uptime",
                             category="System")
        bot.register_command("disk", min_role="user", mutating=False,
                             help="Show disk usage (DB). Usage: !disk",
                             category="System")
        bot.register_command("updates", min_role="user", mutating=False,
                             help="Show pending package updates (DB). Usage: !updates",
                             category="System")
        bot.register_command("failed", min_role="user", mutating=False,
                             help="Show failed systemd units (DB). Usage: !failed",
                             category="System")
        bot.register_command("errors", min_role="user", mutating=False,
                             help="Show recent journal errors summary (DB). Usage: !errors",
                             category="System")
        bot.register_command("services", min_role="user", mutating=False,
                             help="Show watched services summary (DB). Usage: !services",
                             category="System")
        bot.register_command("events", min_role="user", mutating=False,
                             help="Tail recent events (DB). Usage: !events [N]",
                             category="System")

    # If ACL is present, register there too (some of your services use this)
    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register("sys", min_role="user", mutating=False,
                         help="Server health summary from DB. Usage: !sys",
                         category="System")
        bot.acl.register("uptime", min_role="user", mutating=False,
                         help="Show system uptime (DB). Usage: !uptime",
                         category="System")
        bot.acl.register("disk", min_role="user", mutating=False,
                         help="Show disk usage (DB). Usage: !disk",
                         category="System")
        bot.acl.register("updates", min_role="user", mutating=False,
                         help="Show pending package updates (DB). Usage: !updates",
                         category="System")
        bot.acl.register("failed", min_role="user", mutating=False,
                         help="Show failed systemd units (DB). Usage: !failed",
                         category="System")
        bot.acl.register("errors", min_role="user", mutating=False,
                         help="Show recent journal errors summary (DB). Usage: !errors",
                         category="System")
        bot.acl.register("services", min_role="user", mutating=False,
                         help="Show watched services summary (DB). Usage: !services",
                         category="System")
        bot.acl.register("events", min_role="user", mutating=False,
                         help="Tail recent events (DB). Usage: !events [N]",
                         category="System")

    return SysMonService()