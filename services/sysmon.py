import json
import time
from pathlib import Path

HEALTH_JSON = Path("/var/lib/leobot/health.json")
EVENTS_LOG = Path("/var/lib/leobot/events.log")

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

def _load_health():
    if not HEALTH_JSON.exists():
        return None, "health.json not found (collector not running?)"
    try:
        data = json.loads(HEALTH_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        return None, f"health.json unreadable: {e}"
    return data, None

def _tail_lines(path: Path, n: int) -> list[str]:
    if not path.exists():
        return []
    # small file, simple approach is fine
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        return lines[-n:]
    except Exception:
        return []

class SysMonService:
    def __init__(self):
        # Basic spam protection: cooldown per target+command
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
            elif cmd in ("sys", "updates", "errors", "failed", "disk", "uptime"):
                if not self._cooldown_ok(ev.target, cmd, seconds=5):
                    await bot.privmsg(ev.target, f"{ev.nick}: slow down.")
                    return

        data, err = _load_health()
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
                    top_s = ", ".join([f"{x.get('source')}={x.get('count')}" for x in top if x.get("source") and x.get("count") is not None])
                    if top_s:
                        msg += f" (top: {top_s})"
                await bot.privmsg(ev.target, msg + f" (age {age_s})")
            return

        if cmd == "services":
            wl = (data.get("services") or {}).get("watchlist") or []
            if not wl:
                await bot.privmsg(ev.target, f"Services: watchlist empty (age {age_s})")
                return

            # compact single-line summary: show non-active first
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
                await bot.privmsg(ev.target, f"Services (issues): " + " | ".join(bad[:8]) + f" (age {age_s})")
            else:
                await bot.privmsg(ev.target, f"Services: all active ({len(good)} units) (age {age_s})")
            return

        if cmd == "events":
            n = 10
            if len(parts) >= 2 and parts[1].isdigit():
                n = max(1, min(50, int(parts[1])))

            lines = _tail_lines(EVENTS_LOG, n)
            if not lines:
                await bot.privmsg(ev.target, f"Events: none (age {age_s})")
                return

            await bot.privmsg(ev.target, f"Last {len(lines)} events:")
            # rate-limit output a bit to avoid flood
            for ln in lines:
                await bot.privmsg(ev.target, ln)
                if not ev.is_private:
                    await time_sleep(0.8)
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
            f"SYS: up {up} | load {load_s} | {mem_s} | systemd {state} failed={failed_n} | updates={upd_n} | age {age_s}"
        )

# tiny async sleep helper (avoid importing asyncio at top-level if you prefer)
async def time_sleep(sec: float) -> None:
    import asyncio
    await asyncio.sleep(sec)

def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command("sys", min_role="user", mutating=False, help="Server health summary. Usage: !sys [summary|updates|services|errors|events]", category="System")
        bot.register_command("sys services", min_role="user", mutating=False, help="Show watched services + failing ones.", category="System")
        bot.register_command("sys updates", min_role="user", mutating=False, help="Show pending updates.", category="System")
        bot.register_command("sys errors", min_role="user", mutating=False, help="Show recent detected issues.", category="System")
        bot.register_command("sys events", min_role="user", mutating=False, help="Show recent state-change events.", category="System")
    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register("sys", min_role="user", mutating=False, help="Server health summary. Usage: !sys [summary|updates|services|errors|events]", category="System")
        bot.acl.register("sys services", min_role="user", mutating=False, help="Show watched services + failing ones.", category="System")
        bot.acl.register("sys updates", min_role="user", mutating=False, help="Show pending updates.", category="System")
        bot.acl.register("sys errors", min_role="user", mutating=False, help="Show recent detected issues.", category="System")
        bot.acl.register("sys events", min_role="user", mutating=False, help="Show recent state-change events.", category="System")

    return SysMonService()
