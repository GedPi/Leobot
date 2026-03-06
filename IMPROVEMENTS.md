# Leobot: Operational improvements

This document lists identified improvements to overall operation, resilience, and maintainability. Items are ordered by impact; **Critical** and **High** are recommended soon.

---

## Critical

### 1. Windows: signal handlers crash at startup

`loop.add_signal_handler(signal.SIGINT, ...)` raises `NotImplementedError` on Windows. The bot will fail as soon as `main()` runs.

**Fix:** Only register signal handlers on Unix (e.g. `if sys.platform != "win32"`), or wrap in try/except and log a warning on Windows. Optionally use a periodic task or Ctrl+Break handling on Windows for graceful exit.

### 2. Command name collision: `!services`

Core **ServiceCtl** uses `!services` as an alias for “list per-channel service enablement”. **Sysmon** also registers `!services` for “show watched systemd services”. Core handlers run first, so **ServiceCtl always handles `!services`** and sysmon’s implementation is never reached.

**Fix:** Remove the standalone `register_command("services", ...)` from sysmon. Keep “show watched systemd services” only under `!sys services` (already implemented). Then `!services` unambiguously means ServiceCtl’s list.

---

## High

### 3. Graceful shutdown can hang on idle connection — **DONE**

`IRCClient.run()` blocks on `await self.reader.readline()`. The main loop only checks `stop_event` at the top of the loop, so it could stay blocked in `readline()` until the server sends a line or the connection breaks.

**Implemented:** `shutdown()` now closes the IRC connection first (QUIT then `irc.close()`), so `readline()` returns and the read loop exits promptly. Scheduler and store are closed after.

### 4. Raw IRC traffic logged at INFO — **NOT CHANGED (by design)**

Every line from the server is logged with `log.info("<< %s", line)` in the IRC client. Logging of channel traffic to the database (e.g. via the logging service) is deliberate and is not changed. No fix applied.

### 5. Single global Store lock

All DB access goes through one asyncio lock. Under load, many concurrent handlers (e.g. PRIVMSG from several channels) can queue on the same lock and increase latency.

**Fix:** Acceptable for SQLite in many deployments. If needed later, consider short, focused transactions, or moving heavy/background work to a thread pool (e.g. `run_in_executor`) so the event loop is not blocked by long-running queries.

---

## Medium

### 6. No config or service reload without restart — **DONE**

Changing `config.json` required a full process restart.

**Implemented:** Admin-only `!reload` command reloads config from file (default path) and updates `bot.cfg`. Service list and other options take effect on next reload; server/port and similar require a full restart to apply.

### 7. Reconnect creates a new Bot but reuses same config object

On reconnect, a new `Bot(cfg)` is created with the same `cfg` dict. If config were ever mutated (e.g. by a reload), that could be correct; if not, it’s just a reference. No bug today, but worth being explicit if you add reload.

### 8. Scheduler not restarted explicitly on reconnect

After reconnect, the new bot connects and receives `001`; in `on_line` the scheduler is started there. So the scheduler is restarted correctly. No change needed; just documenting for clarity.

### 9. Help text for `!services` can be wrong

Because sysmon overwrites `bot.commands["services"]`, the help entry for `services` can show sysmon’s text (“Show watched services state”) while the actual behavior is ServiceCtl’s (“list service enablement”). Fixing the command collision (item 2) fixes this.

### 10. No automated tests

There are no visible unit or integration tests for ACL, config, Store, or dispatcher behavior. Regressions (e.g. permission or command resolution) are easy to introduce.

**Fix:** Add a small test suite (e.g. pytest) for: config load/validate, ACL `effective_role` / `is_allowed`, Store ACL helpers, and optionally a minimal dispatcher/precheck flow.

---

## Low

### 11. Store migrations run synchronously in `Store.__init__`

`apply_migrations(self._conn)` is synchronous and blocks the event loop during first DB open. Usually fast; only a concern if migrations become very heavy.

### 12. Optional: per-command or global rate limiting

Apart from sysmon’s cooldowns, there is no global or per-command rate limit. A misbehaving user or script could trigger many commands in a short time.

**Fix:** Optional global or per-nick/principal rate limit in ACL precheck or dispatcher (e.g. token bucket or max N commands per minute).

### 13. Optional: health/readiness for orchestration

For running under systemd, k8s, or another process manager, a simple “am I alive and connected?” signal can help (e.g. a small HTTP health endpoint or a file that is touched periodically). Not required for current design.

### 14. Logging: structured fields

Logs are plain text. For log aggregators (e.g. JSON logging), consider a formatter that adds fields (channel, nick, command, service_id) for easier filtering and alerting.

---

## Other change: help/commands reply as PM

**Implemented:** `!help` and `!commands` replies are sent as a private message to the user who issued the command (`ev.nick`), whether they typed in a channel or in PM. This avoids flooding the channel when many commands are listed.
