from __future__ import annotations

import fnmatch
import logging
import time
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger("leobot.greet")


def _now() -> int:
    return int(time.time())


def _lower(s: str) -> str:
    return (s or "").strip().lower()


def _norm(s: str) -> str:
    return (s or "").strip()


def _render(tpl: str, *, nick: str, channel: str) -> str:
    return tpl.replace("{nick}", nick).replace("{channel}", channel)


@dataclass(frozen=True)
class Identity:
    nick: str
    hostmask: str
    userhost: str
    host: str


def _extract_identity(ev) -> Identity:
    nick = getattr(ev, "nick", "") or ""
    user = getattr(ev, "user", "") or getattr(ev, "ident", "") or ""
    host = getattr(ev, "host", "") or getattr(ev, "hostname", "") or ""
    hostmask = f"{nick}!{user}@{host}" if (nick and user and host) else ""
    userhost = f"{user}@{host}" if (user and host) else ""
    return Identity(nick=nick, hostmask=hostmask, userhost=userhost, host=host)


def setup(bot):
    return GreetService(bot)


class GreetService:
    """
    Join greetings (DB-backed).

    Tables:
      - greet_targets: match rules (AND semantics across provided match_* fields)
      - greetings: greetings tied to a target_id
      - greet_cooldowns: persistent cooldown keys

    Enable per channel:
      !service enable greet #Channel

    Commands:
      !greet list
      !greet greets <target_id>
      !greet addnick <nick> <greeting...>
      !greet addhost <host pattern> <greeting...>     (matches host, user@host, or nick!user@host via fnmatch)
      !greet addmask <hostmask pattern> <greeting...> (matches full hostmask via fnmatch)
      !greet deltarget <id>
      !greet delgreet <id>
      !greet enable <id> | !greet disable <id>
      !greet setpri <id> <priority>
      !greet setchan <id> <#channel|any>
      !greet setcd <id> <seconds|0>
      !greet test
    """

    def __init__(self, bot):
        self.bot = bot

        cfg = bot.cfg.get("greet", {}) if isinstance(bot.cfg, dict) else {}
        self.default_nick_cooldown = int(cfg.get("cooldown_per_nick_seconds", 900))
        self.default_chan_cooldown = int(cfg.get("cooldown_per_channel_seconds", 3))
        self.max_list = int(cfg.get("max_list", 15))

        # Register command families
        bot.register_command("greet", min_role="contributor", mutating=False, help="Manage join greetings. Usage: !greet <subcmd>", category="Greet")
        bot.register_command("greet test", min_role="contributor", mutating=False, help="Test greet matching for your current identity.", category="Greet")

    # ----------------------------
    # Cooldowns (DB persisted)
    # ----------------------------

    async def _cooldown_get(self, key: str) -> int:
        row = await self.bot.store.fetchone("SELECT until_ts FROM greet_cooldowns WHERE key=?", (key,))
        return int(row[0]) if row else 0

    async def _cooldown_set(self, key: str, until_ts: int) -> None:
        await self.bot.store.execute(
            """
            INSERT INTO greet_cooldowns(key, until_ts)
            VALUES(?,?)
            ON CONFLICT(key) DO UPDATE SET until_ts=excluded.until_ts
            """,
            (key, int(until_ts)),
        )

    async def _cooldown_ok(self, *, nick: str, channel: str, target_id: int, target_cd: Optional[int]) -> bool:
        now = _now()
        nl = _lower(nick)
        cl = _lower(channel)

        # Keying strategy:
        # - per-nick global (prevents spam across channels)
        # - per-channel short (netsplits / mass rejoin)
        # - per-target+nick (optional override when target has cooldown_seconds)
        nick_key = f"nick:{nl}"
        chan_key = f"chan:{cl}"
        tgt_key = f"tgt:{int(target_id)}:{nl}"

        # check
        if now < await self._cooldown_get(nick_key):
            return False
        if now < await self._cooldown_get(chan_key):
            return False
        if target_cd and target_cd > 0:
            if now < await self._cooldown_get(tgt_key):
                return False

        # set
        await self._cooldown_set(nick_key, now + self.default_nick_cooldown)
        await self._cooldown_set(chan_key, now + self.default_chan_cooldown)
        if target_cd and target_cd > 0:
            await self._cooldown_set(tgt_key, now + int(target_cd))

        return True

    # ----------------------------
    # Matching / selection
    # ----------------------------

    async def _select_target(self, *, ident: Identity, channel: str):
        return await self.bot.store.greet_select_target(
            nick=ident.nick,
            hostmask=ident.hostmask,
            userhost=ident.userhost,
            host=ident.host,
            channel=channel,
        )

    async def _pick_greeting(self, target_id: int) -> Optional[str]:
        return await self.bot.store.greet_pick_greeting(int(target_id))

    # ----------------------------
    # IRC hooks
    # ----------------------------

    async def on_join(self, bot, ev):
        if not ev.channel or not str(ev.channel).startswith("#"):
            return
        if not await bot.store.is_service_enabled(ev.channel, "greet"):
            return

        # don't greet ourselves
        if _lower(ev.nick) == _lower(bot.cfg.get("nick", "")):
            return

        ident = _extract_identity(ev)
        target = await self._select_target(ident=ident, channel=ev.channel)
        if not target:
            return

        target_id = int(target["id"])
        target_cd = target["cooldown_seconds"]
        try:
            target_cd_i = int(target_cd) if target_cd is not None else None
        except Exception:
            target_cd_i = None

        ok = await self._cooldown_ok(
            nick=ev.nick,
            channel=ev.channel,
            target_id=target_id,
            target_cd=target_cd_i,
        )
        if not ok:
            return

        greet = await self._pick_greeting(target_id)
        if not greet:
            return

        await bot.privmsg(ev.channel, _render(greet, nick=ev.nick, channel=ev.channel))

    async def on_privmsg(self, bot, ev):
        txt = (ev.text or "").strip()
        prefix = bot.cfg.get("command_prefix", "!")
        if not txt.startswith(prefix):
            return

        cmdline = txt[len(prefix):].strip()
        if not cmdline:
            return

        parts = cmdline.split()
        if not parts:
            return

        if parts[0].lower() != "greet":
            return

        # simple subcommand router
        sub = parts[1].lower() if len(parts) >= 2 else "help"
        args = parts[2:]

        # ---- greet test ----
        if sub == "test":
            ident = _extract_identity(ev)
            ch = ev.channel or ""
            target = await self._select_target(ident=ident, channel=ch)
            if not target:
                await bot.privmsg(ev.target, f"{ev.nick}: no greet target matched.")
                return
            greet = await self._pick_greeting(int(target["id"]))
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: matched target id={target['id']} pri={target['priority']} chan={target['channel'] or 'any'} greet={greet!r}",
            )
            return

        # ---- greet list ----
        if sub == "list":
            rows = await bot.store.fetchall(
                """
                SELECT id, enabled, priority, channel, match_nick, match_hostmask, match_userhost, match_host, cooldown_seconds
                FROM greet_targets
                ORDER BY enabled DESC, priority DESC, id ASC
                LIMIT ?
                """,
                (int(self.max_list),),
            )
            if not rows:
                await bot.privmsg(ev.target, f"{ev.nick}: no greet targets.")
                return

            bits = []
            for r in rows:
                mid = int(r["id"])
                en = "on" if int(r["enabled"]) == 1 else "off"
                pri = int(r["priority"])
                chan = r["channel"] or "any"
                cd = r["cooldown_seconds"]
                cd_s = f" cd={int(cd)}" if cd is not None else ""
                m = []
                if r["match_nick"]:
                    m.append(f"nick={r['match_nick']}")
                if r["match_hostmask"]:
                    m.append(f"mask={r['match_hostmask']}")
                if r["match_userhost"]:
                    m.append(f"userhost={r['match_userhost']}")
                if r["match_host"]:
                    m.append(f"host={r['match_host']}")
                mtxt = " ".join(m) if m else "(no match?)"
                bits.append(f"{mid}) {en} pri={pri} chan={chan}{cd_s} {mtxt}")

            await bot.privmsg(ev.target, "GREET: " + " | ".join(bits))
            return

        # ---- greet greets <target_id> ----
        if sub == "greets":
            if len(args) != 1 or not args[0].isdigit():
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !greet greets <target_id>")
                return
            tid = int(args[0])
            rows = await bot.store.fetchall(
                "SELECT id, enabled, weight, text FROM greetings WHERE target_id=? ORDER BY id ASC",
                (tid,),
            )
            if not rows:
                await bot.privmsg(ev.target, f"{ev.nick}: no greetings for target {tid}")
                return
            out = []
            for r in rows[:10]:
                out.append(f"{int(r['id'])}) {'on' if int(r['enabled'])==1 else 'off'} w={int(r['weight'])} {r['text']!r}")
            more = f" (+{len(rows)-10} more)" if len(rows) > 10 else ""
            await bot.privmsg(ev.target, f"{ev.nick}: greetings for {tid}: " + " | ".join(out) + more)
            return

        # ---- helpers for adding targets/greetings ----
        async def _upsert_target(*, match_nick=None, match_hostmask=None, match_userhost=None, match_host=None, channel=None, priority=0, cooldown_seconds=None) -> int:
            now = _now()
            # We don't have a natural UNIQUE constraint; do "find existing" by exact match set.
            row = await bot.store.fetchone(
                """
                SELECT id FROM greet_targets
                WHERE
                  COALESCE(match_nick,'')=COALESCE(?, '')
                  AND COALESCE(match_hostmask,'')=COALESCE(?, '')
                  AND COALESCE(match_userhost,'')=COALESCE(?, '')
                  AND COALESCE(match_host,'')=COALESCE(?, '')
                  AND COALESCE(channel,'')=COALESCE(?, '')
                LIMIT 1
                """,
                (
                    match_nick or "",
                    match_hostmask or "",
                    match_userhost or "",
                    match_host or "",
                    channel or "",
                ),
            )
            if row:
                tid = int(row[0])
                await bot.store.execute(
                    """
                    UPDATE greet_targets
                    SET enabled=1, priority=?, cooldown_seconds=?, updated_ts=?
                    WHERE id=?
                    """,
                    (int(priority), cooldown_seconds, now, tid),
                )
                return tid

            await bot.store.execute(
                """
                INSERT INTO greet_targets(
                  enabled, priority, match_nick, match_hostmask, match_userhost, match_host,
                  channel, cooldown_seconds, created_ts, updated_ts
                )
                VALUES(1,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(priority),
                    match_nick,
                    match_hostmask,
                    match_userhost,
                    match_host,
                    channel,
                    cooldown_seconds,
                    now,
                    now,
                ),
            )
            row2 = await bot.store.fetchone("SELECT last_insert_rowid()", ())
            return int(row2[0])

        async def _add_greeting(target_id: int, text: str) -> int:
            now = _now()
            await bot.store.execute(
                """
                INSERT INTO greetings(target_id, text, weight, enabled, created_ts, updated_ts)
                VALUES(?, ?, 1, 1, ?, ?)
                """,
                (int(target_id), text, now, now),
            )
            row = await bot.store.fetchone("SELECT last_insert_rowid()", ())
            return int(row[0])

        # ---- greet addnick <nick> <greeting...> ----
        if sub == "addnick":
            if len(args) < 2:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !greet addnick <nick> <greeting...>")
                return
            nick = _norm(args[0])
            greet_txt = _norm(" ".join(args[1:]))
            if not nick or not greet_txt:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !greet addnick <nick> <greeting...>")
                return
            tid = await _upsert_target(match_nick=nick)
            gid = await _add_greeting(tid, greet_txt)
            await bot.privmsg(ev.target, f"{ev.nick}: added target {tid} (nick={nick}), greeting {gid}.")
            return

        # ---- greet addhost <pattern> <greeting...> ----
        if sub == "addhost":
            if len(args) < 2:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !greet addhost <host pattern> <greeting...>")
                return
            pat = _norm(args[0])
            greet_txt = _norm(" ".join(args[1:]))
            tid = await _upsert_target(match_host=pat)
            gid = await _add_greeting(tid, greet_txt)
            await bot.privmsg(ev.target, f"{ev.nick}: added target {tid} (host={pat}), greeting {gid}.")
            return

        # ---- greet addmask <pattern> <greeting...> ----
        if sub == "addmask":
            if len(args) < 2:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !greet addmask <nick!user@host pattern> <greeting...>")
                return
            pat = _norm(args[0])
            greet_txt = _norm(" ".join(args[1:]))
            tid = await _upsert_target(match_hostmask=pat)
            gid = await _add_greeting(tid, greet_txt)
            await bot.privmsg(ev.target, f"{ev.nick}: added target {tid} (mask={pat}), greeting {gid}.")
            return

        # ---- greet deltarget <id> ----
        if sub == "deltarget":
            if len(args) != 1 or not args[0].isdigit():
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !greet deltarget <id>")
                return
            tid = int(args[0])
            await bot.store.execute("DELETE FROM greet_targets WHERE id=?", (tid,))
            await bot.privmsg(ev.target, f"{ev.nick}: deleted target {tid}.")
            return

        # ---- greet delgreet <id> ----
        if sub == "delgreet":
            if len(args) != 1 or not args[0].isdigit():
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !greet delgreet <id>")
                return
            gid = int(args[0])
            await bot.store.execute("DELETE FROM greetings WHERE id=?", (gid,))
            await bot.privmsg(ev.target, f"{ev.nick}: deleted greeting {gid}.")
            return

        # ---- greet enable/disable <id> ----
        if sub in ("enable", "disable"):
            if len(args) != 1 or not args[0].isdigit():
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !greet {sub} <target_id>")
                return
            tid = int(args[0])
            en = 1 if sub == "enable" else 0
            await bot.store.execute("UPDATE greet_targets SET enabled=?, updated_ts=? WHERE id=?", (en, _now(), tid))
            await bot.privmsg(ev.target, f"{ev.nick}: target {tid} set to {'on' if en else 'off'}.")
            return

        # ---- greet setpri <id> <priority> ----
        if sub == "setpri":
            if len(args) != 2 or (not args[0].isdigit()):
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !greet setpri <target_id> <priority>")
                return
            tid = int(args[0])
            try:
                pri = int(args[1])
            except Exception:
                await bot.privmsg(ev.target, f"{ev.nick}: priority must be an integer")
                return
            await bot.store.execute("UPDATE greet_targets SET priority=?, updated_ts=? WHERE id=?", (pri, _now(), tid))
            await bot.privmsg(ev.target, f"{ev.nick}: target {tid} priority set to {pri}.")
            return

        # ---- greet setchan <id> <#channel|any> ----
        if sub == "setchan":
            if len(args) != 2 or (not args[0].isdigit()):
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !greet setchan <target_id> <#channel|any>")
                return
            tid = int(args[0])
            ch = _norm(args[1])
            if ch.lower() == "any":
                ch = None
            elif ch and not ch.startswith("#"):
                await bot.privmsg(ev.target, f"{ev.nick}: channel must start with # or be 'any'")
                return
            await bot.store.execute("UPDATE greet_targets SET channel=?, updated_ts=? WHERE id=?", (ch, _now(), tid))
            await bot.privmsg(ev.target, f"{ev.nick}: target {tid} channel set to {ch or 'any'}.")
            return

        # ---- greet setcd <id> <seconds|0> ----
        if sub == "setcd":
            if len(args) != 2 or (not args[0].isdigit()):
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !greet setcd <target_id> <seconds|0>")
                return
            tid = int(args[0])
            try:
                sec = int(args[1])
            except Exception:
                await bot.privmsg(ev.target, f"{ev.nick}: seconds must be an integer")
                return
            if sec <= 0:
                sec_val = None
            else:
                sec_val = sec
            await bot.store.execute("UPDATE greet_targets SET cooldown_seconds=?, updated_ts=? WHERE id=?", (sec_val, _now(), tid))
            await bot.privmsg(ev.target, f"{ev.nick}: target {tid} cooldown set to {sec_val or 0}s.")
            return

        # fallback help
        await bot.privmsg(
            ev.target,
            f"{ev.nick}: usage: !greet list | greets <tid> | addnick <nick> <greet> | addhost <pat> <greet> | addmask <pat> <greet> | deltarget <tid> | delgreet <gid> | enable/disable <tid> | setpri <tid> <pri> | setchan <tid> <#chan|any> | setcd <tid> <sec|0> | test",
        )