# Leonidas IRC Bot (Leobot)

A modular, SQLite-backed IRC bot written in Python. Connect to your network, join channels, and extend behaviour with pluggable services—weather, wiki, news, system monitoring, greetings, and more—while keeping a single process, explicit permissions, and per-channel control.

---

## Features

- **Single process** — One Python process, one SQLite database, no external message broker
- **Pluggable services** — Enable only what you need; each service registers its own commands and can be toggled per channel
- **Role-based access** — Guest, user, contributor, admin with config masks, password auth, and optional per-channel policy overrides
- **Per-channel service enablement** — Services are off by default per channel; enable with `!service enable <service> [#channel]`
- **Graceful operation** — Reconnect with backoff, clean shutdown (QUIT then close), and optional config reload without full restart
- **Help in private** — `!help` and `!commands` reply in a private message so channels stay readable

---

## How it works

1. **Startup** — Loads `config/config.json`, creates the Store (SQLite + migrations), builds the Bot with Dispatcher, ACL, Help, and ServiceCtl, then loads each module listed in `config.services` and calls its `setup(bot)`.
2. **Connection** — Connects to IRC (TLS by default), sends NICK/USER, optionally identifies to NickServ, then JOINs configured channels. On `001`, the scheduler starts and each service’s `on_ready` hook runs if present.
3. **Events** — Every IRC line is parsed into a command and params. For PRIVMSG/NOTICE/JOIN/PART/QUIT/NICK/KICK/MODE/TOPIC the bot builds an `Event` (nick, user, host, target, channel, text, …) and passes it to the Dispatcher.
4. **Dispatch** — For PRIVMSG, the Dispatcher runs **ACL precheck** first (resolve command, check effective role vs required min_role; deny with a short message if not allowed). Then it runs **core handlers** (ACL for `!auth`/`!whoami`/`!acl`/`!reload`, Help for `!help`/`!commands`, ServiceCtl for `!service`/`!services`). If one handles the message, it can optionally tee the event to the logging service. Then the Dispatcher invokes each **service**’s hook (e.g. `on_privmsg`) only if that service is enabled for the event’s channel.
5. **Commands** — Services and core register commands via `bot.register_command(name, ..., min_role=..., service_id=..., capability=...)`. The ACL uses the command registry plus DB overrides and per-channel policies to decide who can run what.
6. **Reconnect** — If the connection drops or the bot crashes, the main loop runs shutdown (close IRC, stop scheduler, close store), then waits with exponential backoff and creates a new Bot, reloads services, and connects again—until an exit is requested (e.g. SIGTERM).

---

## Architecture

```
Leobot/
  bot.py                 # Entrypoint: config, Bot, connect, main loop + reconnect
  config/
    config.json          # Your config (gitignored)
    config.example.json  # Template
  data/
    leonidas.db          # SQLite (created automatically)
  system/                # Core runtime
    acl.py               # Roles, !auth/!whoami/!acl/!reload, precheck
    config.py            # Load/validate config, apply defaults
    dispatcher.py        # Route events to core handlers and services
    help.py              # !help / !commands (replies by PM)
    irc_client.py        # TCP/TLS connection, send/recv, PING/PONG
    irc_parse.py         # Parse lines and prefixes
    logging_setup.py     # File + console logging
    migrations.py        # Schema versions and migrations
    scheduler.py         # Repeating background jobs
    servicectl.py        # !service / !services (per-channel enablement)
    store.py             # SQLite wrapper + ACL, news, greet, etc.
    types.py             # Event, Role, CommandInfo
  services/              # Feature modules (loaded from config)
    eightball.py
    insult.py
    joke.py
    wolfram.py
    fact.py
    gemini.py
    greet.py
    weather.py
    wiki.py
    news.py
    logging.py
    lastseen.py
    stats.py
    maintenance.py       # Placeholder
    sysmon.py
    blizzard.py
    pokemon.py
  tests/                 # Pytest suite (config, ACL, Store)
  PERMISSIONS.md         # Full permission model
  IMPROVEMENTS.md        # Operational improvements and status
```

- **Core** — IRC I/O, event dispatch, ACL, help, service enablement, scheduler, and a single Store. No business logic beyond that.
- **Services** — Each provides `setup(bot)`, optionally a `service_id`, and registers commands and hooks (`on_privmsg`, `on_join`, etc.). They use `bot.store` and `bot.register_command`; they do not touch the dispatcher or ACL internals.

---

## Installation

- **Python** — 3.10+ (or equivalent with asyncio and standard library).
- **Dependencies** — None for running the bot (standard library only). See [Testing](#testing) for optional test dependencies.

Clone or download the repo and use a venv if you like:

```bash
cd Leobot
python -m venv venv
# On Windows: venv\Scripts\activate
# On Unix: source venv/bin/activate
```

---

## Quick start

1. **Copy and edit config**

   ```bash
   cp config/config.example.json config/config.json
   ```

   Set at least: `server`, `port`, `nick`, `user`, `realname`, `channels` (list), `services` (list of module names). Optionally set `nickserv_password`, `acl.admins`, `acl.users`, etc.

2. **Run**

   ```bash
   python bot.py
   ```

   The bot connects, joins channels, and starts the scheduler. Use `!help` or `!commands` in a channel; the reply is sent to you in a private message.

3. **Enable services per channel**

   By default, services are disabled per channel. As a user with at least **contributor** role:

   ```
   !service enable weather #YourChannel
   !services
   ```

For production, run under systemd or another process supervisor so the bot restarts on failure.

---

## Configuration

| Key | Description |
|-----|-------------|
| `server`, `port` | IRC server (e.g. `irc.example.net`, `6697`) |
| `use_tls`, `verify_tls` | TLS and certificate verification (default true) |
| `password` | Optional server connection password |
| `nickserv_password` | Sent to NickServ after connect (IDENTIFY) |
| `nick`, `user`, `realname` | IRC identity |
| `channels` | List of channels to JOIN on connect |
| `command_prefix` | Prefix for commands (default `!`) |
| `reconnect_min_seconds`, `reconnect_max_seconds` | Backoff bounds after disconnect |
| `log_path` | Log file path (default `./bot.log`) |
| `db_path` | SQLite path (default `./data/leonidas.db`) |
| `acl` | `admins`, `contributors`, `users` (masks), `guest_allowed.commands`, `master` (bootstrap admin nick) |
| `services` | List of service module names (e.g. `services.weather`, `services.wiki`) |

See `config/config.example.json` for the full shape. After editing config, admins can run **`!reload`** to reload from file without restarting the process (server/port and similar still require a full restart).

---

## Permissions (ACL)

Roles (lowest to highest): **guest** → **user** → **contributor** → **admin**.

- **guest** — Unauthenticated or unknown hostmask; only commands in `acl.guest_allowed.commands` (e.g. `help`, `commands`, `8ball`) are allowed.
- **user** — Matched by `acl.users` hostmask and/or after `!auth` or DB identity.
- **contributor** — For service management; can use `!service enable/disable`.
- **admin** — Can use `!acl` (add/del users, command overrides) and `!reload`.

Core commands available to all (subject to guest whitelist): `!help`, `!commands`, `!auth <password>`, `!whoami`. Admins: `!acl ...`, `!reload`.

Full details (principals, effective role, per-channel policies, command overrides): **[PERMISSIONS.md](PERMISSIONS.md)**.

---

## Service enablement

Services are **disabled by default** per channel so you choose where each feature is active.

| Command | Description |
|---------|-------------|
| `!service` | Short usage |
| `!services` | List services and their on/off state in the current (or specified) channel |
| `!service enable <service> [#channel]` | Enable a service in a channel (default: current) |
| `!service disable <service> [#channel]` | Disable a service in a channel |

Requires at least **contributor**. Enablement is stored in SQLite.

---

## Commands overview

Commands depend on which services are loaded and enabled in the channel. Use `!commands` to list commands available to your role.

### System (always available)

| Command | Role | Description |
|---------|------|-------------|
| `!help [command]` | guest | Help for a command or category (reply by PM) |
| `!commands` | guest | List commands by category (reply by PM) |
| `!auth <password>` | guest | Authenticate for higher role until UTC midnight |
| `!whoami` | guest | Show your effective role and identity |
| `!service` | contributor | Manage per-channel services |
| `!service enable <service> [#channel]` | contributor | Enable a service in a channel |
| `!service disable <service> [#channel]` | contributor | Disable a service in a channel |
| `!services` | contributor | List services and their on/off state |
| `!acl` | admin | ACL management |
| `!acl adduser <nick> <admin\|contributor\|user>` | admin | Add user to DB |
| `!acl deluser <nick> <group>` | admin | Delete user from DB |
| `!acl usrlist <admin\|contributor\|user>` | admin | List users in group |
| `!acl addserv <service> <guest\|user\|contributor\|admin>` | admin | Set command min role override |
| `!acl delserv <service> <group>` | admin | Delete command override |
| `!acl servlist` | admin | List command overrides |
| `!reload` | admin | Reload config from file |

### Fun

| Command | Service | Role | Description |
|---------|---------|------|-------------|
| `!8ball`, `!eightball` | eightball | guest | Magic 8-ball |
| `!gemini`, `!g` | gemini | user | Ask Gemini |
| `!insult <nick>` | insult | user | Insult someone (English) |
| `!insulte <nick>` | insult | user | Insult someone (Greek) |
| `!joke [category] [lang] [safe]` | joke | user | Random joke from JokeAPI |

### Information

| Command | Service | Role | Description |
|---------|---------|------|-------------|
| `!fact [category]` | fact | guest | Random fact; omit category for any |
| `!fact auto on`, `!fact auto off` | fact | contributor | Enable/disable auto facts |
| `!fact auto` | fact | contributor | Show auto status, today's count |
| `!fact auto min N`, `!fact auto max N` | fact | contributor | Set min/max facts per day (1–100) |
| `!wolf <question>` | wolfram | user | Query Wolfram\|Alpha |
| `!wiki <query>` | wiki | guest | Wikipedia lookup |
| `!wikicheck <query>` | wiki | guest | Check if Wikipedia title exists |
| `!wikimon [list]` | wiki | user | List wiki watches |
| `!wikimon add <title> [15m\|2h\|1d]` | wiki | user | Add wiki watch |
| `!wikimon del <id>` | wiki | user | Delete wiki watch |
| `!wikimon clear` | wiki | contributor | Clear all wiki watches |
| `!wikimon lang <code>` | wiki | contributor | Set default wiki language |
| `!wikimon interval <id> <15m\|2h\|1d>` | wiki | user | Change watch interval |

Facts are stored in the database. Import from CSV:  
`python scripts/import_facts.py path/to/facts.csv [--db path/to/leonidas.db]`

### Weather

| Command | Service | Role | Description |
|---------|---------|------|-------------|
| `!weather <location>` | weather | guest | Weather lookup |
| `!weather warn add <location> <type(s)> <duration>` | weather | user | Add warning watch |
| `!weather warn list` | weather | user | List warning watches |
| `!weather del <id>` | weather | user | Delete a watch |
| `!weather warn clear` | weather | user | Clear watches |
| `!weather watch <city1,city2,...> <#channel>` | weather | contributor | Proactive extreme weather announcements |

### News

| Command | Service | Role | Description |
|---------|---------|------|-------------|
| `!news [limit] [category]`, `!headlines` | news | guest | RSS/news headlines |
| `!news sources` | news | contributor | List configured sources |
| `!news categories <id>` | news | contributor | List categories for a source |
| `!news addsource <id> <name>` | news | contributor | Add/update source |
| `!news delsource <id>` | news | contributor | Delete source |
| `!news enable <id>`, `!news disable <id>` | news | contributor | Enable/disable source |
| `!news addcat <id> <category> <url>` | news | contributor | Add/update category |
| `!news delcat <id> <category>` | news | contributor | Delete category |

### Monitoring (sysmon)

| Command | Service | Role | Description |
|---------|---------|------|-------------|
| `!sys [subcmd]` | sysmon | user | Server health summary |
| `!sys services` | sysmon | user | Watched systemd services status |
| `!uptime` | sysmon | user | System uptime |
| `!disk` | sysmon | user | Disk usage |
| `!updates` | sysmon | user | Pending package updates |
| `!failed` | sysmon | user | Failed systemd units |
| `!errors` | sysmon | user | Recent journal errors count |
| `!events [N]` | sysmon | user | Tail sysmon events |

### Blizzard (WoW / Diablo III)

| Command | Service | Role | Description |
|---------|---------|------|-------------|
| `!wow char|guild|realm|item|auction|pvp ...` | blizzard | guest | WoW game data |
| `!wc`, `!wg`, `!wr`, `!wi`, `!wa`, `!wpvp` | blizzard | guest | WoW aliases |
| `!d3 profile|hero|item|leaderboard ...` | blizzard | guest | Diablo III game data |
| `!dp`, `!dh`, `!di`, `!dlb` | blizzard | guest | D3 aliases |

Requires `blizzard.client_id` and `blizzard.client_secret` in config.

### Games (Pokémon)

| Command | Service | Role | Description |
|---------|---------|------|-------------|
| `!pokemon`, `!pkmn` | pokemon | guest | Pokémon trainer: team, help |
| `!pokemon team` | pokemon | guest | View your party |
| `!capture [pokeball\|great_ball\|ultra_ball]` | pokemon | guest | Capture wild Pokémon |
| `!battle @user` | pokemon | guest | Battle another trainer |
| `!levelup <slot>` | pokemon | guest | Level up a Pokémon |
| `!pokemon spawns [N]` | pokemon | contributor | Set wild spawns per day |

PM the bot for `!pkmn team #channel`, `!pkmn items #channel`, `!use potion <slot> #channel`, `!use revive <slot> #channel`, `!levelup <slot> #channel`. Run `python scripts/seed_pokemon.py` to populate species from PokeAPI.

### Greet

| Command | Service | Role | Description |
|---------|---------|------|-------------|
| `!greet list` | greet | contributor | List greeting targets |
| `!greet greets <target_id>` | greet | contributor | List greetings in target's pool |
| `!greet addnick <nick> <greeting...>` | greet | contributor | Add greeting for nick match |
| `!greet addhost <pattern> <greeting...>` | greet | contributor | Add greeting for host pattern |
| `!greet addmask <pattern> <greeting...>` | greet | contributor | Add greeting for hostmask pattern |
| `!greet deltarget <id>` | greet | contributor | Delete a target |
| `!greet enable <id>`, `!greet disable <id>` | greet | contributor | Enable/disable target |
| `!greet setpri <id> <priority>` | greet | contributor | Set target priority |
| `!greet setchan <id> <#channel\|any>` | greet | contributor | Set target channel |
| `!greet setcd <id> <seconds\|0>` | greet | contributor | Set cooldown |
| `!greet test` | greet | contributor | Test greet matching for current identity |
| `!greet pools` | greet | contributor | List greeting pools |
| `!greet pooladd <name>` | greet | contributor | Add pool |
| `!greet poolset <id> <name>` | greet | contributor | Rename pool |
| `!greet poolgreets <pool_id>` | greet | contributor | List greetings in pool |
| `!greet pooladdgreet <pool_id> <text>` | greet | contributor | Add greeting to pool |
| `!greet pooldel <id>` | greet | contributor | Delete pool |
| `!greet delgreet <id>` | greet | contributor | Delete a greeting |

### Utility

| Command | Service | Role | Description |
|---------|---------|------|-------------|
| `!stats [me\|nick\|top] [N] [#channel] [today\|24h\|7d\|all]` | stats | user | Channel stats |
| `!seen <nick>`, `!lastseen <nick>` | lastseen | user | When a user was last seen |

### Services without commands

- **logging** — Records channel traffic to the database when enabled; no user commands.
- **maintenance** — Placeholder; no user commands.

---

## Recent changes and improvements

- **Windows** — Signal handlers (SIGINT/SIGTERM/SIGUSR1) are optional; if the platform doesn’t support them (e.g. Windows), the bot logs a warning and continues (use Ctrl+Break or close the process to exit).
- **Command collision** — `!services` is reserved for the core “list service enablement” command. Sysmon’s “watched systemd services” is only under **`!sys services`**.
- **Shutdown** — IRC connection is closed first so the read loop exits promptly; then scheduler and store are closed.
- **Config reload** — Admins can run **`!reload`** to reload `config.json` and update `bot.cfg` without restarting the process.
- **Help in PM** — **`!help`** and **`!commands`** always reply in a **private message** to the user who asked, so channels are not flooded.
- **Tests** — A small pytest suite in `tests/` covers config validation, ACL (principal, effective role), and Store ACL helpers. Run: `pip install -r requirements-test.txt` then `pytest tests -v`.
- **Permissions** — Unified permission model with roles, principals, command and per-channel policy overrides; see [PERMISSIONS.md](PERMISSIONS.md) and [IMPROVEMENTS.md](IMPROVEMENTS.md).

---

## Database

- **Path** — Set by `db_path` in config (default `./data/leonidas.db`). The directory is created if needed.
- **Schema** — Managed by `system/migrations.py` (versioned; applied on first open). Tables include: settings, service_enablement, ACL (sessions, identities, command_perms, policies), messages, irc_log, news, greet, weather, wiki, sysmon, stats, pokemon (species, moves, trainers, wild spawns), etc.
- **Backup** — Copy the SQLite file while the bot is stopped, or use SQLite’s backup API if the bot is running.

---

## Testing

Optional test dependencies:

```bash
pip install -r requirements-test.txt
pytest tests -v
```

Tests cover: config validation and defaults, ACL principal and effective role (with a mock store), Store ACL identity/command_perms/policies (with a temp DB). See `tests/` and `pytest.ini`.

---

## Design principles

- **Single process** — One process, one SQLite DB; no distributed coordination.
- **Explicit commands** — Every command is registered with a minimum role and optional service/capability; no hidden or dynamic execution.
- **Per-channel control** — Services are off by default per channel and turned on explicitly.
- **Clear separation** — Core handles IRC, dispatch, ACL, and persistence; services implement features and register with the core.
- **Minimal blocking** — Async I/O and a single Store lock; no thread pool required for normal operation.

---

## What Leonidas is not

- Not a bouncer or proxy
- Not a multi-process or distributed bot cluster
- Not a plugin marketplace (services are code you add to the repo and list in config)
- Not multi-tenant across separate servers

It is a single, controllable IRC bot with a fixed set of services you enable per channel and per server.

---

## License

See [LICENSE](LICENSE).
