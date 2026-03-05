# Leonidas IRC Bot (Leobot)

Leonidas is a modular, SQLite-backed IRC bot written in Python.

It is built around a strict separation between:

* **Core runtime** (`system/`)
* **Pluggable services** (`services/`)

The architecture is intentionally simple, deterministic, and extensible without turning into a plugin free-for-all.

---

## Architecture

```
Leobot/
  bot.py

  config/
    config.json              (local only, gitignored)
    config.example.json      (template)

  data/
    leonidas.db              (created automatically)

  system/                    ← core runtime
  services/                  ← feature modules
```

### Core (`system/`)

Responsible for:

* IRC connection and parsing
* Event dispatch
* Middleware ordering
* SQLite persistence
* Schema migrations
* ACL / authentication
* Help / command registry
* Per-channel service control
* Background scheduling

The core is designed to remain stable and minimal.

### Services (`services/`)

Feature modules loaded from config:

* `eightball`
* `gemini`
* `greet`
* `weather`
* `wiki`
* `news`
* `logging`
* `lastseen`
* `stats`
* `sysmon`
* `maintenance` (placeholder)

Services:

* Register their own commands
* Respect ACL roles
* Can be enabled/disabled per channel
* Are isolated from core logic

---

## Design Principles

* Single-process architecture
* SQLite persistence (no external DB required)
* Per-channel feature toggling
* Explicit command registration
* No hidden dynamic execution
* Minimal blocking I/O
* Clear separation of concerns

---

## Configuration

Local configuration file:

```
config/config.json
```

Template:

```
config/config.example.json
```

Config is not committed to the repository.

---

## Database

SQLite database is created automatically:

```
data/leonidas.db
```

Schema is bootstrapped via `system/migrations.py`.

No external dependencies are required.

---

## Role System (ACL)

See **[PERMISSIONS.md](PERMISSIONS.md)** for the full permission model (roles, principals, command vs service permissions, per-channel policies).

Role hierarchy:

```
guest < user < contributor < admin
```

Features:

* Mask-based role matching
* Per-command minimum role enforcement
* Optional daily password re-authentication for mutating commands
* PM-only authentication flow

Core commands:

```
!whoami
!auth <password>
```

---

## Service Enablement (Per Channel)

Services are disabled by default per channel.

Control via:

```
!service
!services
!service enable <service> [#channel]
!service disable <service> [#channel]
```

Enablement state is persisted in SQLite.

---

## Feature Overview

### Fun

* `!8ball`
* `!gemini`

### Information

* `!wiki`
* `!weather`
* `!news`

### Monitoring

* `!sys`
* `!uptime`
* `!disk`
* `!updates`
* `!errors`
* `!services`
* `!events`

### Analytics

* `!stats`
* `!lastseen`

### Operations

* `!help`
* `!service`

---

## Running

1. Copy config template:

   ```
   cp config/config.example.json config/config.json
   ```

2. Edit configuration.

3. Run:

   ```
   python bot.py
   ```

For production deployment, use systemd or your preferred supervisor.

---

## What Leonidas Is Not

* Not a distributed bot cluster
* Not a bouncer
* Not a dynamic plugin marketplace
* Not multi-process

It is deliberately single-process, controlled, and predictable.

---

## Version

This README reflects the first stable release of the refactored architecture (v1 baseline).

---

If you want, I can also produce:

* A short tagline + badge section for the top
* A minimal “enterprise-clean” README version
* Or a more personality-driven “HairyOctopus” branded version