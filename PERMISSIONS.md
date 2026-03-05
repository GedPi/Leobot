# Leobot permissions

This document describes how access control works for users and services in Leobot.

---

## Roles

Roles are ordered (lowest to highest):

| Role         | Typical use                          |
|-------------|---------------------------------------|
| **guest**   | Unauthenticated / unknown hostmask   |
| **user**    | Known or config-matched users        |
| **contributor** | Trusted users (e.g. service management) |
| **admin**   | Full ACL and bot control            |

Role order is defined in `system.acl.ROLE_ORDER`. A principal is allowed to run a command if their **effective role** is at least the command’s **minimum role**.

---

## Identity (principals)

- **Principal** = who is acting. Derived from the IRC event via `principal_from_event(ev)` in `system.acl`.
- Canonical key: `user@host` when available, otherwise `nick` (lowercased).
- **Effective role** is the maximum of:
  1. Config mask match (`config acl.users`) → `user`
  2. Session role (from `!auth` until UTC midnight)
  3. DB identity role (`acl_identities`, keyed by nick)

---

## Command registration and minimum role

- Commands are registered with `bot.register_command(cmd, ..., min_role=..., service_id=..., capability=...)`.
- **Single source of truth** for “can this principal run this command?” is:
  1. **Effective role** of the principal
  2. **Required min_role** for the command, resolved in order:
     - **DB command override** (`acl_command_perms`, via `!acl addserv` / `delserv` / `servlist`)
     - **Per-channel policy** (`acl_policies`), if the command has `service_id` and `capability` and the context is a channel
     - **Command default** from `register_command(..., min_role=...)`

- **Guest whitelist**: Even if a command’s min_role is `guest`, only commands listed in `config acl.guest_allowed.commands` (default: `help`, `commands`) are allowed for guests. Everyone else is treated as “requires user”.

---

## Central permission check

- **`ACL.precheck(bot, ev)`** runs before any command handler. It:
  - Parses the command from the message
  - Skips checks for core commands: `auth`, `whoami`, `help`, `commands`
  - Calls **`ACL.is_allowed(bot, ev, cmd, info)`**, which:
    - Computes effective role
    - Resolves min_role (override → policy → default)
    - Returns True only if role ≥ min_role and guest rules are satisfied
  - If not allowed, sends a short “not allowed” message and returns False so the command is not dispatched.

---

## Per-channel service enablement

- **Service enablement** is separate from roles: it is a simple on/off per (channel, service).
- Controlled via `!service` / `!services` (requires at least **contributor**).
- The dispatcher only invokes a service’s hooks (e.g. `on_privmsg`) for a channel if that service is **enabled** for that channel.
- So: **service enabled** = “this service is on in this channel”; **ACL** = “this principal is allowed to run this command”.

---

## Per-channel policies (optional)

- Table **`acl_policies`**: `(channel, service_id, capability, min_role)`.
- Used only when the command has **service_id** and **capability** set and the context is a channel.
- Resolution: if a row exists for `(channel, service_id, capability)`, that **min_role** is used instead of the command override or default for that context.
- This allows e.g. “in `#ops`, `sysmon:manage` requires **admin**” while elsewhere it stays **contributor**. Policies are optional; without a row, the normal command override or default applies.

---

## Service IDs

- Every loaded service must have a unique **service_id** (e.g. `weather`, `sysmon`, `gemini`).
- If a service does not set `service_id`, it is derived from the module name (e.g. `services.weather` → `weather`).
- Duplicate `service_id`s at startup cause the later service to be skipped and an error to be logged.

---

## Admin commands

- **`!whoami`** – Shows your effective role and principal (identity key).
- **`!auth <password>`** – Authenticate for a higher role until UTC midnight (uses config `acl.admins` / `acl.contributors` masks and password hashes).
- **`!acl`** – Admin-only:
  - **adduser / deluser / usrlist** – Manage DB identities (nick → role). Mutating subcommands require the target nick to be NickServ-identified.
  - **addserv / delserv / servlist** – Manage command-level min_role overrides (by command name).

All ACL persistence goes through **Store** helpers (`acl_*`, `get_acl_session`, `set_acl_session`, etc.); the ACL layer does not use raw SQL for these tables.

---

## Summary

- **Principals**: one canonical key per event (`principal_from_event`).
- **Roles**: guest &lt; user &lt; contributor &lt; admin; effective role = max(mask, session, DB).
- **Commands**: registered with optional `service_id` and `capability`; min_role = override → channel policy → default; guest whitelist applies.
- **Services**: unique `service_id`; per-channel enablement is on/off; optional per-channel policies refine min_role by (service_id, capability).
