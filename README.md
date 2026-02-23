# Leobot (Leonidas IRC Bot)

A modular Python IRC bot with a service/plugin architecture.

- Runtime config: `/etc/leobot/config.json`
- Logs: `/var/log/leobot/bot.log`
- State (SQLite, watchlists, health JSON, etc.): `/var/lib/leobot/`

## Features

- Service/plugin loader (`services.*`)
- Command registration + help routing
- ACL with roles + optional daily auth for mutating commands
- SQLite-backed chat logging / stats / lastseen
- RSS/Atom news headlines (interactive source selection)
- Open-Meteo weather (no API key)
- Wikipedia lookup + watchlist tooling
- Sysmon reads host health snapshot (`/var/lib/leobot/health.json`)

## Repo layout

```text
.
├── bot.py
├── services/
├── config.example.json
├── requirements.txt
└── ...