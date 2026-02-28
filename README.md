# Leonidas IRC Bot (Leobot)

A modular, SQLite-backed IRC bot built in Python, with a clean separation between **core system runtime** and **pluggable services**.

## Key design points

- **Core runtime lives in `system/`** (connection, dispatcher, ACL, help, scheduler, store).
- **Services live in `services/`** (weather, news, greet, stats, etc.).
- **All bot state is persisted in SQLite** at `data/leonidas.db`.
- **Services are disabled by default** and enabled per channel via commands.
- **Local config is not committed**: `config/config.json` is gitignored.

---

## Project structure

```text
Leobot/
  bot.py

  config/
    config.json              (local only, gitignored)
    config.example.json      (committed template)

  data/
    leonidas.db              (created on first run, gitignored)

  system/
    config.py                config I/O + validation
    logging_setup.py         logging setup
    types.py                 shared dataclasses / protocols
    irc_parse.py             IRC parsing + chunking helpers
    irc_client.py            connection + send/recv loop
    dispatcher.py            event dispatch + middleware ordering
    scheduler.py             background job runner
    store.py                 SQLite persistence facade
    migrations.py            schema bootstrap + migrations
    acl.py                   core ACL/auth middleware + commands
    help.py                  core help/commands output
    servicectl.py            core service enable/disable commands

  services/
    eightball.py
    gemini.py
    greet.py
    weather.py
    wiki.py
    news.py
    logging.py
    lastseen.py
    stats.py
    maintenance.py
    sysmon.py