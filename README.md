# Leobot (Leonidas IRC Bot)

Python IRC bot with a modular service/plugin architecture.

## Layout on server (current)
- Runtime code: `/opt/leobot`
- Virtualenv: `/opt/leobot/venv`
- Config: `/etc/leobot/config.json`
- Logs: `/var/log/leobot/bot.log`
- State: `/var/lib/leobot/` (SQLite DB, watchlists, greetings, health snapshots, etc.)

The systemd unit runs as user `leobot` and allows writes only to `/var/log/leobot` and `/var/lib/leobot`.

## Development workflow (recommended)
- Dev + git: `~/Leobot` (owned by your user)
- Deploy target: `/opt/leobot` (owned by `leobot`)

### Deploy (mirror dev tree to runtime)
```bash
cd ~/Leobot

sudo rsync -a --delete \
  --exclude '.git/' \
  --exclude 'venv/' \
  --exclude '__pycache__/' \
  --exclude 'config.json' \
  ./ /opt/leobot/

sudo chown -R leobot:leobot /opt/leobot
sudo systemctl restart leobot.service
sudo systemctl --no-pager --full status leobot.service