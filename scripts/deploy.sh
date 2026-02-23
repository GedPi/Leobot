#!/usr/bin/env bash
set -euo pipefail

SRC="${HOME}/Leobot/"
DST="/opt/leobot/"

sudo rsync -a --delete \
  --exclude '.git/' \
  --exclude 'venv/' \
  --exclude '__pycache__/' \
  --exclude 'config.json' \
  "${SRC}" "${DST}"

sudo chown -R leobot:leobot "${DST}"
sudo systemctl restart leobot.service

echo "Deployed ${SRC} -> ${DST} and restarted leobot.service"