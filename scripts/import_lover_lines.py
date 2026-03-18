#!/usr/bin/env python3
"""
Import lover lines from text or CSV into the Leobot database.

Supported inputs:
  - TXT: one line per row (blank lines skipped)
  - CSV: first column is used as the line text
         optional header is skipped if first cell is "line" or "text"

Usage:
  python scripts/import_lover_lines.py <path/to/lines.txt> [--db path/to/leonidas.db]
  python scripts/import_lover_lines.py <path/to/lines.csv>

If --db is omitted, config/config.json is used for db_path, or ./data/leonidas.db.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from system.store import Store


def _load_db_path(cli_path: str | None) -> str:
    if cli_path:
        p = Path(cli_path)
        return str(p.resolve() if not p.is_absolute() else p)
    config_path = _ROOT / "config" / "config.json"
    if config_path.exists():
        import json

        cfg = json.loads(config_path.read_text(encoding="utf-8"))
        raw = cfg.get("db_path", "./data/leonidas.db")
    else:
        raw = "./data/leonidas.db"
    p = Path(raw)
    if not p.is_absolute():
        p = (_ROOT / raw).resolve()
    return str(p)


def _is_csv(path: Path) -> bool:
    return path.suffix.lower() in {".csv"}


def _looks_like_header(cell0: str) -> bool:
    c = (cell0 or "").strip().lower()
    return c in {"line", "text", "lover_line", "pickup_line"}


async def run(input_path: Path, db_path: str) -> None:
    store = Store(db_path)
    inserted = 0
    skipped = 0
    try:
        if _is_csv(input_path):
            with open(input_path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(f)
                first = True
                for row in reader:
                    if not row:
                        skipped += 1
                        continue
                    text = (row[0] or "").strip()
                    if first and _looks_like_header(text):
                        first = False
                        continue
                    first = False
                    if not text:
                        skipped += 1
                        continue
                    await store.lover_line_insert(text)
                    inserted += 1
        else:
            with open(input_path, "r", encoding="utf-8", errors="replace") as f:
                for raw in f:
                    text = raw.strip()
                    if not text:
                        skipped += 1
                        continue
                    await store.lover_line_insert(text)
                    inserted += 1
        print(f"Imported {inserted} lover lines (skipped {skipped} rows).")
    finally:
        await store.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Import lover lines into Leobot database.")
    ap.add_argument("input", type=Path, help="Path to TXT (one line per row) or CSV file")
    ap.add_argument(
        "--db",
        type=str,
        default=None,
        help="Database path (default: from config or ./data/leonidas.db)",
    )
    args = ap.parse_args()

    if not args.input.exists():
        print(f"Error: input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    db_path = _load_db_path(args.db)
    try:
        asyncio.run(run(args.input, db_path))
    except sqlite3.OperationalError as e:
        if "readonly" in str(e).lower():
            print(
                f"Error: database is read-only: {e}",
                file=sys.stderr,
            )
            print(
                f"The database or its directory is not writable by the current user. "
                f"Either fix permissions on {db_path} (and its directory), run this script as the "
                "user that owns the database (e.g. the bot user), or pass a writable path with --db.",
                file=sys.stderr,
            )
            sys.exit(1)
        raise


if __name__ == "__main__":
    main()
