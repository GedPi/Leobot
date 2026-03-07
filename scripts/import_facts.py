#!/usr/bin/env python3
"""
Import facts from a CSV file into the Leobot database.

CSV format: category,fact
  - First line may be a header (category,fact); it is skipped if it looks like a header.
  - Lines with empty category or fact are skipped.

Usage:
  python scripts/import_facts.py <path/to/facts.csv> [--db path/to/leonidas.db]
  python scripts/import_facts.py facts.csv

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


def _is_header(row: list[str]) -> bool:
    if len(row) < 2:
        return False
    a, b = (s.strip().lower() for s in row[:2])
    return a == "category" and b == "fact"


async def run(csv_path: Path, db_path: str) -> None:
    store = Store(db_path)
    inserted = 0
    skipped = 0
    try:
        with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            first = True
            for row in reader:
                if len(row) < 2:
                    skipped += 1
                    continue
                if first and _is_header(row):
                    first = False
                    continue
                first = False
                category, fact = (s.strip() for s in (row[0], row[1]))
                if not category or not fact:
                    skipped += 1
                    continue
                await store.fact_insert(category, fact)
                inserted += 1
        print(f"Imported {inserted} facts (skipped {skipped} rows).")
    finally:
        await store.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Import facts from CSV into Leobot database.")
    ap.add_argument("csv", type=Path, help="Path to CSV file (category,fact)")
    ap.add_argument("--db", type=str, default=None, help="Database path (default: from config or ./data/leonidas.db)")
    args = ap.parse_args()

    if not args.csv.exists():
        print(f"Error: CSV file not found: {args.csv}", file=sys.stderr)
        sys.exit(1)

    db_path = _load_db_path(args.db)
    try:
        asyncio.run(run(args.csv, db_path))
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
