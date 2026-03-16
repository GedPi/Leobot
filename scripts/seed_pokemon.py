#!/usr/bin/env python3
"""
Seed the Pokemon database from PokeAPI (https://pokeapi.co).

Populates pokemon_species, pokemon_moves, and pokemon_species_moves with
official game data. Run once after enabling the Pokemon service.

Usage:
  python scripts/seed_pokemon.py [--db path/to/leonidas.db] [--limit N]
  python scripts/seed_pokemon.py --limit 151

If --db is omitted, config/config.json is used for db_path.
--limit: max species to fetch (default: all, use 151 for Gen1 only).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from system.migrations import apply_migrations

POKEAPI_BASE = "https://pokeapi.co/api/v2"
UA = "LeobotPokemonSeed/1.0"


def _http_get(url: str, timeout: int = 30) -> tuple[int, bytes]:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def _load_db_path(cli_path: str | None) -> str:
    if cli_path:
        p = Path(cli_path)
        return str(p.resolve() if not p.is_absolute() else p)
    config_path = _ROOT / "config" / "config.json"
    if config_path.exists():
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
        raw = cfg.get("db_path", "./data/leonidas.db")
    else:
        raw = "./data/leonidas.db"
    p = Path(raw)
    if not p.is_absolute():
        p = (_ROOT / raw).resolve()
    return str(p)


def _fetch_json(url: str) -> dict:
    status, raw = _http_get(url)
    if status != 200:
        raise RuntimeError(f"HTTP {status} for {url}")
    return json.loads(raw.decode("utf-8", errors="replace"))


def _run(conn, db_path: str, limit: int | None) -> None:
    now = int(time.time())

    # Ensure migrations applied
    apply_migrations(conn)

    # Fetch species list
    print("Fetching species list...")
    data = _fetch_json(f"{POKEAPI_BASE}/pokemon?limit=2000")
    results = data.get("results") or []
    if limit:
        results = results[:limit]
    print(f"Will fetch {len(results)} species.")

    seen_moves: dict[int, dict] = {}
    species_inserted = 0
    moves_inserted = 0
    species_moves_inserted = 0

    for i, entry in enumerate(results):
        name = entry.get("name") or ""
        url = entry.get("url") or ""
        if not url:
            continue
        try:
            pk = _fetch_json(url)
        except Exception as e:
            print(f"  Skip {name}: {e}")
            continue

        pid = int(pk.get("id") or 0)
        if pid <= 0:
            continue

        # Base stats
        stats_map = {}
        for s in pk.get("stats") or []:
            stat = (s.get("stat") or {}).get("name") or ""
            base = int(s.get("base_stat") or 0)
            stats_map[stat] = base
        hp = stats_map.get("hp", 50)
        atk = stats_map.get("attack", 50)
        df = stats_map.get("defense", 50)
        sp_atk = stats_map.get("special-attack", 50)
        sp_def = stats_map.get("special-defense", 50)
        speed = stats_map.get("speed", 50)

        # Types
        types = []
        for t in pk.get("types") or []:
            tn = (t.get("type") or {}).get("name") or ""
            if tn:
                types.append(tn)
        type1 = types[0] if types else "normal"
        type2 = types[1] if len(types) > 1 else None

        # Capture rate (from species endpoint)
        capture_rate = 255
        try:
            species_url = (pk.get("species") or {}).get("url") or ""
            if species_url:
                sp_data = _fetch_json(species_url)
                capture_rate = int(sp_data.get("capture_rate") or 255)
                capture_rate = max(1, min(255, capture_rate))
        except Exception:
            pass

        conn.execute(
            """INSERT OR REPLACE INTO pokemon_species(
                id, pokedex_number, name, type1, type2,
                hp_base, atk_base, def_base, sp_atk_base, sp_def_base, speed_base,
                capture_rate, created_ts)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                pid,
                pid,
                name.capitalize(),
                type1,
                type2,
                hp,
                atk,
                df,
                sp_atk,
                sp_def,
                speed,
                capture_rate,
                now,
            ),
        )
        species_inserted += 1

        # Moves: level-up only (simplified)
        for m in pk.get("moves") or []:
            move_entry = m.get("move") or {}
            move_url = move_entry.get("url") or ""
            if not move_url:
                continue
            mid = int(move_url.rstrip("/").split("/")[-1])
            if mid <= 0:
                continue

            level = 1
            for vgd in m.get("version_group_details") or []:
                lvl = int(vgd.get("level_learned_at") or 0)
                learn_method = (vgd.get("move_learn_method") or {}).get("name") or ""
                if learn_method == "level-up" and lvl > 0:
                    level = max(level, lvl)

            if mid not in seen_moves:
                try:
                    move_data = _fetch_json(move_url)
                    mname = (move_data.get("name") or "").replace("-", " ").title()
                    mtype = (move_data.get("type") or {}).get("name") or "normal"
                    dmg_class = (move_data.get("damage_class") or {}).get("name") or "status"
                    power = move_data.get("power")
                    accuracy = move_data.get("accuracy")
                    pp = int(move_data.get("pp") or 40)
                    conn.execute(
                        """INSERT OR IGNORE INTO pokemon_moves(
                            id, name, type, category, power, accuracy, pp, created_ts)
                        VALUES(?,?,?,?,?,?,?,?)""",
                        (mid, mname, mtype, dmg_class, power, accuracy, pp, now),
                    )
                    moves_inserted += 1
                    seen_moves[mid] = {"name": mname}
                except Exception:
                    continue

            try:
                conn.execute(
                    """INSERT OR IGNORE INTO pokemon_species_moves(species_id, move_id, level_learned)
                       VALUES(?,?,?)""",
                    (pid, mid, level),
                )
                species_moves_inserted += 1
            except Exception:
                pass

        if (i + 1) % 50 == 0:
            print(f"  Progress: {i + 1}/{len(results)} species")
            time.sleep(0.5)  # Rate limit

    conn.commit()
    print(f"Done. Species: {species_inserted}, Moves: {moves_inserted}, Species-Moves: {species_moves_inserted}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Seed Pokemon database from PokeAPI")
    ap.add_argument("--db", type=str, default=None, help="Database path")
    ap.add_argument("--limit", type=int, default=None, help="Max species (e.g. 151 for Gen1)")
    args = ap.parse_args()

    db_path = _load_db_path(args.db)
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        _run(conn, db_path, args.limit)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
