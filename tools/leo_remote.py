#!/usr/bin/env python3
"""
Leobot remote helper. Deploy to server (e.g. /opt/leobot/tools/leo_remote.py).
Accepts JSON via stdin; returns JSON to stdout. Used by LeoControl GUI over SSH.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

LEOBOT_ROOT = Path(os.environ.get("LEOBOT_ROOT", "/opt/leobot"))


def _paths(req: dict) -> tuple[Path, Path, Path, str]:
    """Resolve config, db, log paths and service name from request or env."""
    cfg = req.get("config_path") or os.environ.get("LEOBOT_CONFIG") or str(LEOBOT_ROOT / "config" / "config.json")
    db = req.get("db_path") or os.environ.get("LEOBOT_DB") or str(LEOBOT_ROOT / "data" / "leonidas.db")
    log = req.get("log_path") or os.environ.get("LEOBOT_LOG") or str(LEOBOT_ROOT / "bot.log")
    svc = req.get("service_name") or os.environ.get("LEOBOT_SERVICE", "leobot")
    return Path(cfg), Path(db), Path(log), svc


def rows_to_list(rows) -> list[dict]:
    return [dict(r) for r in rows] if rows else []


def run_cmd(args: list) -> dict:
    r = subprocess.run(args, capture_output=True, text=True, timeout=30)
    return {"returncode": r.returncode, "stdout": r.stdout, "stderr": r.stderr}


def handle(req: dict) -> dict:
    action = req.get("action")
    if not action:
        return {"error": "missing action"}

    cfg_path, db_path, log_path, service_name = _paths(req)

    try:
        if action == "ping":
            return {"ok": True, "message": "ok"}

        elif action == "get_config":
            if not cfg_path.exists():
                return {"error": f"config not found: {cfg_path}"}
            data = json.loads(cfg_path.read_text(encoding="utf-8"))
            return {"ok": True, "data": data}

        elif action == "set_config":
            data = req.get("config") or req.get("data")
            if data is None:
                return {"error": "missing config/data"}
            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            cfg_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            return {"ok": True}

        elif action == "get_logs":
            tail = int(req.get("tail", 500))
            tail = max(1, min(5000, tail))
            if not log_path.exists():
                return {"ok": True, "lines": [], "path": str(log_path)}
            lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            lines = lines[-tail:] if len(lines) > tail else lines
            return {"ok": True, "lines": lines, "path": str(log_path)}

        elif action in ("bot_status", "bot_start", "bot_stop", "bot_restart"):
            cmd = "status" if action == "bot_status" else action.replace("bot_", "")
            r = run_cmd(["systemctl", cmd, service_name])
            out = (r["stdout"] or "").strip() + (("\n" + (r["stderr"] or "").strip()) if r["stderr"] else "")
            if action == "bot_status":
                return {"ok": r["returncode"] == 0, "status": out.splitlines()[0] if out else "unknown"}
            return {"ok": r["returncode"] == 0, "output": out}

        elif action == "systemctl":
            cmd = req.get("cmd")
            if cmd not in ("start", "stop", "restart", "status"):
                return {"error": f"invalid cmd: {cmd}"}
            r = run_cmd(["systemctl", cmd, service_name])
            return {"ok": r["returncode"] == 0, "returncode": r["returncode"], "stdout": r["stdout"], "stderr": r["stderr"]}

        elif action == "db_tables":
            conn = sqlite3.connect(str(db_path), timeout=10.0)
            try:
                rows = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                ).fetchall()
                return {"ok": True, "tables": [r[0] for r in rows]}
            finally:
                conn.close()

        elif action == "db_schema":
            table = req.get("table")
            if not table:
                return {"error": "missing table"}
            conn = sqlite3.connect(str(db_path), timeout=10.0)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
                return {"ok": True, "columns": [{"name": r[1], "type": r[2], "notnull": r[3], "pk": r[5]} for r in rows]}
            except Exception as e:
                return {"error": str(e)}
            finally:
                conn.close()

        elif action == "db_select":
            table = req.get("table")
            limit = int(req.get("limit", 500))
            limit = max(1, min(5000, limit))
            offset = int(req.get("offset", 0))
            conn = sqlite3.connect(str(db_path), timeout=10.0)
            conn.row_factory = sqlite3.Row
            try:
                check = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
                if not check:
                    return {"error": f"unknown table: {table}"}
                rows = conn.execute(f"SELECT * FROM {table} LIMIT ? OFFSET ?", (limit, offset)).fetchall()
                return {"ok": True, "rows": rows_to_list(rows)}
            except Exception as e:
                return {"error": str(e)}
            finally:
                conn.close()

        elif action == "db_execute":
            sql = req.get("sql")
            params = tuple(req.get("params") or [])
            fetch = bool(req.get("fetch", True))
            if not sql or not sql.strip():
                return {"error": "missing sql"}
            sql_upper = sql.strip().upper()
            if sql_upper.startswith(("DROP", "ALTER", "TRUNCATE")):
                return {"error": "forbidden sql verb"}
            conn = sqlite3.connect(str(db_path), timeout=10.0)
            conn.row_factory = sqlite3.Row
            try:
                cur = conn.execute(sql, params)
                if fetch and sql_upper.startswith("SELECT"):
                    rows = cur.fetchall()
                    return {"ok": True, "rows": rows_to_list(rows)}
                conn.commit()
                return {"ok": True, "rowcount": conn.total_changes}
            except Exception as e:
                conn.rollback()
                return {"error": str(e)}
            finally:
                conn.close()

        elif action == "db_exec":
            sql = req.get("sql")
            params = tuple(req.get("params") or [])
            if not sql or not sql.strip():
                return {"error": "missing sql"}
            sql_upper = sql.strip().upper()
            if sql_upper.startswith(("DROP", "ALTER", "TRUNCATE")):
                return {"error": "forbidden sql verb"}
            conn = sqlite3.connect(str(db_path), timeout=10.0)
            try:
                conn.execute(sql, params)
                conn.commit()
                return {"ok": True, "rowcount": conn.total_changes}
            except Exception as e:
                conn.rollback()
                return {"error": str(e)}
            finally:
                conn.close()

        elif action in ("db_insert", "db_update", "db_delete"):
            table = req.get("table")
            if not table:
                return {"error": "missing table"}
            conn = sqlite3.connect(str(db_path), timeout=10.0)
            try:
                check = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
                if not check:
                    return {"error": f"unknown table: {table}"}
                cur = conn.execute(f"PRAGMA table_info({table})")
                cols = [r[1] for r in cur.fetchall()]

                if action == "db_insert":
                    row = req.get("row", {})
                    keys = [k for k in row if k in cols]
                    if not keys:
                        return {"error": "no valid columns"}
                    placeholders = ", ".join("?" for _ in keys)
                    cols_sql = ", ".join(keys)
                    conn.execute(f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})", [row[k] for k in keys])
                    conn.commit()
                    rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                    return {"ok": True, "last_rowid": rid}

                elif action == "db_update":
                    row = req.get("row", {})
                    pk = req.get("pk")
                    if not pk:
                        return {"error": "missing pk for update"}
                    set_parts = []
                    params = []
                    for k, v in row.items():
                        if k in cols and k not in pk:
                            set_parts.append(f"{k}=?")
                            params.append(v)
                    params.extend(pk.values())
                    where = " AND ".join(f"{k}=?" for k in pk)
                    conn.execute(f"UPDATE {table} SET {', '.join(set_parts)} WHERE {where}", params)
                    conn.commit()
                    return {"ok": True, "rowcount": conn.total_changes}

                elif action == "db_delete":
                    pk = req.get("pk")
                    if not pk:
                        return {"error": "missing pk for delete"}
                    where = " AND ".join(f"{k}=?" for k in pk)
                    conn.execute(f"DELETE FROM {table} WHERE {where}", list(pk.values()))
                    conn.commit()
                    return {"ok": True, "rowcount": conn.total_changes}

            except Exception as e:
                conn.rollback()
                return {"error": str(e)}
            finally:
                conn.close()

        else:
            return {"error": f"unknown action: {action}"}

    except Exception as e:
        return {"error": str(e)}


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "--payload":
        try:
            raw = __import__("base64").b64decode(sys.argv[2]).decode("utf-8")
        except Exception as e:
            print(json.dumps({"error": f"invalid payload: {e}"}))
            sys.exit(1)
    else:
        raw = sys.stdin.read()

    try:
        req = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"invalid json: {e}"}))
        sys.exit(1)

    result = handle(req)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
