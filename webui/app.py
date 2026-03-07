"""
Leobot Web UI backend. Serves API for config, logs, DB (settings, service enablement, facts).
Run with auth file and paths via env (see DEPLOY.md). Bind to 127.0.0.1 and put behind reverse proxy.
"""
from __future__ import annotations

import json
import os
from functools import wraps

from flask import Flask, jsonify, request, session, send_from_directory

import webui.config as ui_config
from webui import auth as ui_auth
from webui import db as ui_db

app = Flask(__name__, static_folder="static", static_url_path="")
app.secret_key = ui_config.SECRET_KEY
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
if os.environ.get("WEBUI_HTTPS"):
    app.config["SESSION_COOKIE_SECURE"] = True


def require_auth(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not session.get("logged_in"):
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapped


@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json() or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400
    if not ui_auth.verify_password(ui_config.WEBUI_USERS, username, password):
        return jsonify({"error": "Invalid credentials"}), 401
    session["logged_in"] = True
    session["username"] = username
    return jsonify({"ok": True, "username": username})


@app.route("/api/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"ok": True})


@app.route("/api/me")
def me():
    if not session.get("logged_in"):
        return jsonify({"authenticated": False}), 401
    return jsonify({"authenticated": True, "username": session.get("username")})


# ---------- Config ----------
@app.route("/api/config")
@require_auth
def get_config():
    path = ui_config.LEOBOT_CONFIG
    if not os.path.isfile(path):
        return jsonify({"error": "Config file not found"}), 404
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/config", methods=["PUT"])
@require_auth
def put_config():
    path = ui_config.LEOBOT_CONFIG
    data = request.get_json()
    if data is None:
        return jsonify({"error": "JSON body required"}), 400
    required = ["server", "port", "nick", "user", "realname", "channels", "services"]
    for k in required:
        if k not in data:
            return jsonify({"error": f"Missing required key: {k}"}), 400
    if not isinstance(data.get("channels"), list) or not data["channels"]:
        return jsonify({"error": "channels must be a non-empty list"}), 400
    if not isinstance(data.get("services"), list):
        return jsonify({"error": "services must be a list"}), 400
    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------- Logs ----------
@app.route("/api/logs")
@require_auth
def get_logs():
    path = ui_config.LEOBOT_LOG
    tail = max(0, min(2000, int(request.args.get("tail", 500))))
    if not os.path.isfile(path):
        return jsonify({"lines": [], "path": path})
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        lines = lines[-tail:] if len(lines) > tail else lines
        return jsonify({"lines": [l.rstrip("\n\r") for l in lines], "path": path})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------- Info ----------
@app.route("/api/info")
@require_auth
def get_info():
    path = ui_config.LEOBOT_CONFIG
    info = {"config_path": path, "db_path": ui_config.LEOBOT_DB, "log_path": ui_config.LEOBOT_LOG}
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            info["channels"] = cfg.get("channels") or []
            info["services"] = cfg.get("services") or []
        except Exception:
            info["channels"] = []
            info["services"] = []
    else:
        info["channels"] = []
        info["services"] = []
    return jsonify(info)


# ---------- DB: settings ----------
@app.route("/api/settings")
@require_auth
def get_settings():
    try:
        conn = ui_db.get_conn(ui_config.LEOBOT_DB)
        try:
            rows = ui_db.list_settings(conn)
            return jsonify({"settings": [{"key": k, "value": v} for k, v in rows]})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/settings", methods=["PUT"])
@require_auth
def put_setting():
    data = request.get_json() or {}
    key = (data.get("key") or "").strip()
    value = data.get("value")
    if not key:
        return jsonify({"error": "key required"}), 400
    if value is None:
        return jsonify({"error": "value required"}), 400
    try:
        conn = ui_db.get_conn(ui_config.LEOBOT_DB)
        try:
            ui_db.set_setting(conn, key, str(value))
            return jsonify({"ok": True})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------- DB: service enablement ----------
@app.route("/api/service_enablement")
@require_auth
def get_service_enablement():
    try:
        conn = ui_db.get_conn(ui_config.LEOBOT_DB)
        try:
            rows = ui_db.list_service_enablement_all(conn)
            return jsonify({"items": rows})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/service_enablement", methods=["POST"])
@require_auth
def post_service_enablement():
    data = request.get_json() or {}
    channel = (data.get("channel") or "").strip()
    service = (data.get("service") or "").strip()
    enabled = bool(data.get("enabled", True))
    if not channel or not service:
        return jsonify({"error": "channel and service required"}), 400
    try:
        conn = ui_db.get_conn(ui_config.LEOBOT_DB)
        try:
            ui_db.set_service_enabled(conn, channel, service, enabled, updated_by=session.get("username"))
            return jsonify({"ok": True})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------- DB: facts ----------
@app.route("/api/facts/categories")
@require_auth
def get_fact_categories():
    try:
        conn = ui_db.get_conn(ui_config.LEOBOT_DB)
        try:
            cats = ui_db.fact_list_categories(conn)
            counts = dict(ui_db.fact_count_by_category(conn))
            total = ui_db.fact_count(conn)
            return jsonify({"categories": cats, "counts": counts, "total": total})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------- Static (optional: serve UI from same app; else copy to /srv/http/ui/)
@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
