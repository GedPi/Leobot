# Leobot Web UI – Deployment

The Web UI lets you manage bot config, view logs, and change DB-backed settings (service enablement, facts, key-value settings) over HTTP. It is **username/password protected**; the backend must run behind your existing web server so only authenticated users can access it.

## Overview

- **Backend**: Flask app (this `webui` package). Run it on **127.0.0.1** (e.g. port 5000). It serves the API and can also serve the static files if you prefer.
- **Static files**: The UI (HTML/CSS/JS) can be served by your existing server from **`/srv/http/ui/`**.
- **Bot paths**: By default the backend reads/writes **`/opt/leobot/`** (config, DB, log). Override with environment variables.

So that the login cookie works, the browser must see the API and the page on the **same origin**. That means either:

1. **Reverse proxy**: Your web server (Apache/Nginx) serves the UI from `/srv/http/ui/` and proxies `/api` to the Flask app on 127.0.0.1, **or**
2. **Flask serves everything**: You don’t use `/srv/http/ui/`; instead you mount the app at a path (e.g. `https://example.com/ui/`) and the Flask app serves both the API and the static files.

This guide assumes option 1: static files in `/srv/http/ui/`, API under `/api` proxied to Flask.

---

## 1. Environment variables

Set these where you run the Flask app (e.g. systemd unit or shell):

| Variable | Meaning | Example |
|----------|---------|--------|
| `LEOBOT_CONFIG` | Bot config JSON path | `/opt/leobot/config/config.json` |
| `LEOBOT_DB` | Bot SQLite DB path | `/opt/leobot/data/leonidas.db` |
| `LEOBOT_LOG` | Bot log file path | `/opt/leobot/bot.log` |
| `WEBUI_USERS` | Path to users JSON (see below) | `/opt/leobot/webui/users.json` |
| `WEBUI_SECRET_KEY` | Flask session secret (random string) | long random string |
| `WEBUI_HTTPS` | Set to `1` if site is HTTPS (secure cookies) | `1` |

Example:

```bash
export LEOBOT_CONFIG=/opt/leobot/config/config.json
export LEOBOT_DB=/opt/leobot/data/leonidas.db
export LEOBOT_LOG=/opt/leobot/bot.log
export WEBUI_USERS=/opt/leobot/webui/users.json
export WEBUI_SECRET_KEY="your-long-random-secret"
export WEBUI_HTTPS=1
```

---

## 2. Create users (passwords)

Users are stored in a JSON file: `{"users": {"username": "bcrypt_hash"}}`.

From the repo root:

```bash
python -m webui.add_user admin
# Enter password when prompted. This creates/updates webui/users.json.
```

Put the users file where `WEBUI_USERS` points (e.g. `/opt/leobot/webui/users.json`). **Do not commit `users.json`**; it’s in `.gitignore`.

---

## 3. Run the backend

Install dependencies (in the project venv or system):

```bash
pip install -r webui/requirements.txt
```

Run Flask (bind only to localhost):

```bash
export FLASK_APP=webui.app
flask run --host=127.0.0.1 --port=5000
```

Or with env vars in one go:

```bash
LEOBOT_CONFIG=/opt/leobot/config/config.json \
LEOBOT_DB=/opt/leobot/data/leonidas.db \
LEOBOT_LOG=/opt/leobot/bot.log \
WEBUI_USERS=/opt/leobot/webui/users.json \
WEBUI_SECRET_KEY="your-secret" \
WEBUI_HTTPS=1 \
flask run --host=127.0.0.1 --port=5000
```

For production, use a process manager (e.g. systemd or Gunicorn) and keep these env vars set.

---

## 4. Copy static files to `/srv/http/ui/`

So your existing web server can serve the UI:

```bash
cp -r /path/to/Leobot/webui/static/* /srv/http/ui/
```

So that the frontend calls the same origin, the page must be served from the same host/path as the API. For example if the site is `https://example.com`, then:

- Page: `https://example.com/` or `https://example.com/ui/` (from `/srv/http/ui/` or `/srv/http/ui/index.html`)
- API: `https://example.com/api/...` (proxied to Flask)

The static `app.js` uses the path `/api` for all requests. So your reverse proxy must expose the Flask app under **`/api`** (e.g. `https://example.com/api` → `http://127.0.0.1:5000/api`).

---

## 5. Reverse proxy (Apache example)

Proxy `/api` to the Flask app:

```apache
# Proxy /api to Flask on 127.0.0.1:5000
ProxyPreserveHost On
ProxyPass /api http://127.0.0.1:5000/api
ProxyPassReverse /api http://127.0.0.1:5000/api
```

If the UI is under `/ui/` and the document root is `/srv/http`:

- `https://example.com/ui/` → `/srv/http/ui/index.html`
- `https://example.com/api/login` → Flask `/api/login`

Then open `https://example.com/ui/` (or wherever your index is); the login form will POST to `/api/login` on the same origin.

---

## 6. Reverse proxy (Nginx example)

```nginx
location /api {
    proxy_pass http://127.0.0.1:5000;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

If you use HTTPS, set `WEBUI_HTTPS=1` so the session cookie is marked Secure.

---

## 7. Optional: serve UI from Flask only

If you don’t want to use `/srv/http/ui/`, you can serve the UI from Flask and mount the app under a path (e.g. with a reverse proxy):

- `https://example.com/leobot/` → Flask (static `index.html`)
- `https://example.com/leobot/api/...` → same Flask app

Then you must set the Flask app’s `APPLICATION_ROOT` or use a blueprint so that the static files and API are under `/leobot/`. The default setup in this repo assumes the frontend is at the same origin as `/api`; if you mount the app at `/leobot`, you’d need to set the API base path in the frontend (e.g. `const API = "/leobot/api"` in `app.js`) and serve static from `/leobot/`.

---

## 8. Security checklist

- Run Flask on **127.0.0.1** only; do not expose it directly to the internet.
- Use **HTTPS** in production and set **`WEBUI_SECRET_KEY`** and **`WEBUI_HTTPS=1`**.
- Keep **`users.json`** outside the document root and with restricted permissions (e.g. `chmod 600`).
- Restrict filesystem permissions on `/opt/leobot/` so only the user running the bot and the Web UI can read/write config and DB.
