import asyncio
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

WATCH_PATH = Path("/var/lib/leobot/weather_watch.json")

UA = "LeonidasIRCbot/1.0 (https://hairyoctopus.net; admin: Ged)"

# WMO-ish codes used by Open-Meteo
WEATHER_CODE = {
    0: "Clear",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Rime fog",
    51: "Light drizzle",
    53: "Drizzle",
    55: "Heavy drizzle",
    56: "Freezing drizzle",
    57: "Heavy freezing drizzle",
    61: "Light rain",
    63: "Rain",
    65: "Heavy rain",
    66: "Freezing rain",
    67: "Heavy freezing rain",
    71: "Light snow",
    73: "Snow",
    75: "Heavy snow",
    77: "Snow grains",
    80: "Rain showers",
    81: "Showers",
    82: "Violent showers",
    85: "Snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm w/ hail",
    99: "Thunderstorm w/ heavy hail",
}

def _http_get_json(url: str, timeout: int = 10) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))

async def _get_json(url: str, timeout: int = 10) -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _http_get_json, url, timeout)

def _load_watch() -> dict:
    if not WATCH_PATH.exists():
        return {"lang": "en", "watches": []}
    try:
        return json.loads(WATCH_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"lang": "en", "watches": []}

def _save_watch(d: dict) -> None:
    WATCH_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = WATCH_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(d, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(WATCH_PATH)

def _norm_city(s: str) -> str:
    return " ".join((s or "").strip().split())

def _parse_timeframe(tokens: list[str]) -> tuple[str, int | None]:
    """
    Returns (mode, days)
    mode: "now" | "today" | "tomorrow" | "ndays"
    days: only used if mode == "ndays"
    """
    if not tokens:
        return ("now", None)

    t0 = tokens[0].lower()
    t1 = tokens[-1].lower()

    def is_ndays(t: str) -> bool:
        return t.endswith("d") and t[:-1].isdigit() and 1 <= int(t[:-1]) <= 7

    # token can be first or last
    for t in (t0, t1):
        if t in ("today", "tomorrow"):
            return (t, None)
        if is_ndays(t):
            return ("ndays", int(t[:-1]))

    return ("now", None)

def _strip_timeframe(tokens: list[str]) -> list[str]:
    if not tokens:
        return tokens

    def is_tf(t: str) -> bool:
        tl = t.lower()
        return tl in ("today", "tomorrow") or (tl.endswith("d") and tl[:-1].isdigit())

    # Remove timeframe token if it is first or last
    if is_tf(tokens[0]):
        tokens = tokens[1:]
    if tokens and is_tf(tokens[-1]):
        tokens = tokens[:-1]
    return tokens

class WeatherService:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.cache = {}   # key -> (exp, val)
        self.cooldown = {}  # (target, cmd) -> until

    def _cooldown_ok(self, target: str, cmd: str, seconds: int) -> bool:
        now = time.time()
        k = (target, cmd)
        until = self.cooldown.get(k, 0)
        if now < until:
            return False
        self.cooldown[k] = now + seconds
        return True

    def _cache_get(self, key):
        item = self.cache.get(key)
        if not item:
            return None
        exp, val = item
        if time.time() >= exp:
            self.cache.pop(key, None)
            return None
        return val

    def _cache_set(self, key, val, ttl: int):
        self.cache[key] = (time.time() + ttl, val)

    async def _geocode(self, name: str, lang: str) -> dict | None:
        ttl = int(self.cfg.get("cache_ttl_seconds", 1800))
        key = ("geo", lang, name.lower())
        cached = self._cache_get(key)
        if cached:
            return cached

        q = urllib.parse.quote(name)
        url = f"https://geocoding-api.open-meteo.com/v1/search?name={q}&count=1&language={lang}&format=json"
        data = await _get_json(url, timeout=10)
        res = (data.get("results") or [])
        if not res:
            return None
        g = res[0]
        out = {
            "name": g.get("name") or name,
            "country": g.get("country_code") or g.get("country") or "",
            "admin1": g.get("admin1") or "",
            "lat": g.get("latitude"),
            "lon": g.get("longitude"),
        }
        self._cache_set(key, out, ttl)
        return out

    async def _forecast(self, lat: float, lon: float) -> dict:
        ttl = int(self.cfg.get("cache_ttl_seconds", 1800))
        key = ("fc", round(lat, 4), round(lon, 4))
        cached = self._cache_get(key)
        if cached:
            return cached

        params = {
            "latitude": str(lat),
            "longitude": str(lon),
            "timezone": "auto",
            "current": "temperature_2m,apparent_temperature,weather_code,wind_speed_10m,wind_gusts_10m,precipitation,rain,showers,snowfall",
            "hourly": "temperature_2m,apparent_temperature,precipitation,precipitation_probability,rain,showers,snowfall,weather_code,wind_speed_10m,wind_gusts_10m",
            "forecast_days": "2",
        }
        qs = "&".join([f"{k}={urllib.parse.quote(v)}" for k, v in params.items()])
        url = f"https://api.open-meteo.com/v1/forecast?{qs}"
        data = await _get_json(url, timeout=12)
        self._cache_set(key, data, ttl)
        return data

    async def on_privmsg(self, bot, ev) -> None:
        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()
        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return

        parts = cmdline.split()
        cmd = parts[0].lower()

        if cmd != "weather":
            return

        # mild channel flood control
        if not ev.is_private:
            if not self._cooldown_ok(ev.target, "weather", int(self.cfg.get("cooldown_seconds", 5))):
                await bot.privmsg(ev.target, f"{ev.nick}: slow down.")
                return

        # subcommands: warn / warn list / warn del / warn types
        if len(parts) >= 2 and parts[1].lower() == "warn":
            await self._handle_warn(bot, ev, parts, cmdline)
            return

        # otherwise: !weather [timeframe] <city>  OR  !weather <city> [timeframe]
        args = cmdline[len("weather"):].strip()
        tokens = args.split()
        mode, days = _parse_timeframe(tokens)
        city_tokens = _strip_timeframe(tokens)
        city = _norm_city(" ".join(city_tokens))

        if not city:
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: usage: !weather <city> | !weather <city> today|tomorrow|3d|5d | !weather today|tomorrow|3d|5d <city>"
            )
            return

        lang = (self.cfg.get("lang") or "en").lower()
        g = await self._geocode(city, lang)
        if not g or g.get("lat") is None or g.get("lon") is None:
            await bot.privmsg(ev.target, f"{ev.nick}: city not found: {city}")
            return

        data = await self._forecast(float(g["lat"]), float(g["lon"]))
        cur = data.get("current") or {}
        tz = data.get("timezone") or "local"

        code = cur.get("weather_code")
        cond = WEATHER_CODE.get(code, "Unknown")
        t = cur.get("temperature_2m")
        feels = cur.get("apparent_temperature")
        wind = cur.get("wind_speed_10m")
        gust = cur.get("wind_gusts_10m")

        # next 12h rain headline
        hourly = data.get("hourly") or {}
        times = hourly.get("time") or []
        pprob = hourly.get("precipitation_probability") or []
        precip = hourly.get("precipitation") or []
        now_idx = 0
        # Find the first future slot with meaningful precip prob
        rain_msg = ""
        for i in range(min(12, len(times))):
            prob = pprob[i] if i < len(pprob) else None
            mm = precip[i] if i < len(precip) else None
            if prob is not None and mm is not None and prob >= 60 and mm >= 0.3:
                rain_msg = f"Rain likely within ~{i+1}h ({int(prob)}%)."
                break

        place = g["name"]
        if g.get("admin1"):
            place += f", {g['admin1']}"
        if g.get("country"):
            place += f" ({g['country']})"

        bits = [f"WEATHER: {place} — {cond}."]
        if t is not None and feels is not None:
            bits.append(f"{t:.0f}°C (feels {feels:.0f}°C).")
        if wind is not None and gust is not None:
            bits.append(f"Wind {wind:.0f}km/h gust {gust:.0f}km/h.")
        if rain_msg:
            bits.append(rain_msg)

        await bot.privmsg(ev.target, " ".join(bits))
        return

    async def _handle_warn(self, bot, ev, parts, cmdline):
        # !weather warn types
        if len(parts) == 3 and parts[2].lower() == "types":
            await bot.privmsg(ev.target, "WEATHER WARN types: rain, snow, wind, any")
            return

        wl = _load_watch()
        wl.setdefault("lang", (self.cfg.get("lang") or "en"))
        wl.setdefault("watches", [])

        # !weather warn list
        if len(parts) >= 3 and parts[2].lower() == "list":
            watches = wl.get("watches") or []
            if not watches:
                await bot.privmsg(ev.target, "WEATHER WARN: no watches set.")
                return
            show = []
            for w in watches[:15]:
                show.append(f"{w.get('city')} ({w.get('duration_hours')}h {','.join(w.get('types',[]))})")
            await bot.privmsg(ev.target, "WEATHER WARN: " + " | ".join(show) + ("" if len(watches) <= 15 else f" (+{len(watches)-15} more)"))
            return

        # !weather warn del <city>
        if len(parts) >= 3 and parts[2].lower() == "del":
            city = _norm_city(cmdline.split("del", 1)[1])
            if not city:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !weather warn del <city>")
                return

            watches = wl.get("watches") or []
            before = len(watches)
            wl["watches"] = [w for w in watches if (w.get("city") or "").lower() != city.lower()]

            try:
                _save_watch(wl)
            except Exception as e:
                await bot.privmsg(
                    ev.target,
                    f"{ev.nick}: failed to save weather watchlist ({type(e).__name__}). "
                    f"Likely service sandboxing; allow writes to /var/lib/leobot."
                )
                return

            if len(wl["watches"]) == before:
                await bot.privmsg(ev.target, f"WEATHER WARN: {city} was not in watchlist.")
            else:
                await bot.privmsg(ev.target, f"WEATHER WARN: removed {city}.")
            return

        # !weather warn <duration> <city> [type]
        # duration forms: 24h, 6h, 48h
        if len(parts) < 4:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !weather warn <duration> <city> [rain|snow|wind|any]")
            return

        dur_raw = parts[2].lower()
        if not dur_raw.endswith("h") or not dur_raw[:-1].isdigit():
            await bot.privmsg(ev.target, f"{ev.nick}: duration must look like 24h, 6h, 48h")
            return
        duration_hours = int(dur_raw[:-1])
        if duration_hours < 1 or duration_hours > 168:
            await bot.privmsg(ev.target, f"{ev.nick}: duration range is 1h..168h")
            return

        # city may be multi-word; type optional last token if matches known types
        known = {"rain", "snow", "wind", "any"}
        typ = None
        if parts[-1].lower() in known:
            typ = parts[-1].lower()
            city = _norm_city(" ".join(parts[3:-1]))
        else:
            city = _norm_city(" ".join(parts[3:]))

        if not city:
            await bot.privmsg(ev.target, f"{ev.nick}: city required")
            return

        types = (self.cfg.get("warn_default_types") or ["rain"])
        if typ:
            types = ["rain", "snow", "wind"] if typ == "any" else [typ]

        # upsert
        watches = wl.get("watches") or []
        watches = [w for w in watches if (w.get("city") or "").lower() != city.lower()]
        watches.append({
            "city": city,
            "duration_hours": duration_hours,
            "types": types,
            "interval_minutes": int(self.cfg.get("warn_check_minutes", 15)),
            "created_epoch": int(time.time()),
            "expires_epoch": int(time.time()) + (duration_hours * 3600),
        })
        wl["watches"] = sorted(watches, key=lambda x: (x.get("city") or "").lower())
        _save_watch(wl)

        await bot.privmsg(ev.target, f"WEATHER WARN: watching {city} for {duration_hours}h ({','.join(types)}).")
        return

def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command("weather", min_role="user", mutating=False, help="Weather lookup. Usage: !weather <city> [today|tomorrow|Nh|Nd]", category="Weather")
        bot.register_command("weather warn", min_role="user", mutating=False, help="Set/list/delete watches. Usage: !weather warn <window> <city> [rain|snow]", category="Weather")
        bot.register_command("weather warn list", min_role="user", mutating=False, help="List watches.", category="Weather")
        bot.register_command("weather warn del", min_role="user", mutating=False, help="Delete a watch. Usage: !weather warn del <city>", category="Weather")
    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register("weather", min_role="user", mutating=False, help="Weather lookup. Usage: !weather <city> [today|tomorrow|Nh|Nd]", category="Weather")
        bot.acl.register("weather warn", min_role="user", mutating=False, help="Set/list/delete watches. Usage: !weather warn <window> <city> [rain|snow]", category="Weather")
        bot.acl.register("weather warn list", min_role="user", mutating=False, help="List watches.", category="Weather")
        bot.acl.register("weather warn del", min_role="user", mutating=False, help="Delete a watch. Usage: !weather warn del <city>", category="Weather")

    return WeatherService(bot.cfg.get('weather', {}) if isinstance(bot.cfg, dict) else {})
