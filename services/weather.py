import asyncio
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

from services.store import Store

WATCH_PATH = Path("/var/lib/leobot/weather_watch.json")

UA = "LeonidasIRCbot/1.0 (https://hairyoctopus.net; admin: Ged)"

WEATHER_CODE = {
    0: "Clear", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast", 45: "Fog", 48: "Rime fog",
    51: "Light drizzle", 53: "Drizzle", 55: "Heavy drizzle", 56: "Freezing drizzle", 57: "Heavy freezing drizzle",
    61: "Light rain", 63: "Rain", 65: "Heavy rain", 66: "Freezing rain", 67: "Heavy freezing rain",
    71: "Light snow", 73: "Snow", 75: "Heavy snow", 77: "Snow grains",
    80: "Rain showers", 81: "Showers", 82: "Violent showers", 85: "Snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm", 96: "Thunderstorm w/ hail", 99: "Thunderstorm w/ heavy hail",
}


def _http_get_json(url: str, timeout: int = 10) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))


async def _get_json(url: str, timeout: int = 10) -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _http_get_json, url, timeout)


def _norm_city(s: str) -> str:
    return " ".join((s or "").strip().split())


def _parse_timeframe(tokens: list[str]) -> tuple[str, int | None]:
    if not tokens:
        return ("now", None)

    t0 = tokens[0].lower()
    t1 = tokens[-1].lower()

    def is_ndays(t: str) -> bool:
        return t.endswith("d") and t[:-1].isdigit() and 1 <= int(t[:-1]) <= 7

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

    if is_tf(tokens[0]):
        tokens = tokens[1:]
    if tokens and is_tf(tokens[-1]):
        tokens = tokens[:-1]
    return tokens


class WeatherService:
    def __init__(self, bot, cfg: dict):
        self.bot = bot
        self.cfg = cfg
        self.cache = {}
        self.cooldown = {}

        db_path = "/var/lib/leobot/db/leobot.db"
        if isinstance(getattr(bot, "cfg", None), dict):
            db_path = bot.cfg.get("chatdb", {}).get("db_path", db_path)
        self.store = Store(db_path)

        self._init_done = False

    async def _init_once(self) -> None:
        if self._init_done:
            return
        await self.store.weather_import_from_legacy_file(WATCH_PATH)
        self._init_done = True

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
        await self._init_once()

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

        if not ev.is_private:
            if not self._cooldown_ok(ev.target, "weather", int(self.cfg.get("cooldown_seconds", 5))):
                await bot.privmsg(ev.target, f"{ev.nick}: slow down.")
                return

        if len(parts) >= 2 and parts[1].lower() == "warn":
            await self._handle_warn(bot, ev, parts, cmdline)
            return

        args = cmdline[len("weather"):].strip()
        tokens = args.split()
        _mode, _days = _parse_timeframe(tokens)
        city_tokens = _strip_timeframe(tokens)
        city = _norm_city(" ".join(city_tokens))

        if not city:
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: usage: !weather <city> | !weather <city> today|tomorrow|3d|5d | !weather today|tomorrow|3d|5d <city>",
            )
            return

        lang = (self.cfg.get("lang") or "en").lower()
        g = await self._geocode(city, lang)
        if not g or g.get("lat") is None or g.get("lon") is None:
            await bot.privmsg(ev.target, f"{ev.nick}: city not found: {city}")
            return

        data = await self._forecast(float(g["lat"]), float(g["lon"]))
        cur = data.get("current") or {}

        code = cur.get("weather_code")
        cond = WEATHER_CODE.get(code, "Unknown")
        t = cur.get("temperature_2m")
        feels = cur.get("apparent_temperature")
        wind = cur.get("wind_speed_10m")
        gust = cur.get("wind_gusts_10m")

        hourly = data.get("hourly") or {}
        times = hourly.get("time") or []
        pprob = hourly.get("precipitation_probability") or []
        precip = hourly.get("precipitation") or []

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

    async def _handle_warn(self, bot, ev, parts, cmdline):
        # !weather warn types
        if len(parts) == 3 and parts[2].lower() == "types":
            await bot.privmsg(ev.target, "WEATHER WARN types: rain, snow, wind, any")
            return

        # !weather warn list
        if len(parts) >= 3 and parts[2].lower() == "list":
            watches = await self.store.weather_list_watches()
            if not watches:
                await bot.privmsg(ev.target, "WEATHER WARN: no watches set.")
                return
            show = []
            for w in watches[:15]:
                show.append(f"{w.get('city')} ({w.get('duration_hours')}h {','.join(w.get('types',[]))})")
            await bot.privmsg(
                ev.target,
                "WEATHER WARN: " + " | ".join(show) + ("" if len(watches) <= 15 else f" (+{len(watches)-15} more)"),
            )
            return

        # !weather warn del <city>
        if len(parts) >= 3 and parts[2].lower() == "del":
            city = _norm_city(cmdline.split("del", 1)[1])
            if not city:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !weather warn del <city>")
                return
            ok = await self.store.weather_del_watch(city)
            if not ok:
                await bot.privmsg(ev.target, f"WEATHER WARN: {city} was not in watchlist.")
            else:
                await bot.privmsg(ev.target, f"WEATHER WARN: removed {city}.")
            return

        # !weather warn <duration> <city...> [type]
        if len(parts) < 4:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !weather warn <24h> <city> [rain|snow|wind|any]")
            return

        dur_raw = parts[2].lower()
        if not dur_raw.endswith("h") or not dur_raw[:-1].isdigit():
            await bot.privmsg(ev.target, f"{ev.nick}: duration must look like 24h, 6h, 48h")
            return

        duration_hours = int(dur_raw[:-1])
        if duration_hours < 1 or duration_hours > 168:
            await bot.privmsg(ev.target, f"{ev.nick}: duration range is 1h..168h")
            return

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

        interval = int(self.cfg.get("warn_check_minutes", 15))
        await self.store.weather_upsert_watch(city=city, duration_hours=duration_hours, types=types, interval_minutes=interval)
        await bot.privmsg(ev.target, f"WEATHER WARN: watching {city} for {duration_hours}h ({','.join(types)}).")


def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command(
            "weather",
            min_role="user",
            mutating=False,
            help="Weather lookup. Usage: !weather <city> [today|tomorrow|3d|5d] (timeframe can also be first)",
            category="Weather",
        )
        bot.register_command(
            "weather warn",
            min_role="user",
            mutating=False,
            help="Manage weather watches. Usage: !weather warn <24h> <city> [rain|snow|wind|any] | !weather warn list | !weather warn del <city>",
            category="Weather",
        )
        bot.register_command("weather warn types", min_role="user", mutating=False, help="List supported watch types. Usage: !weather warn types", category="Weather")
        bot.register_command("weather warn list", min_role="user", mutating=False, help="List watches. Usage: !weather warn list", category="Weather")
        bot.register_command("weather warn del", min_role="user", mutating=False, help="Delete a watch. Usage: !weather warn del <city>", category="Weather")

    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register("weather", min_role="user", mutating=False, help="Weather lookup. Usage: !weather <city> [today|tomorrow|3d|5d]", category="Weather")
        bot.acl.register("weather warn", min_role="user", mutating=False, help="Manage weather watches. Usage: !weather warn ...", category="Weather")
        bot.acl.register("weather warn types", min_role="user", mutating=False, help="List supported watch types.", category="Weather")
        bot.acl.register("weather warn list", min_role="user", mutating=False, help="List watches.", category="Weather")
        bot.acl.register("weather warn del", min_role="user", mutating=False, help="Delete a watch.", category="Weather")

    return WeatherService(bot, bot.cfg.get("weather", {}) if isinstance(bot.cfg, dict) else {})
