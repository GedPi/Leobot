import asyncio
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any

from services.store import Store

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
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        try:
            data = json.loads(body) if body else {}
        except Exception:
            data = {}
        data["_http_status"] = getattr(e, "code", None)
        data["_http_error"] = str(e)
        return data
    except Exception as e:
        return {"_http_status": None, "_http_error": str(e)}


async def _get_json(url: str, timeout: int = 10) -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _http_get_json, url, timeout)


def _norm_city(s: str) -> str:
    return " ".join((s or "").strip().split())


def _parse_timeframe(tokens: list[str]) -> tuple[str, int | None]:
    """
    Backward-compat parsing. Output currently always uses "now + next 12 hours",
    but we keep parsing so existing usage doesn't break.
    """
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


def _safe_list(x: Any) -> list:
    return x if isinstance(x, list) else []


def _safe_num(x: Any) -> float | None:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _trend_word(a: float | None, b: float | None) -> str | None:
    if a is None or b is None:
        return None
    if b > a:
        return "increase"
    if b < a:
        return "decrease"
    return "remain steady"


def _precip_kind_from_code(code: int | None) -> str | None:
    if code is None:
        return None
    if code in (45, 48):
        return "fog"
    if code in (71, 73, 75, 77, 85, 86):
        return "snow"
    if code in (51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82, 95, 96, 99):
        return "rain"
    return None


def _fmt_pct(v: float | None) -> str | None:
    if v is None:
        return None
    return f"{int(round(v))}%"


def _fmt_c(v: float | None) -> str | None:
    if v is None:
        return None
    return f"{v:.0f}°C"


def _fmt_kmh(v: float | None) -> str | None:
    if v is None:
        return None
    return f"{v:.0f}km/h"


class WeatherService:
    def __init__(self, bot, cfg: dict):
        self.bot = bot
        self.cfg = cfg
        self.cache: dict[tuple, tuple[float, dict]] = {}
        self.cooldown: dict[tuple[str, str], float] = {}

        db_path = "/var/lib/leobot/db/leobot.db"
        if isinstance(getattr(bot, "cfg", None), dict):
            db_path = bot.cfg.get("chatdb", {}).get("db_path", db_path)
        self.store = Store(db_path)

        self._init_done = False

    async def _init_once(self) -> None:
        if self._init_done:
            return
        # Keep legacy JSON watch file import (compat with older setups)
        await self.store.weather_import_from_legacy_file(str(WATCH_PATH))
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
            "current": (
                "temperature_2m,"
                "apparent_temperature,"
                "weather_code,"
                "wind_speed_10m,"
                "wind_gusts_10m,"
                "relative_humidity_2m,"
                "cloud_cover"
            ),
            "hourly": (
                "temperature_2m,"
                "apparent_temperature,"
                "weather_code,"
                "wind_speed_10m,"
                "wind_gusts_10m,"
                "relative_humidity_2m,"
                "precipitation_probability,"
                "cloud_cover"
            ),
            "forecast_days": "2",
        }
        qs = "&".join([f"{k}={urllib.parse.quote(v)}" for k, v in params.items()])
        url = f"https://api.open-meteo.com/v1/forecast?{qs}"
        data = await _get_json(url, timeout=12)

        self._cache_set(key, data, ttl)
        return data

    def _place_str(self, g: dict) -> str:
        place = g.get("name") or "Unknown"
        if g.get("admin1"):
            place += f", {g['admin1']}"
        if g.get("country"):
            place += f" ({g['country']})"
        return place

    def _extract_12h_window(self, data: dict) -> dict:
        hourly = data.get("hourly") or {}
        cur = data.get("current") or {}

        times = _safe_list(hourly.get("time"))
        cur_time = cur.get("time")

        start_idx = 0
        if cur_time and times:
            try:
                start_idx = times.index(cur_time)
            except ValueError:
                start_idx = 0

        end_idx = min(start_idx + 12, len(times))

        def sl(key: str) -> list:
            arr = _safe_list(hourly.get(key))
            return arr[start_idx:end_idx] if arr else []

        return {
            "start_idx": start_idx,
            "end_idx": end_idx,
            "times": times[start_idx:end_idx] if times else [],
            "temps": sl("temperature_2m"),
            "feels": sl("apparent_temperature"),
            "winds": sl("wind_speed_10m"),
            "gusts": sl("wind_gusts_10m"),
            "humidity": sl("relative_humidity_2m"),
            "pprob": sl("precipitation_probability"),
            "cloud": sl("cloud_cover"),
            "codes": sl("weather_code"),
        }

    def _format_weatherman_lines(self, *, nick: str, place: str, cur: dict, win: dict) -> tuple[str, str]:
        # --- current snapshot ---
        code = cur.get("weather_code")
        cond = WEATHER_CODE.get(code, "Unknown")
        t = _safe_num(cur.get("temperature_2m"))
        feels = _safe_num(cur.get("apparent_temperature"))
        wind = _safe_num(cur.get("wind_speed_10m"))
        gust = _safe_num(cur.get("wind_gusts_10m"))
        rh = _safe_num(cur.get("relative_humidity_2m"))
        cloud = _safe_num(cur.get("cloud_cover"))

        # Precip probability is hourly-only; use first hour in window if present.
        cur_pprob = None
        pprob_list = win.get("pprob") or []
        if pprob_list:
            cur_pprob = _safe_num(pprob_list[0])

        cur_parts: list[str] = []
        cur_parts.append(f"Hello {nick}, the weather in {place} is currently {cond}")

        if t is not None and feels is not None:
            cur_parts.append(f"at {_fmt_c(t)} (feels {_fmt_c(feels)})")
        elif t is not None:
            cur_parts.append(f"at {_fmt_c(t)}")

        if wind is not None and gust is not None:
            cur_parts.append(f"with winds {_fmt_kmh(wind)} gusting {_fmt_kmh(gust)}")
        elif wind is not None:
            cur_parts.append(f"with winds {_fmt_kmh(wind)}")

        extras: list[str] = []
        if rh is not None:
            extras.append(f"humidity {_fmt_pct(rh)}")
        if cloud is not None:
            extras.append(f"cloud cover {_fmt_pct(cloud)}")
        if cur_pprob is not None:
            extras.append(f"precipitation probability {_fmt_pct(cur_pprob)}")

        if extras:
            cur_line = " ".join(cur_parts) + " (" + ", ".join(extras) + ")."
        else:
            cur_line = " ".join(cur_parts) + "."

        cur_line = cur_line.replace("  ", " ").strip()

        # --- next 12h outlook ---
        temps = [_safe_num(x) for x in (win.get("temps") or [])]
        winds = [_safe_num(x) for x in (win.get("winds") or [])]
        gusts = [_safe_num(x) for x in (win.get("gusts") or [])]
        probs = [_safe_num(x) for x in (win.get("pprob") or [])]
        codes = [int(x) for x in (win.get("codes") or []) if x is not None]

        temp0 = temps[0] if temps else None
        tempN = temps[-1] if temps else None
        prob0 = probs[0] if probs else None
        probN = probs[-1] if probs else None

        temp_trend = _trend_word(temp0, tempN)
        prob_trend = _trend_word(prob0, probN)

        dominant_code = Counter(codes).most_common(1)[0][0] if codes else None
        next_cond = WEATHER_CODE.get(dominant_code, "changing conditions") if dominant_code is not None else "changing conditions"

        precip_kind = _precip_kind_from_code(dominant_code)
        precip_phrase = None
        if precip_kind == "rain":
            precip_phrase = "with rain becoming the main theme"
        elif precip_kind == "snow":
            precip_phrase = "with snow in the mix"
        elif precip_kind == "fog":
            precip_phrase = "with fog likely lingering"

        max_wind = max([v for v in winds if v is not None], default=None) if winds else None
        max_gust = max([v for v in gusts if v is not None], default=None) if gusts else None

        out: list[str] = []
        out.append("Next 12 hours:")

        if temp0 is not None and tempN is not None and temp_trend:
            out.append(f"temps {temp_trend} from {_fmt_c(temp0)} to {_fmt_c(tempN)}")
        else:
            out.append("temps should be fairly steady")

        out.append(f"with conditions leaning {next_cond}")
        if precip_phrase:
            out.append(precip_phrase)

        if prob0 is not None and probN is not None and prob_trend:
            out.append(f"and precipitation probability should {prob_trend} to around {_fmt_pct(probN)}")
        elif probN is not None:
            out.append(f"and precipitation probability sits around {_fmt_pct(probN)}")

        if max_wind is not None and max_gust is not None:
            out.append(f"(winds peaking near {_fmt_kmh(max_wind)} gusting {_fmt_kmh(max_gust)}).")
        else:
            out.append(".")

        outlook_line = " ".join(out).replace("  ", " ").replace(" .", ".").strip()

        return cur_line, outlook_line

    async def on_privmsg(self, bot, ev) -> None:
        await self._init_once()

        prefix = bot.cfg.get("command_prefix", "!")
        text = (ev.text or "").strip()
        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix) :].strip()
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

        # keep WARN subsystem intact
        if len(parts) >= 2 and parts[1].lower() == "warn":
            await self._handle_warn(bot, ev, parts, cmdline)
            return

        # otherwise: !weather [timeframe] <city>  OR  !weather <city> [timeframe]
        args = cmdline[len("weather") :].strip()
        tokens = args.split()
        _mode, _days = _parse_timeframe(tokens)  # kept for compatibility
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
        place = self._place_str(g)
        win = self._extract_12h_window(data)

        # Two separate lines to avoid ugly chunk cut-offs
        line1, line2 = self._format_weatherman_lines(nick=ev.nick, place=place, cur=cur, win=win)
        await bot.privmsg(ev.target, line1)
        await bot.privmsg(ev.target, line2)

    # -----------------------
    # WARN subsystem (KEEP)
    # -----------------------
    async def _handle_warn(self, bot, ev, parts, cmdline):
        # !weather warn types
        if len(parts) >= 3 and parts[2].lower() == "types":
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
                city = w.get("city") or "?"
                dur = w.get("duration_hours")
                types = w.get("types") or []
                show.append(f"{city} ({dur}h {','.join(types)})")

            await bot.privmsg(
                ev.target,
                "WEATHER WARN: " + " | ".join(show) + ("" if len(watches) <= 15 else f" (+{len(watches)-15} more)"),
            )
            return

        # !weather warn del <city...>
        if len(parts) >= 3 and parts[2].lower() == "del":
            tail = cmdline.split("del", 1)[1] if "del" in cmdline else ""
            city = _norm_city(tail)
            if not city:
                await bot.privmsg(ev.target, f"{ev.nick}: usage: !weather warn del <city>")
                return
            await self.store.weather_remove_watch(city)
            await bot.privmsg(ev.target, f"WEATHER WARN: removed {city}.")
            return

        # Add/update: !weather warn <24h> <city...> [type]
        if len(parts) < 4:
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: usage: !weather warn <24h> <city> [rain|snow|wind|any] | !weather warn list | !weather warn del <city>",
            )
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

        if typ is None:
            types = (self.cfg.get("warn_default_types") or ["rain"])
            if not isinstance(types, list) or not types:
                types = ["rain"]
        else:
            types = ["rain", "snow", "wind"] if typ == "any" else [typ]

        interval = int(self.cfg.get("warn_check_minutes", 15))
        created_ts = int(time.time())
        expires_ts = created_ts + duration_hours * 3600

        await self.store.weather_add_watch(
            city=city,
            duration_hours=duration_hours,
            types=types,
            interval_minutes=interval,
            created_ts=created_ts,
            expires_ts=expires_ts,
        )

        await bot.privmsg(ev.target, f"WEATHER WARN: watching {city} for {duration_hours}h ({','.join(types)}).")


def setup(bot):
    if hasattr(bot, "register_command"):
        bot.register_command(
            "weather",
            min_role="user",
            mutating=False,
            help="Weather lookup. Usage: !weather <city> | !weather <city> today|tomorrow|3d|5d | !weather today|tomorrow|3d|5d <city>",
            category="Weather",
        )
        bot.register_command(
            "weather warn",
            min_role="user",
            mutating=False,
            help="Manage weather watches. Usage: !weather warn <24h> <city> [rain|snow|wind|any] | !weather warn list | !weather warn del <city>",
            category="Weather",
        )
        bot.register_command(
            "weather warn types",
            min_role="user",
            mutating=False,
            help="List supported watch types. Usage: !weather warn types",
            category="Weather",
        )
        bot.register_command(
            "weather warn list",
            min_role="user",
            mutating=False,
            help="List watches. Usage: !weather warn list",
            category="Weather",
        )
        bot.register_command(
            "weather warn del",
            min_role="user",
            mutating=False,
            help="Delete a watch. Usage: !weather warn del <city>",
            category="Weather",
        )

    if getattr(bot, "acl", None) is not None and hasattr(bot.acl, "register"):
        bot.acl.register("weather", min_role="user", mutating=False, help="Weather lookup. Usage: !weather <city> [today|tomorrow|3d|5d]", category="Weather")
        bot.acl.register("weather warn", min_role="user", mutating=False, help="Manage weather watches. Usage: !weather warn ...", category="Weather")
        bot.acl.register("weather warn types", min_role="user", mutating=False, help="List supported watch types.", category="Weather")
        bot.acl.register("weather warn list", min_role="user", mutating=False, help="List watches.", category="Weather")
        bot.acl.register("weather warn del", min_role="user", mutating=False, help="Delete a watch. Usage: !weather warn del <city>", category="Weather")

    return WeatherService(bot, bot.cfg.get("weather", {}) if isinstance(bot.cfg, dict) else {})