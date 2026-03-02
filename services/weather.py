import asyncio
import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Iterable

UA = "LeonidasIRCbot/2.0 (https://hairyoctopus.net; admin: Ged)"

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

VALID_TYPES = {"rain", "snow", "wind", "storm", "heat", "frost"}


def _http_get_json(url: str, timeout: int = 12) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))


async def _get_json(url: str, timeout: int = 12) -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _http_get_json, url, timeout)


def _now_ts() -> int:
    return int(time.time())


def _norm_space(s: str) -> str:
    return " ".join((s or "").strip().split())


def _parse_types(raw: str) -> list[str] | None:
    if not raw:
        return None
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    if not parts:
        return None
    # allow "any" as a convenience
    if len(parts) == 1 and parts[0] == "any":
        return sorted(VALID_TYPES)
    for t in parts:
        if t not in VALID_TYPES:
            return None
    # stable order, no dupes
    out: list[str] = []
    for t in parts:
        if t not in out:
            out.append(t)
    return out


def _parse_duration(raw: str) -> int | None:
    """
    Supports: 15m, 2h, 3d (minutes/hours/days).
    Returns seconds.
    """
    r = (raw or "").strip().lower()
    if len(r) < 2:
        return None
    unit = r[-1]
    num = r[:-1]
    if not num.isdigit():
        return None
    n = int(num)
    if n <= 0:
        return None
    if unit == "m":
        if n > 24 * 60:
            return None
        return n * 60
    if unit == "h":
        if n > 7 * 24:
            return None
        return n * 3600
    if unit == "d":
        if n > 30:
            return None
        return n * 86400
    return None


def _mode_str(values: Iterable[str]) -> str:
    counts: dict[str, int] = {}
    best = ""
    best_n = 0
    for v in values:
        if not v:
            continue
        counts[v] = counts.get(v, 0) + 1
        if counts[v] > best_n:
            best = v
            best_n = counts[v]
    return best


def _clamp_int(x: Any, lo: int, hi: int) -> int | None:
    try:
        xi = int(x)
        return max(lo, min(hi, xi))
    except Exception:
        return None


@dataclass(slots=True)
class _ResolvedLocation:
    query: str
    name: str
    country: str
    country_code: str
    admin1: str
    lat: float
    lon: float


@dataclass(slots=True)
class _PendingWeatherPick:
    created_ts: float
    target: str
    nick_lc: str
    query: str
    candidates: list[_ResolvedLocation]


class WeatherService:
    """
    Commands (as agreed):
      !weather <location>
      !weather warn add <location> <type(s)> <duration>
      !weather warn list
      !weather del <id>
      !weather warn clear
      !weather watch <location(s)> <channel>
    """

    def __init__(self, cfg: dict, *, service_name: str = "weather"):
        self.cfg = cfg or {}
        self.service_name = service_name

        self._cooldown: dict[tuple[str, str], float] = {}
        self._mem_cache: dict[tuple, tuple[float, Any]] = {}

        # Interactive disambiguation for !weather (similar to the news picker)
        self._pending_pick: dict[tuple[str, str], _PendingWeatherPick] = {}
        self.pick_timeout_s = int(self.cfg.get("selection_timeout_seconds", 45))
        self.geocode_max_results = int(self.cfg.get("geocode_max_results", 8))

        # polling behaviour
        self.poll_tick_s = float(self.cfg.get("poll_tick_seconds", 60))
        self.prune_tick_s = float(self.cfg.get("prune_tick_seconds", 600))

        # defaults for watches
        self.default_interval_s = int(self.cfg.get("watch_interval_seconds", 900))
        self.alert_min_gap_s = int(self.cfg.get("alert_min_gap_seconds", 1800))  # 30m

        # API caching (forecast/geocode)
        self.cache_ttl_s = int(self.cfg.get("cache_ttl_seconds", 1800))

        # thresholds (tune later if needed)
        self.th_rain_prob = int(self.cfg.get("th_rain_prob", 60))
        self.th_wind_gust_kmh = int(self.cfg.get("th_wind_gust_kmh", 60))
        self.th_storm_gust_kmh = int(self.cfg.get("th_storm_gust_kmh", 70))
        self.th_heat_c = float(self.cfg.get("th_heat_c", 28))
        self.th_frost_c = float(self.cfg.get("th_frost_c", 0))

    # ----------------------------
    # Cache helpers
    # ----------------------------
    def _cache_get(self, key: tuple) -> Any | None:
        it = self._mem_cache.get(key)
        if not it:
            return None
        exp, val = it
        if time.time() >= exp:
            self._mem_cache.pop(key, None)
            return None
        return val

    def _cache_set(self, key: tuple, val: Any, ttl_s: int) -> None:
        self._mem_cache[key] = (time.time() + ttl_s, val)

    def _cooldown_ok(self, target: str, cmd: str, seconds: int) -> bool:
        now = time.time()
        k = (target, cmd)
        until = self._cooldown.get(k, 0)
        if now < until:
            return False
        self._cooldown[k] = now + seconds
        return True

    # ----------------------------
    # Open-Meteo API
    # ----------------------------
    def _split_location_query(self, raw: str) -> tuple[str, str | None]:
        """Split 'City, Region' into (City, Region). Only first comma is treated as separator."""
        s = _norm_space(raw)
        if "," not in s:
            return (s, None)
        a, b = s.split(",", 1)
        a = _norm_space(a)
        b = _norm_space(b)
        return (a, b or None)

    async def _geocode_candidates(self, query: str, lang: str = "en", *, count: int | None = None) -> list[_ResolvedLocation]:
        q = _norm_space(query)
        if not q:
            return []

        if count is None:
            count = self.geocode_max_results
        count = max(1, min(20, int(count)))

        key = ("geo_multi", lang.lower(), q.lower(), count)
        cached = self._cache_get(key)
        if cached:
            return cached

        url = (
            "https://geocoding-api.open-meteo.com/v1/search?"
            + urllib.parse.urlencode(
                {
                    "name": q,
                    "count": str(count),
                    "language": lang.lower(),
                    "format": "json",
                }
            )
        )
        data = await _get_json(url, timeout=12)
        res = (data.get("results") or [])
        out: list[_ResolvedLocation] = []
        for g in res:
            try:
                name = str(g.get("name") or q)
                country = str(g.get("country") or "")
                cc = str(g.get("country_code") or "")
                admin1 = str(g.get("admin1") or "")

                # Display name consistent with your existing output
                disp = name
                if admin1:
                    disp += f", {admin1}"
                if country and cc:
                    disp += f", {country} ({cc})"
                elif cc:
                    disp += f" ({cc})"
                elif country:
                    disp += f" ({country})"

                out.append(
                    _ResolvedLocation(
                        query=q,
                        name=disp,
                        country=country,
                        country_code=cc,
                        admin1=admin1,
                        lat=float(g.get("latitude")),
                        lon=float(g.get("longitude")),
                    )
                )
            except Exception:
                continue

        self._cache_set(key, out, self.cache_ttl_s)
        return out

    async def _geocode(self, bot, query: str, lang: str = "en") -> _ResolvedLocation | None:
        q = _norm_space(query)
        if not q:
            return None

        city, region = self._split_location_query(q)

        # 1) DB cache if available (preferred) — only for non-region queries
        try:
            if region is None and getattr(bot, "store", None) is not None and hasattr(bot.store, "weather_location_get"):
                row = await bot.store.weather_location_get(q)
                if row:
                    return _ResolvedLocation(
                        query=q,
                        name=str(row["name"]),
                        country=str(row["country"] or ""),
                        country_code=str(row["country_code"] or ""),
                        admin1="",
                        lat=float(row["lat"]),
                        lon=float(row["lon"]),
                    )
        except Exception:
            # Don't hard-fail on cache issues.
            pass

        # 2) mem cache (safe for full query string)
        key = ("geo", lang.lower(), q.lower())
        cached = self._cache_get(key)
        if cached:
            return cached

        # 3) fetch candidates
        candidates = await self._geocode_candidates(city if region else q, lang=lang, count=(self.geocode_max_results if region else 1))
        if not candidates:
            return None

        out = candidates[0]
        if region:
            rlc = region.lower()
            filtered = [c for c in candidates if (c.admin1 or "").lower() == rlc or rlc in (c.admin1 or "").lower() or rlc in (c.name or "").lower()]
            if filtered:
                out = filtered[0]

            # Ensure the cached object has the *full* query string
            out = _ResolvedLocation(
                query=q,
                name=out.name,
                country=out.country,
                country_code=out.country_code,
                admin1=out.admin1,
                lat=out.lat,
                lon=out.lon,
            )

        self._cache_set(key, out, self.cache_ttl_s)

        # 4) persist to DB cache if available (non-region queries only)
        try:
            if region is None and getattr(bot, "store", None) is not None and hasattr(bot.store, "weather_location_upsert"):
                await bot.store.weather_location_upsert(
                    query=q,
                    name=out.name,
                    country=out.country or None,
                    country_code=out.country_code or None,
                    lat=out.lat,
                    lon=out.lon,
                )
        except Exception:
            pass

        return out

    async def _forecast(self, lat: float, lon: float) -> dict:
        key = ("fc", round(lat, 4), round(lon, 4))
        cached = self._cache_get(key)
        if cached:
            return cached

        params = {
            "latitude": str(lat),
            "longitude": str(lon),
            "timezone": "auto",
            "forecast_days": "2",
            "current": ",".join(
                [
                    "temperature_2m",
                    "apparent_temperature",
                    "relative_humidity_2m",
                    "cloud_cover",
                    "weather_code",
                    "wind_speed_10m",
                    "wind_gusts_10m",
                ]
            ),
            "hourly": ",".join(
                [
                    "temperature_2m",
                    "precipitation_probability",
                    "weather_code",
                    "wind_speed_10m",
                    "wind_gusts_10m",
                ]
            ),
        }
        url = "https://api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode(params)
        data = await _get_json(url, timeout=14)
        self._cache_set(key, data, self.cache_ttl_s)
        return data

    # ----------------------------
    # Formatting
    # ----------------------------
    def _format_two_line_weather(self, nick: str, loc_name: str, data: dict) -> tuple[str, str]:
        cur = data.get("current") or {}
        hourly = data.get("hourly") or {}

        # Align hourly window to "now"
        times = hourly.get("time") or []
        cur_time = str(cur.get("time") or "")

        def _hour_index() -> int:
            if not times or not cur_time:
                return 0
            try:
                return times.index(cur_time)
            except ValueError:
                # Fall forward to first hour >= current time (ISO strings are comparable here)
                for i, t in enumerate(times):
                    if str(t) >= cur_time:
                        return i
                return 0

        idx0 = _hour_index()

        code = cur.get("weather_code")
        cond = WEATHER_CODE.get(code, "Unknown")

        t = cur.get("temperature_2m")
        feels = cur.get("apparent_temperature")
        hum = cur.get("relative_humidity_2m")
        cloud = cur.get("cloud_cover")
        wind = cur.get("wind_speed_10m")
        gust = cur.get("wind_gusts_10m")

        pprob = hourly.get("precipitation_probability") or []
        p0 = pprob[idx0] if idx0 < len(pprob) else (pprob[0] if pprob else None)

        # ---- line 1 (current) ----
        bits1 = [f"Hello {nick}, the weather in {loc_name} is currently {cond}"]
        if t is not None:
            bits1.append(f"at {float(t):.0f}°C")
        if feels is not None:
            bits1.append(f"(feels {float(feels):.0f}°C)")
        wbit = []
        if wind is not None:
            wbit.append(f"winds {float(wind):.0f}km/h")
        if gust is not None:
            wbit.append(f"gusting {float(gust):.0f}km/h")
        if wbit:
            bits1.append("with " + " ".join(wbit))
        meta = []
        if hum is not None:
            meta.append(f"humidity {int(float(hum))}%")
        if cloud is not None:
            meta.append(f"cloud cover {int(float(cloud))}%")
        if p0 is not None:
            meta.append(f"precipitation probability {int(float(p0))}%")
        if meta:
            bits1.append(f"({', '.join(meta)})")
        line1 = " ".join(bits1) + "."

        # ---- line 2 (next 12 hours from now) ----
        temps = hourly.get("temperature_2m") or []
        codes = hourly.get("weather_code") or []
        gusts = hourly.get("wind_gusts_10m") or []
        winds = hourly.get("wind_speed_10m") or []

        temps12 = temps[idx0: idx0 + 12]
        codes12 = codes[idx0: idx0 + 12] if codes else []
        gusts12 = gusts[idx0: idx0 + 12] if gusts else []
        winds12 = winds[idx0: idx0 + 12] if winds else []
        probs12_src = pprob[idx0: idx0 + 12] if pprob else []

        n = len(temps12)
        if n <= 0:
            return (line1, "Next 12 hours: forecast unavailable.")

        t_start = float(temps12[0])
        t_end = float(temps12[n - 1])
        trend = "increase" if t_end > t_start + 0.25 else "decrease" if t_end < t_start - 0.25 else "stay around"

        conds12 = [WEATHER_CODE.get(c, "") for c in codes12[:n]]
        leaning = _mode_str(conds12) or "Unknown"

        probs12 = [int(float(x)) for x in probs12_src[:n] if x is not None]
        prob_peak = max(probs12) if probs12 else None
        prob_end = probs12[-1] if probs12 else None

        gust_peak = None
        wind_peak = None
        if gusts12:
            try:
                gust_peak = int(max(float(g) for g in gusts12[:n] if g is not None))
            except Exception:
                gust_peak = None
        if winds12:
            try:
                wind_peak = int(max(float(w) for w in winds12[:n] if w is not None))
            except Exception:
                wind_peak = None

        line2_parts = [
            "Next 12 hours:",
            f"temps {trend} from {t_start:.0f}°C to {t_end:.0f}°C",
            f"with conditions leaning {leaning}",
        ]
        if prob_peak is not None and prob_end is not None:
            if prob_end < prob_peak:
                line2_parts.append(f"and precipitation probability should decrease to around {prob_end}% (peak {prob_peak}%)")
            else:
                line2_parts.append(f"and precipitation probability peaking near {prob_peak}%")
        elif prob_peak is not None:
            line2_parts.append(f"and precipitation probability peaking near {prob_peak}%")

        if wind_peak is not None and gust_peak is not None:
            line2_parts.append(f"(winds peaking near {wind_peak}km/h gusting {gust_peak}km/h)")
        elif gust_peak is not None:
            line2_parts.append(f"(gusts peaking near {gust_peak}km/h)")

        line2 = " ".join(line2_parts) + "."
        return (line1, line2)

    # ----------------------------
    # Alert evaluation
    # ----------------------------
    def _evaluate_watch(self, watch_row, data: dict) -> tuple[bool, str, str] | None:
        """
        Returns (triggered, message, fingerprint) or None if no trigger.
        """
        hourly = data.get("hourly") or {}
        temps = hourly.get("temperature_2m") or []
        probs = hourly.get("precipitation_probability") or []
        gusts = hourly.get("wind_gusts_10m") or []
        codes = hourly.get("weather_code") or []

        n6 = min(6, len(probs), len(gusts), len(codes), len(temps))
        n12 = min(12, len(probs), len(gusts), len(codes), len(temps))
        if n12 <= 0:
            return None

        loc = str(watch_row["location_name"])
        types = [t for t in str(watch_row["types"]).split(",") if t]

        # helper to pick peak with index
        def peak_with_idx(vals: list, n: int) -> tuple[float | None, int | None]:
            best_v = None
            best_i = None
            for i in range(n):
                try:
                    v = float(vals[i])
                except Exception:
                    continue
                if best_v is None or v > best_v:
                    best_v = v
                    best_i = i
            return best_v, best_i

        if "storm" in types:
            gpk, gi = peak_with_idx(gusts, n6)
            ppk, pi = peak_with_idx(probs, n6)
            if gpk is not None and ppk is not None:
                if int(gpk) >= self.th_storm_gust_kmh and int(ppk) >= self.th_rain_prob:
                    idx = gi if gi is not None else (pi if pi is not None else 0)
                    fp = f"storm:{idx}:{int(gpk)}:{int(ppk)}"
                    msg = f"WEATHER ALERT: Stormy conditions likely in {loc} within ~{idx+1}h (gusts ~{int(gpk)}km/h, precip prob ~{int(ppk)}%)."
                    return (True, msg, fp)

        if "wind" in types:
            gpk, gi = peak_with_idx(gusts, n6)
            if gpk is not None and int(gpk) >= self.th_wind_gust_kmh:
                fp = f"wind:{gi}:{int(gpk)}"
                msg = f"WEATHER ALERT: Strong winds expected in {loc} within ~{(gi or 0)+1}h (gusts ~{int(gpk)}km/h)."
                return (True, msg, fp)

        if "snow" in types:
            snowish = {71, 73, 75, 77, 85, 86}
            best_i = None
            best_p = 0
            for i in range(n6):
                try:
                    c = int(codes[i])
                    p = int(float(probs[i])) if probs[i] is not None else 0
                except Exception:
                    continue
                if c in snowish and p >= max(40, self.th_rain_prob - 20):
                    if p > best_p:
                        best_p = p
                        best_i = i
            if best_i is not None:
                fp = f"snow:{best_i}:{best_p}"
                msg = f"WEATHER ALERT: Snow risk in {loc} within ~{best_i+1}h (precip prob ~{best_p}%)."
                return (True, msg, fp)

        if "rain" in types:
            ppk, pi = peak_with_idx(probs, n6)
            if ppk is not None and int(ppk) >= self.th_rain_prob:
                fp = f"rain:{pi}:{int(ppk)}"
                msg = f"WEATHER ALERT: Rain likely in {loc} within ~{(pi or 0)+1}h (precip prob ~{int(ppk)}%)."
                return (True, msg, fp)

        if "heat" in types:
            if temps:
                try:
                    tpk = max(float(x) for x in temps[:n12] if x is not None)
                except Exception:
                    tpk = None
                if tpk is not None and tpk >= self.th_heat_c:
                    fp = f"heat:{int(tpk)}"
                    msg = f"WEATHER ALERT: Heat risk in {loc} over next 12h (peak ~{tpk:.0f}°C)."
                    return (True, msg, fp)

        if "frost" in types:
            best_t = None
            best_i = None
            for i in range(n12):
                try:
                    v = float(temps[i])
                except Exception:
                    continue
                if best_t is None or v < best_t:
                    best_t = v
                    best_i = i
            if best_t is not None and best_t <= self.th_frost_c:
                fp = f"frost:{best_i}:{int(round(best_t))}"
                msg = f"WEATHER ALERT: Frost risk in {loc} within ~{(best_i or 0)+1}h (temp ~{best_t:.0f}°C)."
                return (True, msg, fp)

        return None

    # ----------------------------
    # Commands
    # ----------------------------
    async def _maybe_handle_pick(self, bot, ev, text: str) -> bool:
        msg = (text or "").strip()
        if not msg.isdigit():
            return False

        key = (ev.nick.lower(), ev.target)
        pending = self._pending_pick.get(key)
        if not pending:
            return False

        if (time.time() - pending.created_ts) > self.pick_timeout_s:
            self._pending_pick.pop(key, None)
            await bot.privmsg(ev.target, f"{ev.nick}: weather selection timed out. Run !weather {pending.query} again.")
            return True

        choice = int(msg)
        if choice < 1 or choice > len(pending.candidates):
            await bot.privmsg(ev.target, f"{ev.nick}: invalid selection. Choose 1-{len(pending.candidates)}.")
            return True

        loc = pending.candidates[choice - 1]
        self._pending_pick.pop(key, None)

        data = await self._forecast(loc.lat, loc.lon)
        line1, line2 = self._format_two_line_weather(ev.nick, loc.name, data)
        await bot.privmsg(ev.target, line1)
        await bot.privmsg(ev.target, line2)
        return True

    async def _resolve_or_prompt_pick(self, bot, ev, loc_raw: str, *, lang: str) -> _ResolvedLocation | None:
        loc_raw = _norm_space(loc_raw)
        if not loc_raw:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !weather <location>")
            return None

        city, region = self._split_location_query(loc_raw)
        if region:
            # Explicit region hint: resolve directly
            loc = await self._geocode(bot, loc_raw, lang=lang)
            if not loc:
                await bot.privmsg(ev.target, f"{ev.nick}: location not found: {loc_raw}")
            return loc

        # No region hint → fetch multiple candidates and prompt if ambiguous
        candidates = await self._geocode_candidates(city, lang=lang, count=self.geocode_max_results)
        if not candidates:
            await bot.privmsg(ev.target, f"{ev.nick}: location not found: {loc_raw}")
            return None

        if len(candidates) == 1:
            return candidates[0]

        shown = candidates[:6]
        menu = " ".join([f"[{i}] {c.name}" for i, c in enumerate(shown, start=1)])
        await bot.privmsg(ev.target, f"{ev.nick}: multiple matches for '{city}'. Reply with number: {menu}")

        key = (ev.nick.lower(), ev.target)
        self._pending_pick[key] = _PendingWeatherPick(
            created_ts=time.time(),
            target=ev.target,
            nick_lc=ev.nick.lower(),
            query=city,
            candidates=shown,
        )
        return None

    async def on_privmsg(self, bot, ev) -> None:
        text = (ev.text or "").strip()

        # Interactive disambiguation (user replies "1", "2", ...)
        if await self._maybe_handle_pick(bot, ev, text):
            return

        prefix = bot.cfg.get("command_prefix", "!")
        if not text.startswith(prefix):
            return

        cmdline = text[len(prefix):].strip()
        if not cmdline:
            return

        parts = cmdline.split()
        cmd = parts[0].lower()
        if cmd != "weather":
            return

        # mild flood control
        if not ev.is_private:
            cd = int(self.cfg.get("cooldown_seconds", 5))
            if not self._cooldown_ok(ev.target, "weather", cd):
                await bot.privmsg(ev.target, f"{ev.nick}: slow down.")
                return

        # Subcommands
        if len(parts) >= 2:
            sub = parts[1].lower()

            if sub == "warn":
                await self._handle_warn(bot, ev, parts, cmdline)
                return

            if sub == "del":
                await self._handle_del(bot, ev, parts)
                return

            if sub == "watch":
                await self._handle_watch(bot, ev, cmdline)
                return

        # Plain lookup: !weather <location>
        loc_raw = _norm_space(cmdline[len("weather"):].strip())
        if not loc_raw:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !weather <location>")
            return

        lang = str(self.cfg.get("lang") or "en").lower()
        loc = await self._resolve_or_prompt_pick(bot, ev, loc_raw, lang=lang)
        if not loc:
            return

        data = await self._forecast(loc.lat, loc.lon)
        line1, line2 = self._format_two_line_weather(ev.nick, loc.name, data)
        await bot.privmsg(ev.target, line1)
        await bot.privmsg(ev.target, line2)

    async def _handle_warn(self, bot, ev, parts: list[str], cmdline: str) -> None:
        # !weather warn list
        if len(parts) >= 3 and parts[2].lower() == "list":
            if ev.is_private:
                await bot.privmsg(ev.target, f"{ev.nick}: warn list only makes sense in a channel.")
                return
            rows = await bot.store.weather_watch_list(target_channel=ev.target)
            rows = [r for r in rows if int(r["enabled"]) == 1 and int(r["expires_ts"]) > _now_ts()]
            if not rows:
                await bot.privmsg(ev.target, "WEATHER WARN: no active watches in this channel.")
                return

            now = _now_ts()
            chunks = []
            for r in rows[:10]:
                exp_in = max(0, int(r["expires_ts"]) - now)
                mins = exp_in // 60
                chunks.append(f"#{r['id']} {r['location_name']} [{r['types']}] (expires {mins}m)")
            msg = "WEATHER WARN: " + " | ".join(chunks)
            if len(rows) > 10:
                msg += f" (+{len(rows)-10} more)"
            await bot.privmsg(ev.target, msg)
            return

        # !weather warn clear
        if len(parts) >= 3 and parts[2].lower() == "clear":
            if ev.is_private:
                await bot.privmsg(ev.target, f"{ev.nick}: warn clear only makes sense in a channel.")
                return
            n = await bot.store.weather_watch_clear(target_channel=ev.target)
            await bot.privmsg(ev.target, f"WEATHER WARN: cleared {n} watch(es) in {ev.target}.")
            return

        # allow alias: !weather warn del <id>
        if len(parts) >= 3 and parts[2].lower() == "del":
            await self._handle_del(bot, ev, parts[2:])
            return

        # !weather warn add <location> <type(s)> <duration>
        # We accept:
        #   !weather warn add London rain,wind 6h
        if len(parts) < 6 or parts[2].lower() != "add":
            await bot.privmsg(
                ev.target,
                f"{ev.nick}: usage: !weather warn add <location> <type(s)> <duration> | !weather warn list | !weather warn clear",
            )
            return

        if ev.is_private:
            await bot.privmsg(ev.target, f"{ev.nick}: warn add only makes sense in a channel.")
            return

        # Parse from raw cmdline to allow spaces in location.
        # cmdline: "weather warn add <location> <types> <duration>"
        raw = cmdline.split(None, 4)
        if len(raw) < 5:
            await bot.privmsg(ev.target, f"{ev.nick}: invalid syntax.")
            return
        rest = raw[4].strip()

        # Expect last two tokens to be types and duration; location is the remainder.
        toks = rest.split()
        if len(toks) < 3:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !weather warn add <location> <type(s)> <duration>")
            return
        duration_raw = toks[-1]
        types_raw = toks[-2]
        location_raw = " ".join(toks[:-2]).strip()
        if not location_raw:
            await bot.privmsg(ev.target, f"{ev.nick}: location required.")
            return

        types = _parse_types(types_raw)
        if not types:
            await bot.privmsg(ev.target, f"{ev.nick}: invalid types. Use comma-separated: {','.join(sorted(VALID_TYPES))} (or 'any').")
            return

        duration_s = _parse_duration(duration_raw)
        if duration_s is None:
            await bot.privmsg(ev.target, f"{ev.nick}: invalid duration. Use e.g. 15m, 2h, 3d.")
            return

        expires_ts = _now_ts() + duration_s
        lang = str(self.cfg.get("lang") or "en").lower()
        loc = await self._geocode(bot, location_raw, lang=lang)
        if not loc:
            await bot.privmsg(ev.target, f"{ev.nick}: location not found: {location_raw}")
            return

        interval_s = int(self.default_interval_s)
        enabled = 1
        wid = await bot.store.weather_watch_add(
            target_channel=ev.target,
            created_by=ev.nick,
            location_query=location_raw,
            location_name=loc.name,
            lat=loc.lat,
            lon=loc.lon,
            types=",".join(types),
            interval_s=interval_s,
            enabled=enabled,
            expires_ts=expires_ts,
        )

        mins = duration_s // 60
        await bot.privmsg(ev.target, f"WEATHER WARN: added watch #{wid} for {loc.name} [{','.join(types)}] (expires {mins}m).")

    async def _handle_del(self, bot, ev, parts: list[str]) -> None:
        # !weather del <id> or !weather warn del <id>
        if len(parts) < 3:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !weather del <id>")
            return

        wid = _clamp_int(parts[2], 1, 10_000_000)
        if wid is None:
            await bot.privmsg(ev.target, f"{ev.nick}: invalid id.")
            return

        n = await bot.store.weather_watch_del(wid)
        if n <= 0:
            await bot.privmsg(ev.target, f"WEATHER WARN: #{wid} not found.")
            return
        await bot.privmsg(ev.target, f"WEATHER WARN: deleted watch #{wid}.")

    async def _handle_watch(self, bot, ev, cmdline: str) -> None:
        # !weather watch <location(s)> <channel>
        # Example:
        #   !weather watch London;Paris #General
        # Default types: any
        # Duration: default 24h
        if ev.is_private:
            await bot.privmsg(ev.target, f"{ev.nick}: watch only makes sense in a channel.")
            return

        raw = cmdline.split(None, 2)
        if len(raw) < 3:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !weather watch <location(s)> <channel>")
            return

        rest = raw[2].strip()
        toks = rest.split()
        if len(toks) < 2:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !weather watch <location(s)> <channel>")
            return

        channel = toks[-1].strip()
        locs_raw = " ".join(toks[:-1]).strip()
        if not channel.startswith("#"):
            await bot.privmsg(ev.target, f"{ev.nick}: channel must start with #.")
            return

        # split multiple locations by semicolon
        loc_list = [s.strip() for s in locs_raw.split(";") if s.strip()]
        if not loc_list:
            await bot.privmsg(ev.target, f"{ev.nick}: at least one location required.")
            return

        # defaults
        types = sorted(VALID_TYPES)
        duration_s = 24 * 3600
        expires_ts = _now_ts() + duration_s
        interval_s = int(self.default_interval_s)
        enabled = 1

        lang = str(self.cfg.get("lang") or "en").lower()
        added = 0
        for loc_q in loc_list[:10]:
            loc = await self._geocode(bot, loc_q, lang=lang)
            if not loc:
                await bot.privmsg(ev.target, f"WEATHER WARN: location not found: {loc_q}")
                continue
            await bot.store.weather_watch_add(
                target_channel=channel,
                created_by=ev.nick,
                location_query=loc_q,
                location_name=loc.name,
                lat=loc.lat,
                lon=loc.lon,
                types=",".join(types),
                interval_s=interval_s,
                enabled=enabled,
                expires_ts=expires_ts,
            )
            added += 1

        await bot.privmsg(ev.target, f"WEATHER WARN: added {added} watch(es) in {channel} (expires 1440m).")

    # ----------------------------
    # Scheduler integration
    # ----------------------------
    async def start(self, bot) -> None:
        # Called by bot when service starts
        self._bot = bot
        self._task_poll = asyncio.create_task(self._poll_loop(bot))
        self._task_prune = asyncio.create_task(self._prune_loop(bot))

    async def stop(self, bot) -> None:
        for t in (getattr(self, "_task_poll", None), getattr(self, "_task_prune", None)):
            if t:
                t.cancel()

    async def _poll_loop(self, bot) -> None:
        while True:
            try:
                await asyncio.sleep(self.poll_tick_s)
                await self._poll_once(bot)
            except asyncio.CancelledError:
                return
            except Exception:
                # don't crash service loop
                continue

    async def _prune_loop(self, bot) -> None:
        while True:
            try:
                await asyncio.sleep(self.prune_tick_s)
                await self._prune_once(bot)
            except asyncio.CancelledError:
                return
            except Exception:
                continue

    async def _prune_once(self, bot) -> None:
        # disable/expire old watches
        now = _now_ts()
        try:
            await bot.store.weather_watch_disable_expired(now)
        except Exception:
            pass

        # prune dedupe table (if you keep one)
        try:
            if hasattr(bot.store, "weather_alert_prune"):
                await bot.store.weather_alert_prune(now - (7 * 86400))
        except Exception:
            pass

    async def _poll_once(self, bot) -> None:
        # poll enabled watches and emit alerts
        rows = await bot.store.weather_watch_due(_now_ts())
        if not rows:
            return

        now = _now_ts()
        for r in rows:
            try:
                # skip expired/disabled defensively
                if int(r["enabled"]) != 1:
                    continue
                if int(r["expires_ts"]) <= now:
                    continue

                lat = float(r["lat"])
                lon = float(r["lon"])

                data = await self._forecast(lat, lon)
                hit = self._evaluate_watch(r, data)
                if not hit:
                    await bot.store.weather_watch_touch(int(r["id"]), now)
                    continue

                triggered, msg, fp = hit
                if not triggered:
                    await bot.store.weather_watch_touch(int(r["id"]), now)
                    continue

                # dedupe based on alert_min_gap_s and fingerprint if DB supports it
                ok = True
                try:
                    if hasattr(bot.store, "weather_alert_can_emit"):
                        ok = await bot.store.weather_alert_can_emit(
                            watch_id=int(r["id"]),
                            fingerprint=fp,
                            now_ts=now,
                            min_gap_s=int(self.alert_min_gap_s),
                        )
                except Exception:
                    ok = True

                if ok:
                    await bot.privmsg(str(r["target_channel"]), msg)
                    try:
                        if hasattr(bot.store, "weather_alert_record"):
                            await bot.store.weather_alert_record(int(r["id"]), fp, now)
                    except Exception:
                        pass

                await bot.store.weather_watch_touch(int(r["id"]), now)
            except Exception:
                # never crash poll loop on one bad row
                try:
                    await bot.store.weather_watch_touch(int(r["id"]), _now_ts())
                except Exception:
                    pass
                continue