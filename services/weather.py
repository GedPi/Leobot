from __future__ import annotations

import asyncio
import json
import logging
import urllib.parse
import urllib.request

log = logging.getLogger("leobot.weather")


def setup(bot):
    return WeatherService(bot)


def _fetch_json(url: str, timeout: int = 12) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "Leonidas/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", errors="ignore"))


class WeatherService:
    """Weather lookup using Open-Meteo (no API key).

    Disabled by default per channel.
      !service enable weather #Channel

    Usage:
      !weather <city>[, <country>]
    """

    def __init__(self, bot):
        self.bot = bot
        bot.register_command("weather", min_role="guest", mutating=False, help="Weather lookup. Usage: !weather <city>[, <country>]", category="Weather")

    async def on_privmsg(self, bot, ev):
        prefix = bot.cfg.get("command_prefix", "!")
        txt = (ev.text or "").strip()
        if not txt.startswith(prefix):
            return
        cmdline = txt[len(prefix):].strip()
        if not cmdline:
            return
        parts = cmdline.split(maxsplit=1)
        if parts[0].lower() != "weather":
            return

        if ev.channel and not await bot.store.is_service_enabled(ev.channel, "weather"):
            return

        if len(parts) < 2:
            await bot.privmsg(ev.target, f"{ev.nick}: usage: !weather <city>[, <country>] ")
            return

        q = parts[1].strip()

        # Geocode
        geourl = "https://geocoding-api.open-meteo.com/v1/search?" + urllib.parse.urlencode({"name": q, "count": 1, "language": "en", "format": "json"})
        geo = await asyncio.to_thread(_fetch_json, geourl)
        res = (geo.get("results") or [])
        if not res:
            await bot.privmsg(ev.target, f"{ev.nick}: location not found")
            return
        g = res[0]
        lat = g.get("latitude")
        lon = g.get("longitude")
        name = g.get("name")
        country = g.get("country")

        wurl = (
            "https://api.open-meteo.com/v1/forecast?" +
            urllib.parse.urlencode({
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,apparent_temperature,wind_speed_10m,wind_gusts_10m,relative_humidity_2m,cloud_cover,precipitation_probability",
                "hourly": "temperature_2m,precipitation_probability,wind_speed_10m,wind_gusts_10m,cloud_cover",
                "forecast_hours": 12,
                "timezone": "UTC",
            })
        )
        data = await asyncio.to_thread(_fetch_json, wurl)

        cur = data.get("current") or {}
        ctemp = cur.get("temperature_2m")
        cfeel = cur.get("apparent_temperature")
        cwind = cur.get("wind_speed_10m")
        cgust = cur.get("wind_gusts_10m")
        chum = cur.get("relative_humidity_2m")
        ccloud = cur.get("cloud_cover")
        cpop = cur.get("precipitation_probability")

        h = data.get("hourly") or {}
        temps = h.get("temperature_2m") or []
        pops = h.get("precipitation_probability") or []
        winds = h.get("wind_speed_10m") or []
        gusts = h.get("wind_gusts_10m") or []
        clouds = h.get("cloud_cover") or []

        if temps:
            t0, t1 = temps[0], temps[-1]
        else:
            t0 = t1 = None

        def _safe_max(xs):
            try:
                return max([x for x in xs if x is not None])
            except Exception:
                return None

        msg1 = (
            f"{ev.nick}: weather in {name}, {country} is currently {ctemp}°C (feels {cfeel}°C) "
            f"wind {cwind}km/h gust {cgust}km/h (humidity {chum}%, cloud {ccloud}%, pop {cpop}%)."
        )

        msg2 = (
            f"Next 12h: temp {t0}°C→{t1}°C, pop max {_safe_max(pops)}%, "
            f"wind max {_safe_max(winds)}km/h gust max {_safe_max(gusts)}km/h, cloud max {_safe_max(clouds)}%."
        )

        await bot.privmsg(ev.target, msg1)
        await bot.privmsg(ev.target, msg2)
