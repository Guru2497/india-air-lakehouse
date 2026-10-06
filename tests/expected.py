"""Independent pure-Python calculation of daily AQI from a fixture, to check the Spark gold layer."""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airlake.aqi import MIN_HOURS_FOR_SUBINDEX, overall_aqi, sub_index

IST = timedelta(hours=5, minutes=30)
API_TO_COL = {"pm2_5": "pm2_5", "pm10": "pm10", "nitrogen_dioxide": "no2", "sulphur_dioxide": "so2",
              "ozone": "o3", "carbon_monoxide": "co"}


def hourly_rows(response: dict) -> list[tuple[datetime, dict]]:
    h = response["hourly"]
    rows = []
    for i, t in enumerate(h["time"]):
        vals = {col: h[api][i] for api, col in API_TO_COL.items()}
        if vals["co"] is not None:
            vals["co"] = vals["co"] / 1000.0  # ug/m3 -> mg/m3
        rows.append((datetime.fromisoformat(t), vals))
    return rows


def daily(response: dict, day: date) -> dict:
    rows = hourly_rows(response)
    in_day = [(t, v) for t, v in rows if (t + IST).date() == day]
    out: dict = {}
    for p in ("pm2_5", "pm10", "no2", "so2"):
        vals = [v[p] for _, v in in_day if v[p] is not None]
        out[f"{p}_hours"] = len(vals)
        out[f"{p}_value"] = sum(vals) / len(vals) if vals else None
    for p in ("o3", "co"):
        rolling = []
        for t, _ in in_day:
            window = [v[p] for s, v in rows if t - timedelta(hours=7) <= s <= t and v[p] is not None]
            if len(window) >= 6:
                rolling.append(sum(window) / len(window))
        out[f"{p}_hours"] = sum(1 for _, v in in_day if v[p] is not None)
        out[f"{p}_value"] = max(rolling) if rolling else None
    si = {}
    for p in ("pm2_5", "pm10", "no2", "so2", "o3", "co"):
        ok = out[f"{p}_hours"] >= MIN_HOURS_FOR_SUBINDEX
        si[p] = sub_index(p, out[f"{p}_value"]) if ok else None
    out["si"] = si
    out["aqi"], out["prominent"] = overall_aqi(si)
    return out
