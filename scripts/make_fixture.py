"""Generate the deterministic API-shaped fixtures used by the tests and the offline demo.

sample_response.json   2 cities, 72 UTC hours (2026-09-01..03), with a gap in Delhi's SO2.
revised_response.json  the same last 24 hours with Bengaluru's PM2.5 revised upward,
                       to test that a later run updates silver instead of duplicating it.
"""

import json
import math
from datetime import datetime, timedelta
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
VARS = ["pm2_5", "pm10", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "carbon_monoxide"]
UNITS = {v: "μg/m³" for v in VARS}
CITIES = {
    "BLR": ((12.9716, 77.5946), {"pm2_5": 40, "pm10": 75, "nitrogen_dioxide": 22, "sulphur_dioxide": 8,
                                 "ozone": 55, "carbon_monoxide": 900}),
    "DEL": ((28.6139, 77.2090), {"pm2_5": 140, "pm10": 250, "nitrogen_dioxide": 55, "sulphur_dioxide": 18,
                                 "ozone": 40, "carbon_monoxide": 2300}),
}
START = datetime(2026, 9, 1)
HOURS = 72
DEL_SO2_GAP = range(24, 34)  # UTC 2026-09-02 00:00..09:00 -> Delhi has only 14 SO2 hours on IST 2 Sep


def value(base: float, hour: int) -> float:
    return round(base * (1 + 0.3 * math.sin(2 * math.pi * hour / 24)), 1)


def response(city: str, hours: range, bump: float = 1.0) -> dict:
    (lat, lon), bases = CITIES[city]
    hourly = {"time": [(START + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M") for h in hours]}
    for v in VARS:
        col = []
        for h in hours:
            if city == "DEL" and v == "sulphur_dioxide" and h in DEL_SO2_GAP:
                col.append(None)
            else:
                factor = bump if (city == "BLR" and v == "pm2_5") else 1.0
                col.append(round(value(bases[v], h) * factor, 1))
        hourly[v] = col
    return {
        "latitude": round(lat, 2), "longitude": round(lon, 2), "generationtime_ms": 1.2,
        "utc_offset_seconds": 0, "timezone": "GMT", "timezone_abbreviation": "GMT", "elevation": 900.0,
        "hourly_units": {"time": "iso8601", **UNITS}, "hourly": hourly,
    }


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    full = [response(c, range(HOURS)) for c in CITIES]
    revised = [response(c, range(HOURS - 24, HOURS), bump=1.5 if c == "BLR" else 1.0) for c in CITIES]
    (OUT / "sample_response.json").write_text(json.dumps(full, indent=1, ensure_ascii=False))
    (OUT / "revised_response.json").write_text(json.dumps(revised, indent=1, ensure_ascii=False))
    print("wrote", OUT)
