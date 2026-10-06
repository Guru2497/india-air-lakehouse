"""India's National Air Quality Index (NAQI) as published by CPCB.

Source: CPCB, "About National Air Quality Index". The breakpoint table below is copied
from that document. Rules applied:

* Each pollutant gets a sub-index by linear interpolation inside its breakpoint band.
* PM2.5, PM10, NO2 and SO2 use 24-hour averages; O3 and CO use 8-hour averages
  (we take the day's highest 8-hour rolling average). CO is in mg/m3, the rest in ug/m3.
* A sub-index needs at least 16 hours of data in the day.
* Overall AQI = the highest sub-index, and is only reported when at least three
  pollutants have a sub-index and one of them is PM2.5 or PM10.

Project assumption: CPCB's Severe band is open-ended ("PM2.5 250+"). Here the Severe
sub-index rises linearly from 401 at the band's lower limit to 500 at twice that limit,
and is capped at 500.

Everything is defined once in BREAKPOINTS and used two ways: `sub_index()` for plain
Python (unit tests, documentation) and `sub_index_expr()` which builds the same logic as
a Spark column expression, so the two can never drift apart.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import Column

# (index_lo, index_hi, category) per band, in order.
BANDS: list[tuple[int, int, str]] = [
    (0, 50, "Good"),
    (51, 100, "Satisfactory"),
    (101, 200, "Moderate"),
    (201, 300, "Poor"),
    (301, 400, "Very Poor"),
    (401, 500, "Severe"),
]

# Concentration (lo, hi) per band for each pollutant. hi=None marks the open Severe band.
BREAKPOINTS: dict[str, list[tuple[float, float | None]]] = {
    "pm10": [(0, 50), (51, 100), (101, 250), (251, 350), (351, 430), (430, None)],
    "pm2_5": [(0, 30), (31, 60), (61, 90), (91, 120), (121, 250), (250, None)],
    "no2": [(0, 40), (41, 80), (81, 180), (181, 280), (281, 400), (400, None)],
    "o3": [(0, 50), (51, 100), (101, 168), (169, 208), (209, 748), (748, None)],
    "co": [(0, 1.0), (1.1, 2.0), (2.1, 10), (10, 17), (17, 34), (34, None)],  # mg/m3
    "so2": [(0, 40), (41, 80), (81, 380), (381, 800), (801, 1600), (1600, None)],
}

PARTICULATES = ("pm2_5", "pm10")
MIN_HOURS_FOR_SUBINDEX = 16
MIN_POLLUTANTS_FOR_AQI = 3


def _band_hi(lo: float, hi: float | None) -> float:
    """Upper concentration used for interpolation; the open Severe band ends at 2x its start."""
    return hi if hi is not None else lo * 2


def sub_index(pollutant: str, concentration: float | None) -> int | None:
    """Sub-index for one pollutant concentration, or None when the value is missing."""
    if concentration is None:
        return None
    if concentration < 0:
        raise ValueError(f"Negative concentration for {pollutant}: {concentration}")
    for (i_lo, i_hi, _), (c_lo, c_hi) in zip(BANDS, BREAKPOINTS[pollutant], strict=True):
        if c_hi is None or concentration <= c_hi:
            hi = _band_hi(c_lo, c_hi)
            value = i_lo + (i_hi - i_lo) * (concentration - c_lo) / (hi - c_lo)
            # floor(x + 0.5) = round half up, the same rule Spark's round() uses
            return int(math.floor(min(max(value, 0), 500) + 0.5))
    raise AssertionError("unreachable: last band is open-ended")


def category(aqi: int | None) -> str | None:
    if aqi is None:
        return None
    for _, i_hi, name in BANDS:
        if aqi <= i_hi:
            return name
    return BANDS[-1][2]


def overall_aqi(sub_indices: dict[str, int | None]) -> tuple[int | None, str | None]:
    """Return (aqi, prominent_pollutant) following CPCB's minimum-data rules."""
    present = {p: v for p, v in sub_indices.items() if v is not None}
    if len(present) < MIN_POLLUTANTS_FOR_AQI or not any(p in present for p in PARTICULATES):
        return None, None
    prominent = max(present, key=lambda p: present[p])
    return present[prominent], prominent


# ---------------------------------------------------------------- Spark versions


def sub_index_expr(pollutant: str, conc: Column) -> Column:
    """Spark column computing the same sub-index as `sub_index()`."""
    from pyspark.sql import functions as F

    expr = None
    for (i_lo, i_hi, _), (c_lo, c_hi) in zip(BANDS, BREAKPOINTS[pollutant], strict=True):
        hi = _band_hi(c_lo, c_hi)
        value = F.lit(i_lo) + F.lit(i_hi - i_lo) * (conc - F.lit(c_lo)) / F.lit(hi - c_lo)
        bounded = F.round(F.least(F.greatest(value, F.lit(0)), F.lit(500))).cast("int")
        cond = conc.isNotNull() if c_hi is None else conc <= F.lit(c_hi)
        expr = F.when(cond, bounded) if expr is None else expr.when(cond, bounded)
    return expr  # null concentration falls through every branch -> null


def category_expr(aqi: Column) -> Column:
    from pyspark.sql import functions as F

    expr = None
    for _, i_hi, name in BANDS:
        cond = aqi <= F.lit(i_hi)
        expr = F.when(cond, F.lit(name)) if expr is None else expr.when(cond, F.lit(name))
    return expr.otherwise(F.lit(None))
