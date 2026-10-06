"""Paths, constants and city configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CITIES_FILE = REPO_ROOT / "config" / "cities.yaml"

# Pollutants requested from the API, keyed by API variable name -> our column name.
POLLUTANTS: dict[str, str] = {
    "pm2_5": "pm2_5",
    "pm10": "pm10",
    "nitrogen_dioxide": "no2",
    "sulphur_dioxide": "so2",
    "ozone": "o3",
    "carbon_monoxide": "co",
}

API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
LOCAL_TZ = "Asia/Kolkata"

# Re-read this many days before the latest stored hour on every incremental run, so that
# values the upstream model revises after first publication are picked up (late-arriving data).
INCREMENTAL_LOOKBACK_DAYS = 2
# First run with an empty lakehouse loads this many days of history.
DEFAULT_BACKFILL_DAYS = 30
# Long windows are split into chunks of this many days per API call.
MAX_DAYS_PER_REQUEST = 31


@dataclass(frozen=True)
class City:
    city_id: str
    name: str
    state: str
    latitude: float
    longitude: float


def load_cities(path: str | Path | None = None) -> list[City]:
    with open(path or DEFAULT_CITIES_FILE, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    cities = [City(**c) for c in raw["cities"]]
    ids = [c.city_id for c in cities]
    if len(ids) != len(set(ids)):
        raise ValueError(f"Duplicate city_id in cities config: {ids}")
    return cities


@dataclass(frozen=True)
class Paths:
    """Physical locations of every table, all under one lakehouse root."""

    root: Path

    @classmethod
    def from_env(cls, root: str | Path | None = None) -> Paths:
        return cls(Path(root or os.environ.get("LAKEHOUSE_ROOT", REPO_ROOT / "lakehouse")).resolve())

    @property
    def landing(self) -> Path:
        return self.root / "landing" / "air_quality"

    @property
    def bronze(self) -> str:
        return str(self.root / "bronze" / "air_quality_raw")

    @property
    def silver(self) -> str:
        return str(self.root / "silver" / "air_quality_hourly")

    @property
    def gold_daily(self) -> str:
        return str(self.root / "gold" / "daily_city_aqi")

    @property
    def gold_dim_city(self) -> str:
        return str(self.root / "gold" / "dim_city")

    @property
    def runs(self) -> str:
        return str(self.root / "_meta" / "pipeline_runs")

    @property
    def dq_results(self) -> str:
        return str(self.root / "_meta" / "dq_results")
