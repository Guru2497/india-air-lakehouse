"""Extract: call the Open-Meteo Air Quality API and land the raw responses as JSON files.

The landing zone keeps the exact bytes the API returned, so any later bug in parsing can
be fixed by replaying landing -> bronze without calling the API again.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import API_URL, MAX_DAYS_PER_REQUEST, POLLUTANTS, City

log = logging.getLogger(__name__)


def date_chunks(start: date, end: date, max_days: int = MAX_DAYS_PER_REQUEST) -> Iterator[tuple[date, date]]:
    """Split an inclusive [start, end] window into inclusive chunks of at most max_days."""
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=max_days - 1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def _session() -> requests.Session:
    retry = Retry(total=5, backoff_factor=1.5, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET",))
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def fetch_window(cities: list[City], start: date, end: date, session: requests.Session | None = None) -> list[dict]:
    """One API call for all cities over [start, end]. Returns one response object per city, in order."""
    params = {
        "latitude": ",".join(f"{c.latitude:.4f}" for c in cities),
        "longitude": ",".join(f"{c.longitude:.4f}" for c in cities),
        "hourly": ",".join(POLLUTANTS),
        "timezone": "GMT",  # store UTC; IST is derived downstream
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }
    resp = (session or _session()).get(API_URL, params=params, timeout=60)
    resp.raise_for_status()
    body = resp.json()
    results = body if isinstance(body, list) else [body]  # a single location is not wrapped in a list
    if len(results) != len(cities):
        raise ValueError(f"API returned {len(results)} locations for {len(cities)} cities")
    for r in results:
        if r.get("error"):
            raise ValueError(f"API error: {r.get('reason')}")
    return results


def land(results: list[dict], cities: list[City], start: date, end: date, landing_dir: Path,
         run_id: str, source: str) -> Path:
    """Write one landing file: an envelope per city around the untouched API response."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    records = [
        {
            "run_id": run_id,
            "ingested_at": ingested_at,
            "source": source,
            "city_id": city.city_id,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "payload": json.dumps(result, separators=(",", ":")),
        }
        for city, result in zip(cities, results, strict=True)
    ]
    out_dir = landing_dir / f"ingest_date={ingested_at[:10]}"
    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / f"{run_id}_{start:%Y%m%d}_{end:%Y%m%d}.jsonl"
    tmp = final.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")
    os.replace(tmp, final)  # atomic: readers never see a half-written file
    log.info("Landed %d city responses for %s..%s -> %s", len(records), start, end, final)
    return final


def ingest_api(cities: list[City], start: date, end: date, landing_dir: Path, run_id: str) -> list[Path]:
    session = _session()
    files = []
    for c_start, c_end in date_chunks(start, end):
        results = fetch_window(cities, c_start, c_end, session)
        files.append(land(results, cities, c_start, c_end, landing_dir, run_id, source="open-meteo"))
    return files


def ingest_fixture(fixture: Path, cities: list[City], landing_dir: Path, run_id: str) -> list[Path]:
    """Offline mode: land a saved API response (used by tests and demos without network)."""
    with open(fixture, encoding="utf-8") as fh:
        body = json.load(fh)
    results = body if isinstance(body, list) else [body]
    cities = cities[: len(results)]
    times = results[0]["hourly"]["time"]
    start, end = date.fromisoformat(times[0][:10]), date.fromisoformat(times[-1][:10])
    return [land(results, cities, start, end, landing_dir, run_id, source=f"fixture:{fixture.name}")]


def new_run_id() -> str:
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
