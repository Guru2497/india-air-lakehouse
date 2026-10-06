"""Run the pipeline end to end: extract -> bronze -> silver -> quality -> gold, with a run audit row.

Usage:
    airlake run                                     # incremental: picks up where the last run stopped
    airlake run --mode backfill --start 2026-07-01 --end 2026-09-30
    airlake run --fixture tests/fixtures/sample_response.json   # offline demo, no network
    airlake rebuild                                 # replay all of bronze into silver and gold
    airlake show                                    # latest AQI per city
"""

from __future__ import annotations

import argparse
import logging
import traceback
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from . import bronze, gold, ingest, quality, silver
from .config import DEFAULT_BACKFILL_DAYS, INCREMENTAL_LOOKBACK_DAYS, Paths, load_cities

log = logging.getLogger("airlake")

RUNS_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("mode", T.StringType()),
    T.StructField("window_start", T.DateType()),
    T.StructField("window_end", T.DateType()),
    T.StructField("status", T.StringType()),
    T.StructField("bronze_rows", T.LongType()),
    T.StructField("silver_inserted", T.LongType()),
    T.StructField("silver_updated", T.LongType()),
    T.StructField("gold_rows", T.LongType()),
    T.StructField("error", T.StringType()),
])


@dataclass
class RunSummary:
    run_id: str
    mode: str
    window_start: date | None = None
    window_end: date | None = None
    status: str = "running"
    bronze_rows: int = 0
    silver_inserted: int = 0
    silver_updated: int = 0
    gold_rows: int = 0
    error: str | None = None
    landed_files: list[Path] = field(default_factory=list)


def incremental_window(spark: SparkSession, paths: Paths, today: date) -> tuple[date, date]:
    """Start a few days before the newest stored hour, so revised values are re-read."""
    if not DeltaTable.isDeltaTable(spark, paths.silver):
        return today - timedelta(days=DEFAULT_BACKFILL_DAYS), today
    latest = spark.read.format("delta").load(paths.silver).agg(F.max("obs_date_ist")).first()[0]
    if latest is None:
        return today - timedelta(days=DEFAULT_BACKFILL_DAYS), today
    return min(latest - timedelta(days=INCREMENTAL_LOOKBACK_DAYS), today), today


def _record_run(spark: SparkSession, s: RunSummary, runs_path: str) -> None:
    row = [(s.run_id, s.mode, s.window_start, s.window_end, s.status, s.bronze_rows, s.silver_inserted,
            s.silver_updated, s.gold_rows, s.error)]
    (
        spark.createDataFrame(row, RUNS_SCHEMA)
        .withColumn("finished_at", F.current_timestamp())
        .write.format("delta").mode("append").save(runs_path)
    )


def run(
    spark: SparkSession,
    mode: str = "incremental",
    start: date | None = None,
    end: date | None = None,
    fixture: Path | None = None,
    root: str | Path | None = None,
    cities_file: str | Path | None = None,
    run_id: str | None = None,
) -> RunSummary:
    paths = Paths.from_env(root)
    cities = load_cities(cities_file)
    s = RunSummary(run_id=run_id or ingest.new_run_id(), mode="fixture" if fixture else mode)
    log.info("Run %s starting (mode=%s, lakehouse=%s)", s.run_id, s.mode, paths.root)
    try:
        # 1. Extract
        if fixture:
            s.landed_files = ingest.ingest_fixture(Path(fixture), cities, paths.landing, s.run_id)
        else:
            today = datetime.now(timezone.utc).date()
            if mode == "backfill":
                if not (start and end):
                    raise ValueError("backfill needs --start and --end")
                s.window_start, s.window_end = start, end
            else:
                s.window_start, s.window_end = incremental_window(spark, paths, today)
            s.landed_files = ingest.ingest_api(cities, s.window_start, s.window_end, paths.landing, s.run_id)

        # 2. Bronze
        s.bronze_rows = bronze.load(spark, s.landed_files, paths.bronze)

        # 3. Silver
        res = silver.process(spark, paths.bronze, paths.silver, s.run_id)
        s.silver_inserted, s.silver_updated = res.inserted, res.updated
        if res.date_min is None:
            log.warning("No rows in this batch; nothing to publish")
        else:
            s.window_start, s.window_end = s.window_start or res.date_min, s.window_end or res.date_max

            # 4. Quality gate: checks run on the whole silver table after the merge
            silver_df = spark.read.format("delta").load(paths.silver)
            checks = quality.run_checks(silver_df, [c.city_id for c in cities], check_freshness=fixture is None)
            quality.record_and_enforce(spark, checks, s.run_id, paths.dq_results)

            # 5. Gold: recompute only the IST dates this batch touched
            s.gold_rows = gold.build_daily(spark, paths.silver, paths.gold_daily, res.date_min, res.date_max)
            gold.build_dim_city(spark, cities, paths.gold_dim_city)
        s.status = "success"
    except Exception as exc:
        s.status = "failed"
        s.error = f"{type(exc).__name__}: {exc}"
        log.error("Run %s failed:\n%s", s.run_id, traceback.format_exc())
        raise
    finally:
        _record_run(spark, s, paths.runs)
        log.info("Run %s %s: bronze=%d silver +%d/~%d gold=%d", s.run_id, s.status, s.bronze_rows,
                 s.silver_inserted, s.silver_updated, s.gold_rows)
    return s


def rebuild(spark: SparkSession, root: str | Path | None = None, cities_file: str | Path | None = None) -> None:
    """Replay every bronze row through silver and gold, e.g. after fixing a parsing bug."""
    paths = Paths.from_env(root)
    res = silver.process(spark, paths.bronze, paths.silver, run_id=None)
    if res.date_min:
        gold.build_daily(spark, paths.silver, paths.gold_daily, res.date_min, res.date_max)
        gold.build_dim_city(spark, load_cities(cities_file), paths.gold_dim_city)


POLLUTANT_LABELS = {"pm2_5": "PM2.5", "pm10": "PM10", "no2": "NO2", "so2": "SO2", "o3": "O3", "co": "CO"}


def latest_aqi(spark: SparkSession, root: str | Path | None = None):
    """Latest day with a reported AQI for each city, worst first."""
    paths = Paths.from_env(root)
    daily = spark.read.format("delta").load(paths.gold_daily).where("aqi IS NOT NULL")
    dim = spark.read.format("delta").load(paths.gold_dim_city)
    latest = daily.groupBy("city_id").agg(F.max("obs_date_ist").alias("obs_date_ist"))
    return (
        daily.join(latest, ["city_id", "obs_date_ist"])
        .join(dim, "city_id")
        .select("name", "obs_date_ist", "aqi", "aqi_category", "prominent_pollutant", "pm2_5_24h", "pm10_24h")
        .orderBy(F.col("aqi").desc(), "name")
    )


def to_markdown(rows) -> str:
    lines = [
        "## Latest National AQI by city",
        "",
        "| # | City | Date (IST) | AQI | Category | Main pollutant | PM2.5 24h (µg/m³) | PM10 24h (µg/m³) |",
        "| --: | --- | --- | --: | --- | --- | --: | --: |",
    ]
    for i, r in enumerate(rows, 1):
        lines.append(
            f"| {i} | {r.name} | {r.obs_date_ist} | **{r.aqi}** | {r.aqi_category} | "
            f"{POLLUTANT_LABELS.get(r.prominent_pollutant, r.prominent_pollutant)} | {r.pm2_5_24h} | {r.pm10_24h} |"
        )
    lines += ["", "AQI follows CPCB's National AQI method on modelled CAMS data (via Open-Meteo), "
              "not CPCB ground-station readings."]
    return "\n".join(lines) + "\n"


def show(spark: SparkSession, root: str | Path | None = None, markdown_out: str | None = None) -> None:
    df = latest_aqi(spark, root)
    df.show(truncate=False)
    if markdown_out:
        with open(markdown_out, "a", encoding="utf-8") as fh:
            fh.write(to_markdown(df.collect()))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="airlake", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", help="lakehouse folder (default: ./lakehouse or $LAKEHOUSE_ROOT)")
    parser.add_argument("--cities", help="cities YAML (default: config/cities.yaml)")
    sub = parser.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run", help="extract and load new data")
    r.add_argument("--mode", choices=["incremental", "backfill"], default="incremental")
    r.add_argument("--start", type=date.fromisoformat)
    r.add_argument("--end", type=date.fromisoformat)
    r.add_argument("--fixture", type=Path, help="load a saved API response instead of calling the API")
    sub.add_parser("rebuild", help="replay bronze into silver and gold")
    sh = sub.add_parser("show", help="print the latest AQI per city")
    sh.add_argument("--markdown", help="also append a markdown table to this file (e.g. $GITHUB_STEP_SUMMARY)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    from .spark import get_spark

    spark = get_spark()
    if args.command == "run":
        run(spark, args.mode, args.start, args.end, args.fixture, args.root, args.cities)
    elif args.command == "rebuild":
        rebuild(spark, args.root, args.cities)
    else:
        show(spark, args.root, args.markdown)


if __name__ == "__main__":
    main()
