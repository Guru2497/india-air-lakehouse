"""Silver: one clean, typed row per city per hour, upserted with MERGE.

* Parses the raw payload with an explicit schema and explodes the hourly arrays.
* Converts CO from ug/m3 to mg/m3 (the unit CPCB uses).
* Drops hours later than the ingestion time, so forecasts never pass as observations.
* Deduplicates inside the batch (latest ingestion wins).
* MERGEs on (city_id, ts_utc): re-running a batch changes nothing, and a later run that
  brings revised values updates the row. That makes every run idempotent.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import LOCAL_TZ, POLLUTANTS

log = logging.getLogger(__name__)

PAYLOAD_SCHEMA = T.StructType([
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("hourly", T.StructType(
        [T.StructField("time", T.ArrayType(T.StringType()))]
        + [T.StructField(api_name, T.ArrayType(T.DoubleType())) for api_name in POLLUTANTS]
    )),
])

VALUE_COLS = ["pm2_5", "pm10", "no2", "so2", "o3", "co_mg_m3"]
KEY = ["city_id", "ts_utc"]


@dataclass
class SilverResult:
    batch_rows: int
    inserted: int
    updated: int
    date_min: date | None
    date_max: date | None


def transform(bronze: DataFrame) -> DataFrame:
    """Bronze rows (one per city per landing file) -> hourly rows. Pure DataFrame logic, easy to test."""
    parsed = bronze.select(
        "city_id", "run_id", "ingested_at", F.from_json("payload", PAYLOAD_SCHEMA).alias("p")
    )
    exploded = parsed.select(
        "city_id", "run_id", "ingested_at", "p",
        F.posexplode("p.hourly.time").alias("pos", "time_str"),
    )
    hourly = exploded.select(
        "city_id",
        F.to_timestamp("time_str", "yyyy-MM-dd'T'HH:mm").alias("ts_utc"),
        *[
            F.col(f"p.hourly.{api_name}").getItem(F.col("pos")).alias(col)
            for api_name, col in POLLUTANTS.items()
        ],
        F.col("p.latitude").alias("grid_latitude"),
        F.col("p.longitude").alias("grid_longitude"),
        "run_id",
        "ingested_at",
    )
    local_ts = F.from_utc_timestamp("ts_utc", LOCAL_TZ)
    cleaned = (
        hourly
        .withColumn("co_mg_m3", F.col("co") / 1000.0)
        .drop("co")
        .withColumn("obs_date_ist", F.to_date(local_ts))
        .withColumn("obs_hour_ist", F.hour(local_ts))
        .where(F.col("ts_utc").isNotNull())
        .where(F.col("ts_utc") <= F.col("ingested_at"))  # never store future (forecast) hours
        .where(F.coalesce(*[F.col(c) for c in VALUE_COLS]).isNotNull())  # drop hours with no data at all
    )
    latest = Window.partitionBy(*KEY).orderBy(F.col("ingested_at").desc(), F.col("run_id").desc())
    return (
        cleaned.withColumn("_rn", F.row_number().over(latest))
        .where("_rn = 1")
        .drop("_rn")
        .withColumn("updated_at", F.current_timestamp())
        .select(
            "city_id", "ts_utc", "obs_date_ist", "obs_hour_ist", *VALUE_COLS,
            "grid_latitude", "grid_longitude", "run_id", "ingested_at", "updated_at",
        )
    )


def upsert(spark: SparkSession, batch: DataFrame, silver_path: str) -> tuple[int, int]:
    if not DeltaTable.isDeltaTable(spark, silver_path):
        batch.limit(0).write.format("delta").save(silver_path)
    target = DeltaTable.forPath(spark, silver_path)
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in VALUE_COLS)
    (
        target.alias("t")
        .merge(batch.alias("s"), "t.city_id = s.city_id AND t.ts_utc = s.ts_utc")
        .whenMatchedUpdateAll(condition=f"s.ingested_at > t.ingested_at AND ({changed})")
        .whenNotMatchedInsertAll()
        .execute()
    )
    metrics = target.history(1).select("operationMetrics").first()[0] or {}
    return int(metrics.get("numTargetRowsInserted", 0)), int(metrics.get("numTargetRowsUpdated", 0))


def process(spark: SparkSession, bronze_path: str, silver_path: str, run_id: str | None) -> SilverResult:
    """Transform bronze rows from one run (or all of bronze when run_id is None) and MERGE into silver."""
    bronze = spark.read.format("delta").load(bronze_path)
    if run_id is not None:
        bronze = bronze.where(F.col("run_id") == run_id)
    batch = transform(bronze).cache()
    try:
        stats = batch.agg(F.count("*"), F.min("obs_date_ist"), F.max("obs_date_ist")).first()
        rows, dmin, dmax = stats[0], stats[1], stats[2]
        inserted = updated = 0
        if rows:
            inserted, updated = upsert(spark, batch, silver_path)
    finally:
        batch.unpersist()
    log.info("Silver: batch=%d inserted=%d updated=%d dates=%s..%s", rows, inserted, updated, dmin, dmax)
    return SilverResult(rows, inserted, updated, dmin, dmax)
