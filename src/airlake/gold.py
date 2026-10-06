"""Gold: daily National AQI per city (IST calendar day) and the city dimension.

Only the dates touched by a run are recomputed, using Delta's replaceWhere, so a run
over two days of new data does not rewrite years of history and re-runs are idempotent.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, Row, SparkSession, Window
from pyspark.sql import functions as F

from . import aqi
from .config import City

log = logging.getLogger(__name__)

DAILY_AVG = {"pm2_5": "pm2_5", "pm10": "pm10", "no2": "no2", "so2": "so2"}  # 24-hour average pollutants
ROLLING_8H = {"o3": "o3", "co": "co_mg_m3"}  # 8-hour average pollutants: pollutant -> silver column
MIN_HOURS_IN_8H_WINDOW = 6


def _with_8h_rolling(hourly: DataFrame) -> DataFrame:
    """Add trailing 8-hour averages for O3 and CO; windows with fewer than 6 hours are null."""
    w = (
        Window.partitionBy("city_id")
        .orderBy(F.col("ts_utc").cast("long"))
        .rangeBetween(-7 * 3600, 0)  # this hour and the 7 before it
    )
    for pollutant, col in ROLLING_8H.items():
        hourly = hourly.withColumn(
            f"{pollutant}_8h",
            F.when(F.count(col).over(w) >= MIN_HOURS_IN_8H_WINDOW, F.avg(col).over(w)),
        )
    return hourly


def daily_aqi(silver: DataFrame, start: date, end: date) -> DataFrame:
    # Read one extra day before the window so 8-hour averages just after midnight are complete.
    hourly = silver.where(F.col("obs_date_ist").between(start - timedelta(days=1), end))
    hourly = _with_8h_rolling(hourly).where(F.col("obs_date_ist").between(start, end))

    aggs: list[Column] = []
    for p, col in DAILY_AVG.items():
        aggs += [F.avg(col).alias(f"{p}_24h"), F.count(col).alias(f"{p}_hours")]
    for p, col in ROLLING_8H.items():
        aggs += [F.max(f"{p}_8h").alias(f"{p}_8h_max"), F.count(col).alias(f"{p}_hours")]
    daily = hourly.groupBy("city_id", "obs_date_ist").agg(*aggs)

    pollutants = list(DAILY_AVG) + list(ROLLING_8H)
    for p in pollutants:
        value = F.col(f"{p}_24h") if p in DAILY_AVG else F.col(f"{p}_8h_max")
        daily = daily.withColumn(
            f"si_{p}",
            F.when(F.col(f"{p}_hours") >= aqi.MIN_HOURS_FOR_SUBINDEX, aqi.sub_index_expr(p, value)),
        )

    si_cols = [F.col(f"si_{p}") for p in pollutants]
    n_present = sum(F.when(c.isNotNull(), 1).otherwise(0) for c in si_cols)
    has_pm = F.col("si_pm2_5").isNotNull() | F.col("si_pm10").isNotNull()
    daily = daily.withColumn(
        "aqi", F.when((n_present >= aqi.MIN_POLLUTANTS_FOR_AQI) & has_pm, F.greatest(*si_cols))
    )
    prominent = None
    for p in pollutants:  # ties resolve in this order: PM2.5 first
        cond = F.col(f"si_{p}") == F.col("aqi")
        prominent = F.when(cond, F.lit(p)) if prominent is None else prominent.when(cond, F.lit(p))
    return (
        daily.withColumn("prominent_pollutant", prominent)
        .withColumn("aqi_category", aqi.category_expr(F.col("aqi")))
        .withColumn("pollutants_reported", n_present)
        .withColumn("computed_at", F.current_timestamp())
        .select(
            "city_id", "obs_date_ist", "aqi", "aqi_category", "prominent_pollutant", "pollutants_reported",
            *[F.round(f"{p}_24h", 2).alias(f"{p}_24h") for p in DAILY_AVG],
            F.round("o3_8h_max", 2).alias("o3_8h_max"),
            F.round("co_8h_max", 3).alias("co_8h_max_mg_m3"),
            *[f"si_{p}" for p in pollutants],
            *[f"{p}_hours" for p in pollutants],
            "computed_at",
        )
    )


def build_daily(spark: SparkSession, silver_path: str, gold_path: str, start: date, end: date) -> int:
    silver = spark.read.format("delta").load(silver_path)
    df = daily_aqi(silver, start, end)
    writer = df.write.format("delta").mode("overwrite")
    if DeltaTable.isDeltaTable(spark, gold_path):
        writer = writer.option(
            "replaceWhere", f"obs_date_ist >= '{start.isoformat()}' AND obs_date_ist <= '{end.isoformat()}'"
        )
    writer.save(gold_path)
    rows = spark.read.format("delta").load(gold_path).where(F.col("obs_date_ist").between(start, end)).count()
    log.info("Gold daily_city_aqi: %d rows for %s..%s", rows, start, end)
    return rows


def build_dim_city(spark: SparkSession, cities: list[City], gold_path: str) -> None:
    df = spark.createDataFrame([Row(**c.__dict__) for c in cities]).select(
        "city_id", "name", "state", "latitude", "longitude"
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path)
