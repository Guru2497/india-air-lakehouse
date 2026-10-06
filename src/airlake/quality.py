"""Data quality checks on silver, recorded to a Delta table on every run.

Critical checks stop the pipeline before bad data reaches gold. Warnings are recorded
and logged but let the run finish.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .silver import KEY, VALUE_COLS

log = logging.getLogger(__name__)

MAX_PLAUSIBLE = {"pm2_5": 2000, "pm10": 3000, "no2": 2000, "so2": 3000, "o3": 2000, "co_mg_m3": 200}
FRESHNESS_HOURS = 48


class DataQualityError(RuntimeError):
    pass


@dataclass
class Check:
    name: str
    severity: str  # "critical" or "warning"
    passed: bool
    observed: str


def run_checks(silver: DataFrame, expected_cities: list[str], check_freshness: bool) -> list[Check]:
    checks: list[Check] = []

    dupes = silver.groupBy(*KEY).count().where("count > 1").count()
    checks.append(Check("unique_city_hour", "critical", dupes == 0, f"{dupes} duplicate keys"))

    null_keys = silver.where(F.col("city_id").isNull() | F.col("ts_utc").isNull()).count()
    checks.append(Check("not_null_keys", "critical", null_keys == 0, f"{null_keys} rows"))

    out_of_range = silver.where(
        F.greatest(*[(F.col(c) < 0).cast("int") for c in VALUE_COLS]) == 1
    ).count()
    checks.append(Check("no_negative_concentrations", "critical", out_of_range == 0, f"{out_of_range} rows"))

    implausible = silver.where(
        F.greatest(*[(F.col(c) > F.lit(lim)).cast("int") for c, lim in MAX_PLAUSIBLE.items()]) == 1
    ).count()
    checks.append(Check("plausible_concentrations", "warning", implausible == 0, f"{implausible} rows"))

    present = {r.city_id for r in silver.select("city_id").distinct().collect()}
    missing = sorted(set(expected_cities) - present)
    checks.append(Check("all_cities_present", "warning", not missing, f"missing: {missing}"))

    if check_freshness:
        # Compared inside Spark so the result does not depend on the machine's local timezone.
        row = silver.agg(
            F.date_format(F.max("ts_utc"), "yyyy-MM-dd HH:mm").alias("latest"),
            (F.max("ts_utc") >= F.current_timestamp() - F.expr(f"INTERVAL {FRESHNESS_HOURS} HOURS")).alias("fresh"),
        ).first()
        checks.append(Check(f"fresh_within_{FRESHNESS_HOURS}h", "warning", bool(row["fresh"]),
                            f"latest hour {row['latest']} UTC"))
    return checks


DQ_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("check_name", T.StringType()),
    T.StructField("severity", T.StringType()),
    T.StructField("passed", T.BooleanType()),
    T.StructField("observed", T.StringType()),
])


def record_and_enforce(spark: SparkSession, checks: list[Check], run_id: str, dq_path: str) -> None:
    rows = [(run_id, c.name, c.severity, c.passed, c.observed) for c in checks]
    (
        spark.createDataFrame(rows, DQ_SCHEMA)
        .withColumn("checked_at", F.current_timestamp())
        .write.format("delta").mode("append").save(dq_path)
    )
    for c in checks:
        level = logging.INFO if c.passed else (logging.ERROR if c.severity == "critical" else logging.WARNING)
        log.log(level, "DQ %-28s %-8s %s (%s)", c.name, c.severity, "PASS" if c.passed else "FAIL", c.observed)
    failed = [c.name for c in checks if c.severity == "critical" and not c.passed]
    if failed:
        raise DataQualityError(f"Critical data quality checks failed: {failed}")
