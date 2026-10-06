"""Bronze: append landing files to a Delta table, unchanged.

The API payload stays as a JSON string (schema-on-read). If the API adds or renames a
field, bronze keeps loading; only the silver parser needs to change, and it can be
re-run over all bronze history.
"""

from __future__ import annotations

import logging
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

log = logging.getLogger(__name__)

LANDING_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("ingested_at", T.StringType(), False),
    T.StructField("source", T.StringType(), False),
    T.StructField("city_id", T.StringType(), False),
    T.StructField("window_start", T.StringType(), False),
    T.StructField("window_end", T.StringType(), False),
    T.StructField("payload", T.StringType(), False),
])


def read_landing(spark: SparkSession, files: list[Path]) -> DataFrame:
    return (
        spark.read.schema(LANDING_SCHEMA)
        .option("mode", "FAILFAST")  # a corrupt landing file should stop the run, not be skipped silently
        .json([str(f) for f in files])
        .select(
            "run_id",
            F.to_timestamp("ingested_at").alias("ingested_at"),
            "source",
            "city_id",
            F.to_date("window_start").alias("window_start"),
            F.to_date("window_end").alias("window_end"),
            "payload",
            F.col("_metadata.file_name").alias("source_file"),
        )
    )


def load(spark: SparkSession, files: list[Path], bronze_path: str) -> int:
    """Append new landing files to bronze. Files already loaded are skipped, so re-runs are safe."""
    if not files:
        return 0
    df = read_landing(spark, files)
    if DeltaTable.isDeltaTable(spark, bronze_path):
        loaded = spark.read.format("delta").load(bronze_path).select("source_file").distinct()
        df = df.join(loaded, "source_file", "left_anti")
    rows = df.count()
    if rows:
        df.write.format("delta").mode("append").save(bronze_path)
    log.info("Bronze: appended %d rows", rows)
    return rows
