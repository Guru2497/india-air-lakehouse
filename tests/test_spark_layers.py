"""Silver and gold logic on a real local Spark session with Delta Lake."""

from __future__ import annotations

from datetime import date, datetime

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from airlake import aqi, bronze, quality, silver
from airlake.config import load_cities
from airlake.ingest import ingest_fixture

from .conftest import SAMPLE

pytestmark = pytest.mark.spark


def test_spark_sub_index_matches_python(spark):
    concentrations = (0, 0.7, 1.05, 12, 30.4, 45, 61, 99, 130, 251, 380, 431, 760, 1700, 5000)
    grid = [(p, float(c)) for p in aqi.BREAKPOINTS for c in concentrations]
    df = spark.createDataFrame(grid, "pollutant string, conc double")
    exprs = None
    for p in aqi.BREAKPOINTS:
        is_p, branch = F.col("pollutant") == p, aqi.sub_index_expr(p, F.col("conc"))
        exprs = F.when(is_p, branch) if exprs is None else exprs.when(is_p, branch)
    got = {(r.pollutant, r.conc): r.si for r in df.withColumn("si", exprs).collect()}
    for (p, c), si in got.items():
        assert si == aqi.sub_index(p, c), (p, c)


def test_spark_category_matches_python(spark):
    df = spark.createDataFrame([(v,) for v in (0, 50, 51, 100, 101, 250, 301, 401, 500)], "aqi int")
    for r in df.withColumn("cat", aqi.category_expr(F.col("aqi"))).collect():
        assert r.cat == aqi.category(r.aqi)


def _bronze_from_fixture(spark, root):
    files = ingest_fixture(SAMPLE, load_cities(), root / "landing", "run-1")
    return bronze.read_landing(spark, files), files


def test_silver_transform_explodes_hours_and_converts_units(spark, root, sample):
    df, _ = _bronze_from_fixture(spark, root)
    out = silver.transform(df)
    assert out.count() == 2 * 72
    first = out.where("city_id = 'BLR' AND ts_utc = '2026-09-01 00:00:00'").first()
    assert first.obs_date_ist == date(2026, 9, 1) and first.obs_hour_ist == 5  # 00:00 UTC = 05:30 IST
    assert first.co_mg_m3 == pytest.approx(sample[0]["hourly"]["carbon_monoxide"][0] / 1000)
    late = out.where("city_id = 'BLR' AND ts_utc = '2026-09-01 19:00:00'").first()
    assert late.obs_date_ist == date(2026, 9, 2) and late.obs_hour_ist == 0  # crosses midnight in IST


def test_silver_never_keeps_future_hours(spark):
    payload = '{"hourly":{"time":["2026-09-01T00:00","2099-01-01T00:00"],"pm2_5":[10.0,11.0]}}'
    df = spark.createDataFrame(
        [Row(city_id="BLR", run_id="r", ingested_at=datetime(2026, 9, 2), payload=payload)]
    )
    assert silver.transform(df).count() == 1


def test_bronze_skips_files_already_loaded(spark, root):
    _, files = _bronze_from_fixture(spark, root)
    path = str(root / "bronze")
    assert bronze.load(spark, files, path) == 2
    assert bronze.load(spark, files, path) == 0


def test_merge_is_idempotent(spark, root):
    df, _ = _bronze_from_fixture(spark, root)
    batch = silver.transform(df)
    path = str(root / "silver")
    assert silver.upsert(spark, batch, path) == (144, 0)
    assert silver.upsert(spark, batch, path) == (0, 0)  # same data again: nothing changes
    assert spark.read.format("delta").load(path).count() == 144


def test_quality_gate_blocks_duplicate_keys(spark, root):
    row = Row(city_id="BLR", ts_utc=datetime(2026, 9, 1), pm2_5=1.0, pm10=1.0, no2=1.0, so2=1.0, o3=1.0,
              co_mg_m3=0.1)
    df = spark.createDataFrame([row, row])
    checks = quality.run_checks(df, ["BLR"], check_freshness=False)
    assert {c.name: c.passed for c in checks}["unique_city_hour"] is False
    with pytest.raises(quality.DataQualityError):
        quality.record_and_enforce(spark, checks, "run-x", str(root / "dq"))
