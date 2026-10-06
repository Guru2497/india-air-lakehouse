"""Whole pipeline on fixture data: correctness, idempotent re-runs and late revisions."""

from __future__ import annotations

from datetime import date, datetime

import pytest

from airlake.config import Paths
from airlake.pipeline import run

from . import expected
from .conftest import REVISED, SAMPLE

pytestmark = pytest.mark.spark


def _gold(spark, root):
    return {(r.city_id, r.obs_date_ist): r for r in spark.read.format("delta").load(Paths(root).gold_daily).collect()}


def test_first_run_builds_every_layer(spark, root):
    s = run(spark, fixture=SAMPLE, root=root, run_id="run-1")
    assert s.status == "success"
    assert (s.bronze_rows, s.silver_inserted, s.silver_updated) == (2, 144, 0)
    # 72 UTC hours span IST 1-4 Sep: 4 days x 2 cities
    assert s.gold_rows == 8
    runs = spark.read.format("delta").load(Paths(root).runs).collect()
    assert [r.status for r in runs] == ["success"]


@pytest.mark.parametrize("city_index, city_id", [(0, "BLR"), (1, "DEL")])
def test_daily_aqi_matches_independent_calculation(spark, root, sample, city_index, city_id):
    run(spark, fixture=SAMPLE, root=root, run_id="run-1")
    got = _gold(spark, root)[(city_id, date(2026, 9, 2))]
    want = expected.daily(sample[city_index], date(2026, 9, 2))
    for p, si in want["si"].items():
        assert getattr(got, f"si_{p}") == si, p
        assert getattr(got, f"{p}_hours") == want[f"{p}_hours"], p
    assert got.aqi == want["aqi"]
    assert got.prominent_pollutant == want["prominent"]


def test_cpcb_minimum_data_rules(spark, root):
    run(spark, fixture=SAMPLE, root=root, run_id="run-1")
    gold = _gold(spark, root)
    delhi = gold[("DEL", date(2026, 9, 2))]
    assert delhi.so2_hours == 14 and delhi.si_so2 is None  # under 16 hours: no SO2 sub-index
    assert delhi.aqi is not None  # still five pollutants, so AQI is reported
    partial_day = gold[("BLR", date(2026, 9, 4))]  # only 5 hours of 4 Sep (IST) in the data
    assert partial_day.aqi is None and partial_day.aqi_category is None


def test_rerun_is_idempotent(spark, root):
    run(spark, fixture=SAMPLE, root=root, run_id="run-1")
    before = _gold(spark, root)
    s = run(spark, fixture=SAMPLE, root=root, run_id="run-2")
    assert (s.silver_inserted, s.silver_updated) == (0, 0)
    assert spark.read.format("delta").load(Paths(root).silver).count() == 144
    after = _gold(spark, root)
    assert {k: v.aqi for k, v in before.items()} == {k: v.aqi for k, v in after.items()}


def test_late_revision_updates_silver_and_gold(spark, root, sample):
    run(spark, fixture=SAMPLE, root=root, run_id="run-1")
    old = _gold(spark, root)[("BLR", date(2026, 9, 3))]
    s = run(spark, fixture=REVISED, root=root, run_id="run-2")
    assert (s.silver_inserted, s.silver_updated) == (0, 24)  # BLR PM2.5 revised for 24 hours
    silver = spark.read.format("delta").load(Paths(root).silver)
    revised = silver.where("city_id = 'BLR'").where(silver.ts_utc == datetime(2026, 9, 3, 12)).first()
    original = sample[0]["hourly"]["pm2_5"][60]
    assert revised.pm2_5 == pytest.approx(round(original * 1.5, 1))
    assert revised.run_id == "run-2"
    new = _gold(spark, root)[("BLR", date(2026, 9, 3))]
    assert new.pm2_5_24h > old.pm2_5_24h
    assert spark.read.format("delta").load(Paths(root).gold_daily).count() == 8  # replaced, not appended
