"""CPCB National AQI logic in plain Python (no Spark needed)."""

import pytest

from airlake.aqi import BANDS, BREAKPOINTS, category, overall_aqi, sub_index


@pytest.mark.parametrize(
    ("pollutant", "conc", "expected"),
    [
        ("pm2_5", 0, 0),
        ("pm2_5", 30, 50),  # top of Good
        ("pm2_5", 45, 75),  # 51 + 49 * (45 - 31) / 29 = 74.66
        ("pm2_5", 250, 400),  # top of Very Poor
        ("pm2_5", 300, 421),  # Severe: 401 + 99 * 50 / 250 = 420.8
        ("pm2_5", 500, 500),  # Severe reaches 500 at twice its lower limit
        ("pm2_5", 1200, 500),  # capped
        ("pm10", 80, 80),
        ("pm10", 260, 210),  # 201 + 99 * 9 / 99
        ("co", 1.5, 73),  # mg/m3: 51 + 49 * 0.4 / 0.9 = 72.78
        ("co", 2.5, 106),
        ("no2", 20, 25),
        ("o3", 60, 60),
        ("so2", 10, 13),  # 12.5 rounds half up, like Spark
    ],
)
def test_sub_index(pollutant, conc, expected):
    assert sub_index(pollutant, conc) == expected


def test_sub_index_missing_value_is_none():
    assert sub_index("pm10", None) is None


def test_sub_index_rejects_negative():
    with pytest.raises(ValueError):
        sub_index("pm10", -1)


@pytest.mark.parametrize(
    ("aqi", "name"),
    [(0, "Good"), (50, "Good"), (51, "Satisfactory"), (101, "Moderate"), (300, "Poor"),
     (301, "Very Poor"), (401, "Severe"), (500, "Severe"), (None, None)],
)
def test_category(aqi, name):
    assert category(aqi) == name


def test_overall_needs_three_pollutants():
    assert overall_aqi({"pm10": 80, "no2": 25}) == (None, None)


def test_overall_needs_a_particulate():
    assert overall_aqi({"no2": 25, "so2": 13, "o3": 60}) == (None, None)


def test_overall_is_worst_sub_index():
    assert overall_aqi({"pm2_5": 75, "pm10": 80, "no2": 25, "so2": None}) == (80, "pm10")


def test_breakpoint_table_is_well_formed():
    assert len(BANDS) == 6
    for pollutant, bands in BREAKPOINTS.items():
        assert len(bands) == len(BANDS), pollutant
        for (lo, hi), (next_lo, _) in zip(bands, bands[1:], strict=False):
            assert hi is not None and lo < hi <= next_lo + 1, pollutant
        assert bands[-1][1] is None, f"{pollutant}: Severe band must be open-ended"
