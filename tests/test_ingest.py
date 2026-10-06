"""Extraction helpers, tested without network."""

from datetime import date

import pytest

from airlake.config import City, load_cities
from airlake.ingest import date_chunks, fetch_window

BLR = City("BLR", "Bengaluru", "Karnataka", 12.97, 77.59)
DEL = City("DEL", "Delhi", "Delhi", 28.61, 77.21)


class FakeResponse:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, body):
        self.body = body
        self.params = None

    def get(self, url, params, timeout):
        self.params = params
        return FakeResponse(self.body)


def test_date_chunks_split_and_cover_window():
    chunks = list(date_chunks(date(2026, 1, 1), date(2026, 3, 5), max_days=31))
    assert chunks[0] == (date(2026, 1, 1), date(2026, 1, 31))
    assert chunks[-1][1] == date(2026, 3, 5)
    for (_, prev_end), (nxt_start, _) in zip(chunks, chunks[1:], strict=False):
        assert (nxt_start - prev_end).days == 1  # no gaps, no overlaps


def test_date_chunks_rejects_reversed_window():
    with pytest.raises(ValueError):
        list(date_chunks(date(2026, 2, 1), date(2026, 1, 1)))


def test_single_location_response_is_wrapped_in_a_list():
    session = FakeSession({"latitude": 12.97, "hourly": {"time": []}})
    out = fetch_window([BLR], date(2026, 9, 1), date(2026, 9, 1), session)
    assert isinstance(out, list) and len(out) == 1
    assert session.params["timezone"] == "GMT"
    assert "pm2_5" in session.params["hourly"]


def test_location_count_mismatch_fails_loudly():
    with pytest.raises(ValueError, match="2 cities"):
        fetch_window([BLR, DEL], date(2026, 9, 1), date(2026, 9, 1), FakeSession([{"hourly": {}}]))


def test_cities_config_has_unique_ids():
    cities = load_cities()
    assert len(cities) == len({c.city_id for c in cities}) >= 10
