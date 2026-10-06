from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE = FIXTURES / "sample_response.json"
REVISED = FIXTURES / "revised_response.json"


@pytest.fixture(scope="session")
def spark():
    from airlake.spark import get_spark

    session = get_spark("airlake-tests")
    yield session
    session.stop()


@pytest.fixture
def root(tmp_path: Path) -> Path:
    return tmp_path / "lakehouse"


@pytest.fixture(scope="session")
def sample() -> list[dict]:
    return json.loads(SAMPLE.read_text(encoding="utf-8"))
