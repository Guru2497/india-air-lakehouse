.PHONY: install test lint demo run backfill show rebuild clean

install:   ## install the package and dev tools
	pip install -e ".[dev]"

lint:
	ruff check .

test:
	pytest -v

demo:      ## offline run on fixture data, no network needed
	airlake --root /tmp/airlake-demo run --fixture tests/fixtures/sample_response.json
	airlake --root /tmp/airlake-demo show

run:       ## incremental load from the real API into ./lakehouse
	airlake run

backfill:  ## e.g. make backfill START=2026-07-01 END=2026-09-30
	airlake run --mode backfill --start $(START) --end $(END)

show:
	airlake show

rebuild:
	airlake rebuild

clean:
	rm -rf lakehouse spark-warehouse metastore_db derby.log .pytest_cache
