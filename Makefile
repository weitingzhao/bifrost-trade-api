.PHONY: install install-dev test lint clean

install:
	pip install -e .

install-dev:
	pip install -e ../bifrost-trade-core -e ../bifrost-trade-worker -e ../bifrost-trade-socket -e ".[dev]"

test:
	pytest -m 'not ib and not db'

lint:
	ruff check src/ tests/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
