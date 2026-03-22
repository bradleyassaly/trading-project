PYTHON ?= python
PIP ?= $(PYTHON) -m pip
RUFF ?= $(PYTHON) -m ruff
PYTEST ?= $(PYTHON) -m pytest

.PHONY: install test lint smoke doctor

install:
	$(PIP) install --upgrade pip
	$(PIP) install -e .[dev]

test:
	$(PYTEST)

lint:
	$(RUFF) check src tests --select E9,F63,F7,F82

smoke:
	$(PYTEST) tests/test_end_to_end_smoke.py

doctor:
	trading-cli doctor --artifacts-root artifacts --monitoring-config configs/monitoring.yaml --execution-config configs/execution.yaml --broker-config configs/broker.yaml --dashboard-config configs/dashboard.yaml --output-dir artifacts/system_check
