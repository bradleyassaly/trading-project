# Polymarket trading platform — Python backend image.
# Used by api, scheduler, watchdog, and signal-monitor services.
FROM python:3.11-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl sqlite3 ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first to leverage layer cache
COPY pyproject.toml ./
COPY src/ src/
RUN pip install --no-cache-dir -e .

# scikit-learn is an optional dep used by trade_ev_reliability and pulled
# into the import chain via paper.service. Install in the runtime image so
# `trading-cli polymarket *` commands can load without ImportError.
RUN pip install --no-cache-dir scikit-learn

# Scripts + configs (data is mounted at runtime)
COPY scripts/ scripts/
COPY configs/ configs/
COPY tests/conftest.py tests/conftest.py

# The data and reports directories live on the host volume
ENV PYTHONUNBUFFERED=1 \
    TZ=UTC

EXPOSE 8001
