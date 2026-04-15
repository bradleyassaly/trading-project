"""Centralized logging configuration — JSON format for aggregators.

Plain-text logs scattered across 7 docker containers are grep-only;
switching to single-line JSON per record means we can ship them to Loki,
Datadog, or just pipe through `jq` for local analysis. Configure once
per process by calling ``setup_logging()`` at startup.

Env toggles:
  LOG_FORMAT=json|text   (default: json when running in Docker)
  LOG_LEVEL=DEBUG|INFO|WARNING|ERROR (default: INFO)
  LOG_SERVICE=...        (free-form service name included on every record)
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone


# Keys guaranteed on every JSON record. Callers can add more via
# ``logger.info("...", extra={"key": "value"})``.
_BASE_KEYS = {"timestamp", "level", "service", "logger", "message"}


class _JsonFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Promote any extra fields the caller attached.
        for k, v in record.__dict__.items():
            if k in _BASE_KEYS: continue
            if k.startswith("_"): continue
            if k in ("args", "asctime", "created", "exc_info", "exc_text",
                    "filename", "funcName", "levelname", "levelno", "lineno",
                    "module", "msecs", "msg", "name", "pathname", "process",
                    "processName", "relativeCreated", "stack_info", "thread",
                    "threadName", "taskName"):
                continue
            # Only primitive types — anything else gets repr'd.
            try:
                json.dumps(v)
                payload[k] = v
            except Exception:
                payload[k] = repr(v)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(
    service: str | None = None,
    *,
    level: str | None = None,
    fmt: str | None = None,
) -> None:
    """Configure the root logger exactly once.

    If already configured (e.g. uvicorn set up its handlers), this is
    idempotent — we replace the existing handler list so we don't get
    double logs.
    """
    svc = service or os.environ.get("LOG_SERVICE") or "trading-platform"
    lvl = (level or os.environ.get("LOG_LEVEL") or "INFO").upper()
    want_fmt = (fmt or os.environ.get("LOG_FORMAT") or "json").lower()

    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler(sys.stdout)
    if want_fmt == "json":
        handler.setFormatter(_JsonFormatter(svc))
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        ))
    root.addHandler(handler)
    root.setLevel(lvl)

    # Quiet the noisiest third-party loggers unless we really want DEBUG.
    for name in ("urllib3", "asyncio", "websockets"):
        logging.getLogger(name).setLevel("WARNING")
