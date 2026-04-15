"""Per-host circuit breaker for outbound HTTP calls.

When Polymarket's Data API degrades (rate limits, partial outages), a
naive caller will keep hammering it and produce cascading slow-failures
through every downstream task. This module tracks consecutive failures
per host and "opens" the circuit for a cooldown window after N in a row.
While open, calls raise immediately without a network round-trip so the
caller can short-circuit gracefully.

Usage::

    from trading_platform.polymarket.http_circuit import allow_request, record_success, record_failure

    if not allow_request("data-api.polymarket.com"):
        raise RuntimeError("circuit open")
    try:
        resp = requests.get(...)
        resp.raise_for_status()
        record_success("data-api.polymarket.com")
    except Exception:
        record_failure("data-api.polymarket.com")
        raise

Thresholds are conservative (5 failures → 60s open window). Tune via env:
  HTTP_CIRCUIT_FAILURE_THRESHOLD, HTTP_CIRCUIT_OPEN_SECONDS.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

FAILURE_THRESHOLD = int(os.environ.get("HTTP_CIRCUIT_FAILURE_THRESHOLD", "5"))
OPEN_SECONDS = int(os.environ.get("HTTP_CIRCUIT_OPEN_SECONDS", "60"))


@dataclass
class HostState:
    consecutive_failures: int = 0
    opened_at: float = 0.0

    def is_open(self, now: float) -> bool:
        if self.opened_at == 0:
            return False
        return (now - self.opened_at) < OPEN_SECONDS


_state: dict[str, HostState] = {}
_lock = threading.Lock()


def _host_of(url_or_host: str) -> str:
    if "://" in url_or_host:
        return urlparse(url_or_host).netloc.lower()
    return url_or_host.lower()


def allow_request(url_or_host: str) -> bool:
    """Return False if the circuit is open for this host."""
    host = _host_of(url_or_host)
    now = time.time()
    with _lock:
        s = _state.get(host)
        if not s:
            return True
        if s.is_open(now):
            return False
        # Half-open: the cooldown elapsed — allow one probe. Clear the
        # opened_at so a success returns us to closed; a failure re-opens.
        if s.opened_at:
            logger.info("[circuit] %s half-open (probe)", host)
            s.opened_at = 0.0
        return True


def record_success(url_or_host: str) -> None:
    host = _host_of(url_or_host)
    with _lock:
        s = _state.get(host)
        if s:
            if s.consecutive_failures or s.opened_at:
                logger.info("[circuit] %s recovered", host)
            s.consecutive_failures = 0
            s.opened_at = 0.0


def record_failure(url_or_host: str) -> None:
    host = _host_of(url_or_host)
    now = time.time()
    with _lock:
        s = _state.setdefault(host, HostState())
        s.consecutive_failures += 1
        if s.consecutive_failures >= FAILURE_THRESHOLD and not s.opened_at:
            s.opened_at = now
            logger.warning(
                "[circuit] %s OPEN after %d consecutive failures (cooldown %ds)",
                host, s.consecutive_failures, OPEN_SECONDS,
            )


def snapshot() -> dict:
    """Return a serializable view of circuit state — for the readiness endpoint."""
    now = time.time()
    out = {}
    with _lock:
        for host, s in _state.items():
            out[host] = {
                "consecutive_failures": s.consecutive_failures,
                "open": s.is_open(now),
                "open_since_seconds_ago": round(now - s.opened_at, 1) if s.opened_at else None,
            }
    return out
