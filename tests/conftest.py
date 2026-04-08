"""
Project-wide pytest fixtures + collection filter.

This conftest does two jobs:

1. **Credential scrubbing.** Several modules
   (``trading_platform.ops.telegram_sender``, validation alerting,
   Kalshi historical ingest CLI) fall back to environment variables
   when arguments are not provided. Without scrubbing the env, tests
   can actually deliver Telegram messages to the developer's chat,
   leak SMTP/Kalshi credentials into deterministic-output assertions,
   and make CI behave differently from local runs. We unset the
   relevant env vars **before any test imports a module**.

2. **Polymarket-only test scoping.** The trading platform has
   accumulated a large legacy equity / Kalshi / research test suite
   that is no longer in active use. We mechanically skip every test
   file that does not reference Polymarket (plus a small allowlist
   for general infra like the telegram sender). To re-enable a
   skipped file, either add a Polymarket reference to it or include
   its path in ``_KEEP_PATHS`` below.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

_SCRUBBED_ENV_VARS = (
    # Telegram bot — prevents test runs from sending real messages
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    # SMTP — used by validation alerting; presence makes deterministic
    # artifact assertions fail. The reporting layer uses the TP_ALERT_*
    # prefix; the generic SMTP_* names are scrubbed for safety too.
    "TP_ALERT_SMTP_HOST",
    "TP_ALERT_SMTP_PORT",
    "TP_ALERT_SMTP_USERNAME",
    "TP_ALERT_SMTP_PASSWORD",
    "TP_ALERT_SMTP_USE_TLS",
    "TP_ALERT_FROM",
    "TP_ALERT_TO",
    "TP_ALERT_SUBJECT_PREFIX",
    "SMTP_HOST",
    "SMTP_PORT",
    "SMTP_USERNAME",
    "SMTP_PASSWORD",
    "SMTP_FROM",
    "SMTP_TO",
    "SMTP_USE_TLS",
    # Kalshi creds — historical ingest CLI test asserts the public-only
    # default fingerprint
    "KALSHI_API_KEY_ID",
    "KALSHI_PRIVATE_KEY_PATH",
    "KALSHI_PRIVATE_KEY_PEM",
    "KALSHI_API_KEY_FILE",
    "KALSHI_DEMO",
)


def _scrub_env() -> None:
    for name in _SCRUBBED_ENV_VARS:
        os.environ.pop(name, None)


# Run at collection time so it takes effect before any test module imports
# trading_platform code that may snapshot env state at import time.
_scrub_env()


@pytest.fixture(autouse=True)
def _scrub_credential_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Re-scrub credentials at the start of every test in case some test
    or fixture set them back."""
    for name in _SCRUBBED_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


# ── Polymarket-only collection filter ────────────────────────────────────────

# Files that pytest should always collect even though they don't mention
# Polymarket directly. Paths are relative to the tests/ directory.
_KEEP_PATHS = {
    "conftest.py",
    "ops/test_telegram_sender.py",  # Telegram sender used by Polymarket alerts
}

# Substrings (case-insensitive) — if a test file contains any of these,
# it stays. We deliberately use only ``polymarket`` to avoid pulling in
# legacy equity / Kalshi files that mention ``wallet_`` or
# ``smart_money`` for unrelated reasons.
_KEEP_KEYWORDS = (
    "polymarket",
)


def _file_is_polymarket_related(path: Path) -> bool:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore").lower()
    except OSError:
        return False
    return any(k in text for k in _KEEP_KEYWORDS)


def _build_collect_ignore() -> list[str]:
    """Walk tests/ and return paths to skip (everything not Polymarket)."""
    tests_root = Path(__file__).parent
    ignore: list[str] = []
    for path in tests_root.rglob("test_*.py"):
        rel = path.relative_to(tests_root).as_posix()
        if rel in _KEEP_PATHS:
            continue
        if _file_is_polymarket_related(path):
            continue
        ignore.append(rel)
    return ignore


# pytest reads ``collect_ignore`` from conftest.py at collection time.
# Each entry is a path relative to this conftest (i.e. relative to tests/).
collect_ignore = _build_collect_ignore()
