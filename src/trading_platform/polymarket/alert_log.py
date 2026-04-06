"""
Persistent alert log for smart money detections.

Appends alerts to a JSONL file and provides query functions.

Usage::

    from trading_platform.polymarket.alert_log import AlertLog
    log = AlertLog()
    log.append(wallet="0xabc...", token_id="123...", ...)
    recent = log.query(limit=50, wallet="0xabc...")
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_PATH = "data/polymarket/alerts.jsonl"


class AlertLog:
    """Append-only JSONL alert log with query support."""

    def __init__(self, path: str | Path = _DEFAULT_PATH) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def append(
        self,
        *,
        wallet: str,
        token_id: str,
        market_question: str,
        side: str,
        amount_usdc: float,
        wallet_edge: float,
        early_win_rate: float,
        tier: int,
        hours_since_close: float | None = None,
        alert_type: str | None = None,
    ) -> None:
        """Append one alert to the log."""
        entry = {
            "ts": datetime.now(tz=timezone.utc).isoformat(),
            "wallet": wallet,
            "token_id": token_id,
            "market_question": market_question,
            "side": side,
            "amount_usdc": round(amount_usdc, 2),
            "wallet_edge": round(wallet_edge, 4),
            "alert_type": alert_type or "unknown",
            "early_win_rate": round(early_win_rate, 4),
            "tier": tier,
            "hours_since_close": round(hours_since_close, 1) if hours_since_close is not None else None,
        }
        try:
            with self._path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as exc:
            logger.warning("Failed to write alert log: %s", exc)

    def query(
        self,
        *,
        limit: int = 50,
        wallet: str | None = None,
        tier: int | None = None,
    ) -> list[dict[str, Any]]:
        """Read the last N alerts, optionally filtered by wallet or tier."""
        if not self._path.exists():
            return []
        try:
            lines = self._path.read_text(encoding="utf-8").strip().splitlines()
        except Exception:
            return []

        results: list[dict[str, Any]] = []
        for line in reversed(lines):
            if len(results) >= limit:
                break
            try:
                entry = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            if wallet and entry.get("wallet") != wallet:
                continue
            if tier is not None and entry.get("tier") != tier:
                continue
            results.append(entry)
        return results

    @property
    def path(self) -> Path:
        return self._path
