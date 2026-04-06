"""
Order flow data loader for the market scanner.

Loads fill parquet files written by the Goldsky client and provides
a unified interface for signal computation.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class OrderFlowLoader:
    """Load fill data from parquet files."""

    def __init__(self, fills_dir: str = "data/polymarket/orderflow/") -> None:
        self._dir = Path(fills_dir)

    def load_fills(self, token_id: str, *, since_hours: int = 48) -> pd.DataFrame:
        """Load fills for a token ID, filtered to recent hours."""
        path = self._dir / f"{token_id}.parquet"
        if not path.exists():
            # Try truncated name
            candidates = list(self._dir.glob(f"{token_id[:32]}*.parquet"))
            if candidates:
                path = candidates[0]
            else:
                return pd.DataFrame()

        try:
            df = pd.read_parquet(path)
        except Exception:
            return pd.DataFrame()

        if df.empty or "timestamp" not in df.columns:
            return df

        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=since_hours)
        try:
            return df[df["timestamp"] >= cutoff]
        except TypeError:
            return df

    def available_markets(self) -> list[str]:
        """Return token IDs that have fills parquet files."""
        if not self._dir.exists():
            return []
        return [f.stem for f in self._dir.glob("*.parquet")]
