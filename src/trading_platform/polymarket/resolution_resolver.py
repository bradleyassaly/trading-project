"""
Polymarket resolution resolver with token ID normalization.

Resolves the mismatch between numeric token IDs in resolution.csv
(e.g. ``"1059799830661903"``) and hex strings in data API trades
(e.g. ``"0x4bfb..."``). Both represent the same token in different
encodings.

Usage::

    from trading_platform.polymarket.resolution_resolver import ResolutionResolver
    resolver = ResolutionResolver(Path("data/polymarket/blockchain/resolution.csv"))
    price = resolver.resolve("0x4bfb...")  # returns 100.0, 0.0, or None
    won = resolver.is_winner("0x4bfb...", "BUY")  # True/False/None
"""
from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def normalize_token_id(token_id: str) -> str:
    """Normalize a token ID to a canonical decimal string.

    Accepts decimal strings, hex strings (with or without ``0x`` prefix).

    >>> normalize_token_id("1059799830661903")
    '1059799830661903'
    >>> normalize_token_id("0x3c3db7")
    '3948471'
    >>> normalize_token_id("3c3db7")
    '3948471'
    """
    s = str(token_id).strip()
    if not s:
        return ""

    # Already a decimal string
    if s.isdigit():
        return s

    # 0x-prefixed hex
    if s.lower().startswith("0x"):
        try:
            return str(int(s, 16))
        except ValueError:
            return s

    # Try as hex without prefix
    try:
        int(s, 16)
        return str(int(s, 16))
    except ValueError:
        return s


class ResolutionResolver:
    """Resolves market outcomes with token ID normalization."""

    def __init__(self, resolution_csv: str | Path) -> None:
        self._resolutions: dict[str, float] = {}
        self._by_condition: dict[str, float] = {}
        path = Path(resolution_csv)
        if not path.exists():
            logger.warning("Resolution CSV not found: %s", path)
            return

        with path.open(newline="", encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                ticker = row.get("ticker", "").strip()
                rp = row.get("resolution_price", "").strip()
                if not ticker or not rp:
                    continue
                try:
                    price = float(rp)
                except ValueError:
                    continue
                # Store under normalized ticker key
                normalized = normalize_token_id(ticker)
                self._resolutions[normalized] = price
                # Also index by condition_id if available
                cond_id = row.get("condition_id", "").strip()
                if cond_id:
                    self._by_condition[cond_id.lower()] = price

        logger.info("Loaded %d resolutions (%d by condition_id) from %s",
                     len(self._resolutions), len(self._by_condition), path)

    @property
    def count(self) -> int:
        return len(self._resolutions)

    def resolve(self, token_id: str, *, condition_id: str | None = None) -> float | None:
        """Return resolution price (100.0=YES, 0.0=NO) or None if unknown.

        Checks by token_id first, then by condition_id if available.
        """
        key = normalize_token_id(token_id)
        result = self._resolutions.get(key)
        if result is not None:
            return result
        # Fall back to condition_id lookup
        if condition_id:
            return self._by_condition.get(condition_id.strip().lower())
        return None

    def is_winner(self, token_id: str, side: str) -> bool | None:
        """Determine if a trade on the given side won.

        ``side`` should be ``"BUY"`` or ``"SELL"`` (case-insensitive).
        For BUY trades: win if resolution matches the token's outcome.
        Returns None if resolution is unknown.
        """
        price = self.resolve(token_id)
        if price is None:
            return None
        resolved_yes = price >= 99.0
        side_upper = side.strip().upper()
        if side_upper == "BUY":
            return resolved_yes
        elif side_upper == "SELL":
            return not resolved_yes
        return None

    def overlap_with(self, token_ids: set[str]) -> tuple[int, set[str], set[str]]:
        """Compute overlap between resolution keys and a set of token IDs.

        Returns (overlap_count, unmatched_resolutions, unmatched_tokens).
        """
        normalized_tokens = {normalize_token_id(t) for t in token_ids}
        resolution_keys = set(self._resolutions.keys())
        overlap = resolution_keys & normalized_tokens
        unmatched_res = resolution_keys - normalized_tokens
        unmatched_tok = normalized_tokens - resolution_keys
        return len(overlap), unmatched_res, unmatched_tok
