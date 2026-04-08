"""
Kelly criterion position sizer.

Reads measured win rate, average win and average loss from
``polymarket_paper_trades`` for a given signal type and returns a
fractional-Kelly trade size in USDC. Conservative defaults: 25% of
full Kelly, capped at $100 per trade.
"""
from __future__ import annotations

import sqlite3
from typing import Any


class KellySizer:
    """Compute trade size from real outcome history."""

    KELLY_FRACTION = 0.25
    MAX_PCT_OF_BANKROLL = 0.02
    MIN_TRADE_USD = 10
    MAX_TRADE_USD = 100
    BANKROLL = 100_000

    def __init__(self, db_path: str) -> None:
        self._db_path = str(db_path)

    def compute_kelly(self, signal_type: str) -> dict[str, Any]:
        """Compute fractional Kelly + recommended USD size for a signal type."""
        try:
            conn = sqlite3.connect(self._db_path)
            try:
                rows = conn.execute(
                    """SELECT return_pct, outcome
                       FROM polymarket_paper_trades
                       WHERE signal_type = ? AND archived = 0
                         AND exit_ts IS NOT NULL AND return_pct IS NOT NULL""",
                    (signal_type,),
                ).fetchall()
            finally:
                conn.close()
        except Exception as exc:
            return {
                "kelly_full": 0.0, "kelly_fraction": self.KELLY_FRACTION,
                "kelly_fractional": 0.0,
                "recommended_usd": float(self.MIN_TRADE_USD),
                "n": 0, "wr": None, "ev": None,
                "avg_win": None, "avg_loss": None,
                "reason": f"DB error: {exc}",
            }

        n = len(rows)
        if n < 5:
            return {
                "kelly_full": 0.0, "kelly_fraction": self.KELLY_FRACTION,
                "kelly_fractional": 0.0,
                "recommended_usd": float(self.MIN_TRADE_USD),
                "n": n, "wr": None, "ev": None,
                "avg_win": None, "avg_loss": None,
                "reason": "Insufficient data (need ≥5 resolved trades)",
            }

        # Convert pct to fraction. Wins have positive return_pct, losses negative.
        wins = [r[0] / 100.0 for r in rows if (r[1] or "") == "win"]
        losses = [abs(r[0] / 100.0) for r in rows if (r[1] or "") == "loss"]

        wr = len(wins) / n if n > 0 else 0.0
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        ev = wr * avg_win - (1 - wr) * avg_loss

        # Kelly fraction f* = (p*b - q) / b where b = avg_win/avg_loss
        if avg_win > 0 and avg_loss > 0:
            b = avg_win / avg_loss
            full_kelly = (wr * b - (1 - wr)) / b
        else:
            full_kelly = 0.0
        full_kelly = max(0.0, full_kelly)

        fractional_kelly = full_kelly * self.KELLY_FRACTION
        # Cap by absolute MAX_PCT_OF_BANKROLL
        capped = min(fractional_kelly, self.MAX_PCT_OF_BANKROLL)

        kelly_usd = self.BANKROLL * capped
        recommended = max(self.MIN_TRADE_USD, min(self.MAX_TRADE_USD, kelly_usd))

        return {
            "kelly_full": round(full_kelly, 4),
            "kelly_fraction": self.KELLY_FRACTION,
            "kelly_fractional": round(fractional_kelly, 4),
            "kelly_capped": round(capped, 4),
            "recommended_usd": round(recommended, 2),
            "n": n,
            "wr": round(wr, 3),
            "ev": round(ev, 4),
            "avg_win": round(avg_win, 4),
            "avg_loss": round(avg_loss, 4),
            "reason": None,
        }

    def get_trade_size(self, signal_type: str, confidence: float) -> float:
        """Recommended trade size in USD, scaled by signal confidence."""
        kelly = self.compute_kelly(signal_type)
        base = float(kelly.get("recommended_usd") or self.MIN_TRADE_USD)
        # Scale 0.5x at low confidence → 1x at high confidence
        scale = 0.5 + 0.5 * min(max(confidence, 0.0) / 0.8, 1.0)
        sized = base * scale
        return round(min(self.MAX_TRADE_USD, max(self.MIN_TRADE_USD, sized)), 2)
