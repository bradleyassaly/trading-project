"""
Kelly criterion position sizer.

Reads measured win rate, average win and average loss from
``signal_outcomes`` for a given signal type and returns a
fractional-Kelly trade size in USDC. Conservative defaults: 25% of
full Kelly, capped at $100 per trade.

Falls back to the legacy ``polymarket_paper_trades`` reader if
``signal_outcomes`` is missing or has fewer than 5 resolved rows for
the requested signal type.
"""
from __future__ import annotations

import sqlite3
from typing import Any


class KellySizer:
    """Compute trade size from real outcome history."""

    KELLY_FRACTION = 0.25
    MAX_PCT_OF_BANKROLL = 0.02
    MIN_TRADE_USD = 10
    MAX_TRADE_USD = 250
    MIN_LIVE_SAMPLE = 10    # below this we only return the probe size
    BANKROLL = 350          # live bankroll

    def __init__(self, db_path: str, bankroll: float | None = None) -> None:
        self._db_path = str(db_path)
        if bankroll is not None:
            self.BANKROLL = bankroll

    def _fetch_outcomes(
        self,
        signal_type: str,
        category: str | None = None,
    ) -> list[tuple[float, int]]:
        """Return [(outcome_delta, is_win), ...] from signal_outcomes."""
        try:
            conn = sqlite3.connect(self._db_path)
        except Exception:
            return []
        try:
            if category:
                rows = conn.execute(
                    """SELECT outcome_delta, is_win FROM signal_outcomes
                       WHERE signal_type = ? AND category = ?
                         AND resolution_price IS NOT NULL
                         AND outcome_delta IS NOT NULL""",
                    (signal_type, category),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT outcome_delta, is_win FROM signal_outcomes
                       WHERE signal_type = ?
                         AND resolution_price IS NOT NULL
                         AND outcome_delta IS NOT NULL""",
                    (signal_type,),
                ).fetchall()
        except sqlite3.OperationalError:
            # signal_outcomes missing — fall back to polymarket_paper_trades
            rows = conn.execute(
                """SELECT return_pct/100.0,
                          CASE WHEN outcome='win' THEN 1 ELSE 0 END
                   FROM polymarket_paper_trades
                   WHERE signal_type = ? AND archived = 0
                     AND exit_ts IS NOT NULL AND return_pct IS NOT NULL""",
                (signal_type,),
            ).fetchall()
        finally:
            conn.close()
        return [(float(r[0]), int(r[1] or 0)) for r in rows if r[0] is not None]

    def compute_kelly(
        self,
        signal_type: str,
        category: str | None = None,
    ) -> dict[str, Any]:
        """Compute fractional Kelly + recommended USD size for a signal type."""
        rows = self._fetch_outcomes(signal_type, category)

        n = len(rows)
        if n < 5:
            return {
                "kelly_full": 0.0, "kelly_fraction": self.KELLY_FRACTION,
                "kelly_fractional": 0.0,
                "recommended_usd": float(self.MIN_TRADE_USD),
                "n": n, "wr": None, "ev": None,
                "avg_win": None, "avg_loss": None,
                "reason": "Insufficient data (need ≥5 resolved outcomes)",
            }

        wins = [delta for delta, is_win in rows if is_win == 1]
        losses = [abs(delta) for delta, is_win in rows if is_win == 0]

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
        # If the edge is negative, recommended_usd is 0 — caller should skip.
        if full_kelly <= 0:
            recommended = 0.0
        else:
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
            "reason": None if full_kelly > 0 else "negative edge",
        }

    def get_trade_size(
        self,
        signal_type: str,
        confidence: float,
        category: str | None = None,
    ) -> float:
        """Recommended trade size in USD, scaled by signal confidence.

        Returns 0 when Kelly reports negative edge — the caller should
        skip the trade rather than size it at the minimum.
        """
        kelly = self.compute_kelly(signal_type, category)
        base = float(kelly.get("recommended_usd") or 0)
        if base <= 0:
            return 0.0
        # Scale 0.5x at low confidence → 1x at high confidence
        scale = 0.5 + 0.5 * min(max(confidence, 0.0) / 0.8, 1.0)
        sized = base * scale
        return round(min(self.MAX_TRADE_USD, max(self.MIN_TRADE_USD, sized)), 2)

    def get_sizing_report(self) -> dict[str, Any]:
        """Return sizing recommendations for every signal_type with resolved data."""
        try:
            conn = sqlite3.connect(self._db_path)
        except Exception as exc:
            return {"error": str(exc)}
        try:
            types = [
                r[0] for r in conn.execute(
                    """SELECT DISTINCT signal_type FROM signal_outcomes
                       WHERE resolution_price IS NOT NULL AND signal_type IS NOT NULL"""
                ).fetchall()
            ]
        except sqlite3.OperationalError:
            types = []
        finally:
            conn.close()

        out: dict[str, Any] = {}
        for st in sorted(types):
            kelly = self.compute_kelly(st)
            out[st] = {
                "n": kelly.get("n"),
                "win_rate": kelly.get("wr"),
                "ev": kelly.get("ev"),
                "kelly_full": kelly.get("kelly_full"),
                "size_at_conf_50": self.get_trade_size(st, 0.5),
                "size_at_conf_70": self.get_trade_size(st, 0.7),
                "size_at_conf_90": self.get_trade_size(st, 0.9),
                "would_trade": (kelly.get("kelly_full") or 0) > 0
                               and (kelly.get("n") or 0) >= self.MIN_LIVE_SAMPLE,
            }
        return out
