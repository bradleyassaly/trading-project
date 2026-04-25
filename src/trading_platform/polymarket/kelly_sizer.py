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

from trading_platform.polymarket.db_connection import get_connection


class KellySizer:
    """Compute trade size from real outcome history."""

    KELLY_FRACTION = 0.25
    MAX_PCT_OF_BANKROLL = 0.02
    MIN_TRADE_USD = 10
    MAX_TRADE_USD = 250
    MIN_LIVE_SAMPLE = 10    # below this we only return the probe size

    def __init__(self, db_path: str, bankroll: float | None = None) -> None:
        self._db_path = str(db_path)
        if bankroll is not None:
            self.BANKROLL = bankroll
        else:
            # Pull live bankroll from the bankroll module (falls back to
            # env var POLYMARKET_LIVE_BANKROLL_USD or default if live
            # fetch fails). Recomputed on each KellySizer instance so
            # operators can refresh without restarting services.
            from trading_platform.polymarket.bankroll import get_bankroll
            self.BANKROLL = get_bankroll()

    def _calibrated_kelly_fraction(self, signal_type: str) -> float | None:
        """Return SignalEvaluator-calibrated kelly_fraction if it exists.

        Wired 2026-04-24: previously KellySizer used a static 0.25 for every
        signal type, ignoring the SignalEvaluator's per-signal calibration
        already living in `signal_calibration`. This made graduated signals
        size identically to brand-new ones. Returns None when no row is
        present, in which case the caller falls back to KELLY_FRACTION.
        """
        try:
            conn = get_connection(self._db_path)
        except Exception:
            return None
        try:
            row = conn.execute(
                "SELECT kelly_fraction FROM signal_calibration "
                "WHERE signal_type = ? AND kelly_fraction IS NOT NULL",
                (signal_type,),
            ).fetchone()
        except sqlite3.OperationalError:
            row = None
        finally:
            try:
                conn.close()
            except Exception:
                pass
        if not row or row[0] is None:
            return None
        try:
            f = float(row[0])
        except (TypeError, ValueError):
            return None
        # Sanity-clamp: anything outside [0.05, 1.0] is calibration noise.
        if 0.05 <= f <= 1.0:
            return f
        return None

    def _fetch_outcomes(
        self,
        signal_type: str,
        category: str | None = None,
    ) -> list[tuple[float, int]]:
        """Return [(outcome_delta, is_win), ...].

        Primary source: signal_outcomes (live-fired signals with resolutions).
        Fallback: signal_backtest_results — if a signal type has <5 live
        resolved outcomes but has backtest evidence, use the backtest's
        (ev, wr, resolved) to synthesise pseudo-rows so Kelly has something
        to size against. Backtest rows are under-weighted by using only
        50% of their count — same approach as the kill switch blend.
        """
        try:
            conn = get_connection(self._db_path)
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
            rows = []
        # Fallback to polymarket_paper_trades when signal_outcomes has no
        # resolutions for this signal_type. The live_readiness endpoint
        # reads from paper_trades while Kelly historically read only from
        # signal_outcomes — leading to the "ready but Kelly=$0" mismatch
        # that blocked every whale_entry_filtered auto-live fire.
        # (Prior to 2026-04-18 this fallback only triggered on SQLite
        # OperationalError, which never fires under Postgres.)
        if not rows:
            try:
                rows = conn.execute(
                    """SELECT realized_pnl / NULLIF(size_usd, 0),
                              CASE WHEN outcome='win' THEN 1 ELSE 0 END
                       FROM polymarket_paper_trades
                       WHERE signal_type = ? AND archived = 0
                         AND exit_ts IS NOT NULL
                         AND realized_pnl IS NOT NULL AND size_usd > 0""",
                    (signal_type,),
                ).fetchall()
            except Exception:
                rows = []
        try:
            conn.close()
        except Exception:
            pass
        return [(float(r[0]), int(r[1] or 0)) for r in rows if r[0] is not None]

    def _fetch_backtest_stats(self, signal_type: str) -> dict[str, Any] | None:
        """Pull the most recent backtest aggregate for a signal type."""
        try:
            conn = get_connection(self._db_path)
        except Exception:
            return None
        try:
            row = conn.execute(
                "SELECT resolved, wr, ev FROM signal_backtest_results "
                "WHERE signal_type = ? ORDER BY run_ts DESC LIMIT 1",
                (signal_type,),
            ).fetchone()
        except Exception:
            row = None
        finally:
            try:
                conn.close()
            except Exception:
                pass
        if not row or row[0] is None or row[1] is None or row[2] is None:
            return None
        return {"n": int(row[0]), "wr": float(row[1]), "ev": float(row[2])}

    def compute_kelly(
        self,
        signal_type: str,
        category: str | None = None,
    ) -> dict[str, Any]:
        """Compute fractional Kelly + recommended USD size for a signal type.

        Data priority:
          1. Live signal_outcomes (primary)
          2. If fewer than MIN_LIVE_SAMPLE live rows, pull backtest
             aggregate (wr, ev, resolved) from signal_backtest_results
             and use it directly — backtest is downweighted via
             weighted blend, but we trust its (wr, ev) since those
             come from real market resolutions against replayed fires.
        """
        rows = self._fetch_outcomes(signal_type, category)
        n = len(rows)

        bt_stats = self._fetch_backtest_stats(signal_type)

        # If live data is thin, fall back to backtest aggregate directly.
        if n < 5:
            if bt_stats is None or bt_stats["n"] < 10:
                return {
                    "kelly_full": 0.0, "kelly_fraction": self.KELLY_FRACTION,
                    "kelly_fractional": 0.0,
                    "recommended_usd": 0.0,
                    "n": n, "wr": None, "ev": None,
                    "avg_win": None, "avg_loss": None,
                    "reason": f"Insufficient data (live n={n}, backtest n={bt_stats['n'] if bt_stats else 0})",
                }
            # Use the backtest aggregate as if it were live data.
            wr = bt_stats["wr"]
            ev = bt_stats["ev"]
            # Kelly needs (avg_win, avg_loss) but (wr, ev) alone is
            # under-determined. Assume symmetric outcome magnitudes as
            # a starting point — works out mathematically to:
            #   avg = |ev| / (2*wr - 1)   when wr > 0.5
            # Then avg_win = avg_loss = avg. This is conservative because
            # it treats both legs equally; actual markets tend to have
            # asymmetric payoffs which Kelly will discover from live
            # data once enough live_outcomes accumulate.
            if wr <= 0.5 or ev <= 0:
                avg_win, avg_loss = 0.10, 0.10   # Kelly will return 0 anyway
            else:
                avg = abs(ev) / max(2 * wr - 1, 0.01)
                avg_win = avg_loss = max(0.05, min(0.95, avg))
            n_effective = bt_stats["n"] // 2  # half-weight backtest
        else:
            wins = [delta for delta, is_win in rows if is_win == 1]
            losses = [abs(delta) for delta, is_win in rows if is_win == 0]
            wr = len(wins) / n if n > 0 else 0.0
            avg_win = sum(wins) / len(wins) if wins else 0.0
            avg_loss = sum(losses) / len(losses) if losses else 0.0
            ev = wr * avg_win - (1 - wr) * avg_loss
            n_effective = n
        n = n_effective

        # Kelly fraction f* = (p*b - q) / b where b = avg_win/avg_loss
        if avg_win > 0 and avg_loss > 0:
            b = avg_win / avg_loss
            full_kelly = (wr * b - (1 - wr)) / b
        else:
            full_kelly = 0.0
        full_kelly = max(0.0, full_kelly)

        # Use SignalEvaluator's calibrated kelly_fraction when present.
        # Falls back to the static 0.25 for any signal type the evaluator
        # hasn't yet measured. This unblocks graduated signal types that
        # were sizing as if brand-new.
        calibrated_frac = self._calibrated_kelly_fraction(signal_type)
        kelly_frac_used = calibrated_frac if calibrated_frac is not None else self.KELLY_FRACTION
        fractional_kelly = full_kelly * kelly_frac_used
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
            "kelly_fraction": round(kelly_frac_used, 4),
            "kelly_fraction_source": "calibration" if calibrated_frac is not None else "default",
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
            conn = get_connection(self._db_path)
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
