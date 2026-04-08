"""
Bayesian signal performance evaluator.

For each signal type, maintains a rolling performance model in the
``signal_calibration`` table. After every resolved paper trade, call
:meth:`SignalEvaluator.update_one` (or :meth:`SignalEvaluator.update_all`
for a full backfill) to recompute:

  * Bayesian win rate (Beta(1+wins, 1+losses) posterior mean)
  * Expected value per trade (mean return %, fraction)
  * Profit factor (sum of wins / abs(sum of losses))
  * Sharpe-like ratio (mean / std of return %, only if N >= 10)
  * Kelly fraction (clamped to [0, 0.25])
  * Rolling 10-trade WR + Rolling 20-trade EV
  * Consecutive losses tracker

Status machine
--------------
::

    building --> live      sample_size >= 15
                           AND bayesian_wr >  0.52
                           AND profit_factor > 1.2

    live     --> weak      rolling_10_wr < 0.45
                           OR  profit_factor < 0.8

    weak     --> disabled  consecutive_losses >= 5
                           OR  rolling_20_ev < -0.10

    disabled --> building  manual re-enable only (CLI)

Source of truth: ``polymarket_paper_trades`` (archived = 0,
exit_ts IS NOT NULL, return_pct IS NOT NULL).
"""
from __future__ import annotations

import logging
import math
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# All signal types that exist in the system. Mirrors SIGNAL_BANKROLL.
ALL_SIGNAL_TYPES = (
    "wallet_reversal", "cascade", "oversized_bet",
    "accumulation", "market_maker_flip", "convergence",
    "price_velocity", "specialist_entry", "whale_exit",
    "no_position_entry", "pre_deadline_surge",
    "position_reduction", "whale_entry",
)

# Status thresholds — see module docstring
PROMOTE_MIN_SAMPLE = 15
PROMOTE_MIN_BAYESIAN_WR = 0.52
PROMOTE_MIN_PF = 1.2

DEMOTE_LIVE_ROLLING_10_WR = 0.45
DEMOTE_LIVE_PF = 0.8

DISABLE_CONSEC_LOSSES = 5
DISABLE_ROLLING_20_EV = -0.10

# Kelly clamping bounds
KELLY_MIN = 0.0
KELLY_MAX = 0.25


def beta_posterior_mean(wins: int, losses: int, alpha: float = 1.0, beta: float = 1.0) -> float:
    """Mean of a Beta(alpha+wins, beta+losses) posterior."""
    a = alpha + max(0, wins)
    b = beta + max(0, losses)
    if a + b <= 0:
        return 0.5
    return a / (a + b)


def kelly_fraction(wr: float, avg_win: float, avg_loss: float) -> float:
    """Full-Kelly criterion clamped to [KELLY_MIN, KELLY_MAX].

    ``avg_win`` and ``avg_loss`` are positive return fractions
    (e.g. 0.5 for a 50% win, 0.4 for a 40% loss). Returns 0 if the
    edge is negative or the inputs are degenerate.
    """
    if wr is None or wr <= 0 or wr >= 1:
        return 0.0
    if avg_win <= 0 or avg_loss <= 0:
        return 0.0
    b = avg_win / avg_loss
    f = (wr * b - (1 - wr)) / b
    return max(KELLY_MIN, min(KELLY_MAX, f))


def transition_status(
    current_status: str,
    sample_size: int,
    bayesian_wr: float | None,
    profit_factor: float | None,
    rolling_10_wr: float | None,
    rolling_20_ev: float | None,
    consecutive_losses: int,
) -> str:
    """Run the status state machine for one signal type."""
    if current_status == "disabled":
        # Re-enable is manual only
        return "disabled"

    # Demote → disabled
    if (
        current_status in ("live", "weak", "building")
        and consecutive_losses >= DISABLE_CONSEC_LOSSES
        and sample_size >= DISABLE_CONSEC_LOSSES
    ):
        return "disabled"
    if (
        current_status in ("live", "weak")
        and rolling_20_ev is not None
        and rolling_20_ev <= DISABLE_ROLLING_20_EV
    ):
        return "disabled"

    # Demote live → weak
    if current_status == "live":
        if (
            (rolling_10_wr is not None and rolling_10_wr < DEMOTE_LIVE_ROLLING_10_WR)
            or (profit_factor is not None and profit_factor < DEMOTE_LIVE_PF)
        ):
            return "weak"
        return "live"

    # Promote building/weak → live (require enough sample + WR + PF)
    if (
        sample_size >= PROMOTE_MIN_SAMPLE
        and bayesian_wr is not None and bayesian_wr > PROMOTE_MIN_BAYESIAN_WR
        and profit_factor is not None and profit_factor > PROMOTE_MIN_PF
    ):
        return "live"

    # Stay where you are otherwise
    return current_status if current_status in ("building", "weak") else "building"


@dataclass
class CalibrationRow:
    signal_type: str
    sample_size: int
    wins: int
    losses: int
    bayesian_wr: float | None
    ev_per_trade: float | None
    profit_factor: float | None
    sharpe_ratio: float | None
    kelly_fraction: float | None
    rolling_10_wr: float | None
    rolling_20_ev: float | None
    consecutive_losses: int
    status: str

    def to_dict(self) -> dict[str, Any]:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


class SignalEvaluator:
    """Computes calibration metrics from polymarket_paper_trades."""

    def __init__(self, db_path: str) -> None:
        self._db_path = str(db_path)

    def _fetch_resolved(self, conn: sqlite3.Connection, signal_type: str) -> list[tuple[float, str]]:
        """Return (return_pct, outcome) tuples ordered by exit_ts ASC."""
        rows = conn.execute(
            """SELECT return_pct, outcome FROM polymarket_paper_trades
               WHERE signal_type = ? AND archived = 0
                 AND exit_ts IS NOT NULL AND return_pct IS NOT NULL
               ORDER BY exit_ts ASC""",
            (signal_type,),
        ).fetchall()
        return [(float(r[0]), r[1] or "") for r in rows]

    def compute_metrics(self, signal_type: str) -> CalibrationRow:
        """Compute the full metric set for one signal type. Pure read."""
        conn = sqlite3.connect(self._db_path)
        try:
            trades = self._fetch_resolved(conn, signal_type)
            existing_status = self._read_status(conn, signal_type)
        finally:
            conn.close()

        n = len(trades)
        wins = sum(1 for _, o in trades if o == "win")
        losses = n - wins

        if n == 0:
            return CalibrationRow(
                signal_type=signal_type, sample_size=0, wins=0, losses=0,
                bayesian_wr=None, ev_per_trade=None, profit_factor=None,
                sharpe_ratio=None, kelly_fraction=None,
                rolling_10_wr=None, rolling_20_ev=None,
                consecutive_losses=0, status=existing_status or "building",
            )

        # Convert pct to fraction
        returns = [r / 100.0 for r, _ in trades]
        win_returns = [r for r in returns if r > 0]
        loss_returns = [abs(r) for r in returns if r < 0]

        bayesian_wr = beta_posterior_mean(wins, losses)
        ev = sum(returns) / n

        if loss_returns and sum(loss_returns) > 0:
            profit_factor = sum(win_returns) / sum(loss_returns)
        elif win_returns:
            profit_factor = float("inf")
        else:
            profit_factor = 0.0

        sharpe = None
        if n >= 10:
            mean = ev
            var = sum((r - mean) ** 2 for r in returns) / n
            std = math.sqrt(var)
            if std > 0:
                sharpe = mean / std

        avg_win = (sum(win_returns) / len(win_returns)) if win_returns else 0.0
        avg_loss = (sum(loss_returns) / len(loss_returns)) if loss_returns else 0.0
        kelly = kelly_fraction(bayesian_wr, avg_win, avg_loss)

        # Rolling windows
        last_10 = trades[-10:]
        rolling_10_wr = (
            sum(1 for _, o in last_10 if o == "win") / len(last_10)
            if last_10 else None
        )
        last_20 = returns[-20:]
        rolling_20_ev = (sum(last_20) / len(last_20)) if last_20 else None

        # Consecutive losses (from the end)
        consec = 0
        for _, o in reversed(trades):
            if o == "loss":
                consec += 1
            else:
                break

        # Cap profit_factor for storage (inf can't be JSON-serialized)
        pf_stored = round(profit_factor, 4) if math.isfinite(profit_factor) else 99.0

        new_status = transition_status(
            current_status=existing_status or "building",
            sample_size=n,
            bayesian_wr=bayesian_wr,
            profit_factor=pf_stored,
            rolling_10_wr=rolling_10_wr,
            rolling_20_ev=rolling_20_ev,
            consecutive_losses=consec,
        )

        return CalibrationRow(
            signal_type=signal_type,
            sample_size=n,
            wins=wins,
            losses=losses,
            bayesian_wr=round(bayesian_wr, 4),
            ev_per_trade=round(ev, 4),
            profit_factor=pf_stored,
            sharpe_ratio=round(sharpe, 4) if sharpe is not None else None,
            kelly_fraction=round(kelly, 4),
            rolling_10_wr=round(rolling_10_wr, 4) if rolling_10_wr is not None else None,
            rolling_20_ev=round(rolling_20_ev, 4) if rolling_20_ev is not None else None,
            consecutive_losses=consec,
            status=new_status,
        )

    def _read_status(self, conn: sqlite3.Connection, signal_type: str) -> str | None:
        row = conn.execute(
            "SELECT status FROM signal_calibration WHERE signal_type = ?",
            (signal_type,),
        ).fetchone()
        return row[0] if row else None

    def update_one(self, signal_type: str) -> tuple[CalibrationRow, str | None]:
        """Recompute and persist calibration for one signal type.

        Returns (new_row, prev_status). ``prev_status`` is None on first
        write. Use the prev/new status pair to fire transition alerts.
        """
        conn = sqlite3.connect(self._db_path)
        try:
            prev_status = self._read_status(conn, signal_type)
        finally:
            conn.close()

        row = self.compute_metrics(signal_type)
        self._write(row)
        return row, prev_status

    def update_all(self) -> list[tuple[CalibrationRow, str | None]]:
        """Recompute every known signal type. Returns rows + prev statuses."""
        results: list[tuple[CalibrationRow, str | None]] = []
        for sig in ALL_SIGNAL_TYPES:
            try:
                results.append(self.update_one(sig))
            except Exception as exc:
                logger.warning("calibration update failed for %s: %s", sig, exc)
        return results

    def _write(self, row: CalibrationRow) -> None:
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute(
                """INSERT INTO signal_calibration
                    (signal_type, sample_size, wins, losses, bayesian_wr,
                     ev_per_trade, profit_factor, sharpe_ratio, kelly_fraction,
                     rolling_10_wr, rolling_20_ev, consecutive_losses,
                     status, last_updated)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(signal_type) DO UPDATE SET
                       sample_size=excluded.sample_size,
                       wins=excluded.wins,
                       losses=excluded.losses,
                       bayesian_wr=excluded.bayesian_wr,
                       ev_per_trade=excluded.ev_per_trade,
                       profit_factor=excluded.profit_factor,
                       sharpe_ratio=excluded.sharpe_ratio,
                       kelly_fraction=excluded.kelly_fraction,
                       rolling_10_wr=excluded.rolling_10_wr,
                       rolling_20_ev=excluded.rolling_20_ev,
                       consecutive_losses=excluded.consecutive_losses,
                       status=excluded.status,
                       last_updated=excluded.last_updated""",
                (
                    row.signal_type, row.sample_size, row.wins, row.losses,
                    row.bayesian_wr, row.ev_per_trade, row.profit_factor,
                    row.sharpe_ratio, row.kelly_fraction,
                    row.rolling_10_wr, row.rolling_20_ev,
                    row.consecutive_losses, row.status, int(time.time()),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def manual_enable(self, signal_type: str) -> None:
        """Reset a signal back to ``building`` so it can re-accumulate."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute(
                """UPDATE signal_calibration
                   SET status='building', consecutive_losses=0, last_updated=?
                   WHERE signal_type=?""",
                (int(time.time()), signal_type),
            )
            conn.commit()
        finally:
            conn.close()

    def manual_disable(self, signal_type: str) -> None:
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute(
                """INSERT INTO signal_calibration (signal_type, status, last_updated)
                   VALUES (?, 'disabled', ?)
                   ON CONFLICT(signal_type) DO UPDATE SET
                       status='disabled', last_updated=excluded.last_updated""",
                (signal_type, int(time.time())),
            )
            conn.commit()
        finally:
            conn.close()
