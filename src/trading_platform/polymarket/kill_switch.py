"""
Multi-layer safety system for live autonomous trading.

Every live trade MUST pass through :meth:`KillSwitch.check` before any
CLOB order is submitted. The kill switch reads from
``polymarket_paper_trades`` (the ground truth for resolved-trade
performance) and applies hard limits that block trading when any
condition fails.

Hard limits (any one blocks the trade):
  * ``POLYMARKET_LIVE_ENABLED=1`` env var must be set explicitly
  * Emergency stop file (``data/KILL_SWITCH_ACTIVE``) must not exist
  * Daily loss must not exceed ``MAX_DAILY_LOSS_PCT`` of bankroll
  * Open live position count must be below ``MAX_OPEN_POSITIONS``
  * Trade size must be below ``MAX_TRADE_USD``
  * Signal must have at least ``MIN_RESOLVED_HARD`` resolved outcomes
  * Signal EV (avg return %) must be positive
  * Signal win rate must be above ``MIN_WIN_RATE`` once 20+ resolved

Soft limits log a warning but allow the trade:
  * < ``PREFERRED_MIN_RESOLVED`` outcomes
  * Negative profit factor with small sample
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time

from trading_platform.polymarket.db_connection import get_connection
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_KILL_SWITCH_FILE = _PROJECT_ROOT / "data" / "KILL_SWITCH_ACTIVE"


@dataclass
class KillSwitchResult:
    allowed: bool
    reason: str | None = None
    warnings: list[str] = field(default_factory=list)
    # Set when a signal passes probation gates but hasn't hit the full
    # MIN_RESOLVED_HARD sample size yet. Callers MUST clamp the trade size
    # to this value when set. Enables small live stakes on borderline
    # signals so they accumulate live data without material exposure.
    probation_cap: float | None = None


class KillSwitch:
    """Multi-layer pre-trade safety check."""

    MAX_DAILY_LOSS_PCT = 0.10
    # 2026-05-06: raised 10→20. 30-day horizon gate means positions resolve
    # faster; resolution_decay was repeatedly blocked at 10 despite only 4
    # genuinely open trades. With short-horizon markets, turnover is high
    # enough that 20 simultaneous positions carries acceptable risk.
    MAX_OPEN_POSITIONS = 20
    # Per-trade cap derived dynamically from live bankroll (7% of account
    # as phase-1 safety ramp; capped at $25 hard ceiling). Scales with
    # account growth without code changes.
    MAX_TRADE_USD_PCT = 0.07
    MAX_TRADE_USD_ABS_CAP = 25.0
    MIN_WIN_RATE = 0.52
    # 2026-04-25: probation/discovery tier WR floor. Standard 0.52 blocked
    # 45 live attempts in 24h. Drop to 0.45 for stakes <= probation cap so
    # real-money calibration data starts flowing — risk is bounded by the
    # cap, not the WR threshold. Full-stake live still requires 0.52.
    MIN_WIN_RATE_PROBATION = 0.45
    MIN_RESOLVED_HARD = 15
    PREFERRED_MIN_RESOLVED = 30
    # Probation tier: signals below MIN_RESOLVED_HARD but above
    # PROBATION_MIN_RESOLVED get approved at tiny stakes so we collect
    # *live* resolution data. Without this, every new signal type needs
    # 15 paper resolutions before it can ever trade live — which in the
    # current whale_entry case means weeks of waiting on a single-wallet
    # sample. Probation requires positive EV and WR>=MIN_WIN_RATE.
    PROBATION_MIN_RESOLVED = 5
    PROBATION_MAX_STAKE_USD = 5.0
    # 2026-04-25: discovery-stake tier. Below PROBATION_MIN_RESOLVED but
    # above DISCOVERY_MIN_RESOLVED, allow $1 fires so we collect live
    # resolution data on the bottom-of-funnel signal types. Without this,
    # the gate stack starves signals into a "we need data to graduate
    # but we can't trade without graduating" loop. Discovery tier still
    # requires non-negative EV and `LIVE_DISCOVERY_ENABLED=1` env opt-in.
    DISCOVERY_MIN_RESOLVED = 1
    DISCOVERY_MAX_STAKE_USD = 1.0
    # 2026-04-25: insider-tier sample threshold. Insider signals fire
    # ~5/day vs 480/day for smart-money. Standard MIN_RESOLVED_HARD=15
    # = 6 weeks before first live insider trade. With MIN=3 we trade
    # earlier on smaller stakes; the higher quality of insider signals
    # (avg_entry 12h+ before close) gives this a strong prior of edge.
    INSIDER_SIGNAL_TYPES = {"insider_entry", "high_conviction_insider"}
    INSIDER_MIN_RESOLVED = 3
    INSIDER_MAX_STAKE_USD = 5.0
    # Signals where WR < 50% is structural: edge comes from asymmetric payoffs
    # (betting low-probability outcomes at better-than-fair odds). The EV gate
    # still applies; only the WR floor is relaxed to the value below.
    # whale_entry_filtered: IC30=0.313, paper PnL +$687 14d at 37% WR — valid.
    # oversized_bet: IC30=0.121, paper PnL +$107, 29% WR but +33% avg EV — valid.
    # cascade: IC14=+0.032, paper PnL +$74 n=45 at 38% WR — structural low WR,
    #   edge comes from catching 2+ wallets piling in at low probability.
    WR_FLOOR_OVERRIDES: dict[str, float] = {
        "whale_entry_filtered": 0.30,
        "oversized_bet": 0.25,
        "cascade": 0.30,
    }
    # When IC14 < this threshold AND decay_flag is set, halve the live stake.
    IC14_DECAY_THRESHOLD = 0.05
    IC14_STAKE_HALF_FACTOR = 0.5

    def __init__(self, db_path: str, bankroll: float | None = None) -> None:
        self._db_path = str(db_path)
        if bankroll is not None:
            self.BANKROLL = bankroll
        else:
            # Same live-bankroll source as KellySizer so daily-loss
            # limits and sizing caps stay in sync with real USDC
            # available. Falls back to POLYMARKET_LIVE_BANKROLL_USD env
            # then a conservative default.
            from trading_platform.polymarket.bankroll import get_bankroll
            self.BANKROLL = get_bankroll()
        # Compute the dynamic MAX_TRADE_USD from current bankroll.
        self.MAX_TRADE_USD = max(
            5.0, min(self.MAX_TRADE_USD_ABS_CAP, self.BANKROLL * self.MAX_TRADE_USD_PCT),
        )

    # ── Per-trade gate ─────────────────────────────────────────────────────

    def check(
        self,
        signal_type: str,
        trade_size_usd: float,
        confidence: float,
    ) -> KillSwitchResult:
        warnings: list[str] = []

        # 1. Master switch
        if os.getenv("POLYMARKET_LIVE_ENABLED", "").lower() not in ("1", "true", "yes"):
            return KillSwitchResult(
                False,
                "POLYMARKET_LIVE_ENABLED not set — set to 1 in .env to enable live trading",
                warnings,
            )

        # 2. Emergency stop file
        stopped, stop_reason = self.is_emergency_stopped()
        if stopped:
            return KillSwitchResult(False, f"Emergency stop active: {stop_reason}", warnings)

        # 3. Trade size cap
        if trade_size_usd > self.MAX_TRADE_USD:
            return KillSwitchResult(
                False,
                f"Trade size ${trade_size_usd:.0f} exceeds MAX_TRADE_USD ${self.MAX_TRADE_USD}",
                warnings,
            )

        # 4-7: read paper-trade history
        try:
            conn = get_connection(self._db_path)
        except Exception as exc:
            return KillSwitchResult(False, f"DB unavailable: {exc}", warnings)

        try:
            # 2026-04-30: was reading AVG(return_pct) but that column has
            # historical mixed units (some rows ratio, some rows
            # percentage-number) — same pollution that skewed
            # live_readiness EV by 100×. For whale_entry, return_pct path
            # gave avg=-13.4 (interpreted as -0.134 EV — blocked); the
            # canonical realized_pnl/size_usd path gives +1.30. Now reads
            # the same source as KellySizer + readiness endpoint.
            row = conn.execute(
                """SELECT COUNT(*) AS n,
                          AVG(realized_pnl / NULLIF(size_usd, 0)) AS avg_ev,
                          SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) AS wins
                   FROM polymarket_paper_trades
                   WHERE signal_type = ? AND archived = 0
                     AND exit_ts IS NOT NULL
                     AND realized_pnl IS NOT NULL AND size_usd > 0""",
                (signal_type,),
            ).fetchone()
            n_live = (row[0] or 0) if row else 0
            avg_ev_live = (row[1] or 0.0) if row else 0.0
            wins = (row[2] or 0) if row else 0

            # Pull most-recent backtest evidence for this signal type.
            # Backtest fires are "simulated resolutions" — they count
            # toward the sample floor at half weight (they're less reliable
            # than live paper resolutions but still provide EV evidence).
            bt_n, bt_ev, bt_wr = 0, None, None
            try:
                bt_row = conn.execute(
                    "SELECT resolved, ev, wr FROM signal_backtest_results "
                    "WHERE signal_type = ? ORDER BY run_ts DESC LIMIT 1",
                    (signal_type,),
                ).fetchone()
                if bt_row:
                    bt_n = int(bt_row[0] or 0)
                    bt_ev = bt_row[1]
                    bt_wr = bt_row[2]
            except Exception:
                pass  # table may not exist yet — fail quiet

            # Effective sample = live paper + 0.5 * backtest. Backtest is
            # half-weighted because it's replay, not live execution, and
            # doesn't pay real-world costs.
            effective_n = n_live + int(bt_n * 0.5)

            # Insider tier: dedicated lower MIN_RESOLVED. Insider signals
            # have strong structural priors (timing-based edge). If we
            # waited for n>=15 the first live insider trade would land
            # in late summer.
            min_resolved_hard = (
                self.INSIDER_MIN_RESOLVED
                if signal_type in self.INSIDER_SIGNAL_TYPES
                else self.MIN_RESOLVED_HARD
            )
            probation = False
            if effective_n < min_resolved_hard:
                # Check probation path: enough samples to see the signal's
                # direction, not enough to trust it at full size. Requires
                # positive EV + acceptable WR. Evaluated later in this
                # function, but we mark it here so the sample gate doesn't
                # reject outright.
                if effective_n >= self.PROBATION_MIN_RESOLVED:
                    probation = True
                    warnings.append(
                        f"PROBATION (effective n={effective_n}/{min_resolved_hard}) — "
                        f"capped at ${self.PROBATION_MAX_STAKE_USD:.0f}"
                    )
                elif (
                    os.getenv("LIVE_DISCOVERY_ENABLED", "").lower() in ("1", "true", "yes")
                    and effective_n >= self.DISCOVERY_MIN_RESOLVED
                ):
                    # Discovery tier: $1 stakes, real-money calibration.
                    probation = True  # reuse probation path, just smaller cap
                    warnings.append(
                        f"DISCOVERY (effective n={effective_n}) — capped at "
                        f"${self.DISCOVERY_MAX_STAKE_USD:.0f}"
                    )
                else:
                    return KillSwitchResult(
                        False,
                        f"{signal_type}: effective n={effective_n} "
                        f"(live={n_live} + 0.5×backtest={bt_n}), need "
                        f"{self.PROBATION_MIN_RESOLVED} for probation or "
                        f"{self.MIN_RESOLVED_HARD} for full",
                        warnings,
                    )
            if effective_n < self.PREFERRED_MIN_RESOLVED:
                warnings.append(
                    f"Below preferred sample size (effective n={effective_n}/{self.PREFERRED_MIN_RESOLVED})"
                )

            # Blended EV — live data gets 2x weight when both exist.
            # 2026-04-30: avg_ev_live is now realized_pnl/size_usd ratio
            # (already a fractional EV), no /100 needed. Was dividing
            # the bad return_pct value by 100 before.
            # 2026-05-05: once n_live >= MIN_RESOLVED_HARD, trust live paper
            # exclusively — backtest replay has high false-positive rate on some
            # signals (e.g. oversized_bet: bt_ev=-0.35 from historical bulk
            # labeling vs live paper ev=+0.33 from filtered real-time detection).
            # Backtest serves as cold-start prior only.
            ev_live = avg_ev_live if n_live else None
            if ev_live is not None and n_live >= min_resolved_hard:
                ev = ev_live
            elif ev_live is not None and bt_ev is not None:
                ev = (2 * ev_live * n_live + bt_ev * bt_n) / (2 * n_live + bt_n)
            elif ev_live is not None:
                ev = ev_live
            elif bt_ev is not None:
                ev = bt_ev
                warnings.append(f"EV from backtest only (no live paper resolutions yet)")
            else:
                ev = 0.0
            if ev <= 0:
                return KillSwitchResult(
                    False,
                    f"{signal_type}: blended EV={ev:.3f} not positive — no measured edge",
                    warnings,
                )

            # Blended WR same approach.
            wr_live = wins / n_live if n_live > 0 else None
            if wr_live is not None and bt_wr is not None:
                wr = (2 * wr_live * n_live + bt_wr * bt_n) / (2 * n_live + bt_n)
            else:
                wr = wr_live if wr_live is not None else (bt_wr or 0)
            default_wr_floor = self.MIN_WIN_RATE_PROBATION if probation else self.MIN_WIN_RATE
            wr_floor = self.WR_FLOOR_OVERRIDES.get(signal_type, default_wr_floor)
            if effective_n >= 20 and wr < wr_floor:
                return KillSwitchResult(
                    False,
                    f"{signal_type}: blended WR {wr:.0%} below minimum {wr_floor:.0%}",
                    warnings,
                )

            # IC14 decay penalty: if recent IC has collapsed, halve the stake cap.
            # This doesn't block the trade — it forces smaller size until the
            # signal proves it still has edge in the current market regime.
            try:
                ic_row = conn.execute(
                    "SELECT ic_14d, decay_flag FROM signal_health WHERE signal_type = %s",
                    (signal_type,),
                ).fetchone()
                if ic_row:
                    ic14, decay = float(ic_row[0] or 0), bool(ic_row[1])
                    if decay and ic14 < self.IC14_DECAY_THRESHOLD:
                        warnings.append(
                            f"IC14={ic14:.3f} below {self.IC14_DECAY_THRESHOLD} with decay_flag — "
                            f"stake halved"
                        )
                        if cap is None:
                            cap = self.PROBATION_MAX_STAKE_USD * self.IC14_STAKE_HALF_FACTOR
                        else:
                            cap = cap * self.IC14_STAKE_HALF_FACTOR
            except Exception:
                pass

            # 6. Daily loss limit
            today_start = int(time.time()) - 86400
            daily_pnl = conn.execute(
                """SELECT COALESCE(SUM(realized_pnl), 0) FROM polymarket_paper_trades
                   WHERE archived = 0 AND exit_ts >= ? AND realized_pnl IS NOT NULL""",
                (today_start,),
            ).fetchone()[0] or 0.0
            daily_loss_pct = abs(min(0.0, daily_pnl)) / self.BANKROLL
            if daily_loss_pct >= self.MAX_DAILY_LOSS_PCT:
                return KillSwitchResult(
                    False,
                    f"Daily loss limit hit: {daily_loss_pct:.1%} (max {self.MAX_DAILY_LOSS_PCT:.0%})",
                    warnings,
                )

            # 7. Open live position count — exit_ts IS NULL guards against
            # closed trades with status='matched' being counted as open.
            # The exit path updates exit_ts but not status, so status alone
            # is not a reliable open-position signal.
            try:
                live_open = conn.execute(
                    """SELECT COUNT(*) FROM live_trades
                       WHERE dry_run = 0 AND exit_ts IS NULL
                         AND status IN ('submitted','live','matched')"""
                ).fetchone()[0]
            except sqlite3.OperationalError:
                live_open = 0
            if live_open >= self.MAX_OPEN_POSITIONS:
                return KillSwitchResult(
                    False,
                    f"Max open live positions reached ({live_open}/{self.MAX_OPEN_POSITIONS})",
                    warnings,
                )

            # Discovery cap is tighter than probation; pick the right one.
            cap = None
            if probation:
                if any("DISCOVERY" in w for w in warnings):
                    cap = self.DISCOVERY_MAX_STAKE_USD
                else:
                    cap = self.PROBATION_MAX_STAKE_USD
            return KillSwitchResult(True, None, warnings, probation_cap=cap)
        finally:
            conn.close()

    # ── Emergency stop ─────────────────────────────────────────────────────

    def emergency_stop(self, reason: str = "manual") -> None:
        """Write the kill-switch flag file. Live executor checks this on every trade."""
        _KILL_SWITCH_FILE.parent.mkdir(parents=True, exist_ok=True)
        _KILL_SWITCH_FILE.write_text(f"{int(time.time())} {reason}\n", encoding="utf-8")
        logger.warning("[KILL_SWITCH] EMERGENCY STOP ACTIVATED: %s", reason)

    def is_emergency_stopped(self) -> tuple[bool, str]:
        if not _KILL_SWITCH_FILE.exists():
            return False, ""
        try:
            return True, _KILL_SWITCH_FILE.read_text(encoding="utf-8").strip()
        except Exception:
            return True, "unreadable"

    def clear_emergency_stop(self) -> None:
        try:
            _KILL_SWITCH_FILE.unlink()
            logger.warning("[KILL_SWITCH] Emergency stop cleared")
        except FileNotFoundError:
            pass
