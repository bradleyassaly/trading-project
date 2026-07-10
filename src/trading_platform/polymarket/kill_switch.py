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

    # 2026-05-12: tightened from 10% → 5% (~$15 on $300 bankroll).
    # At SELL-only $5/trade, 3 consecutive stop-losses = -$15 = 5%. That's
    # the circuit breaker — pause new entries for the rest of the UTC day.
    MAX_DAILY_LOSS_PCT = 0.05
    # 2026-05-06: raised 10→20. 30-day horizon gate means positions resolve
    # faster; resolution_decay was repeatedly blocked at 10 despite only 4
    # genuinely open trades. With short-horizon markets, turnover is high
    # enough that 20 simultaneous positions carries acceptable risk.
    MAX_OPEN_POSITIONS = 30
    # Per-trade cap derived dynamically from live bankroll (7% of account
    # as phase-1 safety ramp; capped at $25 hard ceiling). Scales with
    # account growth without code changes.
    MAX_TRADE_USD_PCT = 0.07
    MAX_TRADE_USD_ABS_CAP = 25.0
    MIN_WIN_RATE = 0.52
    # 2026-05-10: raised from 20 → 30. At $5/trade, 20-cap deployed $100
    # max ($33% of $300 bankroll) and was blocking hundreds of signals.
    # 30 positions × $5 = $150 deployed (50% of bankroll) — healthier
    # deployment without over-concentrating risk.
    # 2026-04-25: probation/discovery tier WR floor. Standard 0.52 blocked
    # 45 live attempts in 24h. Drop to 0.45 for stakes <= probation cap so
    # real-money calibration data starts flowing — risk is bounded by the
    # cap, not the WR threshold. Full-stake live still requires 0.52.
    MIN_WIN_RATE_PROBATION = 0.45
    MIN_RESOLVED_HARD = 15
    PREFERRED_MIN_RESOLVED = 30
    # 2026-07-07 (audit F5): backtests older than this don't count toward the
    # sample floor / EV blend. A signal's backtest predating a regime change
    # (V2 order format 2026-05-07, honest booking 2026-07-06) is not evidence
    # about how it trades today.
    BACKTEST_MAX_AGE_DAYS = 30
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
    # whale_entry_filtered: IC30=0.313, live 38% WR, 3.4× win:loss = +EV.
    # oversized_bet: IC30=0.121, 29% WR but +33% avg EV — valid.
    # wallet_reversal: IC30=0.160, 56% live WR but structural low WR on paper.
    # specialist_entry: IC30=0.050, 89% backtest WR but small live sample.
    WR_FLOOR_OVERRIDES: dict[str, float] = {
        "whale_entry_filtered": 0.30,
        "oversized_bet": 0.25,
        "cascade": 0.30,
        "wallet_reversal": 0.35,
        "specialist_entry": 0.35,
        "resolution_decay": 0.35,
    }
    # EV threshold above which the WR gate is bypassed entirely. Signals
    # with confirmed positive EV (>= 5%) are profitable regardless of WR
    # when payoffs are asymmetric (e.g. 38% WR × 3.4× ratio >> breakeven).
    # The WR gate remains for borderline-EV signals where a low WR could
    # indicate noise rather than genuine asymmetric edge.
    EV_BYPASS_WR_THRESHOLD = 0.05
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
            # 2026-07-09: outcome IN ('win','loss') — rows with
            # outcome='expired' (market vanished from Gamma, pnl=0, never
            # resolved) are neither wins nor losses, but the old query
            # counted them as losses in the WR numerator/denominator. 67
            # such rows dragged resolution_decay's blended WR from 36.05%
            # to 32.4% and dark-halted the live lane below the 35% floor.
            row = conn.execute(
                """SELECT COUNT(*) AS n,
                          AVG(realized_pnl / NULLIF(size_usd, 0)) AS avg_ev,
                          SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) AS wins
                   FROM polymarket_paper_trades
                   WHERE signal_type = ? AND archived = 0
                     AND exit_ts IS NOT NULL
                     AND outcome IN ('win', 'loss')
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
                # 2026-07-07 (audit F5): recency filter. Without it, a stale
                # backtest (e.g. wallet_reversal's 2026-05-01 n=192, predating
                # the V2 order format and honest booking) alone yields
                # effective_n=96, skips probation, and — with n_live=0 —
                # authorizes a full MAX_TRADE_USD trade on months-old replay.
                # Only backtests from the last BACKTEST_MAX_AGE_DAYS count.
                _bt_cutoff = int(time.time()) - self.BACKTEST_MAX_AGE_DAYS * 86400
                bt_row = conn.execute(
                    "SELECT resolved, ev, wr FROM signal_backtest_results "
                    "WHERE signal_type = ? AND run_ts >= ? "
                    "ORDER BY run_ts DESC LIMIT 1",
                    (signal_type, _bt_cutoff),
                ).fetchone()
                if bt_row:
                    bt_n = int(bt_row[0] or 0)
                    bt_ev = bt_row[1]
                    bt_wr = bt_row[2]
            except Exception:
                pass  # table may not exist yet — fail quiet

            # Effective sample = live paper + 0.5 * backtest. Backtest is
            # half-weighted because it's replay, not live execution, and
            # doesn't pay real-world costs. A signal with ZERO live
            # resolutions can never reach the sample floor on backtest alone:
            # capped so backtest can supply at most (floor - 1) so at least
            # one live/paper resolution is always required before full stake.
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
            # A signal with ZERO real (live/paper) resolutions must not clear
            # the full-stake sample floor on backtest replay alone — cap the
            # effective sample just below the floor so it stays in probation
            # (bounded stake) until at least one real resolution lands.
            if n_live == 0 and effective_n >= min_resolved_hard:
                effective_n = min_resolved_hard - 1
                warnings.append(
                    "backtest-only evidence — held at probation stake until a "
                    "live/paper resolution lands"
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
            # Skip WR gate when EV is meaningfully positive: asymmetric-payoff
            # signals (e.g. whale_entry_filtered 38% WR, 3.4× win:loss) are
            # profitable even at low WR. WR gate only applies when EV is near
            # zero, where low WR could signal noise rather than real asymmetry.
            #
            # 2026-07-02: the bypass is now opt-in via
            # POLYMARKET_EV_BYPASS_ENABLED=1. The EV feeding it comes from
            # live_trades realized_pnl, which the 2026-05-27 audit showed was
            # contaminated by fictitious closes (~$103 inflated over 90d).
            # Until the on-chain ledger reconciliation is complete, an
            # inflated EV must not be able to waive the WR floor. Note both
            # current revenue signals pass their WR floors on their own
            # (whale_entry_filtered 36.7% vs 0.30; resolution_decay 69% vs
            # 0.35), so disabling the bypass does not halt the live book.
            _ev_bypass_on = os.getenv(
                "POLYMARKET_EV_BYPASS_ENABLED", ""
            ).lower() in ("1", "true", "yes")
            _wr_gate_applies = (not _ev_bypass_on) or ev < self.EV_BYPASS_WR_THRESHOLD
            if _wr_gate_applies and effective_n >= 20 and wr < wr_floor:
                return KillSwitchResult(
                    False,
                    f"{signal_type}: blended WR {wr:.0%} below minimum {wr_floor:.0%}",
                    warnings,
                )

            # Discovery cap is tighter than probation; pick the right one.
            # Initialised here so the IC14 block below can modify it safely.
            cap = None
            if probation:
                if any("DISCOVERY" in w for w in warnings):
                    cap = self.DISCOVERY_MAX_STAKE_USD
                else:
                    cap = self.PROBATION_MAX_STAKE_USD

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

            # 6. Daily loss limit — based on LIVE trade P&L only.
            # Paper trades run on every signal including unvalidated types
            # (price_velocity, order_flow_imbalance) and can lose $100+/day
            # on paper while live P&L stays flat. Reading paper P&L here
            # caused the KS to block all live entries despite live losses
            # being well within tolerance (confirmed 2026-05-12).
            # 2026-05-12: use UTC calendar day (midnight→now) not rolling 24h.
            # Rolling window meant hitting the limit at 23:59 would block
            # for a full 24h; UTC-day resets cleanly at midnight.
            import datetime as _dt
            _today_utc = _dt.datetime.utcnow().replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            today_start = int(_today_utc.timestamp())
            # 2026-07-05: window on the ECONOMIC date, not the booking date.
            # Reconciliation channels (resolved_databook etc.) can book
            # weeks-old losses in a single batch; exit_ts then reads as "we
            # lost $115 today" and trips the breaker on stale losses
            # (happened 2026-07-02: May-resolved markets back-booked at
            # once, halting all live entries for 2+ days). When
            # resolution_date is known and earlier than exit_ts, the loss
            # economically belongs to the resolution day. An undated
            # resolved_databook row is a legacy back-booking from before
            # book_resolved_positions stamped resolution_date (it now
            # always does) — its economic date is unknown-but-old, so it
            # maps to 0 and falls out of every window.
            _ECON_TS = (
                "CASE "
                "WHEN exit_reason = 'resolved_databook' "
                "AND COALESCE(resolution_date, 0) <= 0 THEN 0 "
                "WHEN COALESCE(resolution_date, 0) > 0 "
                "AND resolution_date < exit_ts THEN resolution_date "
                "ELSE exit_ts END"
            )
            daily_pnl = conn.execute(
                f"""SELECT COALESCE(SUM(realized_pnl), 0) FROM live_trades
                   WHERE dry_run = 0 AND {_ECON_TS} >= %s AND realized_pnl IS NOT NULL""",
                (today_start,),
            ).fetchone()[0] or 0.0
            daily_loss_pct = abs(min(0.0, daily_pnl)) / self.BANKROLL
            if daily_loss_pct >= self.MAX_DAILY_LOSS_PCT:
                return KillSwitchResult(
                    False,
                    f"Daily loss limit hit: {daily_loss_pct:.1%} (max {self.MAX_DAILY_LOSS_PCT:.0%})",
                    warnings,
                )

            # 6b. Rolling 7d realized P&L circuit breaker (2026-05-19).
            # The daily limit only catches a single bad day. A slow bleed
            # across a week wouldn't trip it but could chew through 15%+
            # of bankroll undetected. Halt new entries when the rolling
            # 7d realized < -10% of bankroll (~$27 on $267) — gives the
            # daily review one cycle to identify the cause before more
            # capital is committed.
            ROLLING_7D_HALT_PCT = 0.10
            week_start = int(time.time()) - 7 * 86400
            week_pnl = conn.execute(
                f"""SELECT COALESCE(SUM(realized_pnl), 0) FROM live_trades
                   WHERE dry_run = 0 AND {_ECON_TS} >= %s AND realized_pnl IS NOT NULL""",
                (week_start,),
            ).fetchone()[0] or 0.0
            week_loss_pct = abs(min(0.0, week_pnl)) / self.BANKROLL
            if week_loss_pct >= ROLLING_7D_HALT_PCT:
                return KillSwitchResult(
                    False,
                    f"7d rolling loss circuit breaker: ${week_pnl:.2f} "
                    f"({week_loss_pct:.1%} >= {ROLLING_7D_HALT_PCT:.0%})",
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
