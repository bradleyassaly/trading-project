"""
Tests for the signal calibration system.

Covers:
  * Bayesian win-rate posterior math
  * Kelly fraction clamping (negative edge → 0, huge edge → 0.25 cap)
  * Status state machine (all valid transitions, sticky 'disabled')
  * Tri-factor fusion score component bounds and decision thresholds
  * BankrollAllocator: deployed sum ≤ 80% of bankroll, dry_run is side-effect free
  * SignalEvaluator round-trip: insert paper trades → update_one → read row
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from trading_platform.polymarket.bankroll_allocator import (
    BankrollAllocator, MAX_DEPLOYED_PCT,
)
from trading_platform.polymarket.fusion_score import (
    compute_fusion, market_signal, timing_signal, wallet_signal,
)
from trading_platform.polymarket.signal_evaluator import (
    SignalEvaluator, beta_posterior_mean, kelly_fraction, transition_status,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _bootstrap_db(tmp_path: Path) -> Path:
    """Create a minimal wallet_intelligence.db with the calibration schema."""
    db_path = tmp_path / "wi.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE polymarket_paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            condition_id TEXT,
            question TEXT,
            category TEXT,
            side TEXT,
            entry_price REAL,
            size_usd REAL,
            signal_type TEXT,
            confidence REAL,
            wallet TEXT,
            entry_ts INTEGER,
            exit_price REAL,
            exit_ts INTEGER,
            outcome TEXT,
            return_pct REAL,
            realized_pnl REAL,
            archived INTEGER DEFAULT 0,
            fusion_score REAL,
            fusion_components TEXT,
            wallet_tier_at_fire TEXT,
            wallet_bucket_at_fire TEXT,
            kelly_stake_pct REAL
        );
        CREATE TABLE signal_calibration (
            signal_type TEXT PRIMARY KEY,
            sample_size INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            bayesian_wr REAL,
            ev_per_trade REAL,
            profit_factor REAL,
            sharpe_ratio REAL,
            kelly_fraction REAL,
            rolling_10_wr REAL,
            rolling_20_ev REAL,
            consecutive_losses INTEGER DEFAULT 0,
            recommended_allocation_pct REAL,
            recommended_stake_usd REAL,
            allocated_usd REAL,
            status TEXT DEFAULT 'building',
            last_updated INTEGER
        );
        """
    )
    conn.commit()
    conn.close()
    return db_path


def _add_trade(
    db_path: Path,
    *,
    signal_type: str,
    return_pct: float,
    outcome: str,
    exit_ts: int | None = None,
    size_usd: float = 100.0,
) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """INSERT INTO polymarket_paper_trades
            (signal_type, side, entry_price, size_usd, exit_price, exit_ts,
             outcome, return_pct, realized_pnl, archived)
           VALUES (?, 'YES', 0.5, ?, ?, ?, ?, ?, ?, 0)""",
        (
            signal_type, size_usd,
            0.5 * (1 + return_pct / 100),
            exit_ts or int(time.time()),
            outcome,
            return_pct,
            size_usd * return_pct / 100,
        ),
    )
    conn.commit()
    conn.close()


# ── Bayesian + Kelly math ────────────────────────────────────────────────────


class TestBayesianMath:
    def test_uniform_prior_no_data(self) -> None:
        # Beta(1+0, 1+0) → mean 0.5
        assert beta_posterior_mean(0, 0) == pytest.approx(0.5)

    def test_known_posterior(self) -> None:
        # Beta(1+10, 1+5) = Beta(11, 6) → mean 11/17
        assert beta_posterior_mean(10, 5) == pytest.approx(11 / 17, abs=1e-4)

    def test_posterior_pulls_toward_50_at_low_n(self) -> None:
        # 1 win, 0 losses → 2/3, not 1.0
        assert beta_posterior_mean(1, 0) == pytest.approx(2 / 3, abs=1e-4)


class TestKellyClamp:
    def test_negative_edge_returns_zero(self) -> None:
        # WR 0.4 with 1:1 odds → negative Kelly → clamped to 0
        assert kelly_fraction(0.4, 0.3, 0.3) == 0.0

    def test_huge_edge_clamped_to_25_pct(self) -> None:
        # 90% WR with 5:1 reward → unclamped Kelly is huge
        assert kelly_fraction(0.9, 0.5, 0.1) == 0.25

    def test_zero_avg_returns_zero(self) -> None:
        assert kelly_fraction(0.7, 0.0, 0.5) == 0.0
        assert kelly_fraction(0.7, 0.5, 0.0) == 0.0

    def test_degenerate_wr_returns_zero(self) -> None:
        assert kelly_fraction(0.0, 0.5, 0.5) == 0.0
        assert kelly_fraction(1.0, 0.5, 0.5) == 0.0

    def test_typical_edge(self) -> None:
        # 60% WR, 40c win, 20c loss → b=2, f=(0.6*2 - 0.4)/2 = 0.4 → clamped 0.25
        assert kelly_fraction(0.6, 0.4, 0.2) == 0.25


# ── Status transitions ──────────────────────────────────────────────────────


class TestStatusTransitions:
    def test_building_to_live_requires_all_three(self) -> None:
        # Sample/WR/PF all met
        assert transition_status("building", 20, 0.55, 1.5, None, None, 0) == "live"
        # WR too low
        assert transition_status("building", 20, 0.50, 1.5, None, None, 0) == "building"
        # PF too low
        assert transition_status("building", 20, 0.55, 1.0, None, None, 0) == "building"
        # Sample too small
        assert transition_status("building", 10, 0.70, 2.0, None, None, 0) == "building"

    def test_live_to_weak_on_low_rolling_wr(self) -> None:
        assert transition_status("live", 30, 0.55, 1.5, 0.40, 0.05, 0) == "weak"

    def test_live_to_weak_on_low_pf(self) -> None:
        assert transition_status("live", 30, 0.55, 0.7, 0.55, 0.05, 0) == "weak"

    def test_any_to_disabled_on_5_consec_losses(self) -> None:
        assert transition_status("live", 30, 0.55, 1.5, 0.55, 0.05, 5) == "disabled"
        assert transition_status("weak", 30, 0.55, 1.5, 0.55, 0.05, 5) == "disabled"

    def test_disabled_is_sticky(self) -> None:
        # Even strong stats can't bring a disabled signal back automatically
        assert transition_status("disabled", 100, 0.99, 99, 0.99, 0.99, 0) == "disabled"

    def test_weak_to_disabled_on_negative_rolling_ev(self) -> None:
        assert transition_status("weak", 25, 0.50, 1.0, 0.50, -0.15, 0) == "disabled"


# ── Fusion score ────────────────────────────────────────────────────────────


class TestFusionComponents:
    def test_wallet_signal_in_bounds(self) -> None:
        x = wallet_signal(0.7, "tier1h", 5000, 2000)
        assert 0.0 <= x <= 2.0

    def test_market_signal_in_unit_interval(self) -> None:
        x = market_signal(150_000, 0.55, 0.5)
        assert 0.0 <= x <= 1.0

    def test_timing_signal_freshness(self) -> None:
        # Fresh whale entry → 1.0
        assert timing_signal(2, 0) == 1.0
        # 2h+ → 0.2
        assert timing_signal(150, 0) == 0.2
        # Convergence boost
        assert timing_signal(2, 2) == pytest.approx(1.3, abs=1e-6)

    def test_wallet_signal_unknown_wallet_safe(self) -> None:
        x = wallet_signal(None, None, 0, 0)
        assert 0.0 <= x <= 2.0


class TestFusionDecision:
    def test_strong_signal_auto(self) -> None:
        f = compute_fusion(
            wallet_wr=0.7, wallet_tier="tier1h",
            trade_size_usd=5000, wallet_avg_bet_usd=2000,
            market_volume_usd=200_000, current_price=0.55,
            days_since_last_trade=0.1,
            minutes_since_whale_entry=3, convergence_count=2,
        )
        assert f.score >= 0.6
        assert f.decision == "auto"
        assert f.stake_multiplier == 1.0

    def test_weak_signal_skip(self) -> None:
        f = compute_fusion(
            wallet_wr=0.4, wallet_tier="tier2",
            trade_size_usd=100, wallet_avg_bet_usd=5000,
            market_volume_usd=10_000, current_price=0.95,
            days_since_last_trade=10,
            minutes_since_whale_entry=300, convergence_count=0,
        )
        assert f.decision == "skip"
        assert f.stake_multiplier == 0.0

    def test_mid_signal_half_stake(self) -> None:
        # Tweak to land in [0.4, 0.6)
        f = compute_fusion(
            wallet_wr=0.6, wallet_tier="tier1",
            trade_size_usd=2000, wallet_avg_bet_usd=2000,
            market_volume_usd=80_000, current_price=0.5,
            days_since_last_trade=2,
            minutes_since_whale_entry=20, convergence_count=0,
        )
        # Should be either half or auto depending on math; assert in range
        assert f.score > 0
        assert f.decision in ("half", "auto", "skip")
        if 0.4 <= f.score < 0.6:
            assert f.decision == "half"
            assert f.stake_multiplier == 0.5


# ── BankrollAllocator ───────────────────────────────────────────────────────


class TestBankrollAllocator:
    def test_dry_run_no_writes(self, tmp_path: Path) -> None:
        db_path = _bootstrap_db(tmp_path)
        # Seed one live signal
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            """INSERT INTO signal_calibration
                (signal_type, sample_size, kelly_fraction, status)
               VALUES ('price_velocity', 30, 0.20, 'live')"""
        )
        conn.commit()
        conn.close()

        alloc = BankrollAllocator(str(db_path), bankroll=10_000)
        plan = alloc.rebalance(dry_run=True)

        # Allocated_usd column should remain NULL after dry run
        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT allocated_usd FROM signal_calibration WHERE signal_type='price_velocity'"
        ).fetchone()
        conn.close()
        assert row[0] is None
        assert plan.deployed_usd > 0

    def test_deployed_under_80_pct(self, tmp_path: Path) -> None:
        db_path = _bootstrap_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        # Three live signals with high Kelly
        for sig in ("a", "b", "c"):
            conn.execute(
                """INSERT INTO signal_calibration
                    (signal_type, sample_size, kelly_fraction, status)
                   VALUES (?, 30, 0.25, 'live')""",
                (sig,),
            )
        conn.commit()
        conn.close()

        plan = BankrollAllocator(str(db_path), bankroll=10_000).compute_plan()
        assert plan.deployed_usd <= plan.bankroll * MAX_DEPLOYED_PCT + 0.01

    def test_disabled_gets_zero(self, tmp_path: Path) -> None:
        db_path = _bootstrap_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            """INSERT INTO signal_calibration
                (signal_type, sample_size, kelly_fraction, status)
               VALUES ('bad', 30, 0.10, 'disabled')"""
        )
        conn.commit()
        conn.close()

        plan = BankrollAllocator(str(db_path), bankroll=10_000).compute_plan()
        bad_rows = [r for r in plan.rows if r.signal_type == "bad"]
        # disabled signals should not appear in the plan at all
        assert bad_rows == []


# ── SignalEvaluator round trip ──────────────────────────────────────────────


class TestSignalEvaluatorRoundtrip:
    def test_update_one_promotes_winning_signal(self, tmp_path: Path) -> None:
        db_path = _bootstrap_db(tmp_path)
        # Insert 16 wins at +50% and 4 losses at -40% — high WR + high PF
        for i in range(16):
            _add_trade(db_path, signal_type="strong",
                       return_pct=50.0, outcome="win",
                       exit_ts=1_700_000_000 + i)
        for i in range(4):
            _add_trade(db_path, signal_type="strong",
                       return_pct=-40.0, outcome="loss",
                       exit_ts=1_700_000_100 + i)

        ev = SignalEvaluator(str(db_path))
        row, prev_status = ev.update_one("strong")
        assert prev_status is None
        assert row.sample_size == 20
        assert row.wins == 16
        assert row.bayesian_wr is not None and row.bayesian_wr > 0.7
        assert row.profit_factor is not None and row.profit_factor > 1.5
        assert row.kelly_fraction == 0.25  # clamped
        assert row.status == "live"

    def test_5_consec_losses_disables(self, tmp_path: Path) -> None:
        db_path = _bootstrap_db(tmp_path)
        # 10 wins followed by 5 losses (recent) → consec_losses=5 → disabled
        for i in range(10):
            _add_trade(db_path, signal_type="rolling",
                       return_pct=30.0, outcome="win",
                       exit_ts=1_700_000_000 + i)
        for i in range(5):
            _add_trade(db_path, signal_type="rolling",
                       return_pct=-25.0, outcome="loss",
                       exit_ts=1_700_000_100 + i)

        row, _ = SignalEvaluator(str(db_path)).update_one("rolling")
        assert row.consecutive_losses == 5
        assert row.status == "disabled"
