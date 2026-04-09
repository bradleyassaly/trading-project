"""
Tests for the wallet profile WR computation.

The previous implementation aggregated win rate per asset (one win/loss
per unique token), which collapsed e.g. 461 resolved trades for our top
conviction wallet down to 32 unique resolved assets, then excluded
sports/tweet_count categories, leaving a tiny biased sample that
overstated the headline WR by 20-46 points across the top wallets.
See reports/data_validation.md DV-1.

The fix computes WR at the trade level using ``pnl > 0`` count vs
``pnl != 0`` count over the full resolved-trade pool.
"""
from __future__ import annotations

from trading_platform.polymarket.wallet_profile_rebuild import _compute_profile


def _trade(*, side: str = "BUY", size: float = 100.0, price: float = 0.5,
           ts: int = 1_700_000_000, pnl: float | None = None,
           resolved: bool = True, asset: str = "tok1",
           category: str = "politics", outcome: str | None = "Yes",
           market_outcome: str | None = "YES") -> dict:
    return {
        "side": side, "size": size, "price": price,
        "timestamp": ts, "pnl": pnl,
        "market_resolved": 1 if resolved else 0,
        "market_outcome": market_outcome,
        "asset": asset, "category": category,
        "outcome": outcome, "event_slug": f"event-{asset}",
    }


class TestWinRateUsesAllResolvedTrades:
    def test_uses_full_pool_not_per_asset(self):
        # 100 resolved trades on a single asset: 60 wins, 40 losses.
        # Old per-asset code would count this as 1 unit (the net
        # position) and yield WR ∈ {0, 1}; new per-trade code yields 0.6.
        trades = (
            [_trade(asset="tok1", pnl=10.0) for _ in range(60)]
            + [_trade(asset="tok1", pnl=-10.0) for _ in range(40)]
        )
        p = _compute_profile(trades)
        assert p["resolved_trades"] == 100
        assert p["directional_win_rate"] == 0.6

    def test_does_not_exclude_sports(self):
        # Sports trades used to be excluded — now they count.
        trades = (
            [_trade(asset=f"a{i}", category="sports", pnl=10.0) for i in range(5)]
            + [_trade(asset=f"b{i}", category="sports", pnl=-10.0) for i in range(5)]
        )
        p = _compute_profile(trades)
        assert p["resolved_trades"] == 10
        assert p["directional_win_rate"] == 0.5

    def test_excludes_unresolved_trades(self):
        trades = (
            [_trade(asset="tok1", pnl=10.0) for _ in range(5)]
            + [_trade(asset="tok2", pnl=None, resolved=False) for _ in range(95)]
        )
        p = _compute_profile(trades)
        assert p["resolved_trades"] == 5
        assert p["directional_win_rate"] == 1.0

    def test_zero_resolved_yields_none(self):
        trades = [_trade(pnl=None, resolved=False)]
        p = _compute_profile(trades)
        assert p["resolved_trades"] == 0
        assert p["directional_win_rate"] is None

    def test_resolved_count_matches_actual(self):
        # 461 resolved trades across many assets, mixed categories
        trades = (
            [_trade(asset=f"a{i}", category="politics", pnl=1.0) for i in range(189)]
            + [_trade(asset=f"b{i}", category="politics", pnl=-1.0) for i in range(272)]
        )
        p = _compute_profile(trades)
        assert p["resolved_trades"] == 461
        # 189/461 = 0.41 — matches Wallet A's actual win rate
        assert abs(p["directional_win_rate"] - 0.41) < 0.005

    def test_break_even_trades_excluded(self):
        trades = (
            [_trade(pnl=10.0) for _ in range(7)]
            + [_trade(pnl=-10.0) for _ in range(3)]
            + [_trade(pnl=0.0) for _ in range(5)]  # break-even, not counted
        )
        p = _compute_profile(trades)
        assert p["resolved_trades"] == 10
        assert p["directional_win_rate"] == 0.7
