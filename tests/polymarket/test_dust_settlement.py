"""Dust-close settlement — zero balance must NOT imply loss.

Pins the 2026-07-02 fix: a zero CONDITIONAL balance means either the
market resolved against us OR we won and Polymarket auto-redeemed the
tokens. Booking every dust close as -(cost basis) recorded wins as
losses — the root cause of DB PnL trailing Polymarket's own numbers.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from trading_platform.polymarket.live_position_monitor import _settle_dust_close


def _call(**overrides):
    kwargs = dict(
        token_id="123456",
        condition_id="0xabc",
        direction="SELL",
        trade_status="matched",
        fill_price=0.80,   # SELL: long NO at (1-0.80)=0.20/share
        db_shares=10.0,
    )
    kwargs.update(overrides)
    return _settle_dust_close(**kwargs)


def test_never_matched_books_zero():
    realized, outcome = _call(trade_status="live")
    assert realized == 0.0 and outcome is None


def test_data_api_cash_pnl_wins_when_available(monkeypatch):
    monkeypatch.setenv("POLYMARKET_WALLET_ADDRESS", "0xwallet")
    fetcher = MagicMock()
    fetcher.fetch_wallet_positions.return_value = [
        {"asset": "123456", "cashPnl": 7.77},
    ]
    with patch(
        "trading_platform.polymarket.position_fetcher.PositionFetcher",
        return_value=fetcher,
    ):
        realized, outcome = _call()
    assert realized == 7.77
    assert outcome == "win"


def test_resolution_file_win_books_payout_not_loss(monkeypatch, tmp_path):
    # No data-api match; resolution file says NO resolved (price 0.0),
    # we are SELL (long NO) → WIN. Payout $1/share, cost 0.20/share.
    monkeypatch.delenv("POLYMARKET_WALLET_ADDRESS", raising=False)
    monkeypatch.delenv("POLYMARKET_FUNDER_ADDRESS", raising=False)
    csv = tmp_path / "gamma_resolution.csv"
    csv.write_text("ticker,resolution_price,condition_id\n123456,0.0,0xabc\n")
    with patch(
        "trading_platform.polymarket.live_position_monitor._GAMMA_CSV", csv
    ):
        realized, outcome = _call()
    assert outcome == "win"
    assert realized == 8.0  # 10 shares × $1 − 10 × $0.20 cost


def test_resolution_file_loss_books_cost_basis(monkeypatch, tmp_path):
    monkeypatch.delenv("POLYMARKET_WALLET_ADDRESS", raising=False)
    monkeypatch.delenv("POLYMARKET_FUNDER_ADDRESS", raising=False)
    csv = tmp_path / "gamma_resolution.csv"
    csv.write_text("ticker,resolution_price,condition_id\n123456,100.0,0xabc\n")
    with patch(
        "trading_platform.polymarket.live_position_monitor._GAMMA_CSV", csv
    ):
        realized, outcome = _call()
    assert outcome == "loss"
    assert realized == -2.0  # -(10 × 0.20 cost basis)


def test_unknown_returns_none_so_caller_leaves_open(monkeypatch, tmp_path):
    monkeypatch.delenv("POLYMARKET_WALLET_ADDRESS", raising=False)
    monkeypatch.delenv("POLYMARKET_FUNDER_ADDRESS", raising=False)
    with patch(
        "trading_platform.polymarket.live_position_monitor._GAMMA_CSV",
        tmp_path / "missing.csv",
    ):
        assert _call() is None
