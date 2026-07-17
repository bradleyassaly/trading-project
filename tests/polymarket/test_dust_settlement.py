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


# ── 2026-07-16: dust-stomped entry basis (trade 195966) ────────────────────
# The hourly share sync overwrites db_shares with the post-liquidation
# on-chain remainder. Settlement must book from the ENTRY basis, not the
# dust — otherwise sale proceeds vs ~$0 cost sign-flips a loss into a win.

def _trade_history_response(trades):
    resp = MagicMock()
    resp.ok = True
    resp.json.return_value = trades
    return resp


def test_dust_stomped_sell_fills_book_entry_basis_loss(monkeypatch):
    # Real numbers from trade 195966: bought 38 @ 0.15 ($5.70 basis),
    # exit SELLs filled 37.996665 @ 0.09 ($3.42 proceeds), share sync
    # stomped db_shares to the 0.003335 remainder. Naive dust basis
    # books +$3.42 "win"; truth is -$2.28 loss.
    monkeypatch.setenv("POLYMARKET_WALLET_ADDRESS", "0xwallet")
    fetcher = MagicMock()
    fetcher.fetch_wallet_positions.return_value = []  # dust drops off /positions
    trades = [
        {"asset": "123456", "side": "BUY", "size": 38.0, "price": 0.15},
        {"asset": "123456", "side": "SELL", "size": 31.666665, "price": 0.09},
        {"asset": "123456", "side": "SELL", "size": 6.33, "price": 0.09},
        {"asset": "999", "side": "SELL", "size": 50.0, "price": 0.5},  # other token
    ]
    with patch(
        "trading_platform.polymarket.position_fetcher.PositionFetcher",
        return_value=fetcher,
    ), patch("requests.get", return_value=_trade_history_response(trades)):
        realized, outcome = _call(
            direction="BUY", fill_price=0.15,
            db_shares=0.003335, size_usd=6.21,
        )
    assert outcome == "loss"
    assert realized == -2.2803  # 37.996665×0.09 − 38×0.15


def test_dust_stomped_resolution_loss_books_full_entry_cost(monkeypatch, tmp_path):
    # No sells (tokens burned/redeemed): resolution-loss must book the
    # reconstructed entry cost (38 × 0.15 = $5.70), not the dust cost.
    monkeypatch.setenv("POLYMARKET_WALLET_ADDRESS", "0xwallet")
    fetcher = MagicMock()
    fetcher.fetch_wallet_positions.return_value = []
    trades = [{"asset": "123456", "side": "BUY", "size": 38.0, "price": 0.15}]
    csv = tmp_path / "gamma_resolution.csv"
    csv.write_text("ticker,resolution_price,condition_id\n123456,0.0,0xabc\n")
    with patch(
        "trading_platform.polymarket.position_fetcher.PositionFetcher",
        return_value=fetcher,
    ), patch("requests.get", return_value=_trade_history_response(trades)), patch(
        "trading_platform.polymarket.live_position_monitor._GAMMA_CSV", csv
    ):
        realized, outcome = _call(
            direction="BUY", fill_price=0.15,
            db_shares=0.003335, size_usd=6.21,
        )
    assert outcome == "loss"
    assert realized == -5.70  # full entry basis, not −(0.003335 × 0.15)


def test_genuinely_small_position_not_reconstructed(monkeypatch, tmp_path):
    # A real 2-share position (value $0.30 < dust floor) whose shares
    # are intact must NOT be inflated to a bigger entry.
    monkeypatch.delenv("POLYMARKET_WALLET_ADDRESS", raising=False)
    monkeypatch.delenv("POLYMARKET_FUNDER_ADDRESS", raising=False)
    csv = tmp_path / "gamma_resolution.csv"
    csv.write_text("ticker,resolution_price,condition_id\n123456,0.0,0xabc\n")
    with patch(
        "trading_platform.polymarket.live_position_monitor._GAMMA_CSV", csv
    ):
        realized, outcome = _call(
            direction="BUY", fill_price=0.15,
            db_shares=2.0, size_usd=0.30,
        )
    assert outcome == "loss"
    assert realized == -0.30  # 2 × 0.15 — unchanged basis


def test_zero_stomped_shares_still_derive_from_size_usd(monkeypatch, tmp_path):
    # Pins the 2026-07-06 behavior through the unified reconstruction:
    # zeroed shares + committed capital derives the entry from size_usd.
    monkeypatch.delenv("POLYMARKET_WALLET_ADDRESS", raising=False)
    monkeypatch.delenv("POLYMARKET_FUNDER_ADDRESS", raising=False)
    csv = tmp_path / "gamma_resolution.csv"
    csv.write_text("ticker,resolution_price,condition_id\n123456,0.0,0xabc\n")
    with patch(
        "trading_platform.polymarket.live_position_monitor._GAMMA_CSV", csv
    ):
        realized, outcome = _call(
            direction="BUY", fill_price=0.10,
            db_shares=0.0, size_usd=5.0,
        )
    assert outcome == "loss"
    assert realized == -5.0  # 50 derived shares × 0.10


def test_zero_shares_no_capital_books_zero_none():
    realized, outcome = _call(db_shares=0.0, size_usd=0.0)
    assert realized == 0.0 and outcome is None
