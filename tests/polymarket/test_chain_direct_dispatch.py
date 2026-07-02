"""Chain-direct dispatch: OrderFilled logs reach the signal engine
without waiting on the Data API.

Pins the 2026-07-02 latency fix. Day-1 instrumentation showed p50
whale-fill→attempt latency of 8.8 minutes because the decoded chain
fill was discarded in favor of a data-api poll (indexing lag). Known
tokens must now dispatch a synthetic data-api-shaped trade immediately.
"""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from trading_platform.polymarket.wallet_stream import (
    ORDER_FILLED_TOPIC,
    WalletStream,
    _addr_from_topic,
    _pad_addr,
)

WHALE = "0x1111111111111111111111111111111111111111"
OTHER = "0x2222222222222222222222222222222222222222"
TOKEN_ID = 987654321
USDC_6 = 10_000_000      # $10.00
SHARES_6 = 25_000_000    # 25 shares → price 0.40


def _u256_hex(v: int) -> str:
    return hex(v)[2:].rjust(64, "0")


def _order_filled_log(maker: str, taker: str, *, maker_asset: int,
                      taker_asset: int, maker_amt: int, taker_amt: int) -> dict:
    return {
        "topics": [
            ORDER_FILLED_TOPIC,
            "0x" + "ab" * 32,          # orderHash
            _pad_addr(maker),
            _pad_addr(taker),
        ],
        "data": "0x" + "".join(
            _u256_hex(v) for v in (maker_asset, taker_asset, maker_amt, taker_amt, 0)
        ),
        "transactionHash": "0x" + "cd" * 32,
    }


def _stream() -> WalletStream:
    return WalletStream("wss://example.invalid", {WHALE})


def test_addr_topic_roundtrip():
    assert _addr_from_topic(_pad_addr(WHALE)) == WHALE


def _run_handler(stream: WalletStream, log: dict) -> None:
    asyncio.run(stream._handle_order_filled(log, log["topics"]))


def test_known_token_dispatches_synthetic_trade(monkeypatch):
    monkeypatch.setenv("CHAIN_DIRECT_DISPATCH", "1")
    stream = _stream()
    stream._market_for_token = MagicMock(return_value={
        "condition_id": "0xcond", "question": "Will X happen?", "outcome": "NO",
    })
    poller = MagicMock()
    poller.process_wallet.return_value = {"new_trades": 1, "signals_fired": 1}
    stream._poller = poller
    # Maker pays USDC (asset 0) for the token → maker (our whale) is BUYING.
    log = _order_filled_log(WHALE, OTHER, maker_asset=0, taker_asset=TOKEN_ID,
                            maker_amt=USDC_6, taker_amt=SHARES_6)
    with patch.object(stream, "_poll_wallet"):
        _run_handler(stream, log)

    assert poller.process_wallet.call_count == 1
    wallet_arg, trades_arg = poller.process_wallet.call_args[0]
    assert wallet_arg == WHALE
    t = trades_arg[0]
    assert t["conditionId"] == "0xcond"
    assert t["side"] == "BUY"
    assert t["outcome"] == "NO"
    assert t["size"] == pytest.approx(25.0)
    assert t["price"] == pytest.approx(0.40)
    assert t["transactionHash"].startswith("0x")
    assert stream._stats["chain_direct_dispatches"] == 1


def test_taker_side_inverts_direction(monkeypatch):
    monkeypatch.setenv("CHAIN_DIRECT_DISPATCH", "1")
    stream = _stream()
    stream._market_for_token = MagicMock(return_value={
        "condition_id": "0xcond", "question": "Q", "outcome": "YES",
    })
    poller = MagicMock()
    poller.process_wallet.return_value = {}
    stream._poller = poller
    # Maker (OTHER) pays USDC → maker buys; our whale is the TAKER → SELL.
    log = _order_filled_log(OTHER, WHALE, maker_asset=0, taker_asset=TOKEN_ID,
                            maker_amt=USDC_6, taker_amt=SHARES_6)
    with patch.object(stream, "_poll_wallet"):
        _run_handler(stream, log)
    assert poller.process_wallet.call_args[0][1][0]["side"] == "SELL"


def test_unknown_token_falls_back_to_poll_only(monkeypatch):
    monkeypatch.setenv("CHAIN_DIRECT_DISPATCH", "1")
    stream = _stream()
    stream._market_for_token = MagicMock(return_value=None)
    poller = MagicMock()
    stream._poller = poller
    log = _order_filled_log(WHALE, OTHER, maker_asset=0, taker_asset=TOKEN_ID,
                            maker_amt=USDC_6, taker_amt=SHARES_6)
    with patch.object(stream, "_poll_wallet"):
        _run_handler(stream, log)
    poller.process_wallet.assert_not_called()
    assert stream._stats["chain_direct_misses"] == 1
    assert stream._stats["polls_triggered"] == 1


def test_flag_off_preserves_old_behavior(monkeypatch):
    monkeypatch.setenv("CHAIN_DIRECT_DISPATCH", "0")
    stream = _stream()
    stream._market_for_token = MagicMock()
    poller = MagicMock()
    stream._poller = poller
    log = _order_filled_log(WHALE, OTHER, maker_asset=0, taker_asset=TOKEN_ID,
                            maker_amt=USDC_6, taker_amt=SHARES_6)
    with patch.object(stream, "_poll_wallet"):
        _run_handler(stream, log)
    stream._market_for_token.assert_not_called()
    poller.process_wallet.assert_not_called()
    assert stream._stats["polls_triggered"] == 1


def test_unwatched_wallets_ignored(monkeypatch):
    monkeypatch.setenv("CHAIN_DIRECT_DISPATCH", "1")
    stream = _stream()
    poller = MagicMock()
    stream._poller = poller
    log = _order_filled_log(OTHER, OTHER, maker_asset=0, taker_asset=TOKEN_ID,
                            maker_amt=USDC_6, taker_amt=SHARES_6)
    with patch.object(stream, "_poll_wallet"):
        _run_handler(stream, log)
    poller.process_wallet.assert_not_called()
    assert stream._stats["polls_triggered"] == 0
