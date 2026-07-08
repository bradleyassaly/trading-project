"""P3-rest: real delivery-lane tag + watched-markets warming.

The lane tag replaces the contaminated detection_latency<=10s proxy: 73
"fast" rows in 14d were poller-timing coincidence while the lane was dark.
Warming closes the 51%+ local-lookup miss rate that sent watched fills to
the slow REST path.
"""
import inspect

from trading_platform.polymarket.wallet_trade_poller import (
    PolledTrade, WalletTradePoller,
)
from trading_platform.polymarket.whale_tripwire import WhaleTrade


def test_polled_trade_defaults_to_poller():
    pt = PolledTrade(wallet="w", condition_id="c", side="BUY", price=0.5,
                     size=1, outcome="YES", title="t", category="sports",
                     timestamp=1, tx_hash="x", slug="s")
    assert pt.source_lane == "poller"


def test_map_trade_carries_chain_direct():
    raw = {"conditionId": "c1", "side": "BUY", "price": 0.4, "size": 2,
           "timestamp": 1_783_000_000, "outcome": "Yes", "title": "q",
           "slug": "s", "transactionHash": "0x1",
           "source_lane": "chain_direct"}
    pt = WalletTradePoller._map_trade("0xw", raw, set())
    assert pt is not None and pt.source_lane == "chain_direct"
    raw.pop("source_lane")
    pt2 = WalletTradePoller._map_trade("0xw", raw, set())
    assert pt2.source_lane == "poller"


def test_whale_trade_default_and_field():
    wt = WhaleTrade(wallet="w", condition_id="c", side="BUY", price=0.5,
                    size=1, outcome="YES", question="q", category="sports",
                    timestamp=1, wallet_tier="tier2",
                    directional_win_rate=0, conviction_score=0,
                    total_volume_usdc=0)
    assert wt.source_lane == "poller"


def test_wiring_end_to_end_source_level():
    from trading_platform.polymarket import wallet_stream as wst
    from trading_platform.polymarket import whale_signal_engine as wse
    from trading_platform.polymarket import polymarket_live_executor as ple
    assert '"source_lane": "chain_direct"' in inspect.getsource(wst)
    assert '"source_lane": getattr(trade, "source_lane"' in inspect.getsource(wse)
    ple_src = inspect.getsource(ple)
    assert "source_lane TEXT" in ple_src           # lazy-ALTER
    assert 'signal.get("source_lane")' in ple_src  # INSERT value


def test_warming_orders_by_recency():
    from trading_platform.polymarket import markets_table as mt
    src = inspect.getsource(mt.warm_watched_markets)
    # Gamma delists resolved markets; an unordered sweep burned 200/200
    # fetches on tombstones. Recency ordering is load-bearing.
    assert "ORDER BY MAX(wt.timestamp) DESC" in src
    assert "_persist_tombstone" in src
