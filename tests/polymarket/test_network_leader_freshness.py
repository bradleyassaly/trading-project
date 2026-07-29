"""network_leader_entry must go quiet when the copy graph stops refreshing.

The daily wallet_copy_graph task was deleted 2026-07-28 (OOM against the
53M-row wallet_trades; copy-entry strategy killed 2026-07-07), leaving
wallet_copy_relationships frozen at its 2026-07-16 state. Without a
freshness gate the signal would fire forever on that fossil graph — these
tests pin the gate: fresh graph fires, stale/empty graph is silent.
"""
import sqlite3
import threading
import time

from trading_platform.polymarket.whale_signal_engine import WhaleSignalEngine
from trading_platform.polymarket.whale_tripwire import WhaleTrade

NOW = int(time.time())
LEADER = "0xleader"


class _StubDB:
    """Minimal WalletDB stand-in: real sqlite conn + lock + profile lookup."""

    def __init__(self, computed_at: int):
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(":memory:")
        self._conn.execute(
            "CREATE TABLE wallet_copy_relationships ("
            " leader_wallet TEXT, follower_wallet TEXT, n_co_entries INTEGER,"
            " n_markets INTEGER, avg_lag_minutes REAL, leader_total_pnl REAL,"
            " follower_same_pnl REAL, first_observed_ts INTEGER,"
            " last_observed_ts INTEGER, computed_at INTEGER)"
        )
        for i in range(4):  # 4 followers, lag 45min > the 10min MM-bot floor
            self._conn.execute(
                "INSERT INTO wallet_copy_relationships VALUES (?,?,?,?,?,?,?,?,?,?)",
                (LEADER, f"0xfollower{i}", 6, 4, 45.0, 10.0, 5.0,
                 NOW - 86400, NOW, computed_at),
            )

    def get_profile(self, wallet: str) -> dict:
        return {"net_pnl_usdc": 25.0, "directional_win_rate": 0.6}


def _engine(computed_at: int) -> WhaleSignalEngine:
    eng = WhaleSignalEngine(db=_StubDB(computed_at))
    # Stub the sink: _fire_signal does markets/Gamma lookups we don't want
    # in a unit test. The unit under test is the gating logic before it.
    eng._fire_signal = lambda *a, **k: {"fired": True}
    return eng


def _trade() -> WhaleTrade:
    return WhaleTrade(wallet=LEADER, condition_id="c1", side="BUY", price=0.5,
                      size=100, outcome="YES", question="q", category="sports",
                      timestamp=NOW, wallet_tier="tier1",
                      directional_win_rate=0.6, conviction_score=1.0,
                      total_volume_usdc=1000)


def test_fires_on_fresh_graph():
    eng = _engine(NOW - 86400)  # graph refreshed 1d ago
    assert eng._check_network_leader_entry(_trade(), NOW) == {"fired": True}


def test_quiet_on_stale_graph():
    stale = NOW - WhaleSignalEngine._NETWORK_LEADER_MAX_AGE_S - 3600
    eng = _engine(stale)
    assert eng._check_network_leader_entry(_trade(), NOW) is None


def test_quiet_on_empty_graph():
    eng = _engine(NOW - 86400)
    eng.db._conn.execute("DELETE FROM wallet_copy_relationships")
    assert eng._check_network_leader_entry(_trade(), NOW) is None


def test_quiet_below_follower_minimum_even_when_fresh():
    eng = _engine(NOW - 86400)
    eng.db._conn.execute(
        "DELETE FROM wallet_copy_relationships WHERE follower_wallet != '0xfollower0'"
    )
    assert eng._check_network_leader_entry(_trade(), NOW) is None
