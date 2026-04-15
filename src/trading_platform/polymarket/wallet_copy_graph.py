"""Wallet copy-relationship miner.

Scans wallet_trades for patterns where wallet B consistently enters
the same (condition_id, side) as wallet A within a short time window
(default 2h) AFTER wallet A's entry. These are "follower" relationships.

Outputs to `wallet_copy_relationships`:
    leader_wallet, follower_wallet, n_co_entries, n_markets,
    avg_lag_minutes, leader_total_pnl, follower_same_trades_pnl,
    last_observed_ts, computed_at

Use cases:
    1. **Signal attribution**: when a tier1 wallet fires, check if they
       are a FOLLOWER of another wallet — if so, the lead wallet's
       action is the originating info and our signal is derivative.
    2. **Leader discovery**: wallets with 5+ followers are by definition
       "leaders" — worth up-weighting.
    3. **Proto-signal**: if wallet A (a known leader with >=3 followers)
       fires, the follower wallets will likely join within 2h.

Limitations:
    - A follower relationship doesn't imply causation. Two wallets may
      both react to the same external signal independently (news, price
      spike). We only measure CO-OCCURRENCE with temporal order.
    - Short-window relationships only; doesn't detect long-hold copyists.
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field

from trading_platform.polymarket.db_connection import db, get_connection

logger = logging.getLogger(__name__)


# Co-entry window: wallet B must enter within this many seconds of A's entry
# on the same (condition_id, side) to count as "following" A on that trade.
COPY_WINDOW_SECONDS = 2 * 3600      # 2 hours
# A pair must have at least this many co-entries to be considered a
# relationship. Low n is coincidence; 5 co-entries is harder to explain.
MIN_CO_ENTRIES = 5
# And across at least this many distinct markets. Same market repeatedly
# is one event, not multiple.
MIN_DISTINCT_MARKETS = 3


_SCHEMA = """
CREATE TABLE IF NOT EXISTS wallet_copy_relationships (
    leader_wallet      TEXT NOT NULL,
    follower_wallet    TEXT NOT NULL,
    n_co_entries       INTEGER NOT NULL,
    n_markets          INTEGER NOT NULL,
    avg_lag_minutes    REAL,
    leader_total_pnl   REAL,
    follower_same_pnl  REAL,
    first_observed_ts  INTEGER,
    last_observed_ts   INTEGER,
    computed_at        INTEGER NOT NULL,
    PRIMARY KEY (leader_wallet, follower_wallet)
);
CREATE INDEX IF NOT EXISTS idx_wcr_leader   ON wallet_copy_relationships(leader_wallet);
CREATE INDEX IF NOT EXISTS idx_wcr_follower ON wallet_copy_relationships(follower_wallet);
CREATE INDEX IF NOT EXISTS idx_wcr_n        ON wallet_copy_relationships(n_co_entries);
"""


def ensure_schema() -> None:
    with db() as c:
        for stmt in _SCHEMA.split(";"):
            if stmt.strip():
                c.execute(stmt)


@dataclass
class _Pair:
    n: int = 0
    markets: set = field(default_factory=set)
    lag_sum: float = 0.0
    leader_pnl: float = 0.0
    follower_pnl: float = 0.0
    first: int = 0
    last: int = 0


def _detect_pairs(trades_by_market: dict[str, list[tuple]]) -> dict[tuple[str, str], _Pair]:
    """Walk each market's trade sequence and record (leader, follower, lag) pairs.

    trades_by_market[cid] = [(wallet, side, timestamp, pnl), ...] sorted by ts.
    """
    pairs: dict[tuple[str, str], _Pair] = defaultdict(_Pair)
    for cid, rows in trades_by_market.items():
        # Group by side so we only match same-side entries
        by_side: dict[str, list[tuple]] = defaultdict(list)
        for r in rows:
            by_side[r[1]].append(r)

        for side, group in by_side.items():
            group.sort(key=lambda x: x[2])
            # For each pair (i, j) where j > i, if ts_j - ts_i <= COPY_WINDOW:
            # wallet_i is a potential leader of wallet_j on this market/side.
            # Only one "lead → follow" event per distinct wallet pair per market.
            seen = set()
            for i in range(len(group)):
                w_i, _, ts_i, pnl_i = group[i]
                for j in range(i + 1, len(group)):
                    w_j, _, ts_j, pnl_j = group[j]
                    lag = ts_j - ts_i
                    if lag > COPY_WINDOW_SECONDS:
                        break  # rest are further out
                    if w_i == w_j:
                        continue
                    key = (w_i, w_j, cid)
                    if key in seen:
                        continue
                    seen.add(key)
                    p = pairs[(w_i, w_j)]
                    p.n += 1
                    p.markets.add(cid)
                    p.lag_sum += lag
                    p.leader_pnl += pnl_i or 0
                    p.follower_pnl += pnl_j or 0
                    p.first = ts_i if not p.first else min(p.first, ts_i)
                    p.last = max(p.last, ts_j)
    return pairs


def refresh(lookback_days: int = 90) -> dict:
    """Compute all pairs over the last N days and persist qualifying relationships."""
    ensure_schema()
    t0 = time.time()
    cutoff = int(time.time() - lookback_days * 86400)

    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT wallet, condition_id, side, timestamp, pnl "
            "FROM wallet_trades WHERE timestamp >= ? AND condition_id IS NOT NULL AND side IS NOT NULL",
            (cutoff,),
        ).fetchall()
    finally:
        conn.close()

    # Group by market (only process markets with multiple wallets)
    by_market: dict[str, list[tuple]] = defaultdict(list)
    for wallet, cid, side, ts, pnl in rows:
        by_market[cid].append((wallet, side, int(ts or 0), pnl))

    # Drop single-wallet markets — no co-entries possible
    multi = {cid: rs for cid, rs in by_market.items() if len({r[0] for r in rs}) >= 2}
    pairs = _detect_pairs(multi)

    # Persist those that pass the thresholds
    now = int(time.time())
    to_insert: list[tuple] = []
    for (leader, follower), p in pairs.items():
        if p.n < MIN_CO_ENTRIES:
            continue
        if len(p.markets) < MIN_DISTINCT_MARKETS:
            continue
        avg_lag_min = round((p.lag_sum / p.n) / 60, 1) if p.n else None
        to_insert.append((
            leader, follower, p.n, len(p.markets),
            avg_lag_min, round(p.leader_pnl, 2), round(p.follower_pnl, 2),
            p.first, p.last, now,
        ))

    with db() as c:
        c.execute("DELETE FROM wallet_copy_relationships")
        if to_insert:
            c.executemany(
                "INSERT INTO wallet_copy_relationships ("
                " leader_wallet, follower_wallet, n_co_entries, n_markets,"
                " avg_lag_minutes, leader_total_pnl, follower_same_pnl,"
                " first_observed_ts, last_observed_ts, computed_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                to_insert,
            )

    elapsed = round(time.time() - t0, 1)
    return {
        "markets_scanned": len(multi),
        "pairs_examined": len(pairs),
        "relationships_written": len(to_insert),
        "elapsed_seconds": elapsed,
    }


def get_followers(leader: str, min_n: int = MIN_CO_ENTRIES) -> list[dict]:
    """Return wallets that follow the given leader."""
    with db() as c:
        rows = c.execute(
            "SELECT follower_wallet, n_co_entries, n_markets, avg_lag_minutes, "
            " follower_same_pnl, leader_total_pnl "
            "FROM wallet_copy_relationships WHERE leader_wallet = ? AND n_co_entries >= ? "
            "ORDER BY n_co_entries DESC",
            (leader, min_n),
        ).fetchall()
    cols = ["follower", "n", "markets", "avg_lag_min", "follower_pnl", "leader_pnl"]
    return [dict(zip(cols, r)) for r in rows]


def get_leaders(follower: str, min_n: int = MIN_CO_ENTRIES) -> list[dict]:
    """Return wallets this wallet follows."""
    with db() as c:
        rows = c.execute(
            "SELECT leader_wallet, n_co_entries, n_markets, avg_lag_minutes, "
            " follower_same_pnl, leader_total_pnl "
            "FROM wallet_copy_relationships WHERE follower_wallet = ? AND n_co_entries >= ? "
            "ORDER BY n_co_entries DESC",
            (follower, min_n),
        ).fetchall()
    cols = ["leader", "n", "markets", "avg_lag_min", "follower_pnl", "leader_pnl"]
    return [dict(zip(cols, r)) for r in rows]


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    result = refresh()
    print(f"copy-graph refresh: {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
