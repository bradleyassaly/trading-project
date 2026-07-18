"""Per-wallet behavioral strategy observer.

Reads wallet_trades and tags each trade against a set of behavioral
strategies. Aggregates into per-(wallet, strategy) statistics for
reverse-engineering.

Strategies detected (all data-driven, no assumptions):

  accumulator     — same wallet, market, side, 3+ trades spaced >=1h apart
  flipper         — BUY then SELL on same market within 24h
  longshot        — entry price < 0.15
  high_conviction — trade USDC notional >= 3x the wallet's own average
  contrarian      — buying against the implied-probability consensus
                    (BUY at price > 0.70 or SELL at price < 0.30)
  category_specialist — 80%+ of trades in one category
  deadline_trader — trade within 48h of market close (needs markets table)
  scaler          — same-market same-side sizes increasing over time

Per strategy we compute: n_trades, n_markets, avg_size, wins, losses,
net_pnl, first_seen, last_seen. This is the raw alpha-decomposition
substrate — "wallet X made $Y from strategy Z across N occurrences".
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable

from trading_platform.polymarket.db_connection import db, get_connection

logger = logging.getLogger(__name__)


STRATEGIES = (
    "accumulator",
    "flipper",
    "longshot",
    "high_conviction",
    "contrarian",
    "category_specialist",
    "deadline_trader",
    "scaler",
)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS wallet_strategy_profiles (
    wallet          TEXT NOT NULL,
    strategy        TEXT NOT NULL,
    n_trades        INTEGER NOT NULL,
    n_markets       INTEGER NOT NULL,
    avg_size_usd    REAL,
    wins            INTEGER,
    losses          INTEGER,
    net_pnl_usd     REAL,
    avg_pnl_per_trade REAL,
    first_seen_ts   INTEGER,
    last_seen_ts    INTEGER,
    computed_at     INTEGER NOT NULL,
    PRIMARY KEY (wallet, strategy)
);
CREATE INDEX IF NOT EXISTS idx_wsp_strategy ON wallet_strategy_profiles(strategy);
CREATE INDEX IF NOT EXISTS idx_wsp_wallet   ON wallet_strategy_profiles(wallet);
CREATE INDEX IF NOT EXISTS idx_wsp_pnl      ON wallet_strategy_profiles(net_pnl_usd);
"""


def ensure_schema() -> None:
    with db() as c:
        for stmt in _SCHEMA.split(";"):
            if stmt.strip():
                c.execute(stmt)


@dataclass
class _Agg:
    n_trades: int = 0
    markets: set = field(default_factory=set)
    size_sum: float = 0.0
    wins: int = 0
    losses: int = 0
    pnl: float = 0.0
    first: int = 0
    last: int = 0

    def add(self, size: float, pnl: float | None, cid: str, ts: int) -> None:
        self.n_trades += 1
        self.size_sum += size
        self.markets.add(cid)
        if pnl is not None:
            if pnl > 0:
                self.wins += 1
            elif pnl < 0:
                self.losses += 1
            self.pnl += pnl
        self.first = ts if not self.first else min(self.first, ts)
        self.last = max(self.last, ts)


def _tag_trades_for_wallet(rows: list[tuple]) -> Iterable[tuple[str, tuple]]:
    """Yield (strategy, (usd, pnl, cid, ts)) tuples for one wallet's trades.

    rows: [(cid, side, size, price, pnl, timestamp, category, market_resolved), ...]
    ordered by timestamp ASC. size is SHARES; the yielded first element is
    USDC notional (size*price) — it feeds the avg_size_usd column.
    """
    # --- pre-compute per-wallet stats for conviction tag (USDC) ---
    usd_sizes = [float(r[2] or 0) * float(r[3] or 0) for r in rows]
    avg_usd = sum(usd_sizes) / len(usd_sizes) if usd_sizes else 0

    # --- pre-compute per-market activity for accumulator/flipper/scaler ---
    by_market: dict[str, list[tuple]] = defaultdict(list)
    for r in rows:
        by_market[r[0]].append(r)

    # --- category specialist: which category dominates >= 80% ---
    cat_counts: dict[str, int] = defaultdict(int)
    for r in rows:
        cat_counts[r[6] or "other"] += 1
    n = len(rows)
    dominant_cat = None
    if n > 0:
        top_cat, top_n = max(cat_counts.items(), key=lambda x: x[1])
        if top_n / n >= 0.80:
            dominant_cat = top_cat

    # --- iterate trades, emit tag tuples ---
    for i, (cid, side, size, price, pnl, ts, cat, resolved) in enumerate(rows):
        size = float(size or 0)
        price = float(price or 0.5)
        usd = size * price
        ts = int(ts or 0)

        # longshot
        if price < 0.15:
            yield "longshot", (usd, pnl, cid, ts)

        # contrarian (buying against consensus implied prob)
        if side == "BUY" and price > 0.70:
            yield "contrarian", (usd, pnl, cid, ts)
        elif side == "SELL" and price < 0.30:
            yield "contrarian", (usd, pnl, cid, ts)

        # high_conviction (money at risk, not share count)
        if avg_usd > 0 and usd >= 3 * avg_usd:
            yield "high_conviction", (usd, pnl, cid, ts)

        # category specialist: every trade counts if wallet has a dominant cat
        if dominant_cat and (cat or "other") == dominant_cat:
            yield "category_specialist", (usd, pnl, cid, ts)

    # --- accumulator: per-market same-side buy sequences spaced >=1h ---
    for cid, mkt_rows in by_market.items():
        by_side: dict[str, list[tuple]] = defaultdict(list)
        for r in mkt_rows:
            by_side[r[1]].append(r)
        for side, group in by_side.items():
            if len(group) < 3:
                continue
            # spaced: consecutive timestamps differ by >=3600s on average
            group_sorted = sorted(group, key=lambda x: x[5])
            timestamps = [int(x[5] or 0) for x in group_sorted]
            gaps = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps)-1)]
            avg_gap = sum(gaps) / max(len(gaps), 1)
            if avg_gap >= 3600:
                for r in group_sorted:
                    yield "accumulator", (float(r[2] or 0) * float(r[3] or 0), r[4], r[0], int(r[5] or 0))

    # --- flipper: both BUY and SELL exist within 24h on same market ---
    for cid, mkt_rows in by_market.items():
        buys = [r for r in mkt_rows if r[1] == "BUY"]
        sells = [r for r in mkt_rows if r[1] == "SELL"]
        if not buys or not sells:
            continue
        for b in buys:
            for s in sells:
                if abs(int(s[5] or 0) - int(b[5] or 0)) <= 86400:
                    yield "flipper", (float(b[2] or 0) * float(b[3] or 0), b[4], b[0], int(b[5] or 0))
                    yield "flipper", (float(s[2] or 0) * float(s[3] or 0), s[4], s[0], int(s[5] or 0))
                    break  # count flipper once per buy

    # --- scaler: same-market same-side, size increasing over time ---
    for cid, mkt_rows in by_market.items():
        for side in ("BUY", "SELL"):
            group = sorted(
                [r for r in mkt_rows if r[1] == side],
                key=lambda x: x[5],
            )
            if len(group) < 3:
                continue
            # Detection stays on SHARES (position accumulation mechanics);
            # the yielded stat is USDC notional like every other strategy.
            sizes_seq = [float(r[2] or 0) for r in group]
            # Strict monotonic (allow tiny jitter — 10% tolerance)
            is_scaling = all(
                sizes_seq[i+1] >= sizes_seq[i] * 0.90
                and sizes_seq[-1] >= sizes_seq[0] * 1.5
                for i in range(len(sizes_seq)-1)
            )
            if is_scaling:
                for r in group:
                    yield "scaler", (float(r[2] or 0) * float(r[3] or 0), r[4], r[0], int(r[5] or 0))


def _tag_deadline_trades(rows: list[tuple], end_dates: dict[str, int]) -> Iterable[tuple]:
    """Yield deadline_trader tags for trades fired <48h before market end.

    rows shape same as above. end_dates: cid -> epoch seconds.
    """
    for cid, side, size, price, pnl, ts, cat, resolved in rows:
        end = end_dates.get(cid)
        if not end or not ts:
            continue
        hours_to_close = (end - int(ts)) / 3600
        if 0 < hours_to_close <= 48:
            yield "deadline_trader", (float(size or 0) * float(price or 0), pnl, cid, int(ts))


def observe_all(min_trades_per_wallet: int = 10) -> dict:
    """Run the observer across every wallet with enough trade history.

    Returns a dict summary: wallets_scanned, rows_written, per-strategy totals.
    """
    ensure_schema()
    t0 = time.time()

    # Load end-dates for deadline detection
    end_dates: dict[str, int] = {}
    try:
        with db() as c:
            for cid, ed in c.execute(
                "SELECT condition_id, end_date_iso FROM markets WHERE end_date_iso IS NOT NULL"
            ):
                try:
                    from datetime import datetime, timezone
                    clean = ed.replace("Z", "+00:00") if ed.endswith("Z") else ed
                    dt = datetime.fromisoformat(clean)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    end_dates[cid] = int(dt.timestamp())
                except Exception:
                    continue
    except Exception as exc:
        logger.debug("end_dates load failed: %s", exc)

    # Find wallets with enough trades
    with db() as c:
        wallets = [
            w for w, in c.execute(
                "SELECT wallet FROM wallet_trades GROUP BY wallet HAVING COUNT(*) >= ?",
                (min_trades_per_wallet,),
            ).fetchall()
        ]

    wallets_scanned = 0
    rows_written = 0
    strategy_totals: dict[str, int] = defaultdict(int)

    for wallet in wallets:
        # Load this wallet's trades
        with db() as c:
            rows = c.execute(
                "SELECT condition_id, side, size, price, pnl, timestamp, category, market_resolved "
                "FROM wallet_trades WHERE wallet = ? ORDER BY timestamp ASC",
                (wallet,),
            ).fetchall()
        if len(rows) < min_trades_per_wallet:
            continue
        wallets_scanned += 1

        # Aggregate per-strategy
        aggs: dict[str, _Agg] = defaultdict(_Agg)
        for strategy, (size, pnl, cid, ts) in _tag_trades_for_wallet(rows):
            aggs[strategy].add(size, pnl, cid, ts)
        # Add deadline detections
        for strategy, (size, pnl, cid, ts) in _tag_deadline_trades(rows, end_dates):
            aggs[strategy].add(size, pnl, cid, ts)

        # Write
        now = int(time.time())
        to_insert = []
        for strategy, a in aggs.items():
            if a.n_trades == 0:
                continue
            to_insert.append((
                wallet, strategy, a.n_trades, len(a.markets),
                round(a.size_sum / a.n_trades, 2) if a.n_trades else None,
                a.wins, a.losses, round(a.pnl, 2),
                round(a.pnl / a.n_trades, 4) if a.n_trades else None,
                a.first, a.last, now,
            ))
            strategy_totals[strategy] += 1

        if to_insert:
            with db() as c:
                c.execute(
                    "DELETE FROM wallet_strategy_profiles WHERE wallet = ?",
                    (wallet,),
                )
                c.executemany(
                    "INSERT INTO wallet_strategy_profiles "
                    "(wallet, strategy, n_trades, n_markets, avg_size_usd, "
                    " wins, losses, net_pnl_usd, avg_pnl_per_trade, "
                    " first_seen_ts, last_seen_ts, computed_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    to_insert,
                )
                rows_written += len(to_insert)

    return {
        "wallets_scanned": wallets_scanned,
        "rows_written": rows_written,
        "strategy_totals": dict(strategy_totals),
        "elapsed_seconds": round(time.time() - t0, 1),
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    result = observe_all()
    print(f"observer result: {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
