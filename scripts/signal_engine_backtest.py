"""Signal-engine backtest — replays historical wallet trades through the
live signal engine and looks up the eventual market resolution to compute
per-signal-type EV / WR / PnL.

Why this exists:
- `signal_outcomes.resolved_at` only captures signals that actually FIRED
  in production. New signal types haven't had time to accumulate data.
- `backtester.py` simulates following a single wallet — doesn't test
  the signal engine at all.
- `signal_backtests.py` tests 6 hardcoded signal hypotheses, not our
  current engine's 16+ signal types.

This script is the missing link: run today's signal engine against the
last N days of wallet_trades, compute hypothetical EV per signal type
from Gamma resolution data. Gives us fast validation signal without
waiting for live trades to resolve.

Output:
- stdout: per-signal-type EV table + top-conviction sample fires
- optional --write: persists to a new `signal_backtest_results` table
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from trading_platform.polymarket.db_connection import db, get_connection
from trading_platform.polymarket.wallet_db import WalletDB
from trading_platform.polymarket.whale_signal_engine import WhaleSignalEngine
from trading_platform.polymarket.whale_tripwire import WhaleTrade


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_GAMMA_CSV = _PROJECT_ROOT / "data" / "polymarket" / "gamma_resolution.csv"


def _load_resolutions() -> dict[str, float]:
    """Return {cid_lower: yes_resolution_price} from the gamma CSV."""
    out: dict[str, float] = {}
    if not _GAMMA_CSV.exists():
        return out
    with _GAMMA_CSV.open(newline="", encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            cid = (row.get("condition_id") or "").strip().lower()
            if not cid:
                continue
            ry = (row.get("resolves_yes") or "").strip().lower()
            if ry == "true":
                out[cid] = 1.0
            elif ry == "false":
                out[cid] = 0.0
    return out


def _outcome_delta(
    direction: str, entry_price: float, res_price: float,
) -> float | None:
    """Stored in YES-token frame: BUY/BUY_YES gain = res - entry;
    SELL/BUY_NO gain = (1-res) - (1-entry)."""
    d = (direction or "").upper()
    if d in ("BUY", "BUY_YES"):
        return res_price - entry_price
    if d in ("SELL", "BUY_NO"):
        return (1.0 - res_price) - (1.0 - entry_price)
    return None


class _SilentDB(WalletDB):
    """No-op writes so the engine doesn't pollute production."""
    def insert_signal(self, **kw): pass
    def insert_alert(self, **kw): pass
    def increment_category_signal(self, *a, **kw): pass
    def update_signal_confidence(self, *a, **kw): pass


def run(days: int, write: bool = False) -> dict:
    t0 = time.time()
    resolutions = _load_resolutions()
    print(f"loaded {len(resolutions):,} market resolutions from gamma CSV")

    engine = WhaleSignalEngine(db=_SilentDB())
    engine._record_signal_outcome = lambda *a, **kw: None
    # Disable live-executor auto-routing during replay
    from trading_platform.polymarket import whale_signal_engine as _wse
    original_fire = engine._fire_signal

    # Capture fires instead of writing them
    fires: list[dict] = []

    def capture(signal_type, trade, confidence, now_ts, extra=None):
        fires.append({
            "signal_type": signal_type,
            "condition_id": trade.condition_id,
            "wallet": trade.wallet,
            "direction": trade.side,
            "entry_price": trade.price,
            "size": trade.size,
            "category": trade.category,
            "tier": trade.wallet_tier,
            "confidence": confidence,
            "fired_at": now_ts,
            "extra": extra or {},
        })
        return {"signal_type": signal_type, "confidence": confidence}

    engine._fire_signal = capture

    # Build tier/wallet metadata
    conn = get_connection()
    tier_map = dict(conn.execute("SELECT wallet, tier FROM leaderboard").fetchall())
    meta = {}
    for w, dwr, cs, tv in conn.execute(
        "SELECT wallet, directional_win_rate, conviction_score, total_volume_usdc FROM wallet_profiles"
    ):
        meta[w] = {"dwr": dwr or 0, "cs": cs or 0, "tv": tv or 0}

    # Pull trades — filter to wallets on the leaderboard (what engine gates on)
    cutoff = int(time.time() - days * 86400)
    trades_rows = conn.execute(
        "SELECT wt.wallet, wt.condition_id, wt.side, wt.price, wt.size, wt.outcome, "
        "wt.title, wt.category, wt.timestamp "
        "FROM wallet_trades wt JOIN leaderboard l ON wt.wallet = l.wallet "
        "WHERE wt.timestamp >= ? AND wt.size >= 25 AND wt.price IS NOT NULL "
        "ORDER BY wt.timestamp ASC",
        (cutoff,),
    ).fetchall()
    conn.close()
    print(f"replaying {len(trades_rows):,} trades from the last {days} days...")

    for i, row in enumerate(trades_rows):
        wallet, cid, side, price, size, outcome, title, cat, ts = row
        m = meta.get(wallet, {})
        wt = WhaleTrade(
            wallet=wallet,
            condition_id=cid or "",
            side=(side or "").upper(),
            price=float(price or 0),
            size=float(size or 0),
            outcome=(outcome or "").upper(),
            question=title or "",
            category=cat or "other",
            timestamp=int(ts or 0),
            wallet_tier=tier_map.get(wallet) or "",
            directional_win_rate=m.get("dwr", 0),
            conviction_score=m.get("cs", 0),
            total_volume_usdc=m.get("tv", 0),
        )
        try:
            engine.on_whale_trade(wt)
        except Exception:
            continue

    print(f"engine replay: {len(fires):,} signal fires captured")

    # ── Resolve each fire ───────────────────────────────────────────────
    by_type: dict[str, list[tuple]] = defaultdict(list)
    for f in fires:
        res = resolutions.get((f["condition_id"] or "").lower())
        if res is None:
            by_type[f["signal_type"]].append(("pending", f))
            continue
        od = _outcome_delta(f["direction"], f["entry_price"], res)
        if od is None:
            by_type[f["signal_type"]].append(("bad_direction", f))
            continue
        win = 1 if od > 0 else 0
        by_type[f["signal_type"]].append(("resolved", f, od, win, res))

    # ── Aggregate + report ──────────────────────────────────────────────
    print("\n" + "=" * 84)
    print(f"  BACKTEST: SIGNAL EV OVER LAST {days} DAYS")
    print("=" * 84)
    print(f"{'signal_type':<26s} {'fires':>7s} {'resolved':>9s} {'wr':>6s} "
          f"{'ev':>9s} {'total_delta':>12s} {'pending':>8s}")
    print("-" * 84)

    rollup = []
    for st in sorted(by_type.keys()):
        entries = by_type[st]
        resolved_entries = [e for e in entries if e[0] == "resolved"]
        pending = sum(1 for e in entries if e[0] == "pending")
        n_res = len(resolved_entries)
        if n_res == 0:
            print(f"  {st:<24s} {len(entries):>7d} {0:>9d}  {'n/a':>5s} {'n/a':>8s} {'n/a':>12s} {pending:>8d}")
            rollup.append({
                "signal_type": st, "fires": len(entries), "resolved": 0,
                "wr": None, "ev": None, "total_delta": None, "pending": pending,
            })
            continue
        wins = sum(e[3] for e in resolved_entries)
        total_delta = sum(e[2] for e in resolved_entries)
        ev = total_delta / n_res
        wr = wins / n_res
        print(f"  {st:<24s} {len(entries):>7d} {n_res:>9d} {wr*100:>5.0f}% {ev:>+9.4f} {total_delta:>+12.2f} {pending:>8d}")
        rollup.append({
            "signal_type": st, "fires": len(entries), "resolved": n_res,
            "wr": round(wr, 4), "ev": round(ev, 4),
            "total_delta": round(total_delta, 2), "pending": pending,
        })

    # ── Verdict ─────────────────────────────────────────────────────────
    print("\n" + "=" * 84)
    print("  VERDICT")
    print("=" * 84)
    for r in sorted(rollup, key=lambda x: -(x["ev"] or -999)):
        if r["resolved"] == 0:
            verdict = "◌ no resolved data"
        elif r["resolved"] < 20:
            verdict = f"? small sample ({r['resolved']})"
        elif (r["ev"] or 0) > 0.05:
            verdict = f"✓ positive EV {r['ev']:+.3f}"
        elif (r["ev"] or 0) < -0.02:
            verdict = f"✗ negative EV {r['ev']:+.3f}"
        else:
            verdict = f"◐ near-zero EV {r['ev']:+.3f}"
        print(f"  {r['signal_type']:<26s} {verdict}")

    if write:
        _persist(rollup, days)

    print(f"\nelapsed: {time.time()-t0:.1f}s")
    return {"fires": len(fires), "signal_types": len(rollup)}


def _persist(rollup: list[dict], days: int) -> None:
    schema = """
    CREATE TABLE IF NOT EXISTS signal_backtest_results (
        run_ts        INTEGER NOT NULL,
        lookback_days INTEGER NOT NULL,
        signal_type   TEXT NOT NULL,
        fires         INTEGER,
        resolved      INTEGER,
        wr            REAL,
        ev            REAL,
        total_delta   REAL,
        pending       INTEGER,
        PRIMARY KEY (run_ts, signal_type)
    );"""
    now = int(time.time())
    with db() as c:
        for stmt in schema.split(";"):
            if stmt.strip():
                c.execute(stmt)
        c.executemany(
            "INSERT OR REPLACE INTO signal_backtest_results ("
            " run_ts, lookback_days, signal_type, fires, resolved, wr, ev, total_delta, pending"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (now, days, r["signal_type"], r["fires"], r["resolved"],
                 r["wr"], r["ev"], r["total_delta"], r["pending"])
                for r in rollup
            ],
        )
    print(f"persisted {len(rollup)} results to signal_backtest_results (run_ts={now})")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=30, help="Lookback window in days")
    p.add_argument("--write", action="store_true", help="Persist results to signal_backtest_results table")
    args = p.parse_args()
    run(args.days, write=args.write)


if __name__ == "__main__":
    main()
