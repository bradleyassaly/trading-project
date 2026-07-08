"""P1: bootstrap + reconcile the canonical market_resolutions table.

--bootstrap  one-time import:
    * gamma_resolution.csv rows → source='gamma_bulk' (same heuristic that
      produced them; the table stops the CSV's rolling-window data loss)
    * UNANIMOUS wallet_trades outcomes → source='wallet_trades_vote'
      (the 6,619 internally-contradictory condition_ids are NEVER imported)

--report     read-only reconciliation matrix across the sources
             (run BEFORE any consumer cutover; writes
             reports/resolution_reconciliation_<date>.csv)

Both are idempotent; record_resolution's monotonic precedence makes
re-running safe.
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trading_platform.polymarket.db_connection import get_connection  # noqa: E402
from trading_platform.polymarket.resolutions import (  # noqa: E402
    ensure_schema, record_resolution, stats,
)

logger = logging.getLogger(__name__)

CSV_PATH = Path("data/polymarket/gamma_resolution.csv")


def bootstrap() -> dict:
    ensure_schema()
    counts = Counter()

    # 1) CSV import (dedupe per condition_id — the file carries 2 rows per
    # market, YES token + NO token, sharing the per-market resolves_yes).
    if CSV_PATH.exists():
        seen: set[str] = set()
        with CSV_PATH.open(newline="", encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                cid = (row.get("condition_id") or "").strip().lower()
                ry = (row.get("resolves_yes") or "").strip().lower()
                if not cid or cid in seen or ry not in ("true", "false"):
                    continue
                seen.add(cid)
                res = record_resolution(
                    cid, source="gamma_bulk",
                    resolves_yes=1 if ry == "true" else 0,
                    question=(row.get("question") or "")[:300] or None,
                )
                counts[f"csv_{res}"] += 1
        print(f"CSV import: {dict(counts)} ({len(seen)} unique cids)")
    else:
        print(f"CSV missing at {CSV_PATH} — skipping CSV import")

    # 2) Unanimous wallet_trades votes. Contradictory cids are excluded by
    # the HAVING clause — they are exactly the rows that poisoned Path 3.
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT condition_id,
                      MIN(market_outcome) AS outcome
                 FROM wallet_trades
                WHERE market_resolved = 1
                  AND market_outcome IN ('YES', 'NO')
                  AND condition_id IS NOT NULL
                GROUP BY condition_id
               HAVING COUNT(DISTINCT market_outcome) = 1""",
        ).fetchall()
    finally:
        try: conn.close()
        except Exception: pass
    for cid, outcome in rows:
        res = record_resolution(
            cid, source="wallet_trades_vote",
            resolves_yes=1 if outcome == "YES" else 0,
        )
        counts[f"wt_{res}"] += 1
    print(f"wallet_trades unanimous votes processed: {len(rows)}")
    print(f"table stats: {stats()}")
    return dict(counts)


def report() -> None:
    """Pairwise disagreement matrix, table vs CSV vs wallet_trades majority
    vs signal_outcomes. Read-only."""
    ensure_schema()
    conn = get_connection()
    try:
        table = {r[0]: int(r[1]) for r in conn.execute(
            "SELECT condition_id, resolves_yes FROM market_resolutions "
            "WHERE resolves_yes IS NOT NULL").fetchall()}
        wt = {}
        for cid, outcome, n_out in conn.execute(
                """SELECT condition_id, MIN(market_outcome),
                          COUNT(DISTINCT market_outcome)
                     FROM wallet_trades
                    WHERE market_resolved = 1 AND market_outcome IN ('YES','NO')
                      AND condition_id IS NOT NULL
                    GROUP BY condition_id""").fetchall():
            wt[cid.lower()] = None if n_out > 1 else (1 if outcome == "YES" else 0)
        so = {r[0].lower(): (1 if float(r[1]) >= 0.5 else 0) for r in conn.execute(
            """SELECT condition_id, resolution_price FROM signal_outcomes
                WHERE resolution_price IS NOT NULL AND condition_id IS NOT NULL
                GROUP BY condition_id, resolution_price""").fetchall()}
        our_cids = {r[0].lower() for r in conn.execute(
            "SELECT DISTINCT condition_id FROM live_trades "
            "WHERE dry_run = 0 AND condition_id IS NOT NULL").fetchall()}
    finally:
        try: conn.close()
        except Exception: pass

    csv_src: dict[str, int] = {}
    if CSV_PATH.exists():
        with CSV_PATH.open(newline="", encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                cid = (row.get("condition_id") or "").strip().lower()
                ry = (row.get("resolves_yes") or "").strip().lower()
                if cid and ry in ("true", "false"):
                    csv_src[cid] = 1 if ry == "true" else 0

    sources = {"table": table, "csv": csv_src,
               "wallet_trades": {k: v for k, v in wt.items() if v is not None},
               "signal_outcomes": so}
    wt_contradictory = sum(1 for v in wt.values() if v is None)

    print(f"\nRESOLUTION RECONCILIATION — {time.strftime('%Y-%m-%d')}")
    for name, src in sources.items():
        print(f"  {name:<16} {len(src)} cids")
    print(f"  wallet_trades internally contradictory: {wt_contradictory} (excluded)")

    names = list(sources)
    disagreements: list[tuple] = []
    print("\n  pairwise overlap / disagreements:")
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            common = set(sources[a]) & set(sources[b])
            dis = [c for c in common if sources[a][c] != sources[b][c]]
            print(f"    {a:<16} vs {b:<16} overlap={len(common):>6} "
                  f"disagree={len(dis):>4}"
                  f"{'  <— includes OUR trades: ' + str(sum(1 for c in dis if c in our_cids)) if dis else ''}")
            disagreements += [(c, a, sources[a][c], b, sources[b][c],
                               c in our_cids) for c in dis]

    out_path = Path("reports") / f"resolution_reconciliation_{time.strftime('%Y-%m-%d')}.csv"
    out_path.parent.mkdir(exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["condition_id", "source_a", "value_a", "source_b",
                    "value_b", "touches_our_trades"])
        w.writerows(disagreements)
    print(f"\n  {len(disagreements)} disagreement rows -> {out_path}")
    ours = [d for d in disagreements if d[5]]
    if ours:
        print(f"  !!! {len(ours)} disagreements touch OUR live trades:")
        for d in ours[:10]:
            print(f"      {d[0][:20]} {d[1]}={d[2]} vs {d[3]}={d[4]}")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", action="store_true")
    ap.add_argument("--report", action="store_true")
    args = ap.parse_args()
    if not (args.bootstrap or args.report):
        ap.error("pass --bootstrap and/or --report")
    if args.bootstrap:
        bootstrap()
    if args.report:
        report()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
