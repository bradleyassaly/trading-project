"""WS-vs-poll reconciliation: detect when the WebSocket layer goes dark.

The two wallet-trade ingestion paths (wallet_stream WebSocket and
wallet_trade_poller 10-min poll) both write to `wallet_trades`. There's
no source column to distinguish them — but the `synced_at - timestamp`
gap is a strong proxy:
  - WS-delivered: synced_at within ~10s of timestamp
  - Poll-delivered: synced_at can lag up to 600s (10-min poll cadence)

When WS dies, the fresh % drops sharply — poller picks up everything
but at 5-10x the latency. This script measures the fresh-rate over a
window and flags degradation.

2026-05-23 motivation: today's Gamma 422 silenced live-collect for 24h.
If we'd had this reconciler, we'd have caught the WS drop within the
first hour. Same shape as future wallet_stream silent failures.

2026-07-09: the volume metric now measures TIMELY ingestion only.
Deep-history backfill jobs (ingest_top_wallets_daily) insert rows whose
trade `timestamp` is up to years older than `synced_at`; a 37k-row
backfill burst on 2026-07-08 inflated the 7d baseline and fired a false
"VOLUME DROP" CRITICAL every run while all ingestion paths were healthy.
Both the 24h window and the 7d baseline exclude rows lagging more than
BACKFILL_LAG_SEC — backfill volume says nothing about whether live
ingestion is up.

Usage:
    python scripts/ws_poll_reconcile.py          # human report
    python scripts/ws_poll_reconcile.py --json   # machine output
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from trading_platform.polymarket.db_connection import get_connection


# Anything with synced_at within FRESH_THRESHOLD_SEC of timestamp counts
# as "fresh" (WS-delivered). Above that = polled/delayed.
FRESH_THRESHOLD_SEC = 30
# Critical thresholds
MIN_FRESH_RATE = 0.20    # If <20% fresh, WS likely degraded
MIN_RATE_RATIO = 0.5     # Vs prior week — alert if rate dropped >50%
# 2026-07-09: rows whose synced_at lags timestamp by more than this are
# deep-history backfill (ingest_top_wallets_daily), not live ingestion —
# excluded from both volume windows. 86400, not tighter: the 2h sync
# job's rows must stay in-window.
BACKFILL_LAG_SEC = 86400


def gather_metrics(conn, now_ts: int) -> dict:
    cutoff_24h = now_ts - 86400
    cutoff_7d = now_ts - 7 * 86400

    # Volume by hour for past 24h (timely rows only — see BACKFILL_LAG_SEC)
    rows = conn.execute("""
        SELECT date_trunc('hour', to_timestamp(synced_at)) hr,
               COUNT(*) total,
               SUM(CASE WHEN (synced_at - timestamp) <= %s THEN 1 ELSE 0 END) fresh
          FROM wallet_trades
         WHERE synced_at >= %s
           AND (synced_at - timestamp) <= %s
         GROUP BY 1 ORDER BY 1
    """, (FRESH_THRESHOLD_SEC, cutoff_24h, BACKFILL_LAG_SEC)).fetchall()
    hours = [(r[0], int(r[1] or 0), int(r[2] or 0)) for r in rows]

    # Past 24h summary
    total_24h = sum(h[1] for h in hours)
    fresh_24h = sum(h[2] for h in hours)
    fresh_rate_24h = (fresh_24h / total_24h) if total_24h else 0

    # Prior 7d baseline (same days-of-week if possible, but simple mean is fine).
    # Same backfill exclusion as the 24h window — a backfill burst in the
    # baseline days makes a healthy 24h look like a drop (2026-07-08 incident).
    row = conn.execute("""
        SELECT COUNT(*) total,
               SUM(CASE WHEN (synced_at - timestamp) <= %s THEN 1 ELSE 0 END) fresh
          FROM wallet_trades
         WHERE synced_at >= %s AND synced_at < %s
           AND (synced_at - timestamp) <= %s
    """, (FRESH_THRESHOLD_SEC, cutoff_7d, cutoff_24h, BACKFILL_LAG_SEC)).fetchone()
    total_7d = int(row[0] or 0)
    fresh_7d = int(row[1] or 0)
    # Per-day rate (baseline) for comparison
    baseline_rate_per_day = total_7d / 6  # 6 days of baseline (7d-prior excluding the 24h window)
    fresh_rate_7d = (fresh_7d / total_7d) if total_7d else 0

    # Volume ratio: today's rate vs 7d baseline
    volume_ratio = (total_24h / baseline_rate_per_day) if baseline_rate_per_day else None

    return {
        "ts": now_ts,
        "window_24h": {
            "total": total_24h,
            "fresh": fresh_24h,
            "fresh_rate": round(fresh_rate_24h, 3),
        },
        "baseline_7d": {
            "total": total_7d,
            "fresh": fresh_7d,
            "fresh_rate": round(fresh_rate_7d, 3),
            "rate_per_day": round(baseline_rate_per_day, 0),
        },
        "volume_ratio": round(volume_ratio, 2) if volume_ratio else None,
        "fresh_threshold_sec": FRESH_THRESHOLD_SEC,
        "hourly": [
            {"hr": str(r[0])[:13], "total": r[1], "fresh": r[2]}
            for r in rows[-12:]  # last 12 hours
        ],
    }


def evaluate(metrics: dict) -> list[str]:
    """Return list of alert strings, empty if all OK.

    Reality check (2026-05-23 lag distribution audit): only ~1% of trades
    arrive within 30s of timestamp. The poller produces 99% of inserts at
    5-60min lag. WS is essentially dead AS A DATA SOURCE but the system
    is healthy because the poller fills the gap.

    Therefore: VOLUME is the primary health signal. The fresh-rate is
    informational only — alert only on a step change vs baseline, not
    absolute levels.
    """
    alerts = []
    rate_24h = metrics["window_24h"]["fresh_rate"]
    rate_7d = metrics["baseline_7d"]["fresh_rate"]
    vol_ratio = metrics["volume_ratio"]
    total_24h = metrics["window_24h"]["total"]

    if total_24h == 0:
        alerts.append(
            "CRITICAL — 0 wallet_trades in 24h. Both WS AND poller are dead."
        )
        return alerts

    if vol_ratio is not None and vol_ratio < MIN_RATE_RATIO:
        alerts.append(
            f"VOLUME DROP — 24h count {total_24h:,} is {vol_ratio:.0%} of 7d "
            f"baseline rate. Ingestion path degraded; check wallet-poller "
            f"+ wallet-stream container logs."
        )

    # Step-change in fresh rate: if baseline was high and now it's low, WS dropped
    if rate_7d > 0.10 and rate_24h < rate_7d * 0.3:
        alerts.append(
            f"WS DEGRADED — fresh rate dropped {rate_7d:.0%} → {rate_24h:.0%} "
            "(step change). WebSocket likely silently disconnected. "
            "Restart polymarket-wallet-stream."
        )

    return alerts


def print_human(metrics: dict, alerts: list[str]) -> None:
    print("=" * 72)
    print(f"  WS-vs-poll wallet ingestion reconciliation")
    print("=" * 72)
    w = metrics["window_24h"]
    b = metrics["baseline_7d"]
    print(f"  Last 24h: {w['total']:,} trades ingested, "
          f"{w['fresh']:,} fresh (≤{metrics['fresh_threshold_sec']}s lag) = "
          f"{w['fresh_rate']*100:.0f}%")
    print(f"  7d prior: {b['total']:,} trades, "
          f"{b['fresh']:,} fresh = {b['fresh_rate']*100:.0f}%  "
          f"(≈ {b['rate_per_day']:,.0f}/day)")
    if metrics["volume_ratio"] is not None:
        print(f"  Volume ratio (24h / baseline-per-day): "
              f"{metrics['volume_ratio']:.2f}x  (1.0x = normal)")
    print()
    if alerts:
        for a in alerts:
            print(f"  ALERT: {a}")
    else:
        print("  OK — both ingestion paths healthy.")
    # Show the recent hours
    if metrics["hourly"]:
        print()
        print("  Last 12 hours:")
        for h in metrics["hourly"]:
            bar = "#" * min(int(h["total"] / 30), 20)
            fresh_pct = (h["fresh"] / h["total"] * 100) if h["total"] else 0
            print(f"    {h['hr']}  n={h['total']:>4d}  fresh={fresh_pct:>3.0f}%  {bar}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    now = int(time.time())
    conn = get_connection()
    try:
        metrics = gather_metrics(conn, now)
    finally:
        try: conn.close()
        except Exception: pass
    alerts = evaluate(metrics)
    if args.json:
        print(json.dumps({"metrics": metrics, "alerts": alerts}, default=str))
    else:
        print_human(metrics, alerts)
    # Exit code 2 = drift detected → scheduler can track
    sys.exit(0 if not alerts else 2)


if __name__ == "__main__":
    raise SystemExit(main())
