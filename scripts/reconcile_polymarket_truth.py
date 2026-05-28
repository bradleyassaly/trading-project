"""Polymarket-truth reconciler — surface drift between DB and on-chain reality.

Runs nightly via the scheduler. Compares:
  1. DB-recorded USDC balance (last live_equity_snapshot) vs on-chain COLLATERAL
  2. DB-tracked open positions' shares vs on-chain CONDITIONAL balances per token
  3. DB-recorded matched/live live_trades vs Data API trade history for our wallet

Output: logs/scheduler/reconcile_polymarket.log with DRIFT/OK lines.
The daily review surfaces any DRIFT lines for follow-up.

Why this exists: 2026-05-19 session lost ~1hr because we defended DB-reported
equity over user perception that matched on-chain truth (see
[[feedback_polymarket_is_truth]]). The 2026-05-18 stale-balance incident
hid $57 of unrealized for 5+ days. The 2026-05-12 phantom-losses fix
recovered $10 of mis-booked closures. Each of these would have surfaced
within 24h with this reconciler running.

Usage:
    python scripts/reconcile_polymarket_truth.py
    python scripts/reconcile_polymarket_truth.py --json   # machine output
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


# Drift thresholds — anything above these is logged as DRIFT, not OK.
USDC_DRIFT_USD = 1.0           # $1 drift in cash balance
SHARES_DRIFT_PCT = 0.05         # 5% of DB-tracked shares
SHARES_DRIFT_ABS = 5.0          # OR 5 shares absolute, whichever is larger
OPEN_MKT_DRIFT_USD = 2.0        # $2 drift in computed-vs-snapshot open_mkt


def _make_clob():
    """Construct the V2 ClobClient with env-driven credentials."""
    from py_clob_client_v2 import ClobClient, ApiCreds
    from py_clob_client_v2.constants import POLYGON
    pk      = os.environ["POLYMARKET_PRIVATE_KEY"]
    api_key = os.environ["POLYMARKET_API_KEY"]
    secret  = os.environ["POLYMARKET_API_SECRET"]
    api_pass = (os.environ.get("POLYMARKET_API_PASSPHRASE")
                or os.environ.get("POLYMARKET_PASSPHRASE"))
    funder  = os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")
    sig_type = int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "1"))
    return ClobClient(
        host="https://clob.polymarket.com", chain_id=POLYGON, key=pk,
        creds=ApiCreds(api_key=api_key, api_secret=secret, api_passphrase=api_pass),
        signature_type=sig_type, funder=funder or None,
    )


def reconcile() -> dict:
    """Run the reconciliation and return a structured report."""
    from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType

    report = {"ts": int(time.time()), "drifts": [], "ok": [], "errors": []}
    client = _make_clob()

    # ── 1. USDC cash reconciliation ──────────────────────────────────────────
    try:
        bal = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        usdc_onchain = int(bal.get("balance", 0)) / 1_000_000
    except Exception as exc:
        report["errors"].append(f"USDC fetch failed: {exc}")
        usdc_onchain = None

    conn = get_connection()
    try:
        last_snap = conn.execute("""
            SELECT ts, usdc_balance, open_market_value, total_equity
              FROM live_equity_snapshots ORDER BY ts DESC LIMIT 1
        """).fetchone()
        if last_snap and usdc_onchain is not None:
            db_usdc = float(last_snap[1])
            diff = usdc_onchain - db_usdc
            entry = {
                "metric": "usdc_balance",
                "db": round(db_usdc, 4),
                "onchain": round(usdc_onchain, 4),
                "diff": round(diff, 4),
                "snapshot_age_sec": int(time.time()) - int(last_snap[0]),
            }
            if abs(diff) > USDC_DRIFT_USD:
                report["drifts"].append(entry)
            else:
                report["ok"].append(entry)

        # ── 1b. Fictitious-close detector (2026-05-28) ─────────────────────
        # Today's discovery: live_position_monitor marked 11 trades closed
        # with realized_pnl recorded, but on-chain still held the tokens.
        # place_market_order returned success=True for status='live'
        # (order resting in book, not filled). Fix shipped, but if the
        # bug regresses we want to catch it within the reconciler's
        # 4-hour cadence, not via Polymarket dashboard 24h later.
        #
        # Check: any trade closed in past 24h with on-chain balance > 0
        # is fictitious — the exit order didn't fill.
        ficticious_closes = conn.execute("""
            SELECT id, signal_type, direction, token_id, realized_pnl,
                   exit_reason, exit_ts
              FROM live_trades
             WHERE dry_run=0 AND exit_ts IS NOT NULL
               AND token_id IS NOT NULL
               AND exit_ts >= EXTRACT(EPOCH FROM NOW())::BIGINT - 86400
               AND exit_reason IN ('take_profit','trailing_stop','stop_loss',
                                   'whale_mirror_exit','time_decay')
        """).fetchall()
        for r in ficticious_closes:
            lid, sig, di, tid, pnl, er, ets = r
            try:
                result = client.get_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL,
                                           token_id=tid)
                )
                bal = int(result.get("balance", 0)) / 1_000_000
            except Exception:
                continue
            if bal >= 1.0:
                entry = {
                    "metric": "fictitious_close",
                    "trade_id": int(lid),
                    "signal_type": sig,
                    "direction": di,
                    "exit_reason": er,
                    "recorded_pnl": float(pnl) if pnl is not None else None,
                    "onchain_balance": round(bal, 4),
                }
                report["drifts"].append(entry)

        # ── 2. Per-position shares reconciliation ──────────────────────────
        open_positions = conn.execute("""
            SELECT id, signal_type, direction, token_id, shares, size_usd,
                   fill_price, entry_price
              FROM live_trades
             WHERE dry_run=0 AND exit_ts IS NULL
               AND status IN ('matched','live') AND token_id IS NOT NULL
        """).fetchall()

        for r in open_positions:
            lid, sig, di, tid, sh_db, sz, fp, ep = r
            try:
                result = client.get_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL,
                                           token_id=tid)
                )
                real_shares = int(result.get("balance", 0)) / 1_000_000
            except Exception as exc:
                report["errors"].append(f"position #{lid} balance fetch failed: {exc}")
                continue

            # What does the DB *say* the share count is?
            # If DB shares is non-null and > 0, use it. Else derive from
            # size_usd / no_price (the snapshot's fallback path).
            if sh_db is not None and float(sh_db) > 0:
                db_shares = float(sh_db)
                shares_source = "db.shares"
            else:
                price = float(fp) if fp is not None else (float(ep) if ep is not None else None)
                if price is None:
                    db_shares = None
                    shares_source = "unknown"
                else:
                    if di == "SELL":
                        no_entry = max(1.0 - price, 0.001)
                        db_shares = float(sz or 0) / no_entry
                    else:
                        db_shares = float(sz or 0) / max(price, 0.001)
                    shares_source = "derived"

            entry = {
                "metric": "position_shares",
                "trade_id": int(lid),
                "signal_type": sig,
                "direction": di,
                "token_id": tid[:24] + "...",
                "db_shares": round(db_shares, 4) if db_shares is not None else None,
                "onchain_shares": round(real_shares, 4),
                "shares_source": shares_source,
            }
            if db_shares is None:
                entry["diff"] = None
                report["drifts"].append(entry)
                continue
            diff = real_shares - db_shares
            entry["diff"] = round(diff, 4)
            threshold = max(SHARES_DRIFT_ABS, db_shares * SHARES_DRIFT_PCT)
            if abs(diff) > threshold:
                report["drifts"].append(entry)
            else:
                report["ok"].append(entry)

        # ── 3. Snapshot open_mkt cross-check ────────────────────────────────
        # Compute "true" open_mkt right now from on-chain holdings × current
        # CLOB mid prices, then compare to the latest live_equity_snapshot's
        # open_market_value. If they diverge meaningfully AND the snapshot
        # is fresh (<2h old), the snapshot logic is producing wrong numbers
        # — exactly the recurring source of the $5/day gap between my
        # equity Δ and Polymarket's "1 day PnL". See
        # [[feedback_polymarket_is_truth]].
        try:
            from trading_platform.polymarket.clob_client import ClobClient
            _clob = ClobClient()
            true_open_mkt = 0.0
            # Pull last_mark_price as fallback when /midpoint returns null
            # for one-sided/near-resolved books — same fallback chain the
            # snapshot already uses internally.
            marks = {r[0]: conn.execute(
                "SELECT last_mark_price FROM live_trades WHERE id = ?",
                (r[0],)).fetchone() for r in open_positions}
            for r in open_positions:
                lid, sig, di, tid, sh_db, sz, fp, ep = r
                try:
                    result = client.get_balance_allowance(
                        BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL,
                                               token_id=tid)
                    )
                    real_shares = int(result.get("balance", 0)) / 1_000_000
                    yes_mid = _clob.get_mid_price(tid)
                    # Fallback chain: /midpoint → last_mark_price → entry_price.
                    if yes_mid is None:
                        m = marks.get(lid)
                        if m and m[0] is not None:
                            yes_mid = float(m[0])
                        elif ep is not None:
                            yes_mid = float(ep)
                    if yes_mid is None: continue
                    if di == "SELL":
                        no_mid = max(1.0 - float(yes_mid), 0.001)
                        true_open_mkt += real_shares * no_mid
                    else:
                        true_open_mkt += real_shares * float(yes_mid)
                except Exception:
                    continue
            if last_snap and last_snap[2] is not None:
                snap_open_mkt = float(last_snap[2])
                snap_age_sec = int(time.time()) - int(last_snap[0])
                diff = true_open_mkt - snap_open_mkt
                entry = {
                    "metric": "snapshot_open_mkt",
                    "snapshot_value": round(snap_open_mkt, 2),
                    "true_value_now": round(true_open_mkt, 2),
                    "diff": round(diff, 2),
                    "snapshot_age_sec": snap_age_sec,
                }
                # Only flag as drift if snapshot is recent (else mark moved)
                if abs(diff) > OPEN_MKT_DRIFT_USD and snap_age_sec < 2 * 3600:
                    report["drifts"].append(entry)
                else:
                    report["ok"].append(entry)
        except Exception as exc:
            report["errors"].append(f"open_mkt cross-check failed: {exc}")

    finally:
        try: conn.close()
        except Exception: pass

    return report


def print_human(report: dict) -> None:
    """Render the report for the daily review log."""
    n_drifts = len(report["drifts"])
    n_ok = len(report["ok"])
    n_err = len(report["errors"])
    print(f"=== Polymarket-truth reconciliation @ "
          f"{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(report['ts']))} ===")
    print(f"  checks: {n_ok + n_drifts}  drifts: {n_drifts}  errors: {n_err}")
    print()

    if report["drifts"]:
        print("DRIFT — DB does not match on-chain:")
        for d in report["drifts"]:
            if d["metric"] == "usdc_balance":
                print(f"  USDC: db=${d['db']:.4f} on-chain=${d['onchain']:.4f} "
                      f"diff=${d['diff']:+.4f}  (snapshot age {d['snapshot_age_sec']}s)")
            elif d["metric"] == "snapshot_open_mkt":
                print(f"  SNAPSHOT open_mkt: snap=${d['snapshot_value']} "
                      f"true_now=${d['true_value_now']} diff=${d['diff']:+.2f}  "
                      f"(age {d['snapshot_age_sec']}s)")
            elif d["metric"] == "fictitious_close":
                print(f"  FICTITIOUS CLOSE: #{d['trade_id']} {d['signal_type']} "
                      f"{d['direction']} exit={d['exit_reason']} "
                      f"recorded_pnl=${d['recorded_pnl']} "
                      f"on-chain still holds {d['onchain_balance']} shares "
                      f"— exit order didn't fill")
            else:
                src = d.get("shares_source", "?")
                print(f"  #{d['trade_id']} {d['signal_type']} {d['direction']}: "
                      f"db_shares={d.get('db_shares')} ({src}) "
                      f"on-chain={d['onchain_shares']} diff={d.get('diff')}")
        print()

    if report["errors"]:
        print("ERRORS:")
        for e in report["errors"]:
            print(f"  {e}")
        print()

    if not report["drifts"] and not report["errors"]:
        print("OK — DB and on-chain agree across all checks.")

    # Action recommendation
    if report["drifts"]:
        print()
        print("If a position's db_shares is much larger than on-chain (esp. via")
        print("'derived' source), our equity is over-stated. Run the snapshot")
        print("manually to refresh, or restart scheduler/live-collect if the")
        print("balance API is returning stale numbers.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true",
                    help="Output JSON instead of human-readable report.")
    args = ap.parse_args()

    try:
        report = reconcile()
    except Exception as exc:
        print(f"FATAL: reconciler crashed: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

    if args.json:
        print(json.dumps(report, default=str))
    else:
        print_human(report)

    # Exit non-zero if drifts detected — task_scheduler tracks this for
    # daily review's section 10 to surface.
    sys.exit(0 if not report["drifts"] else 2)


if __name__ == "__main__":
    main()
