"""Backtest: would faster exit polling recover the resolution-tail bleed?

Consumes the fine marks captured by live_mark_logger (live_position_marks)
and replays the LIVE exit logic (_exit_profile + _decide_exit from
live_position_monitor) over each closed position at several sampling
cadences — the live 5-minute cadence vs faster 60s/30s. For each cadence it
finds the first exit the rules would have triggered on the subsampled path,
prices the exit at that mark's mid, and compares realized P&L to (a) the
actual booked P&L and (b) hold-to-resolution.

Origin: 2026-07-12 eval. resolution_decay sports longshots gap to $0 at the
final whistle; the exit-at-peak counterfactual said the losers were exitable
for +$16 vs actual -$32, but the 5-min monitor sampled too coarsely to catch
the pre-gap window. This quantifies the recoverable delta on REAL captured
paths before any change to live exit cadence.

Runs read-only. Prints "insufficient data" until live_position_marks has
accumulated resolved positions with enough marks (needs the logger running
for ~1-2 weeks). Nothing here changes trading behavior.

    python scripts/backtest_faster_poll.py [--cadences 60,300] [--signal resolution_decay]
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, ValueError):
    pass

from trading_platform.polymarket.db_connection import get_connection
from trading_platform.polymarket.live_position_monitor import (
    _exit_profile, _decide_exit,
)


def _subsample(marks, cadence_s):
    """Take marks at >= cadence_s spacing (simulates a poll every cadence_s)."""
    out = []
    last_ts = None
    for m in marks:
        if last_ts is None or (m["ts"] - last_ts) >= cadence_s:
            out.append(m)
            last_ts = m["ts"]
    return out


def _simulate(trade, marks, cadence_s):
    """Replay the live exit rules over the subsampled path.

    Returns (exit_reason|None, exit_mid, exit_ts). If no rule triggers, the
    position is held to resolution (caller applies the resolved payout).
    """
    profile = _exit_profile(trade["signal_type"], trade["direction"] or "BUY")
    entry = float(trade["entry_price"] or 0)
    side = "BUY" if (trade["direction"] or "BUY").upper() == "BUY" else "SELL"
    attempted = trade["attempted_at"] or (marks[0]["ts"] if marks else 0)
    mfe_pct = 0.0
    for m in _subsample(marks, cadence_s):
        cur = float(m["mid"])
        if side == "BUY":
            unreal = (cur - entry) / max(entry, 0.01)
        else:
            unreal = (entry - cur) / max(1 - entry, 0.01)
        mfe_pct = max(mfe_pct, unreal)
        age_days = (m["ts"] - attempted) / 86400.0
        reason = _decide_exit(side=side, entry=entry, current=cur,
                              mfe_pct=mfe_pct, age_days=age_days, profile=profile)
        if reason:
            return reason, cur, m["ts"]
    return None, None, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cadences", default="60,300",
                    help="comma-separated poll cadences in seconds (default 60,300)")
    ap.add_argument("--signal", default="resolution_decay")
    ap.add_argument("--min-marks", type=int, default=5,
                    help="skip trades with fewer than this many marks")
    args = ap.parse_args()
    cadences = [int(x) for x in args.cadences.split(",") if x.strip()]

    conn = get_connection()
    try:
        # Closed positions of the target signal that have captured marks.
        trades = conn.execute(
            """SELECT lt.id, lt.signal_type, lt.direction, lt.entry_price,
                      lt.attempted_at, lt.exit_ts, lt.outcome, lt.realized_pnl,
                      lt.size_usd, lt.shares
                 FROM live_trades lt
                WHERE lt.dry_run = 0 AND lt.signal_type = %s
                  AND lt.exit_ts IS NOT NULL AND lt.outcome IN ('win','loss')
                  AND EXISTS (SELECT 1 FROM live_position_marks m
                               WHERE m.trade_id = lt.id)
                ORDER BY lt.exit_ts""",
            (args.signal,),
        ).fetchall()
    except Exception as exc:
        print(f"live_position_marks not available yet ({exc}).")
        print("Run the live_mark_logger for ~1-2 weeks first (scheduler task 'live_mark_logger').")
        conn.close()
        return

    if not trades:
        # Show accrual progress so the operator knows how close we are.
        try:
            cov = conn.execute(
                """SELECT COUNT(DISTINCT trade_id), COUNT(*)
                     FROM live_position_marks"""
            ).fetchone()
            print(f"No closed {args.signal} positions have marks yet.")
            print(f"live_position_marks so far: {int(cov[1] or 0)} marks across "
                  f"{int(cov[0] or 0)} positions (still open or pre-resolution).")
        except Exception:
            print("live_position_marks is empty — logger has not captured anything yet.")
        print("Come back once positions with marks have resolved.")
        conn.close()
        return

    print(f"Faster-poll backtest — signal={args.signal}, cadences={cadences}s, "
          f"{len(trades)} resolved positions with marks\n")
    header = f"  {'cadence':>8} {'n':>4} {'exits':>6} {'held':>5} {'sim_pnl':>9} {'actual':>9} {'hold':>9}"
    print(header)

    actual_total = 0.0
    for cad in cadences:
        sim_total = hold_total = 0.0
        n = exits = held = 0
        for t in trades:
            tid = t[0]
            marks = conn.execute(
                """SELECT snapshot_at, mid FROM live_position_marks
                    WHERE trade_id = %s AND mid IS NOT NULL
                    ORDER BY snapshot_at""", (tid,),
            ).fetchall()
            marks = [{"ts": int(m[0]), "mid": float(m[1])} for m in marks]
            if len(marks) < args.min_marks:
                continue
            trade = {
                "id": tid, "signal_type": t[1], "direction": t[2],
                "entry_price": t[3], "attempted_at": t[4],
            }
            entry = float(t[3] or 0)
            size = float(t[8] or 0)
            shares = float(t[9] or 0) or (size / max(entry, 0.01))
            payout = 1.0 if t[6] == "win" else 0.0  # BUY: YES pays 1 on win
            cost = shares * entry
            hold_pnl = shares * payout - cost
            n += 1
            reason, exit_mid, _ = _simulate(trade, marks, cad)
            if reason is None:
                sim_pnl = hold_pnl
                held += 1
            else:
                sim_pnl = shares * float(exit_mid) - cost
                exits += 1
            sim_total += sim_pnl
            hold_total += hold_pnl
            if cad == cadences[0]:
                actual_total += float(t[7] or 0)
        print(f"  {cad:>7}s {n:>4} {exits:>6} {held:>5} {sim_total:>+9.2f} "
              f"{actual_total:>+9.2f} {hold_total:>+9.2f}")

    print("\n  sim_pnl = P&L if exits fired at this cadence on the captured path.")
    print("  Compare cadences: a higher sim_pnl at 60s than 300s = faster polling recovers value.")
    print("  (actual = what was booked; hold = never-exit baseline.)")
    conn.close()


if __name__ == "__main__":
    main()
