"""Daily system review — the recursive improvement loop.

Pulls together every analytic surface added in the 2026-05-12 audit and
emits a single review report. The pattern is: measure → recommend →
optionally auto-fix → log what changed so the system improves on a
deterministic daily cadence rather than waiting for ad-hoc audits.

Sections:
  1. Yesterday's P&L + position state
  2. Slippage and fill quality (uses fixed fill_price)
  3. Trailing-stop leakage (uses peak_no_price / peak_yes_price)
  4. Resolution-date timing (uses resolution_date)
  5. Order rejection breakdown (FOK, timeouts, etc.)
  6. Calibration check + multiplier refresh
  7. Per-signal alpha decay flags

Run once per UTC day (typically 00:30 UTC) via cron. Recommendations are
printed; only sizing multipliers are auto-applied (low-risk, capped,
direction-aware). Anything else is surfaced for manual review.

Usage:
    python scripts/daily_system_review.py [--apply-multipliers]
"""
import sys
import os
import time
import argparse
import subprocess
import datetime as dt

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
from trading_platform.polymarket.db_connection import get_connection


def section(title: str):
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def section_pnl(conn, now_ts):
    section("1. P&L AND POSITION STATE")
    today_utc = dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    today_start = int(today_utc.timestamp())
    yesterday_start = today_start - 86400

    for label, s, u in [
        ("Yesterday (UTC)", yesterday_start, today_start),
        ("Today so far (UTC)", today_start, now_ts),
        ("Last 7d", now_ts - 7 * 86400, now_ts),
        ("Last 30d", now_ts - 30 * 86400, now_ts),
    ]:
        r = conn.execute("""
            SELECT COUNT(*) n,
                   COALESCE(SUM(realized_pnl), 0) pnl,
                   AVG(CASE WHEN outcome='win' THEN 1.0 ELSE 0.0 END) wr
              FROM live_trades
             WHERE dry_run=0 AND exit_ts >= %s AND exit_ts < %s
               AND realized_pnl IS NOT NULL
        """, (s, u)).fetchone()
        n, pnl, wr = int(r[0] or 0), float(r[1] or 0), float(r[2] or 0)
        print(f"  {label:<22}  n={n:3d}  pnl=${pnl:+8.2f}  WR={wr:.0%}")

    # Open positions snapshot
    open_pos = conn.execute("""
        SELECT COUNT(*) n, COALESCE(SUM(size_usd), 0) capital
          FROM live_trades
         WHERE dry_run=0 AND exit_ts IS NULL
           AND status IN ('submitted','live','matched')
    """).fetchone()
    print(f"  Open positions:        {int(open_pos[0])} positions, "
          f"${float(open_pos[1]):.2f} deployed")


def section_slippage(conn, now_ts):
    section("2. FILL QUALITY (slippage, last 7d)")
    rows = conn.execute("""
        SELECT direction,
               COUNT(*) n,
               AVG(ABS(fill_price - entry_price)) avg_slip,
               AVG(fill_time_ms) avg_ms
          FROM live_trades
         WHERE dry_run=0 AND fill_price IS NOT NULL
           AND entry_price IS NOT NULL AND entry_price > 0
           AND ABS(fill_price - entry_price) < 0.3
           AND attempted_at > %s
         GROUP BY direction
    """, (now_ts - 7 * 86400,)).fetchall()
    for r in rows:
        dire, n, slip, ms = r
        ms_str = f"{float(ms or 0):.0f}ms" if ms else "N/A"
        print(f"  {dire:<5}  n={int(n):3d}  avg slippage={float(slip or 0):.4f}  "
              f"avg fill time={ms_str}")
    if not rows:
        print("  No fills with clean fill_price in last 7d (recent fix; "
              "data accumulates from here)")


def section_trailing_leak(conn, now_ts):
    section("3. TRAILING-STOP LEAKAGE (peak vs exit, last 14d)")
    rows = conn.execute("""
        SELECT signal_type, direction,
               COUNT(*) n,
               AVG(peak_no_price - (1 - COALESCE(exit_price, entry_price))) leak_no,
               AVG(peak_yes_price - COALESCE(exit_price, entry_price)) leak_yes
          FROM live_trades
         WHERE dry_run=0 AND exit_reason = 'trailing_stop'
           AND (peak_no_price IS NOT NULL OR peak_yes_price IS NOT NULL)
           AND attempted_at > %s
         GROUP BY signal_type, direction
    """, (now_ts - 14 * 86400,)).fetchall()
    for r in rows:
        sig, dire, n, leak_no, leak_yes = r
        leak = float(leak_no or 0) if dire == "SELL" else float(leak_yes or 0)
        print(f"  {str(sig):<28} {str(dire):<5}  n={int(n):3d}  "
              f"avg ${leak:.3f}/share left on table")
    if not rows:
        print("  No data yet (peak_no/peak_yes_price only populates from "
              "positions opened after 2026-05-12)")


def section_resolution(conn, now_ts):
    section("4. RESOLUTION-DATE TIMING (positions held vs days-to-resolution)")
    rows = conn.execute("""
        SELECT signal_type, direction,
               COUNT(*) n,
               AVG((resolution_date - attempted_at)/86400.0) avg_days_out,
               AVG(CASE WHEN outcome='win' THEN 1.0 ELSE 0.0 END) wr
          FROM live_trades
         WHERE dry_run=0 AND resolution_date IS NOT NULL
           AND outcome IN ('win','loss')
           AND attempted_at > %s
         GROUP BY signal_type, direction
    """, (now_ts - 30 * 86400,)).fetchall()
    for r in rows:
        sig, dire, n, days, wr = r
        print(f"  {str(sig):<28} {str(dire):<5}  n={int(n):3d}  "
              f"avg {float(days or 0):.1f}d to resolution  WR={float(wr or 0):.0%}")
    if not rows:
        print("  No data yet (resolution_date only populates from entries "
              "after 2026-05-12)")


def section_rejections(conn, now_ts):
    section("5. ORDER REJECTIONS (last 7d)")
    rows = conn.execute("""
        SELECT
            CASE WHEN error_msg LIKE '%%timed out%%' THEN 'timeout'
                 WHEN error_msg LIKE '%%FOK%%' THEN 'FOK'
                 WHEN error_msg LIKE '%%couldn%%fully filled%%' THEN 'FOK-fill'
                 WHEN error_msg LIKE '%%invalid amount%%' THEN 'min-stake'
                 WHEN error_msg LIKE '%%invalid signature%%' THEN 'sig'
                 WHEN error_msg LIKE '%%thin liquidity%%' THEN 'thin-liq'
                 ELSE 'other'
            END class,
            direction,
            COUNT(*) n
          FROM live_trades
         WHERE dry_run=0 AND status='error' AND attempted_at > %s
         GROUP BY class, direction ORDER BY n DESC
    """, (now_ts - 7 * 86400,)).fetchall()
    if not rows:
        print("  No rejections in last 7d")
    for r in rows:
        print(f"  {str(r[0]):<10} {str(r[1]):<5}  n={int(r[2])}")


def section_calibration(conn, args):
    section("6. CALIBRATION CHECK + MULTIPLIER REFRESH")
    # Pull current multipliers
    cur = conn.execute("""
        SELECT signal_type, direction, multiplier, n_resolved,
               actual_wr, mean_confidence, updated_at
          FROM signal_sizing_multipliers
         ORDER BY updated_at DESC, signal_type, direction
    """).fetchall()
    if cur:
        print(f"  Currently applied multipliers:")
        print(f"    {'Signal':<28} {'Dir':<5} {'mult':>6} {'WR':>7} {'conf':>7} {'age_h':>6}")
        for r in cur:
            age_h = (time.time() - int(r[6] or 0)) / 3600
            print(f"    {str(r[0]):<28} {str(r[1]):<5} {float(r[2]):>5.2f}x "
                  f"{float(r[4] or 0):>6.1%} {float(r[5] or 0):>6.3f} "
                  f"{age_h:>5.1f}h")
    else:
        print("  No multipliers in table yet")

    if args.apply_multipliers:
        print()
        print("  Refreshing multipliers from latest data...")
        sp = subprocess.run(
            [sys.executable,
             os.path.join(os.path.dirname(__file__), "update_sizing_multipliers.py"),
             "--days", "30"],
            capture_output=True, text=True,
        )
        for line in sp.stdout.splitlines()[-12:]:
            print(f"    {line}")
    else:
        print()
        print("  (use --apply-multipliers to refresh; skipped this run)")


def section_decay(conn, now_ts):
    section("7. ALPHA DECAY FLAGS (per-signal 7d vs 30d WR)")
    rows = conn.execute("""
        SELECT signal_type, direction,
               AVG(CASE WHEN exit_ts > %s AND outcome='win' THEN 1.0
                        WHEN exit_ts > %s THEN 0.0 ELSE NULL END) wr_7d,
               COUNT(CASE WHEN exit_ts > %s AND outcome IN ('win','loss') THEN 1 END) n_7d,
               AVG(CASE WHEN outcome='win' THEN 1.0 WHEN outcome='loss' THEN 0.0 END) wr_30d,
               COUNT(CASE WHEN outcome IN ('win','loss') THEN 1 END) n_30d
          FROM live_trades
         WHERE dry_run=0 AND attempted_at > %s
         GROUP BY signal_type, direction
         HAVING COUNT(CASE WHEN exit_ts > %s AND outcome IN ('win','loss') THEN 1 END) >= 3
    """, (now_ts - 7*86400, now_ts - 7*86400, now_ts - 7*86400,
          now_ts - 30*86400, now_ts - 7*86400)).fetchall()
    for r in rows:
        sig, dire, wr7, n7, wr30, n30 = r
        wr7 = float(wr7 or 0); wr30 = float(wr30 or 0)
        decay = wr30 - wr7
        flag = ""
        if n7 >= 5 and decay > 0.20:
            flag = "  DECAY — 7d WR dropped >20pp vs 30d"
        elif n7 >= 5 and -decay > 0.20:
            flag = "  IMPROVING — 7d WR up >20pp vs 30d"
        print(f"  {str(sig):<28} {str(dire):<5} "
              f"7d={wr7:.0%}(n={int(n7)})  30d={wr30:.0%}(n={int(n30)})  "
              f"delta={decay:+.2f}{flag}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply-multipliers", action="store_true",
                    help="Refresh signal_sizing_multipliers from latest data")
    args = ap.parse_args()

    now_ts = int(time.time())
    today_str = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    print(f"\nDAILY SYSTEM REVIEW — {today_str} UTC")

    conn = get_connection()
    try:
        section_pnl(conn, now_ts)
        section_slippage(conn, now_ts)
        section_trailing_leak(conn, now_ts)
        section_resolution(conn, now_ts)
        section_rejections(conn, now_ts)
        section_calibration(conn, args)
        section_decay(conn, now_ts)
    finally:
        conn.close()

    print()
    print("=" * 72)
    print("END OF REVIEW")
    print("=" * 72)


if __name__ == "__main__":
    main()
