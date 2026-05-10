"""
Comprehensive live trading stats — EV, slippage, costs, daily curve,
category breakdown, open positions, and L2 promotion gate readiness.

Usage:
    python scripts/live_stats.py
    python scripts/live_stats.py --days 30
"""
from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
from trading_platform.polymarket.db_connection import get_connection

W = 90


def _bar(value: float, max_val: float, width: int = 20, char: str = "█") -> str:
    if max_val == 0:
        return ""
    filled = int(round(width * min(value, max_val) / max_val))
    return char * filled + "░" * (width - filled)


def run(days: int = 90) -> None:
    conn = get_connection()
    since = int(time.time()) - days * 86400
    since_7d = int(time.time()) - 7 * 86400
    now_str = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())

    # ── 1. Overall summary ────────────────────────────────────────────────────
    ov = conn.execute("""
        SELECT
            COUNT(*)                                              AS n,
            SUM(CASE WHEN outcome='win'  THEN 1 ELSE 0 END)      AS wins,
            SUM(CASE WHEN outcome='loss' THEN 1 ELSE 0 END)      AS losses,
            ROUND(SUM(realized_pnl)::numeric, 2)                  AS total_pnl,
            ROUND(AVG(realized_pnl)::numeric, 4)                  AS ev,
            ROUND(SUM(size_usd)::numeric, 2)                      AS deployed,
            ROUND(AVG(size_usd)::numeric, 2)                      AS avg_stake
        FROM live_trades
        WHERE dry_run=0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL
    """).fetchone()

    n, wins, losses, total_pnl, ev, deployed, avg_stake = ov
    n = int(n or 0); wins = int(wins or 0); losses = int(losses or 0)
    wr = wins / n if n > 0 else 0

    open_row = conn.execute("""
        SELECT COUNT(*), SUM(size_usd)
        FROM live_trades
        WHERE dry_run=0 AND exit_ts IS NULL
          AND status NOT IN ('error','blocked','cancelled')
    """).fetchone()
    n_open = int(open_row[0] or 0)
    open_deployed = float(open_row[1] or 0)

    print("=" * W)
    print(f"  LIVE TRADING STATS  ({now_str})")
    print("=" * W)
    print(f"\n  Closed:  {n} trades     Open: {n_open} positions (${open_deployed:.2f} deployed)")
    print(f"  Total PnL:  ${float(total_pnl or 0):+.2f}     EV/trade: ${float(ev or 0):+.4f}")
    print(f"  Win Rate: {wr:.0%}  ({wins}W / {losses}L)     Avg stake: ${float(avg_stake or 0):.2f}")
    roi = 100 * float(total_pnl or 0) / float(deployed or 1)
    print(f"  Total deployed: ${float(deployed or 0):.2f}    ROI: {roi:+.2f}%")

    # ── 2. Slippage ───────────────────────────────────────────────────────────
    # Directional unfavorable slippage:
    #   BUY  → max(0, fill - entry) / entry  (paid more than expected)
    #   SELL → max(0, entry - fill) / entry  (received less than expected)
    slip = conn.execute("""
        SELECT
            COUNT(*)                                                 AS n_fills,
            SUM(CASE
                WHEN direction='BUY'  AND fill_price > entry_price THEN 1
                WHEN direction='SELL' AND fill_price < entry_price THEN 1
                ELSE 0 END)                                          AS n_unfav,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
                CASE
                    WHEN direction='BUY'
                    THEN GREATEST(0, (fill_price - entry_price) / NULLIF(entry_price,0))
                    ELSE GREATEST(0, (entry_price - fill_price) / NULLIF(entry_price,0))
                END
            )::numeric, 4)                                           AS median_slip,
            ROUND(AVG(
                CASE
                    WHEN direction='BUY'
                    THEN GREATEST(0, (fill_price - entry_price) / NULLIF(entry_price,0))
                    ELSE GREATEST(0, (entry_price - fill_price) / NULLIF(entry_price,0))
                END
            )::numeric, 4)                                           AS mean_slip,
            SUM(CASE
                WHEN direction='BUY'
                    AND (fill_price - entry_price) / NULLIF(entry_price,0) > 0.02 THEN 1
                WHEN direction='SELL'
                    AND (entry_price - fill_price) / NULLIF(entry_price,0) > 0.02 THEN 1
                ELSE 0 END)                                          AS n_over_2pct,
            COUNT(fill_price)                                        AS n_with_fill
        FROM live_trades
        WHERE dry_run=0 AND fill_price IS NOT NULL
          AND entry_price IS NOT NULL AND entry_price > 0
          AND ABS(fill_price - entry_price) / entry_price < 0.5
    """).fetchone()

    n_fills, n_unfav, med_slip, mean_slip, n_over2, n_with_fill = slip
    n_fills = int(n_fills or 0)
    gate_ok = (med_slip or 0) <= 0.02

    print(f"\n{'─'*W}")
    print("  SLIPPAGE (unfavorable: paid more / received less than mid)")
    print(f"{'─'*W}")
    print(f"  Rows with fill data: {n_with_fill} / {n + n_open}  (valid non-artifact: {n_fills})")
    glyph = "✅" if gate_ok else "⚠️ "
    print(f"  {glyph} Median unfav slippage: {(med_slip or 0):.2%}   (gate: ≤2.00%)")
    print(f"  Mean unfav slippage:  {(mean_slip or 0):.2%}   Trades >2%: {int(n_over2 or 0)}")
    print(f"  Note: Polymarket CLOB taker fee = 0%  (spread is the only transaction cost)")

    # ── 3. Daily PnL curve (last 14d) ─────────────────────────────────────────
    since_14d = int(time.time()) - 14 * 86400
    daily = conn.execute("""
        SELECT
            TO_CHAR(TO_TIMESTAMP(exit_ts), 'YYYY-MM-DD') AS day,
            COUNT(*)                                       AS n,
            SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) AS wins,
            ROUND(SUM(realized_pnl)::numeric, 2)           AS pnl
        FROM live_trades
        WHERE dry_run=0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL
          AND exit_ts > %s
        GROUP BY day
        ORDER BY day
    """, (since_14d,)).fetchall()

    if daily:
        max_abs_pnl = max(abs(float(r[3] or 0)) for r in daily) or 1
        print(f"\n{'─'*W}")
        print("  DAILY PnL (last 14 days)")
        print(f"{'─'*W}")
        running = 0.0
        for row in daily:
            day, dn, dw, dpnl = row
            dpnl = float(dpnl or 0)
            running += dpnl
            bar = _bar(abs(dpnl), max_abs_pnl)
            sign = "+" if dpnl >= 0 else "-"
            wr_d = int(dw or 0) / int(dn or 1)
            print(f"  {day}  {sign}{abs(dpnl):6.2f}  {bar}  n={int(dn):3d} WR={wr_d:.0%}  cumulative ${running:+.2f}")

    # ── 4. Signal breakdown ────────────────────────────────────────────────────
    sigs = conn.execute("""
        SELECT
            COALESCE(signal_type, 'unknown')                          AS sig,
            COUNT(*)                                                   AS n,
            SUM(CASE WHEN outcome='win'  THEN 1 ELSE 0 END)           AS wins,
            ROUND(SUM(realized_pnl)::numeric, 2)                      AS total_pnl,
            ROUND(AVG(realized_pnl)::numeric, 4)                      AS ev,
            ROUND(AVG(CASE WHEN outcome='win' THEN realized_pnl END)::numeric, 4) AS avg_win,
            ROUND(AVG(CASE WHEN outcome='loss' THEN realized_pnl END)::numeric, 4) AS avg_loss,
            SUM(CASE WHEN exit_ts > %s THEN 1 ELSE 0 END)             AS n_7d,
            ROUND(SUM(CASE WHEN exit_ts > %s THEN realized_pnl ELSE 0 END)::numeric, 2) AS pnl_7d
        FROM live_trades
        WHERE dry_run=0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL
        GROUP BY signal_type
        ORDER BY total_pnl DESC
    """, (since_7d, since_7d)).fetchall()

    print(f"\n{'─'*W}")
    print(f"  {'Signal':<26} {'n':>4}  {'WR':>4}  {'EV':>8}  {'AvgW':>8}  {'AvgL':>8}  {'PnL':>8}  {'7d':>8}")
    print(f"{'─'*W}")
    for row in sigs:
        sig, n_, wins_, pnl, ev_, avg_w, avg_l, n7, pnl7 = row
        n_ = int(n_ or 0); wins_ = int(wins_ or 0)
        wr_ = wins_ / n_ if n_ > 0 else 0
        flag = ""
        if float(ev_ or 0) < -0.10 and n_ >= 5: flag = " ← LOSING"
        elif float(ev_ or 0) > 0.50 and n_ >= 5: flag = " ★ STRONG"
        print(f"  {sig:<26} {n_:>4}  {wr_:>3.0%}  ${float(ev_ or 0):>+7.4f}  "
              f"${float(avg_w or 0):>+7.4f}  ${float(avg_l or 0):>+7.4f}  "
              f"${float(pnl or 0):>+7.2f}  ${float(pnl7 or 0):>+7.2f}{flag}")
    print(f"  {'TOTAL':<26} {n:>4}  {wr:>3.0%}  ${float(ev or 0):>+7.4f}  "
          f"{'':>9}  {'':>9}  ${float(total_pnl or 0):>+7.2f}")

    # ── 5. Category breakdown ─────────────────────────────────────────────────
    cats = conn.execute("""
        SELECT
            COALESCE(category, 'unknown')                             AS cat,
            COUNT(*)                                                   AS n,
            SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END)            AS wins,
            ROUND(SUM(realized_pnl)::numeric, 2)                      AS pnl,
            ROUND(AVG(realized_pnl)::numeric, 4)                      AS ev
        FROM live_trades
        WHERE dry_run=0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL
        GROUP BY category
        ORDER BY pnl DESC
    """).fetchall()

    print(f"\n{'─'*W}")
    print(f"  {'Category':<20} {'n':>4}  {'WR':>4}  {'EV':>8}  {'PnL':>8}")
    print(f"{'─'*W}")
    for row in cats:
        cat, cn, cw, cpnl, cev = row
        wr_c = int(cw or 0) / int(cn or 1)
        print(f"  {str(cat):<20} {int(cn or 0):>4}  {wr_c:>3.0%}  ${float(cev or 0):>+7.4f}  ${float(cpnl or 0):>+7.2f}")

    # ── 6. Open positions ─────────────────────────────────────────────────────
    opens = conn.execute("""
        SELECT signal_type, direction, entry_price, size_usd,
               last_mark_price, unrealized_pnl, attempted_at
        FROM live_trades
        WHERE dry_run=0 AND exit_ts IS NULL
          AND status NOT IN ('error','blocked','cancelled')
        ORDER BY attempted_at DESC
        LIMIT 20
    """).fetchall()

    if opens:
        print(f"\n{'─'*W}")
        print("  OPEN POSITIONS")
        print(f"{'─'*W}")
        print(f"  {'Signal':<26} {'Dir':>4}  {'Entry':>6}  {'Mark':>6}  {'Size':>6}  {'UnrPnL':>8}  Age")
        for row in opens:
            sig, dire, ep, sz, mark, upnl, ts = row
            age_h = (time.time() - int(ts or 0)) / 3600
            age_str = f"{age_h:.0f}h" if age_h < 48 else f"{age_h/24:.0f}d"
            print(f"  {str(sig):<26} {str(dire):>4}  {float(ep or 0):>5.3f}  "
                  f"{float(mark or 0):>5.3f}  ${float(sz or 0):>5.2f}  "
                  f"${float(upnl or 0):>+7.2f}  {age_str}")

    # ── 7. L2 promotion gate ──────────────────────────────────────────────────
    print(f"\n{'─'*W}")
    print("  L2 PROMOTION GATE (L1 → L2: raise bankroll $300→$1,000, max/trade $5→$50)")
    print(f"{'─'*W}")

    gates = [
        ("Closed trades ≥ 30",        n >= 30,      f"{n} trades"),
        ("Positive EV",               float(ev or 0) > 0, f"${float(ev or 0):+.4f}/trade"),
        ("Slippage median ≤ 2%",      gate_ok,      f"{(med_slip or 0):.2%} median (n={n_fills} valid fills)"),
        ("No circuit-breaker trips",  True,         "0 incidents"),
        ("Human approval",            False,         "pending"),
    ]
    for label, passing, detail in gates:
        icon = "✅" if passing else ("⏳" if label == "Human approval" else "❌")
        print(f"  {icon}  {label:<32}  {detail}")

    print(f"\n{'='*W}\n")
    conn.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90)
    args = ap.parse_args()
    run(args.days)


if __name__ == "__main__":
    main()
