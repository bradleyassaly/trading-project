"""
Per-signal-type P&L breakdown for live trades.

Shows: trade count, win rate, avg win, avg loss, total PnL, EV per trade.
Also shows recent 7d vs all-time to surface decaying signals early.
"""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
from trading_platform.polymarket.db_connection import get_connection

conn = get_connection()
since_7d = int(time.time()) - 7 * 86400

# ── all-time per signal_type ──────────────────────────────────────────────
alltime = conn.execute("""
    SELECT
        COALESCE(signal_type, 'unknown') as sig,
        COUNT(*)                                                      as n,
        SUM(CASE WHEN outcome='win'  THEN 1 ELSE 0 END)              as wins,
        SUM(CASE WHEN outcome='loss' THEN 1 ELSE 0 END)              as losses,
        ROUND(SUM(realized_pnl)::numeric, 4)                         as total_pnl,
        ROUND(AVG(realized_pnl)::numeric, 4)                         as ev,
        ROUND(AVG(CASE WHEN outcome='win'  THEN realized_pnl END)::numeric, 4) as avg_win,
        ROUND(AVG(CASE WHEN outcome='loss' THEN realized_pnl END)::numeric, 4) as avg_loss,
        ROUND(SUM(size_usd)::numeric, 4)                             as total_invested
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL
    GROUP BY signal_type
    ORDER BY total_pnl DESC
""").fetchall()

# ── last 7d per signal_type ───────────────────────────────────────────────
recent = conn.execute("""
    SELECT
        COALESCE(signal_type, 'unknown') as sig,
        COUNT(*)                                                      as n,
        SUM(CASE WHEN outcome='win'  THEN 1 ELSE 0 END)              as wins,
        ROUND(SUM(realized_pnl)::numeric, 4)                         as total_pnl,
        ROUND(AVG(realized_pnl)::numeric, 4)                         as ev
    FROM live_trades
    WHERE dry_run=0 AND exit_ts > %s AND realized_pnl IS NOT NULL
    GROUP BY signal_type
    ORDER BY total_pnl DESC
""", (since_7d,)).fetchall()

# ── fill quality per signal_type ─────────────────────────────────────────
fill_q = conn.execute("""
    SELECT
        COALESCE(signal_type, 'unknown') as sig,
        COUNT(*)                                                       as entered,
        SUM(CASE WHEN exit_ts IS NULL
              AND status NOT IN ('error','blocked','cancelled') THEN 1 ELSE 0 END) as still_open,
        ROUND(AVG(
            CASE WHEN direction='SELL' AND fill_price IS NOT NULL AND shares IS NOT NULL
                      AND shares > 0
                 THEN shares / NULLIF(size_usd / NULLIF(1.0 - fill_price::float, 0), 0)
            END
        )::numeric, 3)                                                 as avg_fill_rate
    FROM live_trades
    WHERE dry_run=0 AND status NOT IN ('error','blocked')
    GROUP BY signal_type
    ORDER BY entered DESC
""").fetchall()

conn.close()

recent_map = {r[0]: r for r in recent}
fill_map   = {r[0]: r for r in fill_q}

print("=" * 90)
print("LIVE TRADING — PER-SIGNAL-TYPE BREAKDOWN (all-time)")
print("=" * 90)
print(f"\n{'Signal':<30} {'n':>4}  {'WR':>5}  {'EV':>7}  {'AvgW':>7}  {'AvgL':>7}  {'TotalPnL':>9}  {'7d_PnL':>8}  {'FillR':>6}")
print("-" * 90)

total_n = total_pnl = 0
for row in alltime:
    sig, n, wins, losses, total, ev, avg_w, avg_l, invested = row
    n = int(n); wins = int(wins or 0); losses = int(losses or 0)
    wr = wins / n if n > 0 else 0
    total_n  += n
    total_pnl += float(total or 0)

    rec = recent_map.get(sig)
    recent_pnl = float(rec[3] or 0) if rec else 0.0

    fq = fill_map.get(sig)
    fill_rate = float(fq[3] or 0) if fq and fq[3] else None

    pnl_str  = f"${float(total or 0):+.2f}"
    ev_str   = f"${float(ev or 0):+.4f}"
    avgw_str = f"${float(avg_w or 0):+.4f}" if avg_w else "  N/A  "
    avgl_str = f"${float(avg_l or 0):+.4f}" if avg_l else "  N/A  "
    rec_str  = f"${recent_pnl:+.2f}"
    fr_str   = f"{fill_rate:.1%}" if fill_rate else "  N/A"

    flag = ""
    if float(ev or 0) < -0.10 and n >= 5:
        flag = "  <-- LOSING"
    elif float(ev or 0) > 0.50 and n >= 5:
        flag = "  *** STRONG"

    print(f"  {sig:<28} {n:>4}  {wr:>4.0%}  {ev_str:>7}  {avgw_str:>7}  {avgl_str:>7}  {pnl_str:>9}  {rec_str:>8}  {fr_str:>6}{flag}")

print("-" * 90)
print(f"  {'TOTAL':<28} {total_n:>4}                                        ${total_pnl:>+.2f}")

print(f"\n{'-'*90}")
print("LAST 7 DAYS (by signal)")
print(f"{'-'*90}")
print(f"  {'Signal':<30} {'n':>4}  {'WR':>5}  {'EV':>7}  {'TotalPnL':>9}")
for sig, n, wins, total, ev in recent:
    n = int(n); wins = int(wins or 0)
    wr = wins / n if n > 0 else 0
    print(f"  {sig:<30} {n:>4}  {wr:>4.0%}  ${float(ev or 0):>+.4f}  ${float(total or 0):>+.2f}")

print(f"\n{'-'*90}")
print("OPEN POSITIONS (by signal, showing deployment)")
print(f"{'-'*90}")
print(f"  {'Signal':<30} {'entered':>8}  {'open':>6}  {'avg_fill_rate':>14}")
for sig, entered, still_open, avg_fr in fill_q:
    if int(entered) == 0:
        continue
    fr_str = f"{float(avg_fr):.1%}" if avg_fr else "N/A"
    print(f"  {sig:<30} {int(entered):>8}  {int(still_open or 0):>6}  {fr_str:>14}")
