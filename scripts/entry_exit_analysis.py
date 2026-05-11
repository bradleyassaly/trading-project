"""Deep entry/exit pattern analysis for live trades."""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
from trading_platform.polymarket.db_connection import get_connection

conn = get_connection()

# ── 1. Direction breakdown ────────────────────────────────────────────────────
print("=== DIRECTION BREAKDOWN ===")
rows = conn.execute("""
    SELECT direction,
        COUNT(*) n,
        SUM(CASE WHEN outcome='win'  THEN 1 ELSE 0 END) wins,
        ROUND(AVG(realized_pnl)::numeric,4) ev,
        ROUND(AVG(CASE WHEN outcome='win'  THEN realized_pnl END)::numeric,4) avg_win,
        ROUND(AVG(CASE WHEN outcome='loss' THEN realized_pnl END)::numeric,4) avg_loss,
        ROUND(SUM(realized_pnl)::numeric,2) total_pnl
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL
    GROUP BY direction ORDER BY total_pnl DESC
""").fetchall()
for r in rows:
    dire, n, wins, ev, aw, al, pnl = r
    wr = int(wins or 0) / int(n or 1)
    print(f"  {dire:<5} n={int(n):3d}  WR={wr:.0%}  EV=${float(ev or 0):+.4f}"
          f"  AvgW=${float(aw or 0):+.4f}  AvgL=${float(al or 0):+.4f}  Total=${float(pnl or 0):+.2f}")

# ── 2. BUY entry price buckets ────────────────────────────────────────────────
print("\n=== BUY ENTRY PRICE BUCKETS ===")
rows = conn.execute("""
    SELECT
        CASE
            WHEN entry_price < 0.05 THEN '0.00-0.05 (near-NO)'
            WHEN entry_price < 0.15 THEN '0.05-0.15'
            WHEN entry_price < 0.35 THEN '0.15-0.35'
            WHEN entry_price < 0.55 THEN '0.35-0.55 (mid)'
            WHEN entry_price < 0.75 THEN '0.55-0.75'
            ELSE                         '0.75+ (near-YES)'
        END bucket,
        COUNT(*) n,
        SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
        ROUND(AVG(realized_pnl)::numeric,4) ev,
        ROUND(SUM(realized_pnl)::numeric,2) total_pnl
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL AND direction='BUY'
    GROUP BY bucket ORDER BY bucket
""").fetchall()
for r in rows:
    bkt, n, wins, ev, pnl = r
    wr = int(wins or 0) / int(n or 1)
    flag = "  <-- LOSING" if float(ev or 0) < -0.05 and int(n) >= 5 else (
           "  *** WIN" if float(ev or 0) > 0.10 and int(n) >= 5 else "")
    print(f"  {bkt:<24} n={int(n):3d}  WR={wr:.0%}  EV=${float(ev or 0):+.4f}  Total=${float(pnl or 0):+.2f}{flag}")

# ── 3. SELL entry price buckets ───────────────────────────────────────────────
print("\n=== SELL ENTRY PRICE BUCKETS (YES price at entry) ===")
rows = conn.execute("""
    SELECT
        CASE
            WHEN entry_price < 0.50 THEN '<0.50 (mid-market SELL)'
            WHEN entry_price < 0.75 THEN '0.50-0.75'
            WHEN entry_price < 0.90 THEN '0.75-0.90'
            WHEN entry_price < 0.95 THEN '0.90-0.95'
            ELSE                         '0.95+ (near-resolution)'
        END bucket,
        COUNT(*) n,
        SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
        ROUND(AVG(realized_pnl)::numeric,4) ev,
        ROUND(SUM(realized_pnl)::numeric,2) total_pnl
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL AND direction='SELL'
    GROUP BY bucket ORDER BY bucket
""").fetchall()
for r in rows:
    bkt, n, wins, ev, pnl = r
    wr = int(wins or 0) / int(n or 1)
    flag = "  <-- LOSING" if float(ev or 0) < -0.05 and int(n) >= 3 else (
           "  *** WIN" if float(ev or 0) > 0.10 and int(n) >= 3 else "")
    print(f"  {bkt:<28} n={int(n):3d}  WR={wr:.0%}  EV=${float(ev or 0):+.4f}  Total=${float(pnl or 0):+.2f}{flag}")

# ── 4. Exit reason breakdown ──────────────────────────────────────────────────
print("\n=== EXIT REASON ===")
rows = conn.execute("""
    SELECT COALESCE(exit_reason,'unknown') reason,
        COUNT(*) n,
        SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
        ROUND(AVG(realized_pnl)::numeric,4) ev,
        ROUND(SUM(realized_pnl)::numeric,2) total_pnl
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL
    GROUP BY exit_reason ORDER BY total_pnl DESC
""").fetchall()
for r in rows:
    rsn, n, wins, ev, pnl = r
    wr = int(wins or 0) / int(n or 1)
    print(f"  {str(rsn):<30} n={int(n):3d}  WR={wr:.0%}  EV=${float(ev or 0):+.4f}  Total=${float(pnl or 0):+.2f}")

# ── 5. Signal × direction matrix ─────────────────────────────────────────────
print("\n=== SIGNAL × DIRECTION MATRIX ===")
rows = conn.execute("""
    SELECT signal_type, direction,
        COUNT(*) n,
        SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
        ROUND(AVG(realized_pnl)::numeric,4) ev,
        ROUND(SUM(realized_pnl)::numeric,2) total_pnl
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL
    GROUP BY signal_type, direction ORDER BY signal_type, total_pnl DESC
""").fetchall()
last_sig = None
for r in rows:
    sig, dire, n, wins, ev, pnl = r
    wr = int(wins or 0) / int(n or 1)
    if sig != last_sig:
        print(f"  {sig}")
        last_sig = sig
    flag = "  <-- LOSING" if float(ev or 0) < -0.05 and int(n) >= 5 else (
           "  *** CORE EDGE" if float(ev or 0) > 0.30 and int(n) >= 5 else "")
    print(f"    {dire:<5} n={int(n):3d}  WR={wr:.0%}  EV=${float(ev or 0):+.4f}  Total=${float(pnl or 0):+.2f}{flag}")

# ── 6. Time in trade: winners vs losers ──────────────────────────────────────
print("\n=== TIME IN TRADE (hours) ===")
rows = conn.execute("""
    SELECT outcome,
        COUNT(*) n,
        ROUND(AVG((exit_ts - attempted_at)/3600.0)::numeric,1) avg_hours,
        ROUND(MIN((exit_ts - attempted_at)/3600.0)::numeric,1) min_hours,
        ROUND(MAX((exit_ts - attempted_at)/3600.0)::numeric,1) max_hours,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
              (ORDER BY (exit_ts - attempted_at)/3600.0)::numeric,1) median_hours
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL
      AND exit_ts > attempted_at
    GROUP BY outcome
""").fetchall()
for r in rows:
    out, n, avg_h, min_h, max_h, med_h = r
    print(f"  {str(out):<6} n={int(n):3d}  avg={float(avg_h or 0):.1f}h  median={float(med_h or 0):.1f}h  min={float(min_h or 0):.1f}h  max={float(max_h or 0):.1f}h")

# ── 7. Unrealized on open positions (stake-based) ────────────────────────────
print("\n=== OPEN POSITIONS (true unrealized) ===")
opens = conn.execute("""
    SELECT signal_type, category, direction, entry_price, last_mark_price, size_usd,
           attempted_at, exit_reason
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NULL
      AND status NOT IN ('error','blocked','cancelled')
    ORDER BY attempted_at
""").fetchall()
total_unr = 0.0
for r in opens:
    sig, cat, dire, ep, mark, sz, ts, xr = r
    ep = float(ep or 0); mark = float(mark or 0); sz = float(sz or 0)
    age_d = (time.time() - int(ts or 0)) / 86400
    if dire == 'BUY':
        unr = (mark - ep) / ep * sz if ep > 0 else 0
    else:
        no_entry = 1 - ep; no_mark = 1 - mark
        unr = (no_mark - no_entry) / no_entry * sz if no_entry > 0 else 0
    total_unr += unr
    print(f"  {str(sig):<25} {str(cat):<14} {dire}  entry={ep:.3f} mark={mark:.3f}  ${sz:.2f}  unr=${unr:+.2f}  {age_d:.0f}d")
print(f"  Net unrealized: ${total_unr:+.2f}")

conn.close()
