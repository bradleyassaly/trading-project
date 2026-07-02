"""Deep execution audit — where is alpha generated and where lost?

For each closed trade, decompose: signal_quality + entry_quality +
exit_quality + slippage + sizing.
"""
import sys, time
sys.path.insert(0, "src")
from dotenv import load_dotenv; load_dotenv(".env")
from trading_platform.polymarket.db_connection import get_connection
c = get_connection()
NOW = int(time.time())

def hdr(t): print(f"\n{'='*72}\n  {t}\n{'='*72}")

# ── A. Exit reason distribution (where do trades end?) ──────────────────────
hdr("A. EXIT REASON distribution (all closed live trades, action-triggered)")
rows = c.execute("""
    SELECT exit_reason, COUNT(*) n,
           ROUND(SUM(realized_pnl)::numeric, 2) total_pnl,
           ROUND(AVG(realized_pnl)::numeric, 2) avg_pnl,
           SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins
      FROM live_trades
     WHERE dry_run=0 AND exit_ts IS NOT NULL
       AND realized_pnl IS NOT NULL
     GROUP BY 1 ORDER BY 3 DESC
""").fetchall()
total_pnl_all = sum(float(r[2] or 0) for r in rows)
print(f"  {'exit_reason':<25} {'n':>4} {'wins':>5} {'WR':>5} {'total_pnl':>11} {'avg':>8} {'%share':>7}")
for r in rows:
    wr = r[4] / max(r[1], 1) * 100
    share = float(r[2] or 0) / total_pnl_all * 100 if total_pnl_all else 0
    print(f"  {str(r[0]):<25} {r[1]:>4d} {r[4]:>5d} {wr:>4.0f}% ${r[2]:>+8.2f} ${r[3]:>+6.2f} {share:>+6.0f}%")

# ── B. Time-to-fill (latency from attempt to matched/live) ───────────────
hdr("B. ENTRY LATENCY — fill_time_ms distribution")
rows = c.execute("""
    SELECT signal_type,
           COUNT(*) n,
           ROUND(AVG(fill_time_ms)::numeric, 0) avg_ms,
           ROUND(percentile_cont(0.5) within group (order by fill_time_ms)::numeric, 0) med,
           ROUND(percentile_cont(0.9) within group (order by fill_time_ms)::numeric, 0) p90
      FROM live_trades
     WHERE dry_run=0 AND fill_time_ms IS NOT NULL
       AND attempted_at >= %s
     GROUP BY 1 ORDER BY 2 DESC
""", (NOW - 30 * 86400,)).fetchone()
if rows:
    print(f"  Avg fill time: {rows[2]}ms  median {rows[3]}ms  p90 {rows[4]}ms")
else:
    print(f"  No fill_time_ms data")

# ── C. Slippage: signal_price vs fill_price ───────────────────────────────
hdr("C. SLIPPAGE (signal_price → fill_price)")
rows = c.execute("""
    SELECT signal_type, direction,
           COUNT(*) n,
           ROUND(AVG(slippage)::numeric, 4) avg_slip,
           ROUND(AVG(ABS(slippage))::numeric, 4) avg_abs_slip,
           ROUND(SUM(slippage * size_usd / fill_price)::numeric, 2) slip_dollars
      FROM live_trades
     WHERE dry_run=0 AND slippage IS NOT NULL
       AND attempted_at >= %s
       AND fill_price > 0
     GROUP BY 1, 2 ORDER BY 3 DESC
""", (NOW - 30 * 86400,)).fetchall()
print(f"  {'signal':<22} {'dir':<5} {'n':>4} {'avg_slip':>9} {'abs':>8} {'dollars':>10}")
for r in rows:
    print(f"  {r[0]:<22} {r[1]:<5} {r[2]:>4d} {r[3]:>9} {r[4]:>8} ${r[5]:>+8.2f}")

# ── D. Sizing quality: confidence vs actual win rate ────────────────────────
hdr("D. SIZING ACCURACY — confidence vs realized WR")
rows = c.execute("""
    SELECT
      CASE
        WHEN confidence < 0.20 THEN '<0.20'
        WHEN confidence < 0.30 THEN '0.20-0.30'
        WHEN confidence < 0.50 THEN '0.30-0.50'
        WHEN confidence < 0.70 THEN '0.50-0.70'
        WHEN confidence < 0.85 THEN '0.70-0.85'
        ELSE '0.85+'
      END band,
      COUNT(*) n,
      ROUND(AVG(confidence)::numeric, 3) avg_conf,
      ROUND(AVG(CASE WHEN outcome='win' THEN 1.0 ELSE 0.0 END)::numeric, 3) actual_wr,
      ROUND(SUM(realized_pnl)::numeric, 2) total_pnl
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL
      AND outcome IN ('win','loss') AND confidence IS NOT NULL
    GROUP BY 1 ORDER BY 1
""").fetchall()
print(f"  {'conf_band':<12} {'n':>4} {'mean_conf':>10} {'actual_wr':>10} {'gap':>7} {'pnl':>10}")
for r in rows:
    gap = float(r[3] or 0) - float(r[2] or 0)
    print(f"  {r[0]:<12} {r[1]:>4d} {r[2]:>9} {r[3]:>9} {gap:>+6.3f} ${r[4]:>+8.2f}")

# ── E. Position MAE/MFE — leakage at exit ──────────────────────────────────
hdr("E. PEAK CAPTURE — how much of the move did we keep? (MFE-based)")
rows = c.execute("""
    SELECT signal_type, direction,
           COUNT(*) n,
           ROUND(AVG(mfe)::numeric, 2) avg_mfe,
           ROUND(AVG(realized_pnl)::numeric, 2) avg_pnl,
           ROUND(AVG(realized_pnl / NULLIF(mfe, 0))::numeric, 2) capture_ratio
      FROM live_trades
     WHERE dry_run=0 AND exit_ts IS NOT NULL
       AND mfe IS NOT NULL AND mfe > 0
       AND realized_pnl IS NOT NULL AND realized_pnl > 0
     GROUP BY 1, 2
    HAVING COUNT(*) >= 3
     ORDER BY 3 DESC
""").fetchall()
print(f"  {'signal':<22} {'dir':<5} {'n':>3} {'avg_MFE':>9} {'avg_pnl':>9} {'capture':>9}")
for r in rows:
    print(f"  {r[0]:<22} {r[1]:<5} {r[2]:>3d} ${r[3]:>+6.2f} ${r[4]:>+6.2f} {r[5]:>8}")

# ── F. Loss attribution — what kinds of trades lose ───────────────────────
hdr("F. LOSS ATTRIBUTION")
# Loss by exit_reason
rows = c.execute("""
    SELECT exit_reason, COUNT(*) n,
           ROUND(SUM(realized_pnl)::numeric, 2) total_loss,
           ROUND(AVG(fill_price)::numeric, 3) avg_fp
      FROM live_trades
     WHERE dry_run=0 AND outcome='loss'
     GROUP BY 1 ORDER BY 3 ASC LIMIT 10
""").fetchall()
print(f"  All losses by exit_reason (sorted by total loss):")
for r in rows:
    print(f"    {str(r[0]):<25} n={r[1]:>3d} loss=${r[2]:>+8.2f} avg_fp={r[3]}")

# ── G. Most profitable winners — what should we double down on? ─────────
hdr("G. TOP 10 WINNERS (after all corrections)")
rows = c.execute("""
    SELECT id, signal_type, direction, ROUND(realized_pnl::numeric, 2) pnl,
           exit_reason, ROUND(fill_price::numeric, 3) fp,
           LEFT(question, 50) q
      FROM live_trades
     WHERE dry_run=0 AND realized_pnl > 0
     ORDER BY realized_pnl DESC LIMIT 10
""").fetchall()
for r in rows:
    print(f"  #{r[0]} {r[1]:<22} {r[2]} fp={r[5]} pnl=${r[3]} [{r[4]}]")
    print(f"     {r[6]}")

c.close()
