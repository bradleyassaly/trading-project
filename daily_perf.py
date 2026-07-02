"""Today's trading performance summary."""
import sys, time
sys.path.insert(0, "src")
from dotenv import load_dotenv; load_dotenv(".env")
from trading_platform.polymarket.db_connection import get_connection

since = int(time.time()) - 86400

conn = get_connection()

exits = conn.execute("""
    SELECT id, signal_type, direction, fill_price, exit_price, realized_pnl,
           exit_reason, outcome, size_usd, shares,
           to_timestamp(submitted_at) as entry_ts,
           to_timestamp(exit_ts) as exit_ts_fmt,
           LEFT(question, 60) as q
    FROM live_trades
    WHERE dry_run=0 AND exit_ts > %s
    ORDER BY exit_ts DESC
""", (since,)).fetchall()

entries = conn.execute("""
    SELECT id, signal_type, direction, fill_price, size_usd, status,
           to_timestamp(submitted_at) as ts,
           LEFT(question, 60) as q
    FROM live_trades
    WHERE dry_run=0 AND submitted_at > %s
    ORDER BY submitted_at DESC
""", (since,)).fetchall()

open_pos = conn.execute("""
    SELECT direction, COUNT(*) as n,
           SUM(size_usd) as total_size,
           SUM(unrealized_pnl) as total_unrealized,
           AVG(fill_price) as avg_fill
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NULL AND status NOT IN ('error','blocked','cancelled')
    GROUP BY direction
""").fetchall()

alltime = conn.execute("""
    SELECT outcome, COUNT(*) as n, ROUND(SUM(realized_pnl)::numeric, 2) as pnl
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL
    GROUP BY outcome
    ORDER BY outcome
""").fetchall()

errors = conn.execute("""
    SELECT status, LEFT(error_msg,80) as msg, COUNT(*) as n
    FROM live_trades
    WHERE dry_run=0 AND submitted_at > %s AND status IN ('error','blocked')
    GROUP BY status, LEFT(error_msg,80)
    ORDER BY n DESC
    LIMIT 15
""", (since,)).fetchall()

conn.close()

print("=" * 70)
print("LIVE TRADING — LAST 24h PERFORMANCE")
print("=" * 70)

wins = [e for e in exits if e[7] == 'win']
losses = [e for e in exits if e[7] == 'loss']
total_pnl = sum(float(e[5] or 0) for e in exits)

print(f"\nEXITS ({len(exits)} total) | Wins: {len(wins)}  Losses: {len(losses)}  Net: ${total_pnl:+.2f}")
for e in exits:
    lid, sig, direction, fp, ep, pnl, reason, outcome, size, shares, ets, exit_fmt, q = e
    fp_str = f"{float(fp):.3f}" if fp else "N/A"
    ep_str = f"{float(ep):.3f}" if ep else "N/A"
    pnl_str = f"${float(pnl or 0):+.2f}"
    print(f"  #{lid} {outcome or '?'} {direction} {reason} | {fp_str}->{ep_str} {pnl_str} | {q}")

ok = [e for e in entries if e[5] not in ('error', 'blocked')]
err = [e for e in entries if e[5] in ('error', 'blocked')]
print(f"\nNEW ENTRIES ({len(entries)} total) | Placed: {len(ok)}  Blocked/Error: {len(err)}")
for e in entries[:10]:
    lid, sig, direction, fp, size, status, ts, q = e
    fp_str = f"{float(fp):.3f}" if fp else "N/A"
    print(f"  #{lid} [{status}] {direction} @ {fp_str} ${float(size or 0):.2f} | {q}")

print(f"\nOPEN POSITIONS")
for row in open_pos:
    direction, n, total_size, total_unr, avg_fill = row
    print(f"  {direction}: {n} pos | size=${float(total_size or 0):.2f} | unrealized=${float(total_unr or 0):+.2f} | avg_fill={float(avg_fill or 0):.3f}")

print(f"\nALL-TIME RESOLVED")
total_at_pnl = 0
for outcome, n, pnl in alltime:
    total_at_pnl += float(pnl or 0)
    print(f"  {outcome}: {n} trades | ${float(pnl or 0):+.2f}")
print(f"  NET: ${total_at_pnl:+.2f}")

print(f"\nERRORS/BLOCKS (last 24h)")
if errors:
    for status, msg, n in errors:
        print(f"  [{status}] n={n}: {msg}")
else:
    print("  None")
