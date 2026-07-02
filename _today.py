"""Today's check-in: verify yesterday's fixes are holding."""
import sys, time, os, datetime as dt
sys.path.insert(0, "src")
from dotenv import load_dotenv; load_dotenv(".env")
from trading_platform.polymarket.db_connection import get_connection
c = get_connection()
NOW = int(time.time())
DAY_AGO = NOW - 86400

def hdr(t): print(f"\n{'='*72}\n  {t}\n{'='*72}")

# ── 1. Closes past 24h ─────────────────────────────────────────────────────
hdr("1. CLOSES past 24h")
rows = c.execute("""
    SELECT id, signal_type, direction, ROUND(realized_pnl::numeric, 2) pnl,
           outcome, exit_reason,
           to_char(to_timestamp(exit_ts), 'MM-DD HH24:MI') ts,
           LEFT(question, 50) q
      FROM live_trades
     WHERE dry_run=0 AND exit_ts >= %s AND realized_pnl IS NOT NULL
     ORDER BY exit_ts DESC
""", (DAY_AGO,)).fetchall()
total = 0; w=l=0
for r in rows:
    total += float(r[3] or 0)
    if r[4]=='win': w+=1
    elif r[4]=='loss': l+=1
    print(f"  #{r[0]} {r[6]} {r[1]:<22} {r[2]:<5s} {str(r[4]):<5s} pnl=${r[3]:>6.2f} [{r[5]}]")
    print(f"     {r[7]}")
print(f"\n  Realized 24h: ${total:+.2f}  n={len(rows)}  W/L={w}/{l}")

# ── 2. Did whale_entry_filtered live trades resume? ────────────────────────
hdr("2. FIX VERIFICATION: auto-demote fix — live entries resume?")
rows = c.execute("""
    SELECT id, signal_type, direction, status, size_usd,
           to_char(to_timestamp(attempted_at), 'MM-DD HH24:MI') ts,
           LEFT(question, 45) q
      FROM live_trades
     WHERE dry_run=0 AND attempted_at >= %s
     ORDER BY attempted_at DESC LIMIT 10
""", (DAY_AGO,)).fetchall()
print(f"  Live attempts past 24h: {len(rows)}")
for r in rows:
    print(f"  #{r[0]} {r[5]} [{r[3]:<10s}] {r[1]:<22} {r[2]} sz=${r[4]}")
    print(f"     {r[6]}")

# ── 3. Did CLOB-failure noise drop? ───────────────────────────────────────
hdr("3. FIX VERIFICATION: CLOB price-fetch failures (was 193/7d)")
for label, lo, hi in [("48-24h ago", NOW - 2*86400, DAY_AGO),
                      ("0-24h ago", DAY_AGO, NOW)]:
    n = c.execute("""
        SELECT COUNT(*) FROM live_trades
         WHERE attempted_at >= %s AND attempted_at < %s
           AND error_msg LIKE %s
    """, (lo, hi, '%Could not fetch current price%')).fetchone()[0]
    print(f"  {label}: {n}")

# ── 4. Equity & on-chain ──────────────────────────────────────────────────
hdr("4. EQUITY")
eq = c.execute("""SELECT total_equity, usdc_balance, open_market_value,
                          realized_pnl_cumulative
                   FROM live_equity_snapshots ORDER BY ts DESC LIMIT 1""").fetchone()
prev = c.execute("""SELECT total_equity, realized_pnl_cumulative
                     FROM live_equity_snapshots WHERE ts < %s
                    ORDER BY ts DESC LIMIT 1""", (DAY_AGO,)).fetchone()
print(f"  Now:  eq=${float(eq[0]):.2f}  cash=${float(eq[1]):.2f}  "
      f"mkt=${float(eq[2] or 0):.2f}  cum=${float(eq[3] or 0):.2f}")
if prev:
    de = float(eq[0]) - float(prev[0])
    dc = float(eq[3] or 0) - float(prev[1] or 0)
    print(f"  Δ 24h: equity ${de:+.2f}  cum_realized ${dc:+.2f}")

# On-chain USDC
try:
    from py_clob_client_v2 import ClobClient, ApiCreds
    from py_clob_client_v2.constants import POLYGON
    from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
    pk = os.environ["POLYMARKET_PRIVATE_KEY"]
    api_key = os.environ["POLYMARKET_API_KEY"]
    secret = os.environ["POLYMARKET_API_SECRET"]
    api_pass = (os.environ.get("POLYMARKET_API_PASSPHRASE")
                or os.environ.get("POLYMARKET_PASSPHRASE"))
    funder = os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")
    sig_type = int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "1"))
    client = ClobClient(host="https://clob.polymarket.com", chain_id=POLYGON, key=pk,
                        creds=ApiCreds(api_key=api_key, api_secret=secret, api_passphrase=api_pass),
                        signature_type=sig_type, funder=funder or None)
    bal = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    usdc = int(bal.get("balance", 0)) / 1_000_000
    print(f"  On-chain USDC: ${usdc:.4f}  diff vs DB: ${usdc - float(eq[1]):+.4f}")
except Exception as e:
    print(f"  USDC fetch failed: {e}")

# ── 5. Trends ─────────────────────────────────────────────────────────────
hdr("5. 7d / 30d")
for label, days in [("7d", 7), ("30d", 30)]:
    r = c.execute("""SELECT COUNT(*), SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END),
                            ROUND(SUM(realized_pnl)::numeric, 2)
                       FROM live_trades
                      WHERE dry_run=0 AND exit_ts >= %s AND realized_pnl IS NOT NULL
                  """, (NOW - days*86400,)).fetchone()
    n = int(r[0] or 0); ww = int(r[1] or 0)
    print(f"  {label}: n={n} WR={ww/max(n,1)*100:.0f}% realized=${r[2]}")

# ── 6. Open positions ─────────────────────────────────────────────────────
hdr("6. OPEN POSITIONS")
rows = c.execute("""SELECT id, signal_type, direction, fill_price, last_mark_price,
                            size_usd, ROUND(unrealized_pnl::numeric, 2) unr
                       FROM live_trades
                      WHERE dry_run=0 AND exit_ts IS NULL AND status IN ('matched','live')
                      ORDER BY unrealized_pnl DESC NULLS LAST""").fetchall()
total_unr = 0
for r in rows[:15]:
    total_unr += float(r[6] or 0)
    fp = f"{float(r[3]):.3f}" if r[3] else "NULL"
    mk = f"{float(r[4]):.3f}" if r[4] else "NULL"
    print(f"  #{r[0]} {r[1]:<22} {r[2]} fp={fp} mark={mk} sz=${r[5]} unr=${r[6]}")
print(f"\n  Total unrealized: ${total_unr:+.2f}  ({len(rows)} positions)")

# ── 7. Monitor health ─────────────────────────────────────────────────────
hdr("7. MONITORS")
for name in ["reconcile_polymarket_truth", "ws_poll_reconcile", "wallet_attribution",
             "monitor_alerts", "sync_shares_from_onchain", "refit_per_slice_calibration"]:
    log = f"logs/scheduler/{name}.log"
    if os.path.exists(log):
        age = (NOW - int(os.path.getmtime(log)))//60
        print(f"  {name}: last run {age}m ago")

# Reconciler verdict
log = "logs/scheduler/reconcile_polymarket_truth.log"
if os.path.exists(log):
    with open(log) as f: lines = f.readlines()
    last = None
    for i in range(len(lines)-1, -1, -1):
        if "Polymarket-truth reconciliation" in lines[i]: last = i; break
    if last is not None:
        print()
        for ln in lines[last:last+4]: print(f"  {ln.rstrip()}")

c.close()
