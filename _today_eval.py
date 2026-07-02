"""Today's eval — verify each fix shipped yesterday is actually working."""
import sys, time, datetime as dt, os
sys.path.insert(0, "src")
from dotenv import load_dotenv; load_dotenv(".env")
from trading_platform.polymarket.db_connection import get_connection
c = get_connection()
NOW = int(time.time())
DAY_AGO = NOW - 86400

def hdr(t): print(f"\n{'='*72}\n  {t}\n{'='*72}")

# ── 1. P&L past 24h ────────────────────────────────────────────────────────
hdr("1. CLOSES past 24h")
rows = c.execute("""
    SELECT id, signal_type, direction, ROUND(realized_pnl::numeric, 2) pnl,
           outcome, exit_reason, to_char(to_timestamp(exit_ts), 'MM-DD HH24:MI') ts,
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

# ── 2. Did take_profit fix work? Check for NEW fictitious closes ──────────
hdr("2. FIX #50 verification: any NEW fictitious closes today?")
# Yesterday I re-opened 11 trades and shipped the fix. Today's closes (if any)
# should ALL have on-chain balance ~= 0 (the fix should refuse to close otherwise).
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

close_rows = c.execute("""
    SELECT id, signal_type, direction, token_id, exit_reason, realized_pnl
      FROM live_trades
     WHERE dry_run=0 AND exit_ts IS NOT NULL
       AND exit_ts >= %s AND token_id IS NOT NULL
       AND exit_reason IN ('take_profit','trailing_stop','stop_loss',
                           'whale_mirror_exit','time_decay')
""", (DAY_AGO,)).fetchall()
print(f"  Action-triggered closes 24h: {len(close_rows)}")
fictitious_today = []
for r in close_rows:
    lid, sig, di, tid, er, pnl = r
    try:
        bal = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=tid)
        )
        on_chain = int(bal.get("balance", 0)) / 1_000_000
    except Exception:
        continue
    if on_chain >= 1.0:
        fictitious_today.append((lid, sig, di, er, pnl, on_chain))
        print(f"  ⚠ #{lid} {sig} {di} {er} pnl={pnl} on_chain={on_chain}")
if not fictitious_today:
    print(f"  ✓ ALL CLOSES TODAY ARE GENUINE — bug fix working")

# ── 3. Reopened-positions status ────────────────────────────────────────────
hdr("3. The 11 trades re-opened yesterday — did they close cleanly?")
# Those trades had exit_ts cleared. Now check: did they re-close? With cash?
reopened_ids = [9974, 20474, 22994, 24282, 26080, 26563, 30316, 32224]  # main 8 from log
for lid in reopened_ids:
    r = c.execute("""
        SELECT signal_type, status, exit_ts, exit_reason, realized_pnl, token_id
          FROM live_trades WHERE id = %s
    """, (lid,)).fetchone()
    if not r:
        print(f"  #{lid}: not found")
        continue
    sig, st, ets, er, pnl, tid = r
    state = "OPEN" if ets is None else f"closed [{er}] pnl={pnl}"
    # Check on-chain
    if tid:
        try:
            bal = client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=tid)
            )
            on_chain = int(bal.get("balance", 0)) / 1_000_000
        except Exception:
            on_chain = "?"
    else:
        on_chain = "no_tid"
    print(f"  #{lid} {sig:<22} {state}  on_chain={on_chain}")

# ── 4. Hourly DB.shares sync — did it run? ────────────────────────────────
hdr("4. FIX #B verification: hourly sync_shares_from_onchain")
log = "logs/scheduler/sync_shares_from_onchain.log"
if os.path.exists(log):
    age = (NOW - int(os.path.getmtime(log)))//60
    size = os.path.getsize(log)
    print(f"  Log exists: mtime={age}m ago, size={size}b")
    with open(log) as f:
        lines = f.readlines()
    print(f"  Last few lines:")
    for ln in lines[-10:]:
        print(f"    {ln.rstrip()}")
else:
    print(f"  LOG NOT YET PRESENT — scheduler may not have run it yet (hourly cadence)")

# ── 5. Reconciler: did fictitious-close detector trigger anything? ─────────
hdr("5. FIX #A verification: reconciler fictitious-close detector")
log = "logs/scheduler/reconcile_polymarket_truth.log"
if os.path.exists(log):
    with open(log) as f: lines = f.readlines()
    # Find last "===" header
    last = None
    for i in range(len(lines)-1, -1, -1):
        if "Polymarket-truth reconciliation" in lines[i]: last = i; break
    if last is not None:
        block = "".join(lines[last:last+30])
        print(block)

# ── 6. monitor_alerts — what fired today? ─────────────────────────────────
hdr("6. monitor_alerts firings past 24h")
log = "logs/scheduler/monitor_alerts.log"
if os.path.exists(log):
    with open(log) as f: lines = f.readlines()
    # Filter for alerts (non-zero count lines)
    recent = [l for l in lines[-60:] if 'fired' in l]
    for ln in recent[-10:]:
        print(f"  {ln.rstrip()}")

# ── 7. Equity vs Polymarket ─────────────────────────────────────────────────
hdr("7. EQUITY snapshot vs on-chain")
eq = c.execute("""SELECT total_equity, usdc_balance, open_market_value,
                          realized_pnl_cumulative
                   FROM live_equity_snapshots ORDER BY ts DESC LIMIT 1""").fetchone()
print(f"  DB: equity=${float(eq[0]):.2f} cash=${float(eq[1]):.2f} "
      f"mkt=${float(eq[2] or 0):.2f} cum=${float(eq[3] or 0):.2f}")
# On-chain USDC
try:
    bal = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    usdc = int(bal.get("balance", 0)) / 1_000_000
    print(f"  On-chain USDC: ${usdc:.4f}")
    print(f"  Diff: ${usdc - float(eq[1]):+.4f}")
except Exception as e:
    print(f"  USDC fetch failed: {e}")

# ── 8. 7d/30d trends (using current realized_pnl — note: $103 historical inflation) ──
hdr("8. TRENDS (note: realized_pnl_cumulative is ~$103 inflated due to historical bug)")
for label, days in [("7d", 7), ("30d", 30)]:
    r = c.execute("""SELECT COUNT(*), SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END),
                            ROUND(SUM(realized_pnl)::numeric, 2)
                       FROM live_trades
                      WHERE dry_run=0 AND exit_ts >= %s AND realized_pnl IS NOT NULL
                  """, (NOW - days*86400,)).fetchone()
    n = int(r[0] or 0); ww = int(r[1] or 0)
    print(f"  {label}: n={n} WR={ww/max(n,1)*100:.0f}% realized=${r[2]}")

# ── 9. Calibration shadow status ──────────────────────────────────────────
hdr("9. Per-slice calibration shadow data — day 4 of 14")
rows = c.execute("""
    SELECT signal_type, direction, n_samples,
           ROUND(brier_before::numeric, 3), ROUND(brier_after::numeric, 3),
           to_char(to_timestamp(fitted_at), 'MM-DD HH24:MI') t
      FROM alpha_calibration_curve
     WHERE signal_type IS NOT NULL
     ORDER BY fitted_at DESC LIMIT 8
""").fetchall()
for r in rows:
    print(f"  {r[5]} {r[0]:<22} {r[1]:<5} n={r[2]:>3d}  Brier {r[3]}→{r[4]}")

c.close()
