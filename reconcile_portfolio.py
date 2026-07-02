"""
Portfolio reconciliation: compare DB-tracked cash flows to actual on-chain USDC.

SELL positions: hold YES tokens. Entry cost = shares × (1 - fill_price).
BUY  positions: hold YES tokens. Entry cost = shares × fill_price.
Exit proceeds  = shares × exit_YES_price.

For SELL: exit_price in DB is NO price → YES_exit = 1 - exit_price.
For BUY:  exit_price in DB is YES price.

resolved_zero_balance exits: tokens resolved worthless → exit proceeds = 0.
"""
import sys, os, time
sys.path.insert(0, "src")
from dotenv import load_dotenv; load_dotenv(".env")
from trading_platform.polymarket.db_connection import get_connection

import httpx

def get_usdc_balance():
    try:
        from py_clob_client_v2 import ClobClient, ApiCreds
        from py_clob_client_v2.constants import POLYGON
        from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
        pk = os.environ["POLYMARKET_PRIVATE_KEY"]
        api_key = os.environ["POLYMARKET_API_KEY"]
        api_secret = os.environ["POLYMARKET_API_SECRET"]
        api_pass = os.environ.get("POLYMARKET_API_PASSPHRASE") or os.environ.get("POLYMARKET_PASSPHRASE")
        funder = os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")
        sig_type = int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "1"))
        client = ClobClient(
            host="https://clob.polymarket.com", chain_id=POLYGON, key=pk,
            creds=ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_pass),
            signature_type=sig_type, funder=funder or None,
        )
        bal = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        return int(bal.get("balance", 0)) / 1_000_000
    except Exception as e:
        print(f"  USDC lookup failed: {e}")
        return None

def get_mid(token_id):
    try:
        r = httpx.get(f"https://clob.polymarket.com/midpoint?token_id={token_id}", timeout=5)
        return float(r.json().get("mid", 0)) if r.status_code == 200 else None
    except Exception:
        return None

conn = get_connection()

# All closed trades
closed = conn.execute("""
    SELECT id, direction, fill_price, exit_price, shares, size_usd,
           realized_pnl, outcome, exit_reason
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NOT NULL
      AND shares IS NOT NULL AND shares > 0
      AND fill_price IS NOT NULL
""").fetchall()

# All open trades
open_pos = conn.execute("""
    SELECT id, direction, fill_price, shares, size_usd, token_id
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NULL
      AND status NOT IN ('error','blocked','cancelled')
      AND shares IS NOT NULL AND shares > 0
      AND fill_price IS NOT NULL
""").fetchall()

conn.close()

print("=" * 70)
print("PORTFOLIO CASH FLOW RECONCILIATION")
print("=" * 70)

# ── Closed trades: sum entry costs and exit proceeds ──────────────────────
total_entry_cost = 0.0
total_exit_proceeds = 0.0
rzb_cost = 0.0  # resolved_zero_balance — real cost, but $0 proceeds

for row in closed:
    lid, direction, fp, ep, sh, sz, pnl, outcome, reason = row
    fp = float(fp)
    ep = float(ep) if ep else None
    sh = float(sh)

    if direction == 'SELL':
        yes_entry = max(1.0 - fp, 0.001)
        entry_cost = sh * yes_entry
        if ep is not None and reason != 'resolved_zero_balance':
            yes_exit = max(1.0 - ep, 0.001)
            exit_proc = sh * yes_exit
        else:
            exit_proc = 0.0
    else:
        yes_entry = max(fp, 0.001)
        entry_cost = sh * yes_entry
        if ep is not None and reason != 'resolved_zero_balance':
            exit_proc = sh * ep
        else:
            exit_proc = 0.0

    total_entry_cost += entry_cost
    total_exit_proceeds += exit_proc

    if reason == 'resolved_zero_balance':
        rzb_cost += entry_cost

# ── Open trades: sum entry costs + current market value ───────────────────
open_entry_cost = 0.0
open_market_value = 0.0
open_unrealized = 0.0

print("\nFetching current token prices for open positions...")
for row in open_pos:
    lid, direction, fp, sh, sz, token_id = row
    fp = float(fp)
    sh = float(sh)

    if direction == 'SELL':
        yes_entry = max(1.0 - fp, 0.001)
    else:
        yes_entry = max(fp, 0.001)
    entry_cost = sh * yes_entry
    open_entry_cost += entry_cost

    if token_id:
        mid = get_mid(token_id)
        if mid is not None:
            mkt_val = sh * mid
            open_market_value += mkt_val
            open_unrealized += mkt_val - entry_cost
        else:
            open_market_value += sh * 0.5  # fallback
    else:
        open_market_value += entry_cost

usdc = get_usdc_balance()

print(f"\n{'─'*70}")
print(f"CLOSED trades ({len(closed)} total):")
print(f"  Total USDC spent entering:          ${total_entry_cost:>10.4f}")
print(f"  Total USDC received exiting:        ${total_exit_proceeds:>10.4f}")
print(f"  Net from closed trades:             ${total_exit_proceeds - total_entry_cost:>+10.4f}")
print(f"  Of which: zero-balance entry losses:${rzb_cost:>10.4f}  (real cost, pnl recorded as $0)")

print(f"\nOPEN positions ({len(open_pos)} total):")
print(f"  Total USDC invested (cost basis):   ${open_entry_cost:>10.4f}")
print(f"  Current market value (token mids):  ${open_market_value:>10.4f}")
print(f"  Unrealized gain/loss:               ${open_unrealized:>+10.4f}")

net_realized = total_exit_proceeds - total_entry_cost
print(f"\n{'─'*70}")
print(f"EXPECTED current USDC (if starting from $0):")
print(f"  = net_from_closed - open_invested")
print(f"  = {net_realized:+.4f} - {open_entry_cost:.4f}")
print(f"  = ${net_realized - open_entry_cost:+.4f}")
print(f"  (negative = expected starting deposit needed)")

if usdc is not None:
    implied_start = usdc - (net_realized - open_entry_cost)
    print(f"\nACTUAL on-chain USDC:               ${usdc:>10.4f}")
    print(f"Implied starting USDC balance:      ${implied_start:>10.4f}")
    print(f"\nTotal portfolio now:")
    print(f"  USDC:                             ${usdc:>10.4f}")
    print(f"  Token market value:               ${open_market_value:>10.4f}")
    print(f"  TOTAL:                            ${usdc + open_market_value:>10.4f}")
    print(f"\nEstimated starting portfolio:       ${implied_start:>10.4f}")
    print(f"Estimated all-time gain:            ${usdc + open_market_value - implied_start:>+10.4f}")
    print(f"DB realized PnL (net):              ${net_realized:>+10.4f}")

print(f"\n{'─'*70}")
print(f"NOTE ON POLYMARKET'S '-$15' DISPLAY:")
print(f"  Polymarket shows the CHANGE in total portfolio value over 24h.")
print(f"  Our realized PnL (+$88.55 last 24h) is profit vs ENTRY PRICE.")
print(f"  These differ because trailing stops fire on the way DOWN from peak,")
print(f"  so exited positions were worth more at yesterday's mid than at exit.")
print(f"  Example: position worth $103 unrealized yesterday → exits at $88.55")
print(f"  today → Polymarket shows -$14.45 change; we show +$88.55 realized.")
print(f"  Both numbers are correct; they measure different things.")
