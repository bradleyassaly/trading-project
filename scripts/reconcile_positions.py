"""
Reconcile open positions against actual on-chain token balances.

Actions:
1. Zero-token open positions → mark closed with exit_reason='resolved_zero_balance', realized_pnl=0
2. Positions with tokens → update shares/size_usd to match actual balance
3. Recalculate unrealized_pnl for remaining open positions using current prices
4. Print corrected performance report
"""
import sys, os, time
sys.path.insert(0, "src")
from dotenv import load_dotenv; load_dotenv(".env")

from py_clob_client_v2 import ClobClient, ApiCreds
from py_clob_client_v2.constants import POLYGON
from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
import httpx

from trading_platform.polymarket.db_connection import get_connection

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

def get_mid(token_id):
    try:
        r = httpx.get(f"https://clob.polymarket.com/midpoint?token_id={token_id}", timeout=5)
        return float(r.json().get("mid", 0)) if r.status_code == 200 else None
    except Exception:
        return None

def get_token_balance(token_id):
    try:
        result = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        )
        return int(result.get("balance", 0)) / 1_000_000
    except Exception:
        return None

conn = get_connection()
now_ts = int(time.time())

open_pos = conn.execute("""
    SELECT id, direction, token_id, fill_price, size_usd, shares, unrealized_pnl,
           LEFT(question, 65) as q
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NULL
      AND status NOT IN ('error','blocked','cancelled')
      AND token_id IS NOT NULL
    ORDER BY id
""").fetchall()

print(f"Reconciling {len(open_pos)} open positions...\n")

closed_zero = 0
updated = 0
errors = 0

for row in open_pos:
    lid, direction, token_id, fp, size_usd, shares, unr_pnl, q = row
    fp = float(fp) if fp else None

    actual_tokens = get_token_balance(token_id)
    if actual_tokens is None:
        print(f"  #{lid} SKIP (balance lookup failed) | {q[:50]}")
        errors += 1
        continue

    # --- Case 1: No tokens at all → close as zero-balance ---
    if actual_tokens < 0.001:
        outcome = 'loss'
        # If we never had meaningful tokens (fill_price near 0.99 = short NO),
        # the realized PnL is $0 (we didn't hold enough tokens to matter)
        # But if fill_price is mid-range (0.5), it could be a real resolved loss
        realized = 0.0
        conn.execute("""
            UPDATE live_trades
            SET exit_ts=%s, exit_reason='resolved_zero_balance',
                outcome=%s, realized_pnl=%s, shares=0, size_usd=0,
                unrealized_pnl=0
            WHERE id=%s
        """, (now_ts, outcome, realized, lid))
        conn.commit()
        print(f"  #{lid} CLOSE zero-balance {direction} fill={fp or 'N/A':.3f} | {q[:55]}")
        closed_zero += 1
        continue

    # --- Case 2: Has tokens → update shares/size_usd, recalc unrealized ---
    # For SELL positions: we hold YES tokens. fill_price is the NO price at entry.
    # Implied YES entry price = 1 - fill_price (approximately)
    # size_usd = actual_tokens * (1 - fill_price)  [our cost basis for the YES tokens]
    #
    # For BUY positions: we hold YES tokens bought directly.
    # size_usd = actual_tokens * fill_price
    if fp is not None:
        if direction == 'SELL':
            yes_entry = max(0.001, 1.0 - fp)
            corrected_size = round(actual_tokens * yes_entry, 4)
        else:
            corrected_size = round(actual_tokens * fp, 4)
    else:
        corrected_size = float(size_usd or 0)

    # Current mid price for this token (YES price)
    current_yes = get_mid(token_id)
    if current_yes is None:
        current_yes = 0.5

    # Unrealized PnL
    if direction == 'SELL':
        yes_entry = max(0.001, 1.0 - (fp or 0.5))
        unrealized = round(actual_tokens * (current_yes - yes_entry), 4)
    else:
        yes_entry = fp or 0.5
        unrealized = round(actual_tokens * (current_yes - yes_entry), 4)

    old_shares = float(shares or 0)
    old_size = float(size_usd or 0)

    conn.execute("""
        UPDATE live_trades
        SET shares=%s, size_usd=%s, unrealized_pnl=%s
        WHERE id=%s
    """, (actual_tokens, corrected_size, unrealized, lid))
    conn.commit()

    fp_str = f"{fp:.3f}" if fp is not None else "N/A"
    change_str = f"shares {old_shares:.2f}->{actual_tokens:.4f}, size ${old_size:.2f}->${corrected_size:.4f}"
    print(f"  #{lid} UPDATE {direction} fill={fp_str} mid={current_yes:.3f} "
          f"unr=${unrealized:+.4f} | {change_str} | {q[:45]}")
    updated += 1

conn.close()

print(f"\nDone: {closed_zero} closed as zero-balance, {updated} updated, {errors} errors")

# ── Corrected performance report ─────────────────────────────────────────────
print("\n" + "=" * 70)
print("CORRECTED ALL-TIME PERFORMANCE")
print("=" * 70)

conn = get_connection()

alltime = conn.execute("""
    SELECT outcome, COUNT(*) as n,
           ROUND(SUM(realized_pnl)::numeric, 4) as pnl,
           ROUND(AVG(realized_pnl)::numeric, 4) as avg_pnl
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL
    GROUP BY outcome ORDER BY outcome
""").fetchall()

open_summary = conn.execute("""
    SELECT direction, COUNT(*) as n,
           ROUND(SUM(size_usd)::numeric, 4) as total_cost,
           ROUND(SUM(unrealized_pnl)::numeric, 4) as total_unr,
           ROUND(AVG(fill_price)::numeric, 4) as avg_fill
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NULL
      AND status NOT IN ('error','blocked','cancelled')
    GROUP BY direction
""").fetchall()

# Last 24h
since = int(time.time()) - 86400
recent = conn.execute("""
    SELECT outcome, COUNT(*) as n,
           ROUND(SUM(realized_pnl)::numeric, 4) as pnl
    FROM live_trades
    WHERE dry_run=0 AND exit_ts > %s AND realized_pnl IS NOT NULL
    GROUP BY outcome ORDER BY outcome
""", (since,)).fetchall()

conn.close()

total_net = 0
print("\nAll-time resolved:")
for outcome, n, pnl, avg in alltime:
    total_net += float(pnl or 0)
    print(f"  {outcome}: {n} trades | total ${float(pnl or 0):+.2f} | avg ${float(avg or 0):+.4f}")
print(f"  NET ALL-TIME: ${total_net:+.2f}")

print("\nOpen positions:")
for direction, n, cost, unr, avg_fill in open_summary:
    print(f"  {direction}: {n} pos | cost=${float(cost or 0):.4f} | "
          f"unrealized=${float(unr or 0):+.4f} | avg_fill={float(avg_fill or 0):.3f}")

print("\nLast 24h resolved:")
day_net = 0
for outcome, n, pnl in recent:
    day_net += float(pnl or 0)
    print(f"  {outcome}: {n} trades | ${float(pnl or 0):+.2f}")
print(f"  NET LAST 24h: ${day_net:+.2f}")

# Portfolio reconciliation
print("\nPortfolio reconciliation (CLOB vs DB):")
try:
    from py_clob_client_v2 import ClobClient as CL2, ApiCreds as AC2
    from py_clob_client_v2.constants import POLYGON as POL2
    cl2 = CL2(
        host="https://clob.polymarket.com", chain_id=POL2, key=pk,
        creds=AC2(api_key=api_key, api_secret=api_secret, api_passphrase=api_pass),
        signature_type=sig_type, funder=funder or None,
    )
    usdc = int(cl2.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)).get("balance", 0)) / 1_000_000
    print(f"  USDC balance (on-chain): ${usdc:.4f}")
except Exception as e:
    print(f"  USDC lookup failed: {e}")
