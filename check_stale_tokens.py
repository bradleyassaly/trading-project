"""Find NO tokens sitting in wallet that have matching YES tokens we also hold (redeemable pairs)."""
import sys, os
sys.path.insert(0, "src")
from dotenv import load_dotenv; load_dotenv(".env")

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

from trading_platform.polymarket.db_connection import get_connection
conn = get_connection()

# Get all open positions with token_ids — both SELL and BUY
open_pos = conn.execute("""
    SELECT id, direction, token_id, condition_id, fill_price, size_usd, shares,
           LEFT(question, 60) as q
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NULL
      AND status NOT IN ('error','blocked','cancelled')
      AND token_id IS NOT NULL
    ORDER BY direction, fill_price DESC
""").fetchall()
conn.close()

print(f"Checking {len(open_pos)} open positions for actual token balances...")
print()

total_token_value = 0
stale_no_tokens = []

for row in open_pos:
    lid, direction, token_id, condition_id, fp, size_usd, shares, q = row
    try:
        bal = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        )
        actual = int(bal.get("balance", 0)) / 1_000_000
        if actual > 0.001:
            # Get current market price
            import httpx
            r = httpx.get(f"https://clob.polymarket.com/midpoint?token_id={token_id}", timeout=5)
            mid = float(r.json().get("mid", 0)) if r.status_code == 200 else 0
            value = actual * mid
            total_token_value += value
            fp_str = f"{float(fp):.3f}" if fp else "N/A"
            print(f"  #{lid} {direction} {actual:.4f} tokens @ mid={mid:.3f} = ${value:.4f} | fill={fp_str} | {q[:55]}")
            stale_no_tokens.append((lid, direction, token_id, actual, mid, value))
    except Exception as e:
        pass

print(f"\nTotal token value in open positions: ${total_token_value:.4f}")

usdc_bal = int(client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)).get("balance", 0)) / 1_000_000
print(f"USDC balance: ${usdc_bal:.4f}")
print(f"Estimated total portfolio: ${usdc_bal + total_token_value:.4f}")
