"""Check actual USDC and conditional token balances on Polymarket."""
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

# USDC balance (collateral)
result = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
usdc_balance = int(result.get("balance", 0)) / 1_000_000
print(f"USDC balance (proxy wallet): ${usdc_balance:.4f}")

print(f"Open GTC orders: (skipped)")

# Check recent trades from Polymarket's perspective
from trading_platform.polymarket.db_connection import get_connection
conn = get_connection()

# All open positions with their token_ids
open_pos = conn.execute("""
    SELECT id, direction, token_id, fill_price, size_usd, shares,
           LEFT(question, 55) as q
    FROM live_trades
    WHERE dry_run=0 AND exit_ts IS NULL AND status NOT IN ('error','blocked','cancelled')
      AND token_id IS NOT NULL
    ORDER BY size_usd DESC
    LIMIT 20
""").fetchall()
conn.close()

print(f"\nTop 20 open positions by size:")
total_invested = 0
for row in open_pos:
    lid, direction, token_id, fp, size_usd, shares, q = row
    fp_str = f"{float(fp):.3f}" if fp else "N/A"
    size_str = f"${float(size_usd or 0):.2f}"
    total_invested += float(size_usd or 0)

    # Get actual conditional balance
    try:
        bal_result = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        )
        actual_tokens = int(bal_result.get("balance", 0)) / 1_000_000
    except Exception as e:
        actual_tokens = None

    tok_str = f"{actual_tokens:.4f}" if actual_tokens is not None else "err"
    print(f"  #{lid} {direction} fill={fp_str} size={size_str} actual_tokens={tok_str} | {q}")

print(f"\nTotal size_usd in DB: ${total_invested:.2f}")
