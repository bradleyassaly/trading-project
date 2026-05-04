"""Debug/verification script for CLOB order placement.

Run this after re-authorizing API access in the Polymarket web UI:
  1. Log into polymarket.com with your wallet
  2. Go to Profile → API → Authorize (or re-derive your API key)
  3. Run: docker exec polymarket-scheduler python scripts/debug_clob_order.py

Expected output if working:
  - Balance > 0 (or at least a numeric value, not error)
  - post_order response: status=not_found (FOK at 0.01 won't fill but is ACCEPTED)

If you still see order_version_mismatch after re-authorization, the private key
in POLYMARKET_PRIVATE_KEY may not match the EOA linked to your Polymarket account.
"""
import os
from dotenv import load_dotenv
load_dotenv("/app/.env")

from py_clob_client_v2 import ClobClient as PyClobClient
from py_clob_client_v2 import ApiCreds, OrderArgs, OrderType, PartialCreateOrderOptions
from py_clob_client_v2.constants import POLYGON
from eth_account import Account

key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
api_key = os.environ.get("POLYMARKET_API_KEY", "")
api_secret = os.environ.get("POLYMARKET_API_SECRET", "")
api_pass = (os.environ.get("POLYMARKET_API_PASSPHRASE")
            or os.environ.get("POLYMARKET_PASSPHRASE") or "")
funder = os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")
sig_type = int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "1"))

eoa = Account.from_key(key).address if key else "???"
print(f"EOA (from key):  {eoa}")
print(f"Proxy (funder):  {funder}")
print(f"sig_type:        {sig_type}")
print(f"API key:         {api_key[:12]}..." if api_key else "API key: NOT SET")

if not (key and api_key and api_secret and api_pass):
    print("\nERROR: Missing credentials in .env")
    raise SystemExit(1)

client = PyClobClient(
    host="https://clob.polymarket.com",
    chain_id=POLYGON, key=key,
    creds=ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_pass),
    signature_type=sig_type, funder=funder or None,
)

# 1. API keys check
try:
    keys = client.get_api_keys()
    print(f"\nAPI keys registered: {[k for k in keys.get('apiKeys', [])]}")
except Exception as e:
    print(f"\nget_api_keys failed: {e}")

# 2. Balance check
try:
    from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
    bal = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    raw = int(bal.get("balance", 0))
    usdc = raw / 1e6
    allowances_set = all(int(v) > 0 for v in bal.get("allowances", {}).values())
    print(f"USDC balance:    ${usdc:.2f}  (allowances set: {allowances_set})")
except Exception as e:
    print(f"Balance check failed: {e}")

# 3. Get a liquid active market
from trading_platform.polymarket.db_connection import get_connection
conn = get_connection()
try:
    row = conn.execute(
        """SELECT yes_token_id, slug FROM markets
            WHERE yes_token_id IS NOT NULL
              AND end_date_iso > to_char(NOW() + interval '2 days', 'YYYY-MM-DD"T"HH24:MI:SS')
            ORDER BY CAST(volume_24h AS FLOAT) DESC NULLS LAST LIMIT 1"""
    ).fetchone()
finally:
    conn.close()

if not row:
    print("\nERROR: No active markets in DB")
    raise SystemExit(1)

token_id, slug = row
print(f"\nTest market: {slug}")

# 4. Try signing + posting
try:
    nr = client.get_neg_risk(token_id)
    opts = PartialCreateOrderOptions(tick_size="0.01", neg_risk=nr)
    order_args = OrderArgs(token_id=token_id, price=0.01, size=2.0, side="BUY")
    print(f"create_and_post_order: attempting (neg_risk={nr})...")
    print("Posting FOK @ 0.01 (safe test — won't fill)...")
    resp = client.create_and_post_order(order_args, options=opts, order_type=OrderType.FOK)
    print(f"post_order RESPONSE: {resp}")
    if isinstance(resp, dict) and resp.get("status") not in ("error",):
        print("\n✓ ORDER ACCEPTED — proxy wallet authorization is working!")
    else:
        print(f"\nStatus: {resp.get('status') if isinstance(resp, dict) else resp}")
except Exception as e:
    if "order_version_mismatch" in str(e):
        print(f"\n✗ order_version_mismatch — proxy wallet authorization NOT registered.")
        print("  Action needed: log into polymarket.com and re-authorize API access.")
        print(f"  Your EOA: {eoa}")
        print(f"  Your proxy: {funder}")
    else:
        print(f"\nFailed: {e}")
