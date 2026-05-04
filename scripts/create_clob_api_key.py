"""One-shot script to create a new CLOB API key with proxy-wallet authorization.

The existing key (cc2fce4d) was created without the proxy wallet registered,
causing order_version_mismatch. This script creates a new key that links:
  EOA signer  → 0x3Fbd61C04797850596412beb8301450B17b4B229
  Proxy wallet → 0x515f03872A47Bc39b290C134EC727642bcE43583

Run once:
  docker exec polymarket-scheduler python scripts/create_clob_api_key.py

Then copy the printed KEY / SECRET / PASSPHRASE into .env and re-run
debug_clob_order.py to confirm orders are accepted.
"""
import os
from dotenv import load_dotenv
load_dotenv("/app/.env")

from py_clob_client.client import ClobClient as PyClobClient
from py_clob_client.constants import POLYGON
from eth_account import Account

key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
funder = os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")
sig_type = int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "1"))

if not key:
    print("ERROR: POLYMARKET_PRIVATE_KEY not set")
    raise SystemExit(1)

eoa = Account.from_key(key).address
print(f"EOA:    {eoa}")
print(f"Proxy:  {funder}")
print(f"sig_type: {sig_type}")

if not funder:
    print("ERROR: POLYMARKET_FUNDER_ADDRESS not set — needed for proxy wallet registration")
    raise SystemExit(1)

# Build client WITHOUT existing creds — create_api_key uses L1 headers (EOA sig only)
client = PyClobClient(
    host="https://clob.polymarket.com",
    chain_id=POLYGON,
    key=key,
    signature_type=sig_type,
    funder=funder,
)

print("\nCreating new API key with proxy wallet registration...")
try:
    creds = client.create_api_key()
    print("\n=== NEW CREDENTIALS — update .env with these ===")
    print(f"POLYMARKET_API_KEY={creds.api_key}")
    print(f"POLYMARKET_API_SECRET={creds.api_secret}")
    print(f"POLYMARKET_API_PASSPHRASE={creds.api_passphrase}")
    print("=================================================")
    print("\nAfter updating .env, restart the scheduler container and run:")
    print("  docker exec polymarket-scheduler python scripts/debug_clob_order.py")
except Exception as e:
    print(f"\nFailed: {e}")
    if "not_found" in str(e).lower() or "404" in str(e):
        print("\nHint: The proxy wallet 0x515f... may not exist in the CLOB yet.")
        print("You need to deposit funds to it first via polymarket.com to activate it.")
    elif "unauthorized" in str(e).lower() or "401" in str(e):
        print("\nHint: Private key may not match the account on polymarket.com.")
