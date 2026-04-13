"""
Generate CLOB API credentials from your private key.

Requires POLYMARKET_PRIVATE_KEY set in .env.
Run: python scripts/generate_clob_credentials.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
os.chdir(str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

pk = os.getenv("POLYMARKET_PRIVATE_KEY") or os.getenv("PK")
if not pk:
    print("Set POLYMARKET_PRIVATE_KEY in .env first")
    sys.exit(1)

print("Connecting to Polymarket CLOB (Polygon mainnet)...")

try:
    from py_clob_client.client import ClobClient

    client = ClobClient(
        host="https://clob.polymarket.com",
        chain_id=137,
        key=pk,
    )

    print("Deriving API credentials...")
    creds = client.create_or_derive_api_creds()

    print("\nAdd these to your .env file:\n")
    print(f"POLYMARKET_API_KEY={creds.api_key}")
    print(f"POLYMARKET_API_SECRET={creds.api_secret}")
    print(f"POLYMARKET_PASSPHRASE={creds.api_passphrase}")
    print("\nThen restart all services.")

except Exception as exc:
    print(f"Failed: {exc}")
    print("\nMake sure py-clob-client is installed: pip install py-clob-client")
    sys.exit(1)
