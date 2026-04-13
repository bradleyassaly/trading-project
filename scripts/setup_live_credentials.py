"""
Check and report status of Polymarket CLOB credentials.

Run: python scripts/setup_live_credentials.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
os.chdir(str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

REQUIRED = {
    "POLYMARKET_PRIVATE_KEY": "Polygon wallet private key (from MetaMask export)",
    "POLYMARKET_API_KEY": "Generated via ClobClient.create_or_derive_api_creds()",
    "POLYMARKET_API_SECRET": "Generated alongside API key",
    "POLYMARKET_PASSPHRASE": "Generated alongside API key",
}

OPTIONAL = {
    "POLYMARKET_LIVE_ENABLED": "Set to 1 to enable live trading",
    "TRADING_MODE": "paper | dry_run | micro | live (default: paper)",
}

print("=== POLYMARKET CREDENTIAL STATUS ===\n")
missing = []
for key, desc in REQUIRED.items():
    val = os.getenv(key)
    if val:
        print(f"  OK  {key}: configured ({val[:8]}...)")
    else:
        alt = os.getenv("PK") if key == "POLYMARKET_PRIVATE_KEY" else None
        if alt:
            print(f"  OK  {key}: using PK alias ({alt[:8]}...)")
        else:
            print(f"  --  {key}: MISSING - {desc}")
            missing.append(key)

print()
for key, desc in OPTIONAL.items():
    val = os.getenv(key, "not set")
    print(f"  {key}: {val} ({desc})")

if missing:
    print(f"\n{len(missing)} required credentials missing.")
    if "POLYMARKET_PRIVATE_KEY" in missing:
        print("\nStep 1: Export your wallet private key from MetaMask")
        print("        Add to .env: POLYMARKET_PRIVATE_KEY=0x...")
    if any(k in missing for k in ["POLYMARKET_API_KEY", "POLYMARKET_API_SECRET", "POLYMARKET_PASSPHRASE"]):
        print("\nStep 2: Run: python scripts/generate_clob_credentials.py")
else:
    print("\nAll credentials configured. Ready for dry-run testing.")
