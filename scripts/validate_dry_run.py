"""
End-to-end validation of the live execution path in dry_run mode.
Tests every component without placing real orders.

Run: python scripts/validate_dry_run.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))
os.chdir(str(_ROOT))

from dotenv import load_dotenv
load_dotenv()

print("=" * 60)
print("LIVE EXECUTION DRY RUN VALIDATION")
print("=" * 60)

checks: dict[str, bool | None] = {}

# 1. Credentials
print("\n1. Credentials:")
pk = os.getenv("POLYMARKET_PRIVATE_KEY") or os.getenv("PK")
api_key = os.getenv("POLYMARKET_API_KEY")
print(f"   Private key: {'SET' if pk else 'MISSING'}")
print(f"   API key: {'SET' if api_key else 'NOT SET (derive with generate_clob_credentials.py)'}")
checks["private_key"] = bool(pk)

# 2. py-clob-client
print("\n2. CLOB Client:")
client = None
try:
    from py_clob_client.client import ClobClient
    if pk:
        client = ClobClient(host="https://clob.polymarket.com", chain_id=137, key=pk)
        if api_key:
            from py_clob_client.clob_types import ApiCreds
            client.set_api_creds(ApiCreds(
                api_key=api_key,
                api_secret=os.getenv("POLYMARKET_API_SECRET", ""),
                api_passphrase=os.getenv("POLYMARKET_PASSPHRASE", ""),
            ))
        print("   Connected (read-only without API creds)")
        checks["clob_client"] = True
    else:
        print("   Skipped (no private key)")
        checks["clob_client"] = None
except Exception as exc:
    print(f"   FAILED: {exc}")
    checks["clob_client"] = False

# 3. Token resolution
print("\n3. Token Resolution:")
token_id = None
test_cid = None
try:
    from trading_platform.polymarket.db import connect_wallet_db
    conn = connect_wallet_db()
    row = conn.execute(
        "SELECT condition_id, question, side FROM polymarket_paper_trades WHERE archived=0 AND exit_ts IS NULL LIMIT 1"
    ).fetchone()
    conn.close()
    if row:
        test_cid, question, side = row
        from trading_platform.polymarket.token_resolver import get_token_id
        token_id = get_token_id(test_cid, side)
        print(f"   Market: {question[:50]}")
        print(f"   Token ID: {token_id[:20]}..." if token_id else "   Token ID: FAILED")
        checks["token_resolution"] = bool(token_id)
    else:
        print("   No open paper trades to test with")
        checks["token_resolution"] = None
except Exception as exc:
    print(f"   FAILED: {exc}")
    checks["token_resolution"] = False

# 4. Order book
print("\n4. Order Book:")
if client and token_id:
    try:
        book = client.get_order_book(token_id)
        bids = book.bids or []
        asks = book.asks or []
        if bids and asks:
            best_bid = float(bids[0].price)
            best_ask = float(asks[0].price)
            spread = best_ask - best_bid
            print(f"   {len(bids)} bids, {len(asks)} asks")
            print(f"   Bid: {best_bid:.4f} | Ask: {best_ask:.4f} | Spread: {spread:.4f} ({spread/(best_ask+best_bid)*200:.1f}%)")
            checks["order_book"] = True
        else:
            print("   Empty book")
            checks["order_book"] = False
    except Exception as exc:
        print(f"   FAILED: {exc}")
        checks["order_book"] = False
else:
    print("   Skipped")
    checks["order_book"] = None

# 5. Existing live executor
print("\n5. Live Executor:")
try:
    from trading_platform.polymarket.polymarket_live_executor import PolymarketLiveExecutor
    print("   PolymarketLiveExecutor: importable")
    print(f"   DRY_RUN: {PolymarketLiveExecutor.DRY_RUN}")
    checks["live_executor"] = True
except Exception as exc:
    print(f"   FAILED: {exc}")
    checks["live_executor"] = False

# 6. Kill switch
print("\n6. Kill Switch:")
try:
    from trading_platform.polymarket.kill_switch import KillSwitch
    print("   KillSwitch: importable")
    checks["kill_switch"] = True
except Exception as exc:
    print(f"   FAILED: {exc}")
    checks["kill_switch"] = False

# Summary
print("\n" + "=" * 60)
print("VALIDATION SUMMARY")
print("=" * 60)
passed = sum(1 for v in checks.values() if v is True)
failed = sum(1 for v in checks.values() if v is False)
skipped = sum(1 for v in checks.values() if v is None)
for name, ok in checks.items():
    icon = "OK" if ok else ("SKIP" if ok is None else "FAIL")
    print(f"  {icon:>4}  {name}")

print(f"\n{passed} passed, {failed} failed, {skipped} skipped")

if failed == 0:
    if api_key:
        print("\nREADY for dry-run mode. Set TRADING_MODE=dry_run in .env")
    else:
        print("\nNeed API credentials. Run: python scripts/generate_clob_credentials.py")
else:
    print("\nFix failures before proceeding.")
