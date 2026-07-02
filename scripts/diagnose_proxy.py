"""Diagnose which proxy wallet / sig_type the CLOB expects for this EOA.

Tries multiple approaches to determine correct configuration.
Run: docker exec polymarket-scheduler python scripts/diagnose_proxy.py
"""
import os, json, time
import requests
from dotenv import load_dotenv
load_dotenv("/app/.env")

from py_clob_client.client import ClobClient as PyClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType, PartialCreateOrderOptions
from py_clob_client.constants import POLYGON
from py_clob_client.headers.headers import create_level_1_headers
from eth_account import Account

key      = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
api_key  = os.environ.get("POLYMARKET_API_KEY", "")
api_sec  = os.environ.get("POLYMARKET_API_SECRET", "")
api_pass = os.environ.get("POLYMARKET_API_PASSPHRASE") or os.environ.get("POLYMARKET_PASSPHRASE") or ""
funder   = os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")

eoa = Account.from_key(key).address
print(f"EOA:    {eoa}")
print(f"Funder: {funder}")
print()

# ── Test 1: sig_type=0 (EOA direct, no proxy) ──────────────────────────────
print("=== Test 1: sig_type=0 (EOA direct, no proxy) ===")
try:
    c0 = PyClobClient(
        host="https://clob.polymarket.com", chain_id=POLYGON, key=key,
        creds=ApiCreds(api_key=api_key, api_secret=api_sec, api_passphrase=api_pass),
        signature_type=0,
    )
    # pick any token — just need to test signing/post
    from trading_platform.polymarket.db_connection import get_connection
    conn = get_connection()
    row = conn.execute(
        "SELECT yes_token_id, slug FROM markets WHERE yes_token_id IS NOT NULL "
        "AND end_date_iso > '2026-05-04' ORDER BY CAST(volume_24h AS FLOAT) DESC NULLS LAST LIMIT 1"
    ).fetchone()
    conn.close()
    if row:
        tid, slug = row
        nr = c0.get_neg_risk(tid)
        opts = PartialCreateOrderOptions(neg_risk=nr)
        order_args = OrderArgs(token_id=tid, price=0.01, size=2.0, side="BUY")
        signed = c0.create_order(order_args, opts)
        resp = c0.post_order(signed, OrderType.FOK)
        print(f"  post_order response: {resp}")
        if isinstance(resp, dict) and resp.get("status") not in ("error",):
            print("  ✓ sig_type=0 ACCEPTED — EOA signing works. Set POLYMARKET_SIGNATURE_TYPE=0")
            print("    and REMOVE POLYMARKET_FUNDER_ADDRESS. Fund EOA directly.")
        else:
            status = resp.get("errorMsg") or resp.get("status") if isinstance(resp, dict) else str(resp)
            print(f"  ✗ sig_type=0 rejected: {status}")
    else:
        print("  (no market in DB to test)")
except Exception as e:
    print(f"  ✗ sig_type=0 failed: {e}")

print()

# ── Test 2: derive API key (no CLOB write) ─────────────────────────────────
print("=== Test 2: derive_api_key with sig_type=1 + funder ===")
try:
    c1 = PyClobClient(
        host="https://clob.polymarket.com", chain_id=POLYGON, key=key,
        signature_type=1, funder=funder,
    )
    derived = c1.derive_api_key()
    print(f"  derived key:        {derived.api_key}")
    print(f"  current .env key:   {api_key}")
    match = derived.api_key == api_key
    print(f"  match .env?  {'YES — key already derived from this EOA+proxy combo' if match else 'NO — different key'}")
except Exception as e:
    print(f"  derive_api_key failed: {e}")

print()

# ── Test 3: raw L1 profile/account lookup ─────────────────────────────────
print("=== Test 3: CLOB profile lookup for this EOA ===")
try:
    from py_clob_client.signer import Signer
    signer = Signer(key, chain_id=POLYGON)
    hdrs = create_level_1_headers(signer, nonce=0)
    r = requests.get("https://clob.polymarket.com/auth/api-key", headers=hdrs, timeout=10)
    print(f"  GET /auth/api-key: {r.status_code} {r.text[:300]}")
except Exception as e:
    print(f"  profile lookup failed: {e}")
