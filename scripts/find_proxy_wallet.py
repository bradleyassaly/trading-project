"""Query Gamma API and on-chain to find the correct proxy wallet for this EOA.

Run: docker exec polymarket-scheduler python scripts/find_proxy_wallet.py
"""
import os, requests, json
from dotenv import load_dotenv
load_dotenv("/app/.env")
from eth_account import Account

key    = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
funder = os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")
eoa    = Account.from_key(key).address if key else "???"

print(f"EOA in .env:    {eoa}")
print(f"Funder in .env: {funder}")
print()

# ── 1. Gamma API profile lookup ────────────────────────────────────────────
print("=== Gamma API: profile for this EOA ===")
for url in [
    f"https://gamma-api.polymarket.com/profiles?address={eoa}",
    f"https://gamma-api.polymarket.com/profiles?proxyWallet={eoa}",
    f"https://gamma-api.polymarket.com/user?address={eoa}",
]:
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and r.text.strip() not in ("[]", "null", "{}"):
            print(f"  {url}")
            print(f"  → {r.text[:400]}")
            break
        else:
            print(f"  {url} → {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"  {url} → error: {e}")

print()

# ── 2. CLOB: get profile for the API key ──────────────────────────────────
print("=== CLOB: /auth/api-key (POST derive = check registered key) ===")
from py_clob_client.client import ClobClient as PyClobClient
from py_clob_client.clob_types import ApiCreds
from py_clob_client.constants import POLYGON
from py_clob_client.headers.headers import create_level_1_headers
from py_clob_client.signer import Signer

signer = Signer(key, chain_id=POLYGON)
hdrs = create_level_1_headers(signer, nonce=0)
try:
    r = requests.post("https://clob.polymarket.com/auth/api-key", headers=hdrs, timeout=10)
    print(f"  POST /auth/api-key (no proxy): {r.status_code} {r.text[:300]}")
except Exception as e:
    print(f"  error: {e}")

print()

# ── 3. CLOB: /data/trading-profiles or /accounts ─────────────────────────
print("=== CLOB: account endpoints ===")
api_key  = os.environ.get("POLYMARKET_API_KEY", "")
api_sec  = os.environ.get("POLYMARKET_API_SECRET", "")
api_pass = os.environ.get("POLYMARKET_API_PASSPHRASE") or os.environ.get("POLYMARKET_PASSPHRASE") or ""
from py_clob_client.headers.headers import create_level_2_headers
from py_clob_client.clob_types import RequestArgs

c = PyClobClient(
    host="https://clob.polymarket.com", chain_id=POLYGON, key=key,
    creds=ApiCreds(api_key=api_key, api_secret=api_sec, api_passphrase=api_pass),
    signature_type=1, funder=funder,
)
for path in ["/profile", "/accounts", "/data/user"]:
    try:
        req = RequestArgs(method="GET", request_path=path, body="", serialized_body="")
        hdrs2 = create_level_2_headers(c.signer, c.creds, req)
        r = requests.get(f"https://clob.polymarket.com{path}", headers=hdrs2, timeout=10)
        print(f"  GET {path}: {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"  GET {path}: error {e}")

print()

# ── 4. Check if funder appears in CLOB trade history at all ──────────────
print("=== CLOB: any trades for proxy wallet? ===")
try:
    trades = c.get_trades()
    if trades:
        for t in (trades if isinstance(trades, list) else trades.get("data", []))[:3]:
            print(f"  maker={t.get('maker_address','')} owner={t.get('owner','')} side={t.get('side','')} price={t.get('price','')} size={t.get('size','')}")
    else:
        print("  (no trades)")
except Exception as e:
    print(f"  get_trades error: {e}")
