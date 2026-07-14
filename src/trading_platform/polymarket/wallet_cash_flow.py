"""Per-wallet on-chain USDC cash flow (#1 — borrow from Predicts.guru).

    net_cash_out = external USDC out − external USDC in

It is the hardest-to-fake wallet-skill signal. Wash trades, survivorship, and
our own modeled trade PnL can all be inflated, but real USDC withdrawn from a
wallet to an external address is money the wallet actually extracted — it
cannot be faked without literally moving funds. We use it as a wallet-quality /
wash-defeat FEATURE for the resolution engine's wallet graph. (Copy-entry
itself is retired — see reports/mirror_copy_kill_rule.md; the wallet graph
survives only as a feature source.)

Design (competitor analysis 2026-07-13, borrow #1, Stage A):
  * A USDC Transfer touching wallet W is classified by its counterparty:
      - counterparty is a Polymarket contract  → INTERNAL (trading/settlement)
      - W is the recipient, counterparty external → DEPOSIT  (cash in)
      - W is the sender,   counterparty external → WITHDRAWAL (cash out)
  * Classification accuracy depends entirely on POLYMARKET_INTERNAL being
    complete. It is seeded from the addresses wallet_stream.py already tracks
    plus known Polymarket infra; extend it as new contracts appear, and
    VALIDATE the output against a known wallet (needs Polygon RPC) before any
    gate consumes net_cash_out.

The decode / classify / aggregate / cohort logic here is pure and unit-tested.
The RPC fetch + backfill is a thin, separated layer that needs live Polygon
RPC to run; nothing gates on these numbers yet (Stage A is instrument-only).
"""
from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from typing import Any, Iterable

logger = logging.getLogger(__name__)

# ── Contract constants ──────────────────────────────────────────────────────
# Canonical source is wallet_stream.py; duplicated here (plain address strings)
# so this module has no dependency on the websocket stack. Keep in sync.
USDC_E = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"      # bridged USDC.e
USDC_NATIVE = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"  # native Polygon USDC
USDC_CONTRACTS = [USDC_E, USDC_NATIVE]

# Polymarket internal contracts — a USDC transfer to/from any of these is
# trading/settlement, not an external deposit/withdrawal. Extend as needed.
_CTF_EXCHANGE = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
_NEG_RISK_CTF_EXCHANGE = "0xc5d563a36ae78145c45a50134d48a1215220f80a"
_CTF_EXCHANGE_V2 = "0xe111180000d2663c0091e4f400237545b87b996b"
_NEG_RISK_CTF_EXCHANGE_V2 = "0xe2222d279d744050d28e00520010520000310f59"
_COLLATERAL_V2 = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb"
_CONDITIONAL_TOKENS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"  # CTF (Polygon)
_NEG_RISK_ADAPTER = "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296"
_PROXY_FACTORY = "0xab45c5a4b0c941a2f231c04c3f49182e1a254052"  # Polymarket proxy

POLYMARKET_INTERNAL = {
    _CTF_EXCHANGE, _NEG_RISK_CTF_EXCHANGE, _CTF_EXCHANGE_V2,
    _NEG_RISK_CTF_EXCHANGE_V2, _COLLATERAL_V2, _CONDITIONAL_TOKENS,
    _NEG_RISK_ADAPTER, _PROXY_FACTORY,
}

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_ADDR = "0x" + "0" * 40
USDC_DECIMALS = 1_000_000  # 6dp

# Polymarket's Polygon deployment era — no USDC cash flow predates this, so we
# never scan below it. Overridable for tests / re-tuning.
DEFAULT_START_BLOCK = int(os.environ.get("CASH_FLOW_START_BLOCK", "22000000"))
# eth_getLogs providers cap range/result count; page conservatively.
BLOCK_WINDOW = int(os.environ.get("CASH_FLOW_BLOCK_WINDOW", "50000"))


# ── Pure logic (unit-tested; no I/O) ────────────────────────────────────────

def _norm(addr: str | None) -> str:
    return (addr or "").lower()


def _pad_addr(addr: str) -> str:
    """20-byte address → 32-byte topic form (0x + 64 hex)."""
    return "0x" + _norm(addr).replace("0x", "").rjust(64, "0")


def _addr_from_topic(topic: str) -> str:
    """32-byte topic → 20-byte address (0x + 40 hex), lowercased."""
    t = (topic or "").replace("0x", "")
    return "0x" + t[-40:]


def decode_usdc_transfer(log: dict) -> dict | None:
    """Decode one eth_getLogs USDC Transfer entry → {from, to, amount_usdc}.

    Returns None if the log is not a well-formed Transfer (wrong topic, missing
    fields, unparseable amount).
    """
    try:
        topics = log.get("topics") or []
        if len(topics) < 3 or _norm(topics[0]) != TRANSFER_TOPIC:
            return None
        frm = _addr_from_topic(topics[1])
        to = _addr_from_topic(topics[2])
        raw = log.get("data")
        amount = int(raw, 16) / USDC_DECIMALS
    except (TypeError, ValueError, IndexError):
        return None
    if amount <= 0:
        return None
    return {"from": frm, "to": to, "amount_usdc": amount}


def classify_transfer(frm: str, to: str, wallet: str) -> str:
    """Classify a decoded transfer relative to `wallet`.

    Returns one of: 'deposit' (external → wallet), 'withdrawal' (wallet →
    external), 'internal' (counterparty is a Polymarket contract), 'self'
    (wallet ↔ wallet), or 'unrelated' (wallet not involved). The zero address
    (bridge mint/burn) is treated as external cash flow.
    """
    w, frm, to = _norm(wallet), _norm(frm), _norm(to)
    w_is_to, w_is_from = (to == w), (frm == w)
    if not (w_is_to or w_is_from):
        return "unrelated"
    if w_is_to and w_is_from:
        return "self"
    counterparty = frm if w_is_to else to
    if counterparty in POLYMARKET_INTERNAL:
        return "internal"
    return "deposit" if w_is_to else "withdrawal"


def aggregate_cash_flow(logs: Iterable[dict], wallet: str) -> dict:
    """Fold decoded-or-raw Transfer logs into per-wallet cash-flow totals.

    Accepts either raw eth_getLogs dicts (auto-decoded) or pre-decoded
    {from,to,amount_usdc} dicts. net_cash_out = withdrawals − deposits.
    """
    deposits = withdrawals = 0.0
    n_dep = n_wd = n_internal = 0
    for log in logs or []:
        d = log if "amount_usdc" in log else decode_usdc_transfer(log)
        if not d:
            continue
        kind = classify_transfer(d["from"], d["to"], wallet)
        amt = d["amount_usdc"]
        if kind == "deposit":
            deposits += amt
            n_dep += 1
        elif kind == "withdrawal":
            withdrawals += amt
            n_wd += 1
        elif kind == "internal":
            n_internal += 1
    return {
        "deposits_usdc": round(deposits, 4),
        "withdrawals_usdc": round(withdrawals, 4),
        "net_cash_out": round(withdrawals - deposits, 4),
        "n_deposits": n_dep,
        "n_withdrawals": n_wd,
        "n_internal": n_internal,
    }


def select_cohort(conn, limit: int | None = None) -> list[str]:
    """The wallets we actually use as features: leaderboard ∪ copyable-alpha ∪
    insiders ∪ copyable-archetypes. Defensive to any table being absent.
    """
    queries = [
        "SELECT DISTINCT wallet FROM leaderboard",
        "SELECT DISTINCT wallet FROM wallet_alpha_scores WHERE is_copyable = 1",
        "SELECT DISTINCT wallet FROM insider_wallets",
        "SELECT DISTINCT wallet FROM wallet_archetypes WHERE copyable = 1",
    ]
    out: set[str] = set()
    for q in queries:
        try:
            for r in conn.execute(q).fetchall():
                if r and r[0]:
                    out.add(_norm(r[0]))
        except Exception as exc:
            logger.debug("cohort query skipped (%s): %s", q[:40], exc)
    wallets = sorted(out)
    return wallets[:limit] if limit else wallets


# ── RPC layer (needs live Polygon RPC; not unit-tested) ─────────────────────

def _rpc_urls() -> list[str]:
    return [r for r in (
        os.environ.get("POLYGON_RPC_URL"),
        os.environ.get("POLYGON_WSS_URL", "")
            .replace("wss://", "https://").replace("ws://", "https://"),
        "https://polygon-rpc.com",
        "https://polygon.llamarpc.com",
        "https://polygon.drpc.org",
    ) if r and r.startswith("http")]


def _rpc_call(method: str, params: list, timeout: float = 20.0) -> Any:
    """JSON-RPC POST with RPC failover (mirrors force_live_test.py)."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1,
                       "method": method, "params": params}).encode()
    headers = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
    last_exc = None
    for rpc in _rpc_urls():
        try:
            req = urllib.request.Request(rpc, data=body, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                data = json.loads(r.read())
            if "result" in data:
                return data["result"]
            last_exc = data.get("error")
        except Exception as exc:  # noqa: BLE001 — failover to next RPC
            last_exc = exc
            logger.debug("RPC %s %s failed: %s", rpc, method, exc)
    raise RuntimeError(f"all RPCs failed for {method}: {last_exc}")


def _latest_block() -> int:
    return int(_rpc_call("eth_blockNumber", []), 16)


def fetch_usdc_transfers(wallet: str, from_block: int, to_block: int) -> list[dict]:
    """eth_getLogs for USDC Transfers where `wallet` is the sender OR recipient,
    paged over [from_block, to_block] in BLOCK_WINDOW chunks. Two filters per
    window (wallet-as-from, wallet-as-to) across both USDC contracts.
    """
    padded = _pad_addr(wallet)
    logs: list[dict] = []
    start = from_block
    while start <= to_block:
        end = min(start + BLOCK_WINDOW - 1, to_block)
        fb, tb = hex(start), hex(end)
        for topics in ([TRANSFER_TOPIC, padded], [TRANSFER_TOPIC, None, padded]):
            try:
                res = _rpc_call("eth_getLogs", [{
                    "address": USDC_CONTRACTS, "fromBlock": fb, "toBlock": tb,
                    "topics": topics,
                }])
                if res:
                    logs.extend(res)
            except Exception as exc:  # noqa: BLE001
                logger.warning("getLogs %s-%s failed for %s: %s",
                               fb, tb, wallet[:10], exc)
        start = end + 1
    return logs


def compute_wallet_cash_flow(wallet: str, from_block: int | None = None,
                             to_block: int | None = None) -> dict:
    """Fetch + aggregate one wallet's cash flow. Needs live RPC."""
    fb = from_block if from_block is not None else DEFAULT_START_BLOCK
    tb = to_block if to_block is not None else _latest_block()
    logs = fetch_usdc_transfers(wallet, fb, tb)
    result = aggregate_cash_flow(logs, wallet)
    result["scanned_from_block"] = fb
    result["scanned_to_block"] = tb
    return result


def backfill(limit: int | None = None, dry_run: bool = True) -> dict:
    """Compute cash flow for the feature cohort and (unless dry_run) persist
    deposits_usdc / withdrawals_usdc / net_cash_out / cash_flow_synced_at to
    wallet_profiles. Requires live Polygon RPC + the primary DB.
    """
    from trading_platform.polymarket.db_connection import get_connection
    from trading_platform.polymarket.wallet_db import WalletDB

    conn = get_connection()
    try:
        cohort = select_cohort(conn, limit=limit)
    finally:
        try: conn.close()
        except Exception: pass

    logger.info("[cash_flow] cohort=%d dry_run=%s", len(cohort), dry_run)
    to_block = _latest_block()
    db = WalletDB() if not dry_run else None
    now = int(time.time())
    done = 0
    sample: list[dict] = []
    for w in cohort:
        try:
            cf = compute_wallet_cash_flow(w, to_block=to_block)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[cash_flow] %s failed: %s", w[:10], exc)
            continue
        if not dry_run and db is not None:
            db.upsert_profile(
                w,
                deposits_usdc=cf["deposits_usdc"],
                withdrawals_usdc=cf["withdrawals_usdc"],
                net_cash_out=cf["net_cash_out"],
                cash_flow_synced_at=now,
            )
        done += 1
        if len(sample) < 5:
            sample.append({"wallet": w, **cf})
    return {"cohort": len(cohort), "computed": done,
            "dry_run": dry_run, "sample": sample}


def main() -> int:
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    p = argparse.ArgumentParser(description="Per-wallet USDC cash flow")
    p.add_argument("--wallet", help="compute a single wallet (validation)")
    p.add_argument("--limit", type=int, default=None, help="cap cohort size")
    p.add_argument("--write", action="store_true",
                   help="persist to wallet_profiles (default: dry run)")
    args = p.parse_args()
    if args.wallet:
        print(json.dumps(compute_wallet_cash_flow(args.wallet), indent=2))
    else:
        print(json.dumps(backfill(limit=args.limit, dry_run=not args.write),
                         indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
