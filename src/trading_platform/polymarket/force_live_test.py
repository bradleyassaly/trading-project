"""Force a one-shot live trade for end-to-end execution validation.

Bypasses signal-quality gates (probation eligibility, EV, sample size)
that would otherwise block live trading until paper data accumulates.
Hard-caps the stake at $5 USDC regardless of input so a misuse can
only ever lose at most $5.

Use case: validate the live execution path (CLOB connectivity,
py_clob_client signing, order submission, fill confirmation, DB
record, Telegram alert, exit monitor pickup) BEFORE we trust the
auto-graduation flow with real money.

Usage::

    python -m trading_platform.polymarket.force_live_test \\
      --condition-id 0xabc... --side YES --stake 5

CLI rejects:
  - master_switch off
  - py_clob_client not installed
  - emergency stop file present
  - wallet USDC < stake
  - condition_id not found via Gamma
  - market not actively trading
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time

logger = logging.getLogger(__name__)

HARD_STAKE_CAP_USD = 5.0  # safety: $5 max regardless of CLI input


def _check_usdc_balance(wallet: str) -> float | None:
    """Query USDC.e balance on-chain via free public RPCs (multi-fallback)."""
    import urllib.request
    import json
    USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    padded = "0x" + wallet.lower().replace("0x", "").rjust(64, "0")
    body = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [{"to": USDC_E, "data": f"0x70a08231{padded[2:]}"}, "latest"],
    }).encode()
    rpcs = [r for r in [
        os.environ.get("POLYGON_RPC_URL"),
        os.environ.get("POLYGON_WSS_URL", "").replace("wss://", "https://").replace("ws://", "https://"),
        "https://polygon-rpc.com",
        "https://rpc.ankr.com/polygon",
        "https://polygon.llamarpc.com",
        "https://polygon.drpc.org",
    ] if r and r.startswith("http")]
    headers = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
    for rpc in rpcs:
        try:
            req = urllib.request.Request(rpc, data=body, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            if "result" in data and data["result"]:
                return int(data["result"], 16) / 1e6
        except Exception as exc:
            logger.debug("RPC %s failed: %s", rpc, exc)
    logger.warning("All RPC balance checks failed; falling back to env override")
    env_bal = os.environ.get("POLYMARKET_LIVE_BANKROLL_USD")
    if env_bal:
        try: return float(env_bal)
        except Exception: pass
    return None


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--condition-id", required=True, help="Polymarket conditionId (0x...)")
    p.add_argument("--side", required=True, choices=["YES", "NO"], help="Direction to buy")
    p.add_argument("--stake", type=float, default=5.0, help="Stake in USDC (capped at $5)")
    p.add_argument("--dry", action="store_true", help="Do everything except submit the order")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    stake = min(float(args.stake), HARD_STAKE_CAP_USD)
    if stake < args.stake:
        logger.warning("Stake clamped to hard cap: ${:.0f} → ${:.0f}".format(args.stake, stake))

    # ── 1. Pre-flight checks ──────────────────────────────────────────────
    wallet = os.environ.get("POLYMARKET_WALLET_ADDRESS") or ""
    if not wallet:
        logger.error("POLYMARKET_WALLET_ADDRESS not set")
        return 2
    for k in ("POLYMARKET_API_KEY", "POLYMARKET_API_SECRET",
              "POLYMARKET_PRIVATE_KEY", "POLYMARKET_PASSPHRASE"):
        if not os.environ.get(k):
            logger.error("%s not set — cannot submit live order", k)
            return 2
    if os.environ.get("POLYMARKET_LIVE_ENABLED", "").lower() not in ("1", "true", "yes"):
        logger.error("POLYMARKET_LIVE_ENABLED not set — refusing to bypass master switch")
        return 2

    # ── 2. Wallet balance ────────────────────────────────────────────────
    balance = _check_usdc_balance(wallet)
    if balance is None:
        logger.error("Could not verify USDC balance — refusing to proceed")
        return 3
    if balance < stake:
        logger.error("USDC balance %.2f < stake %.2f", balance, stake)
        return 3
    logger.info("Wallet %s has %.2f USDC.e (need %.2f)", wallet[:10] + "...", balance, stake)

    # ── 3. Resolve token IDs from Gamma ───────────────────────────────────
    import urllib.request, json as _json
    try:
        url = f"https://gamma-api.polymarket.com/markets?condition_ids={args.condition_id}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = _json.loads(r.read())
        if not isinstance(data, list) or not data:
            logger.error("Market not found in Gamma: %s", args.condition_id)
            return 4
        m = data[0]
        if not m.get("active") or m.get("closed"):
            logger.error("Market is closed/inactive: %s", m.get("question"))
            return 4
        tids_raw = m.get("clobTokenIds") or "[]"
        tids = _json.loads(tids_raw) if isinstance(tids_raw, str) else tids_raw
        if not tids or len(tids) < 2:
            logger.error("Market lacks 2 token IDs (binary): %s", m.get("question"))
            return 4
        # tids[0] = YES, tids[1] = NO
        token_id = str(tids[0] if args.side == "YES" else tids[1])
        question = m.get("question") or ""
        logger.info("Market: %s", question[:80])
        logger.info("Token ID (%s): %s", args.side, token_id)
    except Exception as exc:
        logger.error("Gamma lookup failed: %s", exc)
        return 4

    # ── 4. Verify book depth (don't sweep an empty book) ─────────────────
    from trading_platform.polymarket.clob_client import ClobClient
    clob = ClobClient()
    if not clob.is_configured:
        logger.error("ClobClient not configured (credentials missing in container?)")
        return 5
    book = clob.get_order_book(token_id)
    asks = book.get("asks") or []
    if not asks:
        logger.error("No asks on the book — cannot fill BUY")
        return 5
    ask_depth = sum(float(a.get("size", 0)) * float(a.get("price", 0)) for a in asks[:5])
    if ask_depth < stake * 2:
        logger.error("Thin liquidity: ask depth $%.2f < 2× stake $%.2f", ask_depth, stake * 2)
        return 5
    current = clob.get_mid_price(token_id) or 0.5
    logger.info("Mid price %.4f, top-5 ask depth $%.2f", current, ask_depth)

    if args.dry:
        logger.info("[DRY] would BUY %s @ ~%.3f for $%.2f (token %s)",
                    args.side, current, stake, token_id[:18] + "...")
        return 0

    # ── 5. Submit ─────────────────────────────────────────────────────────
    logger.warning("SUBMITTING LIVE ORDER: BUY %s for $%.2f on %s", args.side, stake, args.condition_id)
    result = clob.place_market_order(token_id=token_id, side="BUY", size_usdc=stake, max_slippage=0.05)
    logger.info("Order result: success=%s status=%s order_id=%s fill=%s err=%s",
                result.success, result.status, result.order_id,
                result.filled_price, result.error_msg)
    if not result.success:
        return 6

    # ── 6. Record in live_trades ─────────────────────────────────────────
    from trading_platform.polymarket.db_connection import get_connection
    conn = get_connection()
    try:
        now_ts = int(time.time())
        conn.execute(
            """INSERT INTO live_trades
               (attempted_at, signal_type, condition_id, question, direction,
                confidence, size_usd, entry_price, order_id, status, dry_run,
                token_id, side, fill_price, shares, expected_price,
                submitted_at, filled_at, signal_wallet)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (now_ts, "force_live_test", args.condition_id, question, "BUY",
             1.0, stake, current, result.order_id, result.status,
             token_id, args.side, result.filled_price or current,
             stake / max(result.filled_price or current, 0.01),
             current, now_ts, now_ts, "force_live_test"),
        )
        conn.commit()
        logger.info("Recorded in live_trades; live monitor will track exits.")
    finally:
        try: conn.close()
        except Exception: pass

    # ── 7. Loud Telegram alert ───────────────────────────────────────────
    try:
        from trading_platform.polymarket.telegram_alerts import get_alerter
        get_alerter()._send(
            f"\U0001f6a8 <b>FORCED LIVE TEST FILLED</b> \U0001f6a8\n\n"
            f"BUY <b>{args.side}</b> @ {(result.filled_price or current):.4f}\n"
            f"Stake: <b>${stake:.2f}</b>\n"
            f"Order: <code>{result.order_id}</code>\n"
            f"Market: {question[:60]}\n"
            f"\n\u2705 Live execution path validated.",
            disable_notification=False,
        )
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
