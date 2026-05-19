"""
Hourly live portfolio equity snapshot.

Captures: on-chain USDC, open position cost + market value, cumulative
realized PnL, and daily change vs the prior snapshot.  Mirrors the
paper_equity_snapshot task so both equity curves share the same shape.

Table auto-created on first run (idempotent).
"""
from __future__ import annotations
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

import httpx
from trading_platform.polymarket.db_connection import get_connection

# ── schema ────────────────────────────────────────────────────────────────

ENSURE_TABLE = """
CREATE TABLE IF NOT EXISTS live_equity_snapshots (
    ts                      BIGINT PRIMARY KEY,
    usdc_balance            DOUBLE PRECISION,
    open_cost_basis         DOUBLE PRECISION,
    open_market_value       DOUBLE PRECISION,
    total_equity            DOUBLE PRECISION,
    open_count              BIGINT,
    realized_pnl_cumulative DOUBLE PRECISION,
    unrealized_pnl          DOUBLE PRECISION,
    daily_change            DOUBLE PRECISION
)
"""


def _get_usdc() -> float | None:
    try:
        from py_clob_client_v2 import ClobClient, ApiCreds
        from py_clob_client_v2.constants import POLYGON
        from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
        pk      = os.environ["POLYMARKET_PRIVATE_KEY"]
        api_key = os.environ["POLYMARKET_API_KEY"]
        secret  = os.environ["POLYMARKET_API_SECRET"]
        api_pass = (os.environ.get("POLYMARKET_API_PASSPHRASE")
                    or os.environ.get("POLYMARKET_PASSPHRASE"))
        funder   = os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")
        sig_type = int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "1"))
        client = ClobClient(
            host="https://clob.polymarket.com", chain_id=POLYGON, key=pk,
            creds=ApiCreds(api_key=api_key, api_secret=secret, api_passphrase=api_pass),
            signature_type=sig_type, funder=funder or None,
        )
        bal = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        return int(bal.get("balance", 0)) / 1_000_000
    except Exception as e:
        print(f"  USDC lookup failed: {e}")
        return None


_clob_singleton = None


def _get_clob():
    """Lazy ClobClient singleton — has order-book fallback when /midpoint returns null."""
    global _clob_singleton
    if _clob_singleton is None:
        from trading_platform.polymarket.clob_client import ClobClient
        _clob_singleton = ClobClient()
    return _clob_singleton


def _get_mid(token_id: str) -> float | None:
    """Midpoint with fallbacks: /midpoint → order book → last-trade.

    Near-resolution markets often have one-sided books (no /midpoint), so
    the order-book and last-trade fallbacks matter — without them, mkt_price
    silently equals entry and unrealized P&L shows $0 on big winners.
    """
    try:
        mid = _get_clob().get_mid_price(token_id)
        if mid is not None:
            return float(mid)
    except Exception:
        pass
    # Last resort: last traded price.
    try:
        last = _get_clob().get_last_price(token_id)
        if last is not None:
            return float(last)
    except Exception:
        pass
    return None


def _get_conditional_balance(token_id: str) -> float | None:
    """Return on-chain conditional token balance (ground truth share count)."""
    try:
        from py_clob_client_v2 import ClobClient, ApiCreds
        from py_clob_client_v2.constants import POLYGON
        from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
        pk       = os.environ["POLYMARKET_PRIVATE_KEY"]
        api_key  = os.environ["POLYMARKET_API_KEY"]
        secret   = os.environ["POLYMARKET_API_SECRET"]
        api_pass = (os.environ.get("POLYMARKET_API_PASSPHRASE")
                    or os.environ.get("POLYMARKET_PASSPHRASE"))
        funder   = os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")
        sig_type = int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "1"))
        client = ClobClient(
            host="https://clob.polymarket.com", chain_id=POLYGON, key=pk,
            creds=ApiCreds(api_key=api_key, api_secret=secret, api_passphrase=api_pass),
            signature_type=sig_type, funder=funder or None,
        )
        result = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        )
        return int(result.get("balance", 0)) / 1_000_000
    except Exception:
        return None


def _position_value(direction: str, shares: float, fill_price: float,
                    token_id: str | None,
                    last_mark_yes: float | None = None) -> tuple[float, float]:
    """Return (cost_basis, market_value) for one open position.

    For SELL (long NO): cost = shares * NO_entry = shares * (1 - fill_price).
                        mkt  = shares * (1 - YES_mid).
    For BUY  (long YES): cost = shares * fill_price.
                         mkt  = shares * YES_mid.

    Mid fallback chain: CLOB /midpoint → last_price → last_mark_price (from
    position monitor's minute-by-minute updates) → entry. Without the
    last_mark fallback, near-resolution markets with one-sided books would
    silently revert to entry, hiding winning SELL positions whose YES has
    collapsed to ~0 (real value = shares × 0.999 but reported = shares ×
    NO_entry ≈ shares × 0.3). Caused the 5/15-5/18 $5.43 stuck-snapshot.
    """
    mid = _get_mid(token_id) if token_id else None
    if mid is None and last_mark_yes is not None:
        mid = float(last_mark_yes)
    if direction == "SELL":
        entry = max(1.0 - fill_price, 0.001)
        mkt_price = max(1.0 - mid, 0.001) if mid is not None else entry
    else:
        entry = max(fill_price, 0.001)
        mkt_price = mid if mid is not None else entry
    return shares * entry, shares * mkt_price


def take_snapshot() -> dict:
    now_ts = int(time.time())
    conn = get_connection()

    conn.execute(ENSURE_TABLE)
    conn.commit()

    # ── open positions tracked in DB ──────────────────────────────────────
    # GTC place_market_order doesn't echo fill_price (NULL in DB) and may
    # not update shares (stays 0). Both columns can be NULL/0 even for a
    # position that's filled and held on-chain. Use entry_price as price
    # fallback, and resolve actual shares from on-chain CONDITIONAL balance.
    open_rows = conn.execute("""
        SELECT direction, fill_price, entry_price, shares, token_id, size_usd,
               last_mark_price
        FROM live_trades
        WHERE dry_run=0 AND exit_ts IS NULL
          AND status NOT IN ('error','blocked','cancelled')
          AND token_id IS NOT NULL
          AND (fill_price IS NOT NULL OR entry_price IS NOT NULL)
    """).fetchall()

    open_cost = 0.0
    open_mkt  = 0.0
    counted = 0
    for direction, fp, ep, sh, token_id, size_usd, last_mark in open_rows:
        price = float(fp) if fp is not None else float(ep)
        # On-chain balance is ground truth; DB shares is stale for GTC fills.
        on_chain = _get_conditional_balance(token_id) if token_id else None
        if on_chain is not None and on_chain > 0:
            shares = on_chain
        elif sh is not None and float(sh) > 0:
            shares = float(sh)
        else:
            # Last resort: derive shares from intended allocation. Skips if
            # neither balance nor shares nor size_usd is meaningful.
            no_price = max(1.0 - price, 0.001) if direction == "SELL" else max(price, 0.001)
            shares = (float(size_usd or 0) / no_price) if size_usd else 0.0
            if shares <= 0:
                continue
        cost, mkt = _position_value(direction, shares, price, token_id,
                                    last_mark_yes=last_mark)
        open_cost += cost
        open_mkt  += mkt
        counted += 1

    # ── cumulative realized PnL ───────────────────────────────────────────
    row = conn.execute("""
        SELECT COALESCE(SUM(realized_pnl), 0)
        FROM live_trades
        WHERE dry_run=0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL
    """).fetchone()
    realized_cumul = float(row[0])

    # ── prior snapshot for daily_change ──────────────────────────────────
    prior = conn.execute("""
        SELECT total_equity FROM live_equity_snapshots
        WHERE ts < %s
        ORDER BY ts DESC LIMIT 1
    """, (now_ts,)).fetchone()

    usdc = _get_usdc()
    if usdc is None:
        print("  USDC on-chain lookup failed; skipping snapshot.")
        conn.close()
        return {}

    total_equity = usdc + open_mkt
    unrealized   = open_mkt - open_cost
    daily_change = (total_equity - float(prior[0])) if prior else None

    conn.execute("""
        INSERT INTO live_equity_snapshots
            (ts, usdc_balance, open_cost_basis, open_market_value,
             total_equity, open_count, realized_pnl_cumulative,
             unrealized_pnl, daily_change)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts) DO UPDATE
            SET usdc_balance            = EXCLUDED.usdc_balance,
                open_cost_basis         = EXCLUDED.open_cost_basis,
                open_market_value       = EXCLUDED.open_market_value,
                total_equity            = EXCLUDED.total_equity,
                open_count              = EXCLUDED.open_count,
                realized_pnl_cumulative = EXCLUDED.realized_pnl_cumulative,
                unrealized_pnl          = EXCLUDED.unrealized_pnl,
                daily_change            = EXCLUDED.daily_change
    """, (
        now_ts, round(usdc, 4), round(open_cost, 4), round(open_mkt, 4),
        round(total_equity, 4), counted,
        round(realized_cumul, 4), round(unrealized, 4),
        round(daily_change, 4) if daily_change is not None else None,
    ))
    conn.commit()
    conn.close()

    result = {
        "ts": now_ts,
        "usdc": round(usdc, 4),
        "open_cost": round(open_cost, 4),
        "open_mkt": round(open_mkt, 4),
        "total_equity": round(total_equity, 4),
        "open_count": counted,
        "realized_cumul": round(realized_cumul, 4),
        "unrealized": round(unrealized, 4),
        "daily_change": round(daily_change, 4) if daily_change is not None else None,
    }
    print(
        f"[live_equity] equity=${result['total_equity']:.2f} "
        f"(usdc=${result['usdc']:.2f} + tokens=${result['open_mkt']:.2f}) "
        f"realized={result['realized_cumul']:+.2f} "
        f"unrealized={result['unrealized']:+.2f} "
        + (f"24h_chg={result['daily_change']:+.2f}" if result['daily_change'] is not None else "")
    )
    return result


if __name__ == "__main__":
    take_snapshot()
