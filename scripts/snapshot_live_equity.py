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
    daily_change            DOUBLE PRECISION,
    deployed_pct            DOUBLE PRECISION,
    idle_usdc               DOUBLE PRECISION,
    unredeemed_value        DOUBLE PRECISION
)
"""

# N2 capital-efficiency columns — idempotent ALTERs for the already-existing
# prod table (CREATE TABLE IF NOT EXISTS won't add them).
_CAPITAL_MIGRATIONS = [
    "ALTER TABLE live_equity_snapshots ADD COLUMN IF NOT EXISTS deployed_pct DOUBLE PRECISION",
    "ALTER TABLE live_equity_snapshots ADD COLUMN IF NOT EXISTS idle_usdc DOUBLE PRECISION",
    "ALTER TABLE live_equity_snapshots ADD COLUMN IF NOT EXISTS unredeemed_value DOUBLE PRECISION",
]


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


_resolver_singleton = None


def _get_resolution_payout(token_id: str) -> float | None:
    """This token's resolved payout (1.0 / 0.0) from gamma_resolution.csv,
    or None if the market hasn't resolved (or isn't in the file)."""
    global _resolver_singleton
    try:
        if _resolver_singleton is None:
            from pathlib import Path
            from trading_platform.polymarket.resolution_resolver import ResolutionResolver
            _csv = Path(__file__).resolve().parents[1] / "data" / "polymarket" / "gamma_resolution.csv"
            _resolver_singleton = ResolutionResolver(str(_csv))
        price = _resolver_singleton.resolve(token_id)
        if price is None:
            return None
        if price >= 99.0:
            return 1.0
        if price <= 1.0:
            return 0.0
    except Exception:
        pass
    return None


def _get_book_prices(token_id: str) -> tuple[float | None, float | None]:
    """(best_bid, best_ask) of the QUERIED token's own book.

    2026-07-08: post-migration the CLOB quotes each token in ITS OWN space
    (probed: a NO token worth $0.049 quotes bid 0.035/ask 0.063). The old
    'YES-space for all token queries' behavior is gone — no flips here."""
    try:
        book = _get_clob().get_order_book(token_id)
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        bid = float(bids[0]["price"]) if bids else None
        ask = float(asks[0]["price"]) if asks else None
        return bid, ask
    except Exception:
        return None, None


def _fetch_dataapi_positions() -> dict[str, float]:
    """{asset(token_id): curPrice} from the data-api positions endpoint —
    Polymarket's OWN per-position valuation (what the site displays),
    held-token space by definition. The truth-first valuation source."""
    try:
        import requests
        wallet = (os.environ.get("POLYMARKET_FUNDER_ADDRESS")
                  or os.environ.get("POLYMARKET_WALLET_ADDRESS"))
        if not wallet:
            return {}
        r = requests.get("https://data-api.polymarket.com/positions",
                         params={"user": wallet, "limit": 300}, timeout=15)
        if not r.ok:
            return {}
        out = {}
        for p in r.json():
            asset = str(p.get("asset") or "")
            cp = p.get("curPrice")
            if asset and cp is not None:
                out[asset] = float(cp)
        return out
    except Exception:
        return {}


def _position_value(direction: str, shares: float, fill_price: float,
                    token_id: str | None,
                    last_mark_yes: float | None = None,
                    dataapi_price: float | None = None) -> tuple[float, float]:
    """Return (cost_basis, market_value) for one open position.

    2026-07-08 rework: the CLOB /book and /midpoint now quote each token in
    ITS OWN space (empirically probed after the exchange migration). The
    old `1 - x` flips on SELL therefore INVERTED values — 5-cent NO tokens
    valued at ~95 cents — and the snapshot ran ~$19 above the Polymarket
    site ($312.50 vs the site's $293). Valuation ladder:
      0. data-api curPrice — Polymarket's OWN per-position price (what the
         site shows). Truth by construction, held-token space, no flips.
      1. Resolved payout (gamma_resolution.csv) — exact, covers dead books
      2. Best BID of the held token's own book — the price a liquidation
         hits, same expression for both directions (no flips)
      3. /midpoint (held space) → last_mark_price (monitor's YES-space
         contract — flip for SELL) → entry (floor; the 5/15-5/18 $5.43
         stuck-snapshot is why entry stays the floor)
    """
    entry = max(1.0 - fill_price, 0.001) if direction == "SELL" \
        else max(fill_price, 0.001)

    if dataapi_price is not None:
        return shares * entry, shares * max(float(dataapi_price), 0.0)

    mkt_price: float | None = None
    if token_id:
        payout = _get_resolution_payout(token_id)
        if payout is not None:
            # CSV rows are per-token: payout is already in the held
            # token's own space for both directions.
            return shares * entry, shares * payout
        held_bid, _held_ask = _get_book_prices(token_id)
        if held_bid is not None:
            mkt_price = held_bid

    if mkt_price is None:
        mid = _get_mid(token_id) if token_id else None
        if mid is not None:
            mkt_price = mid  # held-token space — no flip
        elif last_mark_yes is not None:
            # the monitor writes YES-space marks; flip for SELL (held NO)
            m = float(last_mark_yes)
            mkt_price = max(1.0 - m, 0.001) if direction == "SELL" else m
        else:
            mkt_price = entry
    return shares * entry, shares * mkt_price


def take_snapshot() -> dict:
    now_ts = int(time.time())
    conn = get_connection()

    conn.execute(ENSURE_TABLE)
    for _mig in _CAPITAL_MIGRATIONS:
        try:
            conn.execute(_mig)
        except Exception:
            pass
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
    unredeemed_value = 0.0  # N2: resolved-but-not-yet-swept payout value
    counted = 0
    # 2026-07-08: one truth fetch per snapshot — Polymarket's own per-asset
    # curPrice (the number the site shows). Aligns equity with the site by
    # construction instead of reconstructing it from books/mids.
    _truth_prices = _fetch_dataapi_positions()
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
                                    last_mark_yes=last_mark,
                                    dataapi_price=_truth_prices.get(str(token_id)))
        open_cost += cost
        open_mkt  += mkt
        counted += 1
        # N2: this position's market has RESOLVED (payout known) but we still
        # hold on-chain tokens — capital we're owed but haven't redeemed to
        # USDC. Payout is per-token; losers contribute 0.
        if token_id and on_chain is not None and on_chain > 0:
            _payout = _get_resolution_payout(token_id)
            if _payout is not None:
                unredeemed_value += shares * _payout

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
    # N2 capital efficiency: idle USDC is the free collateral; deployed_pct is
    # the fraction of equity in open token market value.
    idle_usdc = usdc
    deployed_pct = (open_mkt / total_equity) if total_equity > 0 else 0.0

    conn.execute("""
        INSERT INTO live_equity_snapshots
            (ts, usdc_balance, open_cost_basis, open_market_value,
             total_equity, open_count, realized_pnl_cumulative,
             unrealized_pnl, daily_change,
             deployed_pct, idle_usdc, unredeemed_value)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts) DO UPDATE
            SET usdc_balance            = EXCLUDED.usdc_balance,
                open_cost_basis         = EXCLUDED.open_cost_basis,
                open_market_value       = EXCLUDED.open_market_value,
                total_equity            = EXCLUDED.total_equity,
                open_count              = EXCLUDED.open_count,
                realized_pnl_cumulative = EXCLUDED.realized_pnl_cumulative,
                unrealized_pnl          = EXCLUDED.unrealized_pnl,
                daily_change            = EXCLUDED.daily_change,
                deployed_pct            = EXCLUDED.deployed_pct,
                idle_usdc               = EXCLUDED.idle_usdc,
                unredeemed_value        = EXCLUDED.unredeemed_value
    """, (
        now_ts, round(usdc, 4), round(open_cost, 4), round(open_mkt, 4),
        round(total_equity, 4), counted,
        round(realized_cumul, 4), round(unrealized, 4),
        round(daily_change, 4) if daily_change is not None else None,
        round(deployed_pct, 4), round(idle_usdc, 4), round(unredeemed_value, 4),
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
        "deployed_pct": round(deployed_pct, 4),
        "idle_usdc": round(idle_usdc, 4),
        "unredeemed_value": round(unredeemed_value, 4),
    }
    print(
        f"[live_equity] equity=${result['total_equity']:.2f} "
        f"(usdc=${result['usdc']:.2f} + tokens=${result['open_mkt']:.2f}) "
        f"deployed={result['deployed_pct']:.0%} unredeemed=${result['unredeemed_value']:.2f} "
        f"realized={result['realized_cumul']:+.2f} "
        f"unrealized={result['unrealized']:+.2f} "
        + (f"chg_vs_prior={result['daily_change']:+.2f}" if result['daily_change'] is not None else "")
    )
    return result


if __name__ == "__main__":
    take_snapshot()
