"""
pmxt smoke test.

Confirms the four data types we need from the pmxt library are working:
markets, OHLCV candles, order book, and trades. Run this once after
``pip install pmxt`` to make sure your environment can talk to the
sidecar before relying on it from the trading platform.

Usage::

    python -m trading_platform.polymarket.pmxt_smoke_test
"""
from __future__ import annotations

import sys
import time
from typing import Any


def _safe_call(label: str, fn) -> tuple[bool, Any]:
    print(f"  {label}...", end=" ", flush=True)
    t0 = time.time()
    try:
        result = fn()
        elapsed = time.time() - t0
        print(f"OK ({elapsed:.1f}s)")
        return True, result
    except Exception as exc:
        elapsed = time.time() - t0
        print(f"FAIL ({elapsed:.1f}s): {type(exc).__name__}: {str(exc)[:120]}")
        return False, None


def main() -> int:
    print("pmxt smoke test starting...")
    print()

    try:
        import pmxt
    except ImportError:
        print("\u274c pmxt is not installed. Run: pip install pmxt")
        return 1

    print(f"pmxt version: {pmxt.__version__}")
    print()

    try:
        ex = pmxt.Polymarket()
    except Exception as exc:
        print(f"\u274c pmxt.Polymarket() failed: {exc}")
        return 1

    failures: list[str] = []

    # 1. Markets — search for "trump"
    ok, markets = _safe_call(
        "fetch_market(slug='presidential-election-winner-2024')",
        lambda: ex.fetch_market(slug="presidential-election-winner-2024"),
    )
    if not ok or markets is None:
        failures.append("markets")
        market = None
    else:
        market = markets
        print(f"     question: {market.question[:60]}")
        print(f"     market_id: {market.market_id}")
        if market.outcomes:
            for o in market.outcomes[:3]:
                print(f"     outcome: {o.label} price={o.price} id={o.outcome_id[:20]}...")

    if market is None or not market.outcomes:
        print()
        print("\u274c Could not fetch test market — aborting downstream checks.")
        return 1

    yes_outcome = market.outcomes[0]
    outcome_id = yes_outcome.outcome_id

    # 2. OHLCV
    ok, candles = _safe_call(
        f"fetch_ohlcv(outcome_id, resolution='1h', limit=24)",
        lambda: ex.fetch_ohlcv(outcome_id, resolution="1h", limit=24),
    )
    if not ok:
        failures.append("ohlcv")
    elif candles:
        print(f"     {len(candles)} candles")
        c = candles[-1]
        print(
            f"     latest: t={getattr(c, 'timestamp', None)} "
            f"o={getattr(c, 'open', None)} h={getattr(c, 'high', None)} "
            f"l={getattr(c, 'low', None)} c={getattr(c, 'close', None)} "
            f"v={getattr(c, 'volume', None)}"
        )

    # 3. Order book
    ok, book = _safe_call(
        f"fetch_order_book(outcome_id)",
        lambda: ex.fetch_order_book(outcome_id),
    )
    if not ok:
        failures.append("order_book")
    elif book:
        bids = getattr(book, "bids", []) or []
        asks = getattr(book, "asks", []) or []
        print(f"     {len(bids)} bids, {len(asks)} asks")
        if bids:
            b = bids[0]
            print(f"     best bid: price={getattr(b, 'price', None)} size={getattr(b, 'size', None)}")
        if asks:
            a = asks[0]
            print(f"     best ask: price={getattr(a, 'price', None)} size={getattr(a, 'size', None)}")

    # 4. Trades
    ok, trades = _safe_call(
        f"fetch_trades(outcome_id)",
        lambda: ex.fetch_trades(outcome_id),
    )
    if not ok:
        failures.append("trades")
    elif trades:
        print(f"     {len(trades)} trades returned")
        for t in trades[:3]:
            print(
                f"     {getattr(t, 'side', None)} "
                f"size={getattr(t, 'amount', None)} "
                f"price={getattr(t, 'price', None)} "
                f"ts={getattr(t, 'timestamp', None)}"
            )

    print()
    if failures:
        print(f"\u274c pmxt smoke test failed — {', '.join(failures)} did not return data.")
        print("   Some pmxt endpoints require authentication.")
        print("   The trading platform will fall back to its existing data sources.")
        return 1
    print("\u2705 pmxt smoke test passed — markets, OHLCV, order book, trades all working")
    return 0


if __name__ == "__main__":
    sys.exit(main())
