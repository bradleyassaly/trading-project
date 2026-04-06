"""
Backfill historical Goldsky fills for resolved Polymarket markets.

Fetches on-chain fill data from the Goldsky orderbook subgraph for
markets that have already resolved, enabling immediate wallet profiling.

Supports balanced token selection to avoid YES/NO bias in the backfill.

Usage::

    from trading_platform.polymarket.goldsky_backfill import backfill_goldsky_fills
    result = backfill_goldsky_fills(limit=100, min_volume=10000)
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def backfill_goldsky_fills(
    *,
    resolution_csv: str | Path | None = None,
    output_dir: str | Path | None = None,
    limit: int = 100,
    min_volume: float = 10000.0,
    strategy: str = "balanced",
    skip_existing: bool = False,
) -> dict[str, Any]:
    from trading_platform.polymarket.goldsky_client import GoldskyClient

    resolution_csv = Path(resolution_csv or PROJECT_ROOT / "data" / "polymarket" / "gamma_resolution.csv")
    output_dir = Path(output_dir or PROJECT_ROOT / "data" / "polymarket" / "goldsky_resolved_fills")
    output_dir.mkdir(parents=True, exist_ok=True)

    if not resolution_csv.exists():
        print(f"[ERROR] Resolution CSV not found: {resolution_csv}")
        return {"markets": 0, "fills": 0}

    gamma = pd.read_csv(resolution_csv)
    resolved = gamma[gamma["resolution_price"].isin([0.0, 100.0])].copy()
    resolved["volume"] = pd.to_numeric(resolved["volume"], errors="coerce").fillna(0)
    resolved = resolved[resolved["volume"] >= min_volume]

    if strategy == "balanced":
        selected = _select_balanced(resolved, limit)
    else:
        selected = resolved.nlargest(limit, "volume")

    # Report token selection balance
    yes_count = int((selected["resolution_price"] >= 99).sum())
    no_count = int((selected["resolution_price"] <= 1).sum())
    total_selected = yes_count + no_count
    base_rate = yes_count / total_selected * 100 if total_selected > 0 else 0

    print(f"Backfilling Goldsky fills ({strategy} strategy)")
    print(f"  Token selection: {yes_count} YES tokens, {no_count} NO tokens ({base_rate:.1f}% base rate)")
    if base_rate < 40 or base_rate > 60:
        print(f"  WARNING: Base rate {base_rate:.1f}% is outside 40-60% target range")
    print(f"  Total markets: {len(selected)} (min_vol={min_volume:,.0f})")
    print()

    # Check existing files for skip-existing mode
    existing_tokens: set[str] = set()
    if skip_existing:
        for p in output_dir.glob("*.parquet"):
            try:
                df = pd.read_parquet(p, columns=["taker_asset_id"])
                if not df.empty:
                    existing_tokens.update(df["taker_asset_id"].astype(str).str.strip().unique())
            except Exception:
                continue
        print(f"  Existing fills: {len(existing_tokens)} tokens (will skip)")

    client = GoldskyClient()
    total_fills = 0
    markets_done = 0
    skipped = 0

    for i, (_, row) in enumerate(selected.iterrows()):
        token_id = str(row["ticker"])
        question = str(row.get("question", ""))[:50]
        rp = row["resolution_price"]
        outcome = "YES" if rp >= 99 else "NO"

        if skip_existing and token_id in existing_tokens:
            skipped += 1
            continue

        if (i + 1) % 10 == 0 or i == 0:
            print(f"  [{i+1}/{len(selected)}] ({outcome}) {question}")

        # Save with full token ID as filename
        out_path = output_dir / f"{token_id[:32]}.parquet"
        if out_path.exists():
            try:
                existing = pd.read_parquet(out_path)
                if len(existing) > 0:
                    total_fills += len(existing)
                    markets_done += 1
                    continue
            except Exception:
                pass

        try:
            fills = client.fetch_fills(token_id, since_hours=8760)
        except Exception as exc:
            logger.debug("Fill fetch failed for %s: %s", token_id[:16], exc)
            continue

        if fills.empty:
            continue

        fills.to_parquet(out_path, index=False)
        total_fills += len(fills)
        markets_done += 1

        if (i + 1) % 10 == 0 or i == 0:
            print(f"    -> {len(fills)} fills")

        # Rate limit courtesy
        time.sleep(0.5)

    print(f"\n[DONE] Backfill complete.")
    print(f"  Markets with fills: {markets_done}")
    print(f"  Skipped (existing): {skipped}")
    print(f"  Total fills: {total_fills:,}")

    return {"markets": markets_done, "fills": total_fills}


def _select_balanced(resolved: pd.DataFrame, limit: int) -> pd.DataFrame:
    """Select equal numbers of YES and NO tokens, sorted by volume."""
    yes_tokens = resolved[resolved["resolution_price"] >= 99].copy()
    no_tokens = resolved[resolved["resolution_price"] <= 1].copy()

    # Sort each by volume descending
    yes_tokens = yes_tokens.sort_values("volume", ascending=False)
    no_tokens = no_tokens.sort_values("volume", ascending=False)

    # Take equal halves
    half = limit // 2
    yes_selected = yes_tokens.head(half)
    no_selected = no_tokens.head(half)

    # If one side has fewer than half, fill from the other
    yes_actual = len(yes_selected)
    no_actual = len(no_selected)
    if yes_actual < half:
        extra = half - yes_actual
        no_selected = no_tokens.head(no_actual + extra)
    elif no_actual < half:
        extra = half - no_actual
        yes_selected = yes_tokens.head(yes_actual + extra)

    combined = pd.concat([yes_selected, no_selected], ignore_index=True)
    return combined
