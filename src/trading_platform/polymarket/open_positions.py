"""
Compute open positions for smart money wallets.

Reads wallet fills from goldsky_resolved_fills, cross-references
against gamma_resolution.csv to find unresolved markets, and
computes net position per (wallet, token).

Usage::

    from trading_platform.polymarket.open_positions import compute_open_positions
    df = compute_open_positions()
    df.to_parquet("data/polymarket/wallet_open_positions.parquet")
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _batch_fetch_gamma_questions(token_ids: list[str]) -> dict[str, str]:
    """Fetch market questions from Gamma API one token at a time."""
    import requests
    import time as _time

    result: dict[str, str] = {}
    for i, tid in enumerate(token_ids):
        if (i + 1) % 50 == 0:
            logger.info("  Gamma question fetch %d/%d...", i + 1, len(token_ids))
        try:
            resp = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={"clob_token_ids": tid},
                timeout=8,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            if not data:
                continue
            mkt = data[0] if isinstance(data, list) else data
            question = mkt.get("question", "")
            if question:
                result[tid] = question
        except Exception:
            pass
        _time.sleep(0.2)

    return result


def _load_questions_from_prices_db() -> dict[str, str]:
    """Load yes_token_id -> question mapping from live prices.db."""
    db_path = PROJECT_ROOT / "data" / "polymarket" / "live" / "prices.db"
    if not db_path.exists():
        return {}
    try:
        import sqlite3
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        rows = conn.execute(
            "SELECT yes_token_id, question FROM markets WHERE yes_token_id IS NOT NULL AND question IS NOT NULL"
        ).fetchall()
        conn.close()
        return {r[0]: r[1] for r in rows if r[0] and r[1]}
    except Exception:
        return {}


def compute_open_positions(
    *,
    profiles_path: str | Path | None = None,
    fills_dir: str | Path | None = None,
    resolution_csv: str | Path | None = None,
    min_edge: float = 0.20,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """Compute net open positions for smart money wallets on unresolved markets.

    Returns DataFrame with columns: wallet, token_id, market_question,
    net_side, net_amount_usdc, fill_count, first_entry_ts, last_entry_ts, wallet_edge.
    """
    profiles_path = Path(profiles_path or PROJECT_ROOT / "data" / "polymarket" / "wallet_profiles.parquet")
    fills_dir = Path(fills_dir or PROJECT_ROOT / "data" / "polymarket" / "goldsky_resolved_fills")
    resolution_csv = Path(resolution_csv or PROJECT_ROOT / "data" / "polymarket" / "gamma_resolution.csv")

    if not profiles_path.exists():
        logger.warning("No wallet profiles: %s", profiles_path)
        return pd.DataFrame()

    t0 = time.time()

    # Step 1: Load smart money wallets
    profiles = pd.read_parquet(profiles_path)
    edge_col = "edge" if "edge" in profiles.columns else "early_win_rate"
    smart = profiles[profiles[edge_col] >= min_edge]
    smart_wallets = set(smart["wallet"].tolist())
    wallet_edges = dict(zip(smart["wallet"], smart[edge_col]))
    logger.info("Smart wallets with edge >= %.2f: %d", min_edge, len(smart_wallets))

    if not smart_wallets:
        return pd.DataFrame()

    # Step 2: Load resolution map (resolved tokens to exclude)
    resolved_tokens: set[str] = set()
    questions: dict[str, str] = {}
    if resolution_csv.exists():
        import csv
        with resolution_csv.open(newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                tid = row.get("ticker", "").strip()
                rp = row.get("resolution_price", "")
                q = row.get("question", "")
                if tid and q:
                    questions[tid] = q
                if tid and rp:
                    try:
                        rp_val = float(rp)
                        if rp_val == 0.0 or rp_val == 100.0:
                            resolved_tokens.add(tid)
                    except ValueError:
                        pass
    logger.info("Resolved tokens: %d, questions loaded: %d", len(resolved_tokens), len(questions))

    # Step 3: Load fills and filter to smart wallets
    if not fills_dir.exists():
        return pd.DataFrame()

    all_dfs: list[pd.DataFrame] = []
    files = list(fills_dir.glob("*.parquet"))
    for i, path in enumerate(files):
        if path.name == "combined.parquet":
            continue
        try:
            df = pd.read_parquet(path)
            if not df.empty:
                # Pre-filter to smart wallets for memory efficiency
                wallet_col = "maker_wallet" if "maker_wallet" in df.columns else df.columns[0]
                smart_fills = df[df[wallet_col].isin(smart_wallets)]
                if not smart_fills.empty:
                    all_dfs.append(smart_fills)
        except Exception:
            continue

    if not all_dfs:
        logger.info("No fills found for smart wallets")
        return pd.DataFrame()

    fills = pd.concat(all_dfs, ignore_index=True)
    logger.info("Smart wallet fills: %d", len(fills))

    # Step 4: Determine direction and token for each fill
    fills["token"] = fills["taker_asset_id"].astype(str).str.strip()
    fills["wallet"] = fills["maker_wallet"].astype(str).str.strip()
    fills["maker_asset"] = fills["maker_asset_id"].astype(str).str.strip()
    fills["amount"] = pd.to_numeric(fills["maker_amount"], errors="coerce").fillna(0)
    fills["ts"] = pd.to_datetime(fills["timestamp"], utc=True, errors="coerce")

    # YES = buying tokens (maker_asset == "0"), NO = selling
    fills["side_sign"] = fills["maker_asset"].apply(lambda x: 1.0 if x == "0" else -1.0)
    fills["signed_amount"] = fills["amount"] * fills["side_sign"]

    # Step 5: Filter to UNRESOLVED tokens only
    fills = fills[~fills["token"].isin(resolved_tokens)]
    logger.info("Fills on unresolved tokens: %d", len(fills))

    if fills.empty:
        return pd.DataFrame()

    # Step 6: Aggregate per (wallet, token)
    grouped = fills.groupby(["wallet", "token"]).agg(
        net_amount=("signed_amount", "sum"),
        buy_volume=("amount", lambda x: x[fills.loc[x.index, "side_sign"] > 0].sum()),
        fill_count=("amount", "count"),
        first_entry_ts=("ts", "min"),
        last_entry_ts=("ts", "max"),
    ).reset_index()

    # Net side
    grouped["net_side"] = grouped["net_amount"].apply(lambda x: "YES" if x > 0 else "NO")
    grouped["net_amount_usdc"] = grouped["net_amount"].abs()

    # Filter out tiny positions
    grouped = grouped[grouped["net_amount_usdc"] >= 10.0]

    # Add question from gamma CSV, then fill gaps from prices.db
    grouped["market_question"] = grouped["token"].map(questions).fillna("")

    # Fill missing questions from prices.db (has token_id -> question mapping)
    missing = grouped["market_question"] == ""
    if missing.any():
        db_questions = _load_questions_from_prices_db()
        if db_questions:
            grouped.loc[missing, "market_question"] = (
                grouped.loc[missing, "token"].map(db_questions).fillna("")
            )
        still_missing = (grouped["market_question"] == "").sum()
        logger.info("Questions: %d from gamma, %d from prices.db, %d still missing",
                     (~missing).sum(), missing.sum() - still_missing, still_missing)

    # Fill remaining from Gamma API (batch fetch, top positions only)
    still_blank = grouped["market_question"].eq("")
    if still_blank.any():
        blank_tokens = grouped.loc[still_blank, "token"].unique()
        # Only fetch for top positions by volume to limit API calls
        top_blank = grouped.loc[still_blank].nlargest(200, "net_amount_usdc")["token"].unique()
        gamma_q = _batch_fetch_gamma_questions(list(top_blank))
        if gamma_q:
            grouped.loc[still_blank, "market_question"] = (
                grouped.loc[still_blank, "token"].map(gamma_q).fillna("")
            )
            filled = (grouped.loc[still_blank, "market_question"] != "").sum()
            logger.info("Questions: %d filled from Gamma API (of %d attempted)", filled, len(top_blank))

    grouped["wallet_edge"] = grouped["wallet"].map(wallet_edges).fillna(0)

    # Select output columns
    result = grouped[[
        "wallet", "token", "market_question", "net_side",
        "net_amount_usdc", "fill_count", "first_entry_ts", "last_entry_ts",
        "wallet_edge",
    ]].rename(columns={"token": "token_id"})

    result = result.sort_values(["wallet_edge", "net_amount_usdc"], ascending=[False, False])

    elapsed = time.time() - t0
    logger.info("Open positions: %d across %d wallets in %.1fs",
                 len(result), result["wallet"].nunique(), elapsed)

    # Save if output path given
    if output_path:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        result.to_parquet(out, index=False)
        logger.info("Saved to %s", out)

    return result
