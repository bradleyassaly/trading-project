"""
Wallet profiler using Goldsky on-chain fill data.

Wallet-centric approach: loads ALL fill files at once, resolves
outcomes via gamma_resolution.csv, then deduplicates to one outcome
per unique (wallet, token) pair using the earliest fill for timing
and price. This prevents fill-count inflation.

Includes domain classification, confidence scoring, and recency analysis.
"""
from __future__ import annotations

import logging
import math
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.polymarket.resolution_resolver import ResolutionResolver, normalize_token_id

logger = logging.getLogger(__name__)

# ── Minimum thresholds ───────────────────────────────────────────────────────

MIN_VOLUME_USDC = 500.0
MIN_RESOLVED_TRADES = 5
MIN_UNCERTAIN_EARLY_FOR_OUTPUT = 3
MIN_UNCERTAIN_EARLY_INFORMED = 10
MIN_EARLY_WIN_RATE_INFORMED = 0.70
MIN_EDGE_INFORMED = 0.15
MIN_VOLUME_INFORMED = 5000.0
MIN_CONFIDENCE_SCORE = 0.70
MIN_UNIQUE_RESOLVED_MARKETS = 5
MIN_LOSING_TOKENS = 1  # Must have at least 1 loss to rule out incomplete data


def classify_market(question: str) -> str:
    """Classify a market question into a domain."""
    q = question.lower()
    if any(w in q for w in ["fed", "cpi", "gdp", "pce", "jobs",
                             "inflation", "recession", "rate", "fomc", "economy"]):
        return "economics"
    if any(w in q for w in ["iran", "russia", "ukraine", "israel",
                             "war", "conflict", "military", "nato", "nuclear"]):
        return "geopolitics"
    if any(w in q for w in ["trump", "biden", "election", "congress",
                             "senate", "president", "democrat", "republican", "vote"]):
        return "politics"
    if any(w in q for w in ["bitcoin", "btc", "eth", "crypto",
                             "ethereum", "solana"]):
        return "crypto"
    if any(w in q for w in ["ufc", "nba", "nfl", "nhl", "mlb",
                             "soccer", "championship", "fight", "match", "game",
                             " fc ", "premier league", "la liga", "serie a",
                             "bundesliga", "mls ", "world cup", "champions league",
                             "blackhawks", "lakers", "celtics", "warriors",
                             "manchester", "chelsea", "arsenal", "liverpool",
                             "barcelona", "real madrid", "bayern",
                             "victory fc", "united fc"]):
        return "sports"
    return "other"


class GoldskyWalletProfiler:
    """Build wallet profiles from Goldsky fill parquets."""

    def __init__(
        self,
        fills_dir: str | Path,
        resolution_csv: str | Path,
        close_time_db_path: str | Path | None = None,
        *,
        early_hours_threshold: float = 24.0,
    ) -> None:
        self._fills_dir = Path(fills_dir)
        self._resolver = ResolutionResolver(resolution_csv)
        self._early_hours = early_hours_threshold

        from trading_platform.polymarket.market_close_times import MarketCloseTimeMap
        gamma_csv = Path(resolution_csv)
        db_path = Path(close_time_db_path) if close_time_db_path else Path("data/polymarket/live/prices.db")
        self._close_times = MarketCloseTimeMap.from_all_sources(
            gamma_csv=gamma_csv if gamma_csv.exists() else None,
            live_db=db_path if db_path.exists() else None,
        )

        # Load question text for domain classification
        self._questions: dict[str, str] = {}
        import csv
        if gamma_csv.exists():
            try:
                with gamma_csv.open(newline="", encoding="utf-8-sig") as f:
                    for row in csv.DictReader(f):
                        tid = row.get("ticker", "").strip()
                        q = row.get("question", "")
                        if tid and q:
                            self._questions[tid] = q
            except Exception:
                pass

        # Build resolution lookup
        self._rp_map: dict[str, float] = {}
        if gamma_csv.exists():
            try:
                with gamma_csv.open(newline="", encoding="utf-8-sig") as f:
                    for row in csv.DictReader(f):
                        tid = row.get("ticker", "").strip()
                        rp = row.get("resolution_price", "")
                        if tid and rp:
                            self._rp_map[tid] = float(rp)
            except Exception:
                pass

    def build_profiles(self, *, min_resolved: int = MIN_RESOLVED_TRADES) -> pd.DataFrame:
        if not self._fills_dir.exists():
            return pd.DataFrame()

        # Step 1: Load ALL fill files
        all_files = list(self._fills_dir.glob("*.parquet"))
        if not all_files:
            return pd.DataFrame()

        t0 = time.time()
        all_dfs: list[pd.DataFrame] = []
        for i, path in enumerate(all_files):
            if (i + 1) % 100 == 0 or i == 0:
                logger.info("Loading fills... %d/%d files", i + 1, len(all_files))
            try:
                df = pd.read_parquet(path)
                if not df.empty:
                    all_dfs.append(df)
            except Exception:
                continue

        if not all_dfs:
            return pd.DataFrame()

        fills = pd.concat(all_dfs, ignore_index=True)
        total_fills = len(fills)
        t_load = time.time() - t0
        logger.info("Loaded %d fills from %d files in %.1fs", total_fills, len(all_files), t_load)

        # Step 2: Filter to purchases (maker_asset_id == "0")
        if "maker_asset_id" in fills.columns:
            purchases = fills[fills["maker_asset_id"].astype(str) == "0"].copy()
        else:
            purchases = fills.copy()
        logger.info("Token purchases: %d", len(purchases))

        # Step 3: Resolve outcomes
        t1 = time.time()
        purchases["token"] = purchases["taker_asset_id"].astype(str).str.strip()
        purchases["wallet"] = purchases["maker_wallet"].astype(str).str.strip()
        purchases["rp"] = purchases["token"].map(pd.Series(self._rp_map))

        resolved = purchases.dropna(subset=["rp"]).copy()
        resolved["is_win"] = resolved["rp"] >= 99.0
        resolved["maker_amount"] = pd.to_numeric(resolved["maker_amount"], errors="coerce").fillna(0)
        resolved["taker_amount"] = pd.to_numeric(resolved["taker_amount"], errors="coerce").fillna(0)
        resolved["entry_price"] = resolved["maker_amount"] / resolved["taker_amount"].replace(0, float("nan"))
        resolved["entry_price"] = resolved["entry_price"].fillna(0)

        logger.info("Resolved fills: %d (%.1f%% of purchases)", len(resolved),
                     100 * len(resolved) / len(purchases) if len(purchases) > 0 else 0)

        # Step 4: Domain classification
        q_series = pd.Series(self._questions, name="question")
        resolved["question"] = resolved["token"].map(q_series).fillna("")
        resolved["domain"] = resolved["question"].apply(classify_market)

        # Step 5: Close time + timestamps
        close_time_map = {}
        if self._close_times:
            for token in resolved["token"].unique():
                ct = self._close_times.get(token)
                if ct:
                    close_time_map[token] = ct
        resolved["close_dt"] = resolved["token"].map(pd.Series(close_time_map))
        resolved["fill_dt"] = pd.to_datetime(resolved["timestamp"], utc=True, errors="coerce")

        # Compute hours_before_close
        mask_times = resolved["close_dt"].notna() & resolved["fill_dt"].notna()
        resolved["hours_before"] = float("nan")
        if mask_times.any():
            close_dts = pd.to_datetime(resolved.loc[mask_times, "close_dt"], utc=True, errors="coerce")
            resolved.loc[mask_times, "hours_before"] = (
                (close_dts - resolved.loc[mask_times, "fill_dt"]).dt.total_seconds() / 3600.0
            )

        t_resolve = time.time() - t1
        logger.info("Resolution + timestamps in %.1fs", t_resolve)

        # ─────────────────────────────────────────────────────────────────────
        # Step 6: Deduplicate to ONE ROW per (wallet, token) using earliest fill
        # This is the critical fix: count wins per unique market, not per fill.
        # ─────────────────────────────────────────────────────────────────────
        t2 = time.time()

        # Sort by fill_dt ascending so .first() picks the earliest fill
        resolved = resolved.sort_values("fill_dt", ascending=True)

        # Per (wallet, token): use earliest fill for price/timing, aggregate volume
        wt_group = resolved.groupby(["wallet", "token"])
        token_level = wt_group.agg(
            is_win=("is_win", "first"),           # same for all fills on same token
            earliest_price=("entry_price", "first"),
            earliest_hours=("hours_before", "first"),
            earliest_dt=("fill_dt", "first"),
            total_volume=("maker_amount", "sum"),
            fill_count=("maker_amount", "count"),
            domain=("domain", "first"),
            question=("question", "first"),
        ).reset_index()

        # Recompute early/uncertain flags on earliest fill per market
        token_level["is_early"] = token_level["earliest_hours"] > self._early_hours
        token_level["is_uncertain_early"] = (
            token_level["is_early"]
            & (token_level["earliest_price"] >= 0.15)
            & (token_level["earliest_price"] <= 0.85)
        )

        # Recency on earliest fill
        now = datetime.now(tz=timezone.utc)
        thirty_days_ago = pd.Timestamp(now - timedelta(days=30))
        ninety_days_ago = pd.Timestamp(now - timedelta(days=90))
        token_level["is_30d"] = token_level["earliest_dt"] >= thirty_days_ago
        token_level["is_90d"] = token_level["earliest_dt"] >= ninety_days_ago

        total_token_outcomes = len(token_level)
        avg_fills_per_token = token_level["fill_count"].mean()
        logger.info("Deduplicated: %d unique (wallet, token) pairs, avg %.1f fills/token",
                     total_token_outcomes, avg_fills_per_token)

        # Step 7: Aggregate per wallet from token-level data
        wg = token_level.groupby("wallet")

        stats = wg.agg(
            resolved_tokens=("is_win", "count"),
            token_wins=("is_win", "sum"),
            total_volume=("total_volume", "sum"),
            total_fills=("fill_count", "sum"),
        )

        # Unique win/lose token counts (already at token level)
        win_tokens = token_level[token_level["is_win"]].groupby("wallet")["token"].nunique().rename("win_tokens")
        lose_tokens = token_level[~token_level["is_win"]].groupby("wallet")["token"].nunique().rename("lose_tokens")

        # Early token counts (per unique market)
        early_data = token_level[token_level["is_early"]]
        early_agg = early_data.groupby("wallet").agg(
            et=("is_win", "count"),
            ew=("is_win", "sum"),
        )

        uet_data = token_level[token_level["is_uncertain_early"]]
        uet_agg = uet_data.groupby("wallet").agg(
            uet=("is_win", "count"),
            uew=("is_win", "sum"),
        )

        # 30d/90d token-level
        d30 = token_level[token_level["is_30d"]].groupby("wallet").agg(
            trades_30d=("is_win", "count"), wins_30d=("is_win", "sum")
        )
        d90 = token_level[token_level["is_90d"]].groupby("wallet").agg(
            trades_90d=("is_win", "count"), wins_90d=("is_win", "sum")
        )

        # Total unique tokens from purchases (including unresolved)
        markets_traded = purchases.groupby("wallet")["token"].nunique().rename("markets_traded")

        stats = stats.join(win_tokens, how="left")
        stats = stats.join(lose_tokens, how="left")
        stats = stats.join(early_agg, how="left")
        stats = stats.join(uet_agg, how="left")
        stats = stats.join(d30, how="left")
        stats = stats.join(d90, how="left")
        stats = stats.join(markets_traded, how="left")
        stats = stats.fillna(0)

        t_agg = time.time() - t2
        logger.info("Token-level aggregation in %.1fs, %d wallets", t_agg, len(stats))

        # Step 8: Filter
        qualified = stats[
            (stats["total_volume"] >= MIN_VOLUME_USDC)
            & (stats["resolved_tokens"] >= min_resolved)
            & ((stats["win_tokens"] + stats["lose_tokens"]) >= MIN_UNIQUE_RESOLVED_MARKETS)
            & (stats["lose_tokens"] >= MIN_LOSING_TOKENS)
            & (stats["uet"] >= MIN_UNCERTAIN_EARLY_FOR_OUTPUT)
        ]

        filtered_volume = int((stats["total_volume"] < MIN_VOLUME_USDC).sum())
        filtered_incomplete = int(
            ((stats["win_tokens"] + stats["lose_tokens"]) < MIN_UNIQUE_RESOLVED_MARKETS).sum()
            + (stats["lose_tokens"] < MIN_LOSING_TOKENS).sum()
        )
        filtered_sample = int((stats["uet"] < MIN_UNCERTAIN_EARLY_FOR_OUTPUT).sum())

        logger.info("Qualified wallets: %d (filtered: %d vol, %d incomplete, %d sample)",
                     len(qualified), filtered_volume, filtered_incomplete, filtered_sample)

        # Step 9: Build profile dicts for qualified wallets
        profiles: list[dict[str, Any]] = []
        qualified_wallets = set(qualified.index)

        # Domain stats from token-level (per unique market, not per fill)
        qual_tokens = token_level[token_level["wallet"].isin(qualified_wallets)]
        domain_agg = qual_tokens.groupby(["wallet", "domain"]).agg(
            d_trades=("is_win", "count"),
            d_wins=("is_win", "sum"),
        ).reset_index()

        # Baseline: average earliest entry price on winning tokens
        win_prices = qual_tokens[qual_tokens["is_win"]].groupby("wallet")["earliest_price"].apply(list).to_dict()

        # Average hours before close per wallet
        hours_data = qual_tokens[qual_tokens["earliest_hours"].notna() & (qual_tokens["earliest_hours"] > 0)]
        avg_hours = hours_data.groupby("wallet")["earliest_hours"].mean().to_dict()

        for wallet in qualified_wallets:
            ws = qualified.loc[wallet]
            resolved_ct = int(ws["resolved_tokens"])
            wins_ct = int(ws["token_wins"])
            volume = float(ws["total_volume"])
            total_fills_w = int(ws["total_fills"])
            uet = int(ws["uet"])
            uew = int(ws["uew"])

            win_rate = wins_ct / resolved_ct if resolved_ct > 0 else 0.0
            early_wr = uew / uet if uet > 0 else 0.0

            wp = win_prices.get(wallet, [])
            baseline_wr = sum(wp) / len(wp) if wp else 0.5
            edge = early_wr - baseline_wr

            confidence_score = early_wr * math.log10(uet + 1) * (edge + 0.5)

            # Domain analysis — assign by highest trade count, ties by win rate
            wallet_domains = domain_agg[domain_agg["wallet"] == wallet]
            best_domain = "other"
            best_domain_wr = 0.0
            best_domain_count = 0
            for _, dr in wallet_domains.iterrows():
                if dr["d_trades"] >= 3:
                    dwr = dr["d_wins"] / dr["d_trades"]
                    count = int(dr["d_trades"])
                    if count > best_domain_count or (count == best_domain_count and dwr > best_domain_wr):
                        best_domain = dr["domain"]
                        best_domain_wr = dwr
                        best_domain_count = count

            # Recency
            t30 = int(ws["trades_30d"])
            w30 = int(ws["wins_30d"])
            t90 = int(ws["trades_90d"])
            w90 = int(ws["wins_90d"])
            wr_30d = w30 / t30 if t30 > 0 else 0.0
            wr_90d = w90 / t90 if t90 > 0 else 0.0
            is_degraded = early_wr - wr_90d > 0.15 if t90 >= 5 else False

            unique_resolved = int(ws["win_tokens"]) + int(ws["lose_tokens"])

            is_early = (
                early_wr >= MIN_EARLY_WIN_RATE_INFORMED
                and uet >= MIN_UNCERTAIN_EARLY_INFORMED
                and edge >= MIN_EDGE_INFORMED
                and volume >= MIN_VOLUME_INFORMED
                and confidence_score >= MIN_CONFIDENCE_SCORE
            )
            is_highly = early_wr >= 0.80 and uet >= 15 and edge >= 0.15

            profiles.append({
                "wallet": wallet,
                "total_fills": total_fills_w,
                "resolved_trades": resolved_ct,
                "wins": wins_ct,
                "win_rate": round(win_rate, 4),
                "early_trades": int(ws["et"]),
                "early_wins": int(ws["ew"]),
                "uncertain_early_trades": uet,
                "uncertain_early_wins": uew,
                "early_win_rate": round(early_wr, 4),
                "baseline_win_rate": round(baseline_wr, 4),
                "edge": round(edge, 4),
                "confidence_score": round(confidence_score, 4),
                "avg_hours_before_close": round(avg_hours.get(wallet, 0), 1),
                "total_volume_usdc": round(volume, 2),
                "avg_trade_size": round(volume / total_fills_w, 2) if total_fills_w > 0 else 0,
                "markets_traded": int(ws["markets_traded"]),
                "unique_resolved_markets": unique_resolved,
                "resolved_win_tokens": int(ws["win_tokens"]),
                "resolved_lose_tokens": int(ws["lose_tokens"]),
                "avg_fills_per_token": round(total_fills_w / resolved_ct, 1) if resolved_ct > 0 else 0,
                "best_domain": best_domain,
                "best_domain_win_rate": round(best_domain_wr, 4),
                "win_rate_30d": round(wr_30d, 4),
                "win_rate_90d": round(wr_90d, 4),
                "is_degraded": is_degraded,
                "is_early_informed": is_early,
                "is_highly_informed": is_highly,
                "smart_money": is_early,
            })

        if not profiles:
            return pd.DataFrame()

        df = pd.DataFrame(profiles)
        df = df.sort_values("confidence_score", ascending=False)

        t_total = time.time() - t0
        logger.info("Profiling complete: %d wallets in %.1fs (avg %.1f fills/token)",
                     len(df), t_total, avg_fills_per_token)
        return df
