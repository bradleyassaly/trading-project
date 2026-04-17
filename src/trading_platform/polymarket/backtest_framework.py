"""Systematic backtest framework for signal strategies.

Replays historical signal_outcomes with configurable strategy parameters
and computes performance metrics. Supports walk-forward validation with
train/test splits.

Usage::

    from trading_platform.polymarket.backtest_framework import run_backtest
    result = run_backtest(BacktestConfig(
        signal_types=["whale_entry", "accumulation"],
        categories=["geopolitics", "politics"],
        min_confidence=0.40,
        entry_price_range=(0.10, 0.85),
    ))
    print(result["summary"])
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class BacktestConfig:
    signal_types: list[str] | None = None
    categories: list[str] | None = None
    min_confidence: float = 0.0
    entry_price_range: tuple[float, float] = (0.10, 0.85)
    wallet_tiers: list[str] | None = None
    min_alpha_score: float = 0.0
    flat_stake: float = 10.0
    max_concurrent: int = 50
    train_pct: float = 0.70
    lookback_days: int = 60


def run_backtest(config: BacktestConfig | None = None) -> dict[str, Any]:
    """Replay resolved signal_outcomes against a strategy config.

    Returns summary metrics, per-signal breakdown, walk-forward results,
    and the raw trade log.
    """
    if config is None:
        config = BacktestConfig()

    from trading_platform.polymarket.db_connection import db as _db

    with _db() as conn:
        cutoff = int(time.time()) - config.lookback_days * 86400
        rows = conn.execute(
            """SELECT signal_type, category, direction, confidence, entry_price,
                      resolution_price, fired_at, wallet, wallet_tier,
                      resolved_at, hold_days
               FROM signal_outcomes
               WHERE resolution_price IS NOT NULL
                 AND entry_price > 0 AND entry_price < 1
                 AND fired_at >= ?
               ORDER BY fired_at""",
            (cutoff,),
        ).fetchall()

    cols = ["signal_type", "category", "direction", "confidence", "entry_price",
            "resolution_price", "fired_at", "wallet", "wallet_tier",
            "resolved_at", "hold_days"]
    signals = [dict(zip(cols, r)) for r in rows]

    def passes_filter(s: dict) -> bool:
        if config.signal_types and s["signal_type"] not in config.signal_types:
            return False
        if config.categories and (s["category"] or "other") not in config.categories:
            return False
        if (s["confidence"] or 0) < config.min_confidence:
            return False
        ep = s["entry_price"] or 0
        if ep < config.entry_price_range[0] or ep > config.entry_price_range[1]:
            return False
        if config.wallet_tiers and (s["wallet_tier"] or "") not in config.wallet_tiers:
            return False
        return True

    filtered = [s for s in signals if passes_filter(s)]

    def compute_metrics(trades: list[dict]) -> dict[str, Any]:
        if not trades:
            return {"n": 0, "wr": 0, "ev": 0, "pnl": 0, "sharpe": 0,
                    "max_dd": 0, "profit_factor": 0, "avg_hold_days": 0}

        wins = 0
        returns = []
        gross_profit = 0.0
        gross_loss = 0.0

        for t in trades:
            ep = t["entry_price"]
            rp = t["resolution_price"]
            d = t["direction"]
            if d == "BUY":
                ret = (rp - ep) / ep if ep > 0 else 0
                is_win = rp >= 0.95
            else:
                ret = (ep - rp) / (1 - ep) if ep < 1 else 0
                is_win = rp <= 0.05
            if is_win:
                wins += 1
            dollar_pnl = ret * config.flat_stake
            if dollar_pnl > 0:
                gross_profit += dollar_pnl
            else:
                gross_loss += abs(dollar_pnl)
            returns.append(ret)
            t["_return"] = ret
            t["_pnl"] = dollar_pnl
            t["_win"] = is_win

        n = len(trades)
        wr = wins / n if n else 0
        ev = sum(returns) / n if n else 0
        total_pnl = sum(r * config.flat_stake for r in returns)

        # Sharpe (annualized, assuming daily returns)
        if len(returns) > 1:
            import statistics
            mean_r = statistics.mean(returns)
            std_r = statistics.stdev(returns)
            sharpe = (mean_r / std_r * math.sqrt(252)) if std_r > 0 else 0
        else:
            sharpe = 0

        # Max drawdown
        equity = 0.0
        peak = 0.0
        max_dd = 0.0
        for r in returns:
            equity += r * config.flat_stake
            peak = max(peak, equity)
            dd = (peak - equity) / max(peak, 1)
            max_dd = max(max_dd, dd)

        pf = gross_profit / gross_loss if gross_loss > 0 else float("inf") if gross_profit > 0 else 0
        hold_days = [t.get("hold_days") or 0 for t in trades if t.get("hold_days")]
        avg_hold = sum(hold_days) / len(hold_days) if hold_days else 0

        return {
            "n": n, "wr": round(wr, 4), "ev": round(ev, 4),
            "pnl": round(total_pnl, 2), "sharpe": round(sharpe, 2),
            "max_dd": round(max_dd, 4), "profit_factor": round(pf, 2),
            "avg_hold_days": round(avg_hold, 1),
        }

    # Full-sample metrics
    full_metrics = compute_metrics(filtered)

    # Per-signal-type breakdown
    by_signal: dict[str, dict] = {}
    for t in filtered:
        st = t["signal_type"]
        if st not in by_signal:
            by_signal[st] = []
        by_signal[st].append(t)
    signal_breakdown = {
        st: compute_metrics(trades) for st, trades in by_signal.items()
    }

    # Per-category breakdown
    by_cat: dict[str, list] = {}
    for t in filtered:
        cat = t.get("category") or "other"
        if cat not in by_cat:
            by_cat[cat] = []
        by_cat[cat].append(t)
    category_breakdown = {
        cat: compute_metrics(trades) for cat, trades in by_cat.items()
    }

    # Walk-forward validation
    split_idx = int(len(filtered) * config.train_pct)
    train = filtered[:split_idx]
    test = filtered[split_idx:]
    walk_forward = {
        "train": compute_metrics(train),
        "test": compute_metrics(test),
        "train_size": len(train),
        "test_size": len(test),
    }

    return {
        "config": {
            "signal_types": config.signal_types,
            "categories": config.categories,
            "min_confidence": config.min_confidence,
            "entry_price_range": config.entry_price_range,
            "flat_stake": config.flat_stake,
            "lookback_days": config.lookback_days,
        },
        "total_signals_in_window": len(signals),
        "filtered_signals": len(filtered),
        "summary": full_metrics,
        "by_signal_type": dict(sorted(signal_breakdown.items(), key=lambda x: x[1]["pnl"], reverse=True)),
        "by_category": dict(sorted(category_breakdown.items(), key=lambda x: x[1]["pnl"], reverse=True)),
        "walk_forward": walk_forward,
    }


def run_ensemble_backtest(lookback_days: int = 60) -> dict[str, Any]:
    """Backtest the ensemble scorer against historical resolved signals.

    Replays each resolved signal through _compute_ensemble (simulated)
    and measures whether higher ensemble scores predict better outcomes.
    """
    from trading_platform.polymarket.db_connection import db as _db

    with _db() as conn:
        cutoff = int(time.time()) - lookback_days * 86400
        rows = conn.execute(
            """SELECT signal_type, category, direction, confidence, entry_price,
                      resolution_price, wallet_tier, fired_at
               FROM signal_outcomes
               WHERE resolution_price IS NOT NULL
                 AND entry_price >= 0.10 AND entry_price <= 0.85
                 AND fired_at >= ?
               ORDER BY fired_at""",
            (cutoff,),
        ).fetchall()

    # Simulate ensemble scoring
    _CATEGORY_EV = {
        "geopolitics": 0.44, "entertainment": 0.32, "politics": 0.31,
        "sports": -0.06, "crypto": -0.27, "other": -0.27,
    }
    tier_scores = {"tier1h": 1.0, "tier1": 0.8, "tier2": 0.5, "market": 0.3}

    buckets: dict[str, list[float]] = {
        "0.0-0.2": [], "0.2-0.4": [], "0.4-0.6": [], "0.6-0.8": [], "0.8-1.0": [],
    }

    for r in rows:
        sig_type, cat, direction, conf, ep, rp, tier, _ = r
        cat = (cat or "other").lower()
        score = (
            0.20 * (conf or 0.5)
            + 0.15 * tier_scores.get(tier or "", 0.3)
            + 0.10 * max(0, min(1, 0.5 + _CATEGORY_EV.get(cat, 0)))
            + 0.10 * (1.0 if 0.10 <= ep <= 0.50 else 0.7)
            + 0.45 * 0.5
        )
        score = max(0.05, min(0.95, score))

        if direction == "BUY":
            ret = (rp - ep) / ep if ep > 0 else 0
        else:
            ret = (ep - rp) / (1 - ep) if ep < 1 else 0

        if score < 0.2:
            buckets["0.0-0.2"].append(ret)
        elif score < 0.4:
            buckets["0.2-0.4"].append(ret)
        elif score < 0.6:
            buckets["0.4-0.6"].append(ret)
        elif score < 0.8:
            buckets["0.6-0.8"].append(ret)
        else:
            buckets["0.8-1.0"].append(ret)

    result = {}
    for bucket, rets in buckets.items():
        n = len(rets)
        if n > 0:
            avg_ret = sum(rets) / n
            wr = sum(1 for r in rets if r > 0) / n
            result[bucket] = {"n": n, "avg_return": round(avg_ret, 4), "win_rate": round(wr, 3)}
        else:
            result[bucket] = {"n": 0, "avg_return": 0, "win_rate": 0}

    return {
        "total_signals": len(rows),
        "lookback_days": lookback_days,
        "score_buckets": result,
    }
