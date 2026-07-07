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


# Cold-start / fallback category-EV prior, copied verbatim from the live
# scorer polymarket_paper_executor._CATEGORY_EV. Used ONLY when a walk-forward
# fold's train slice lacks a category (roadmap N5). The live constants must
# NOT be scored against the same rows that fit them — that is the tautology.
_DEFAULT_CATEGORY_EV = {
    "geopolitics": 0.44, "entertainment": 0.32, "politics": 0.31,
    "sports": -0.06, "crypto": -0.27, "other": -0.27,
    "economics": 0.0, "science": 0.0, "tech": 0.0, "finance": 0.0,
}


def _fit_category_ev(train_trades: list[dict], prior: dict[str, float],
                     min_n: int = 8, shrink: float = 8.0) -> dict[str, float]:
    """Refit per-category realized EV from a fold's TRAIN slice, shrunk toward
    the prior (roadmap N5). Uses the same return formula as compute_metrics.
    Categories with < min_n train trades fall back to the prior."""
    by_cat: dict[str, list[float]] = {}
    for t in train_trades:
        ep = t.get("entry_price") or 0
        rp = t.get("resolution_price")
        if rp is None or ep <= 0 or ep >= 1:
            continue
        if (t.get("direction") or "BUY") == "BUY":
            ret = (rp - ep) / ep
        else:
            ret = (ep - rp) / (1 - ep)
        by_cat.setdefault(t.get("category") or "other", []).append(ret)
    fitted: dict[str, float] = {}
    for cat, rets in by_cat.items():
        n = len(rets)
        p = prior.get(cat, 0.0)
        if n < min_n:
            fitted[cat] = p
        else:
            emp = sum(rets) / n
            w = n / (n + shrink)  # James-Stein / Bayesian shrink toward prior
            fitted[cat] = w * emp + (1 - w) * p
    return fitted


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
    train_pct: float = 0.70   # legacy (single-split); superseded by n_folds
    lookback_days: int = 60
    n_folds: int = 5
    min_train: int = 30


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

    # Walk-forward validation — ROLLING expanding-window folds (roadmap N5).
    # The prior single 70/30 chronological cut was one fold mislabeled
    # "walk-forward" and refit nothing. Now: reserve the first min_train rows
    # to seed training, split the remaining tail into n_folds equal test
    # blocks, and for each fold refit category-EV priors on THAT fold's train
    # slice only (the honest, non-tautological version).
    n = len(filtered)
    tail = n - config.min_train
    folds: list[dict] = []
    insufficient = tail < config.n_folds or config.min_train < 1
    if not insufficient:
        block = tail // config.n_folds
        for k in range(config.n_folds):
            cut0 = config.min_train + k * block
            cut1 = n if k == config.n_folds - 1 else config.min_train + (k + 1) * block
            train_k = filtered[:cut0]
            test_k = filtered[cut0:cut1]
            if not train_k or not test_k:
                continue
            fitted_ev = _fit_category_ev(train_k, _DEFAULT_CATEGORY_EV)
            folds.append({
                "fold": k + 1,
                "train_size": len(train_k),
                "test_size": len(test_k),
                "train_metrics": compute_metrics(train_k),
                "test_metrics": compute_metrics(test_k),
                "fitted_category_ev": {c: round(v, 4) for c, v in fitted_ev.items()},
            })
    test_evs = [f["test_metrics"]["ev"] for f in folds]
    import statistics as _st
    aggregate = {
        "n_folds_evaluated": len(folds),
        "mean_test_ev": round(sum(test_evs) / len(test_evs), 4) if test_evs else None,
        "std_test_ev": round(_st.stdev(test_evs), 4) if len(test_evs) > 1 else None,
    }
    walk_forward: dict[str, Any] = {"folds": folds, "aggregate": aggregate}
    if insufficient:
        walk_forward["insufficient_data"] = True
        walk_forward["note"] = (
            f"n={n} resolved rows over {config.lookback_days}d; walk-forward "
            f"needs >= {config.min_train + config.n_folds}; not validatable")
    # Back-compat: keep the legacy single-split train/test keys populated from
    # the last fold (or the whole set) for any dashboard still reading them.
    if folds:
        walk_forward["train"] = folds[-1]["train_metrics"]
        walk_forward["test"] = folds[-1]["test_metrics"]
        walk_forward["train_size"] = folds[-1]["train_size"]
        walk_forward["test_size"] = folds[-1]["test_size"]
    else:
        walk_forward["train"] = compute_metrics(filtered)
        walk_forward["test"] = compute_metrics([])
        walk_forward["train_size"] = n
        walk_forward["test_size"] = 0

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


_TIER_SCORES = {"tier1h": 1.0, "tier1": 0.8, "tier2": 0.5, "market": 0.3}


def _ensemble_score(conf, tier, ep, cat_ev: dict[str, float], cat: str) -> float:
    """The reduced ensemble score for the columns the backtest has, using the
    supplied (per-fold fitted) category-EV map — NOT baked-in constants."""
    score = (
        0.20 * (conf or 0.5)
        + 0.15 * _TIER_SCORES.get(tier or "", 0.3)
        + 0.10 * max(0, min(1, 0.5 + cat_ev.get(cat, 0.0)))
        + 0.10 * (1.0 if 0.10 <= ep <= 0.50 else 0.7)
        + 0.45 * 0.5
    )
    return max(0.05, min(0.95, score))


def _bucket_of(score: float) -> str:
    if score < 0.2: return "0.0-0.2"
    if score < 0.4: return "0.2-0.4"
    if score < 0.6: return "0.4-0.6"
    if score < 0.8: return "0.6-0.8"
    return "0.8-1.0"


def run_ensemble_backtest(lookback_days: int = 60, n_folds: int = 5) -> dict[str, Any]:
    """Backtest the ensemble scorer OUT-OF-SAMPLE (roadmap N5).

    The prior version scored every row using the exact _CATEGORY_EV priors the
    live scorer uses — a tautology (the scorer was fit with hindsight over the
    very rows it scored). Now: walk-forward folds, fit category-EV on each
    fold's TRAIN slice only, and score the TEST rows with those fitted priors.
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

    trades = [{"signal_type": r[0], "category": r[1], "direction": r[2],
               "confidence": r[3], "entry_price": r[4], "resolution_price": r[5],
               "wallet_tier": r[6]} for r in rows]

    def _ret(t) -> float:
        ep, rp = t["entry_price"], t["resolution_price"]
        if (t["direction"] or "BUY") == "BUY":
            return (rp - ep) / ep if ep > 0 else 0
        return (ep - rp) / (1 - ep) if ep < 1 else 0

    min_train = 30
    n = len(trades)
    tail = n - min_train
    per_fold: list[dict] = []
    agg_buckets: dict[str, list[float]] = {
        "0.0-0.2": [], "0.2-0.4": [], "0.4-0.6": [], "0.6-0.8": [], "0.8-1.0": []}
    insufficient = tail < n_folds
    if not insufficient:
        block = tail // n_folds
        for k in range(n_folds):
            cut0 = min_train + k * block
            cut1 = n if k == n_folds - 1 else min_train + (k + 1) * block
            train_k, test_k = trades[:cut0], trades[cut0:cut1]
            if not train_k or not test_k:
                continue
            fitted_ev = _fit_category_ev(train_k, _DEFAULT_CATEGORY_EV)
            fold_buckets: dict[str, list[float]] = {b: [] for b in agg_buckets}
            for t in test_k:
                s = _ensemble_score(t["confidence"], t["wallet_tier"],
                                    t["entry_price"], fitted_ev,
                                    (t["category"] or "other").lower())
                b = _bucket_of(s)
                fold_buckets[b].append(_ret(t))
                agg_buckets[b].append(_ret(t))
            per_fold.append({
                "fold": k + 1, "train_size": len(train_k), "test_size": len(test_k),
                "fitted_category_ev": {c: round(v, 4) for c, v in fitted_ev.items()},
            })

    def _bucketize(bmap):
        out = {}
        for b, rets in bmap.items():
            m = len(rets)
            out[b] = ({"n": m, "avg_return": round(sum(rets) / m, 4),
                       "win_rate": round(sum(1 for r in rets if r > 0) / m, 3)}
                      if m else {"n": 0, "avg_return": 0, "win_rate": 0})
        return out

    oos = _bucketize(agg_buckets)
    # Monotonicity: does OOS avg_return increase across non-empty buckets?
    non_empty = [oos[b]["avg_return"] for b in agg_buckets if oos[b]["n"] > 0]
    monotonic = all(non_empty[i] <= non_empty[i + 1] for i in range(len(non_empty) - 1)) \
        if len(non_empty) > 1 else False

    result = {
        "total_signals": len(rows),
        "lookback_days": lookback_days,
        "n_folds": len(per_fold),
        "score_buckets_oos_aggregate": oos,
        "monotonic_oos": monotonic,
        "per_fold": per_fold,
    }
    if insufficient:
        result["insufficient_data"] = True
        result["note"] = (
            f"n={n} resolved rows; ensemble walk-forward needs >= "
            f"{min_train + n_folds}; OOS validation not run")
    return result
