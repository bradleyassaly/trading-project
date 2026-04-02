"""
Artifact readers for the trading platform FastAPI backend.

Reads from the same file-based artifacts as the Flask dashboard on port 8000.
All functions degrade gracefully — they never raise; they return
``{"available": False, "reason": "..."}`` when files are missing or corrupt.
"""
from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


# Roots — override via environment for testing or non-default layouts
ARTIFACTS_ROOT = Path(os.environ.get("ARTIFACTS_ROOT", "artifacts"))
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "data"))


# ── Low-level helpers ────────────────────────────────────────────────────────


def _safe(value: Any) -> Any:
    """Coerce a value to a JSON-serializable type."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (pd.Timestamp, datetime)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if hasattr(value, "item"):  # numpy scalar
        return _safe(value.item())
    return value


def _safe_row(row: dict[str, Any]) -> dict[str, Any]:
    return {k: _safe(v) for k, v in row.items()}


def _read_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def _read_jsonl(path: Path) -> list[dict[str, Any]] | None:
    if not path.exists():
        return None
    try:
        lines: list[dict[str, Any]] = []
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                stripped = line.strip()
                if stripped:
                    lines.append(json.loads(stripped))
        return lines
    except Exception:
        return None


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _compute_sharpe(returns: pd.Series) -> float | None:
    clean = pd.to_numeric(returns, errors="coerce").dropna()
    if len(clean) < 2:
        return None
    std = float(clean.std())
    if std == 0.0 or math.isnan(std):
        return None
    return float(clean.mean()) / std * math.sqrt(min(len(clean), 252))


def _compute_max_drawdown(equity: pd.Series) -> float | None:
    clean = pd.to_numeric(equity, errors="coerce").dropna()
    if clean.empty:
        return None
    running_max = clean.cummax()
    dd = (clean - running_max) / running_max.replace(0.0, float("nan"))
    val = dd.min()
    return float(val) if not math.isnan(val) else None


# ── System status ────────────────────────────────────────────────────────────


def read_system_status() -> dict[str, Any]:
    control_dir = ARTIFACTS_ROOT / "control"
    kill_active = (control_dir / "KILL_SWITCH").exists()
    trigger_pending = (control_dir / "TRIGGER_NOW").exists()

    if kill_active:
        loop_state = "stopped"
    elif trigger_pending:
        loop_state = "trigger_pending"
    else:
        loop_state = "running"

    last_run_timestamp: str | None = None
    next_scheduled_run: str | None = None

    decision_log_path = ARTIFACTS_ROOT / "decision_journal" / "decision_log.jsonl"
    entries = _read_jsonl(decision_log_path)
    if entries:
        last = entries[-1]
        last_run_timestamp = _safe(last.get("timestamp"))
        next_scheduled_run = _safe(last.get("next_run"))

    active_strategy_count = 0
    portfolio = _read_json(ARTIFACTS_ROOT / "strategy_portfolio" / "strategy_portfolio.json")
    if portfolio and isinstance(portfolio, dict) and "strategies" in portfolio:
        active_strategy_count = len(portfolio["strategies"])
    if active_strategy_count == 0:
        portfolio_csv = _read_csv(ARTIFACTS_ROOT / "strategy_portfolio" / "strategy_portfolio.csv")
        if portfolio_csv is not None:
            active_strategy_count = len(portfolio_csv)

    return {
        "available": True,
        "loop_state": loop_state,
        "last_run_timestamp": last_run_timestamp,
        "next_scheduled_run": next_scheduled_run,
        "active_strategy_count": active_strategy_count,
        "kill_switch_active": kill_active,
        "trigger_now_pending": trigger_pending,
    }


# ── P&L / equity ─────────────────────────────────────────────────────────────


def read_equity_curve() -> dict[str, Any]:
    path = ARTIFACTS_ROOT / "paper" / "paper_equity_curve.csv"
    df = _read_csv(path)
    if df is None or df.empty:
        return {"available": False, "reason": "No paper equity curve found", "data": []}

    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        records.append(_safe_row(dict(row)))

    return {"available": True, "data": records}


def read_pnl_summary() -> dict[str, Any]:
    equity_path = ARTIFACTS_ROOT / "paper" / "paper_equity_curve.csv"
    positions_path = ARTIFACTS_ROOT / "paper" / "paper_positions.csv"

    df = _read_csv(equity_path)
    if df is None or df.empty:
        return {"available": False, "reason": "No P&L data found"}

    result: dict[str, Any] = {"available": True}

    equity_col = next((c for c in ["equity", "portfolio_value", "value"] if c in df.columns), None)
    if equity_col:
        equity = pd.to_numeric(df[equity_col], errors="coerce").dropna()
        if not equity.empty:
            result["total_pnl"] = _safe(float(equity.iloc[-1] - equity.iloc[0]))
            result["current_equity"] = _safe(float(equity.iloc[-1]))
            result["max_drawdown"] = _safe(_compute_max_drawdown(equity))

    daily_col = next((c for c in ["daily_return", "daily_pnl", "return"] if c in df.columns), None)
    if daily_col:
        daily = pd.to_numeric(df[daily_col], errors="coerce").dropna()
        if not daily.empty:
            result["today_pnl"] = _safe(float(daily.iloc[-1]))
            result["sharpe"] = _safe(_compute_sharpe(daily))
    elif equity_col and "equity" in result:
        equity_s = pd.to_numeric(df[equity_col], errors="coerce").dropna()
        if len(equity_s) > 1:
            daily_returns = equity_s.pct_change().dropna()
            result["today_pnl"] = _safe(float(daily_returns.iloc[-1]))
            result["sharpe"] = _safe(_compute_sharpe(daily_returns))

    positions_df = _read_csv(positions_path)
    if positions_df is not None and not positions_df.empty:
        result["open_positions_count"] = int(len(positions_df))
        val_col = next((c for c in ["market_value", "value", "notional"] if c in positions_df.columns), None)
        if val_col:
            result["open_positions_value"] = _safe(
                float(pd.to_numeric(positions_df[val_col], errors="coerce").sum())
            )
    else:
        result["open_positions_count"] = 0
        result["open_positions_value"] = 0.0

    return result


# ── Signal performance ───────────────────────────────────────────────────────


def read_signals_performance() -> dict[str, Any]:
    backtest_path = ARTIFACTS_ROOT / "kalshi_research" / "backtest" / "backtest_results.csv"
    leaderboard_path = ARTIFACTS_ROOT / "kalshi_research" / "leaderboard.csv"

    df = _read_csv(backtest_path)
    source = "backtest_results"
    if df is None or df.empty:
        df = _read_csv(leaderboard_path)
        source = "leaderboard"
    if df is None or df.empty:
        return {"available": False, "reason": "No signal performance data found", "data": []}

    records = [_safe_row(dict(row)) for _, row in df.iterrows()]
    return {"available": True, "source": source, "data": records}


def read_signals_correlation() -> dict[str, Any]:
    # Try multiple possible feature directory locations
    candidates = [
        DATA_ROOT / "kalshi" / "features" / "real",
        DATA_ROOT / "kalshi" / "features",
        DATA_ROOT / "kalshi" / "synthetic",
    ]
    features_dir: Path | None = next((p for p in candidates if p.exists()), None)

    if features_dir is None:
        return {"available": False, "reason": "No Kalshi feature directory found", "matrix": [], "signals": []}

    signal_cols = [
        "calibration_drift_z",
        "volume_spike_z",
        "tension",
        "taker_imbalance",
        "large_order_direction",
        "base_rate_edge",
        "signal_value",
        "volume_z",
    ]

    frames: list[pd.DataFrame] = []
    for fpath in sorted(features_dir.glob("*.parquet"))[:100]:
        try:
            df = pd.read_parquet(fpath)
            available = [c for c in signal_cols if c in df.columns]
            if available:
                frames.append(df[available].tail(20))  # last 20 rows per market
        except Exception:
            continue

    if not frames:
        return {"available": False, "reason": "No signal features found in parquets", "matrix": [], "signals": []}

    combined = pd.concat(frames, ignore_index=True)
    numeric_cols = [
        c for c in combined.columns
        if combined[c].dtype.kind in ("f", "i") and combined[c].notna().sum() >= 5
    ]
    if len(numeric_cols) < 2:
        return {"available": False, "reason": "Insufficient numeric signals for correlation", "matrix": [], "signals": []}

    corr = combined[numeric_cols].corr()
    matrix = [[_safe(corr.loc[r, c]) for c in corr.columns] for r in corr.index]
    return {"available": True, "signals": list(corr.index), "matrix": matrix}


# ── Kalshi markets ────────────────────────────────────────────────────────────


def _features_dir() -> Path | None:
    candidates = [
        DATA_ROOT / "kalshi" / "features" / "real",
        DATA_ROOT / "kalshi" / "features",
        DATA_ROOT / "kalshi" / "synthetic",
    ]
    return next((p for p in candidates if p.exists()), None)


def read_kalshi_markets() -> dict[str, Any]:
    fdir = _features_dir()
    if fdir is None:
        return {"available": False, "reason": "No Kalshi feature data found", "data": []}

    markets: list[dict[str, Any]] = []
    for fpath in sorted(fdir.glob("*.parquet"))[:200]:
        ticker = fpath.stem
        try:
            df = pd.read_parquet(fpath)
            if df.empty:
                continue
            last = df.iloc[-1]
            market: dict[str, Any] = {
                "ticker": ticker,
                "title": _safe(last.get("title", ticker)),
                "yes_price": _safe(last.get("close")),
                "volume": _safe(last.get("volume")),
                "days_to_close": _safe(last.get("days_to_close")),
                "signals": {
                    "calibration_drift_z": _safe(last.get("calibration_drift_z")),
                    "volume_spike_z": _safe(last.get("volume_spike_z")),
                    "tension": _safe(last.get("tension")),
                    "taker_imbalance": _safe(last.get("taker_imbalance")),
                    "large_order_direction": _safe(last.get("large_order_direction")),
                    "base_rate_edge": _safe(last.get("base_rate_edge")),
                },
            }
            markets.append(market)
        except Exception:
            continue

    if not markets:
        return {"available": False, "reason": "No Kalshi market data found", "data": []}

    return {"available": True, "data": markets}


def read_kalshi_market_history(ticker: str) -> dict[str, Any]:
    fdir = _features_dir()
    if fdir is None:
        return {"available": False, "reason": "No Kalshi feature directory found", "data": []}

    fpath = fdir / f"{ticker}.parquet"
    if not fpath.exists():
        return {"available": False, "reason": f"No history for market {ticker}", "data": []}

    try:
        df = pd.read_parquet(fpath)
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}

    if df.empty:
        return {"available": False, "reason": f"Empty feature file for {ticker}", "data": []}

    ts_col = next((c for c in ["timestamp", "date", "time"] if c in df.columns), None)
    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        record: dict[str, Any] = {
            "timestamp": _safe(row.get(ts_col)) if ts_col else None,
            "yes_price": _safe(row.get("close")),
            "calibration_drift_z": _safe(row.get("calibration_drift_z")),
            "volume_spike_z": _safe(row.get("volume_spike_z")),
            "volume_z": _safe(row.get("volume_z")),
        }
        records.append(record)

    return {"available": True, "ticker": ticker, "data": records}


# ── Reasoning / trade decisions ───────────────────────────────────────────────


def read_reasoning_trades() -> dict[str, Any]:
    decisions_path = ARTIFACTS_ROOT / "decision_journal" / "trade_decisions.csv"
    candidates_path = ARTIFACTS_ROOT / "decision_journal" / "candidate_snapshot.csv"

    df_dec = _read_csv(decisions_path)
    df_cand = _read_csv(candidates_path)

    if (df_dec is None or df_dec.empty) and (df_cand is None or df_cand.empty):
        return {"available": False, "reason": "No trade decision data found", "data": []}

    records: list[dict[str, Any]] = []
    if df_dec is not None and not df_dec.empty:
        for _, row in df_dec.tail(50).iterrows():
            records.append(_safe_row(dict(row)))
    elif df_cand is not None and not df_cand.empty:
        for _, row in df_cand.tail(50).iterrows():
            records.append(_safe_row(dict(row)))

    return {"available": True, "data": records}


# ── Loop decisions ────────────────────────────────────────────────────────────


def read_loop_decisions() -> dict[str, Any]:
    path = ARTIFACTS_ROOT / "decision_journal" / "decision_log.jsonl"
    entries = _read_jsonl(path)
    if entries is None:
        return {"available": False, "reason": "No loop decision log found", "data": []}

    safe_entries = [_safe_row(e) for e in entries[-20:]]
    return {"available": True, "data": safe_entries}
