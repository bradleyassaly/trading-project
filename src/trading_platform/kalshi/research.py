"""
Standalone alpha research runner for Kalshi prediction market signals.

Reads feature parquets from ``feature_dir``, evaluates the three Kalshi signal
families using a simplified IC / forward-return framework appropriate for
binary markets, and writes structured artifacts to ``output_dir``.

This pipeline is completely isolated from the equity research pipeline.
No equity research code is imported or modified.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from trading_platform.kalshi.signals import (
    ALL_KALSHI_SIGNAL_FAMILIES,
    KALSHI_SIGNAL_FAMILY_NAMES,
    KalshiSignalFamily,
    compute_kalshi_signal,
    get_kalshi_signal_family,
)


@dataclass(frozen=True)
class KalshiResearchConfig:
    feature_dir: str = "data/kalshi/features/real"
    output_dir: str = "artifacts/kalshi_research"
    signal_families: tuple[str, ...] = KALSHI_SIGNAL_FAMILY_NAMES
    resolution_data_path: str | None = None
    run_backtest: bool = False
    forward_horizon_bars: int = 10
    min_rows: int = 30
    run_id: str | None = None


@dataclass
class KalshiResearchResult:
    run_id: str
    output_dir: Path
    signal_summary: list[dict]
    leaderboard: list[dict]
    best_family: str | None
    artifact_paths: dict[str, Path] = field(default_factory=dict)


def _safe_ic(signal: pd.Series, forward: pd.Series) -> float:
    valid = pd.concat([signal, forward], axis=1).dropna()
    if len(valid) < 5:
        return float("nan")
    return float(valid.iloc[:, 0].corr(valid.iloc[:, 1]))


def _safe_sharpe(returns: pd.Series) -> float:
    if len(returns) < 2:
        return float("nan")
    std = float(returns.std())
    if std == 0 or math.isnan(std):
        return float("nan")
    return float(returns.mean()) / std * math.sqrt(min(len(returns), 252))


def _evaluate_family_on_frame(
    df: pd.DataFrame,
    family: KalshiSignalFamily,
    *,
    horizon: int,
) -> tuple[list[float], list[float]]:
    """Return (signal_values, forward_returns) aligned pairs for IC computation."""
    if "close" not in df.columns:
        return [], []

    signal = compute_kalshi_signal(df, family)
    close = pd.to_numeric(df["close"], errors="coerce")
    forward_ret = close.shift(-horizon) / close - 1.0

    valid = pd.concat([signal, forward_ret], axis=1).dropna()
    if valid.empty:
        return [], []
    return list(valid.iloc[:, 0]), list(valid.iloc[:, 1])


def run_kalshi_alpha_research(config: KalshiResearchConfig) -> KalshiResearchResult:
    """
    Run alpha research on Kalshi feature parquets.

    :param config:  :class:`KalshiResearchConfig` specifying paths and options.
    :returns:       :class:`KalshiResearchResult` with artifact paths and summary.
    """
    run_id = config.run_id or datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%S+00-00")
    feature_dir = Path(config.feature_dir)
    output_dir = Path(config.output_dir) / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    families: list[KalshiSignalFamily] = []
    for name in config.signal_families:
        try:
            families.append(get_kalshi_signal_family(name))
        except ValueError:
            pass
    if not families:
        families = list(ALL_KALSHI_SIGNAL_FAMILIES)

    feature_files = sorted(feature_dir.glob("*.parquet"))
    n_markets = len(feature_files)

    family_signals: dict[str, list[float]] = {f.name: [] for f in families}
    family_forwards: dict[str, list[float]] = {f.name: [] for f in families}

    for fpath in feature_files:
        try:
            df = pd.read_parquet(fpath)
        except Exception:
            continue
        if len(df) < config.min_rows:
            continue
        for family in families:
            sigs, fwds = _evaluate_family_on_frame(df, family, horizon=config.forward_horizon_bars)
            family_signals[family.name].extend(sigs)
            family_forwards[family.name].extend(fwds)

    signal_summary: list[dict] = []
    for family in families:
        sigs = pd.Series(family_signals[family.name])
        fwds = pd.Series(family_forwards[family.name])
        n_obs = int(sigs.notna().sum())
        ic = _safe_ic(sigs, fwds) if n_obs >= 5 else float("nan")

        long_trades = fwds[sigs > 0.5]
        short_trades = -fwds[sigs < -0.5]
        all_edges = pd.concat([long_trades, short_trades]).dropna()

        win_rate = float((all_edges > 0).mean()) if len(all_edges) > 0 else float("nan")
        mean_edge = float(all_edges.mean()) if len(all_edges) > 0 else float("nan")
        sharpe = _safe_sharpe(all_edges) if len(all_edges) >= 2 else float("nan")

        signal_summary.append({
            "signal_family": family.name,
            "n_observations": n_obs,
            "n_markets": n_markets,
            "ic": ic,
            "win_rate": win_rate,
            "mean_edge": mean_edge,
            "sharpe": sharpe,
            "description": family.description,
        })

    leaderboard = sorted(
        signal_summary,
        key=lambda row: float("nan") if math.isnan(row.get("ic") or float("nan")) else -abs(row.get("ic", 0)),
    )
    best_family = leaderboard[0]["signal_family"] if leaderboard else None

    leaderboard_df = pd.DataFrame(leaderboard)
    leaderboard_path = output_dir / "leaderboard.csv"
    leaderboard_df.to_csv(leaderboard_path, index=False)

    summary = {
        "run_id": run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "n_markets_analyzed": n_markets,
        "n_signal_families": len(families),
        "best_family": best_family,
        "signal_summary": signal_summary,
    }
    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    artifact_paths: dict[str, Path] = {
        "leaderboard_csv": leaderboard_path,
        "summary_json": summary_path,
    }

    if config.run_backtest:
        resolution_data = pd.DataFrame()
        if config.resolution_data_path:
            rpath = Path(config.resolution_data_path)
            if rpath.exists():
                try:
                    resolution_data = pd.read_csv(rpath) if rpath.suffix == ".csv" else pd.read_parquet(rpath)
                except Exception:
                    pass

        if not resolution_data.empty or feature_files:
            from trading_platform.kalshi.backtest import KalshiBacktester

            backtester = KalshiBacktester()
            backtest_output_dir = output_dir / "backtest"
            backtester.run(
                feature_dir=feature_dir,
                resolution_data=resolution_data,
                signal_families=families,
                output_dir=backtest_output_dir,
            )
            bt_path = backtest_output_dir / "backtest_results.csv"
            if bt_path.exists():
                artifact_paths["backtest_results_csv"] = bt_path

    return KalshiResearchResult(
        run_id=run_id,
        output_dir=output_dir,
        signal_summary=signal_summary,
        leaderboard=leaderboard,
        best_family=best_family,
        artifact_paths=artifact_paths,
    )
