"""
CLI command: trading-cli research kalshi-full-backtest

Runs the Kalshi backtester across all eight signal families on real historical
data downloaded by ``trading-cli data kalshi historical-ingest``.

Signals evaluated
-----------------
From signals.py (original three):
  kalshi_calibration_drift   — probability mean-reversion on log-odds
  kalshi_volume_spike        — volume spike at extreme probability levels
  kalshi_time_decay          — fading high tension near resolution

From signals_base_rate.py / signals_metaculus.py:
  kalshi_base_rate           — base rate mean-reversion vs historical priors
  kalshi_metaculus_divergence — Metaculus community vs market price divergence

From signals_informed_flow.py (new):
  kalshi_taker_imbalance     — sustained directional taker-side volume imbalance
  kalshi_large_order         — large-order footprint (>5× median size)
  kalshi_unexplained_move    — anomalous moves outside scheduled event windows

Outputs
-------
  artifacts/kalshi_research/full_backtest_results.csv
  artifacts/kalshi_research/full_backtest_summary.md

Usage
-----
    trading-cli research kalshi-full-backtest
    trading-cli research kalshi-full-backtest --config configs/kalshi_research.yaml
    trading-cli research kalshi-full-backtest \\
        --feature-dir data/kalshi/features \\
        --resolution-data data/kalshi/raw/resolution.csv \\
        --output-dir artifacts/kalshi_research
"""
from __future__ import annotations

import argparse
import math
from datetime import UTC, datetime
from pathlib import Path


def cmd_kalshi_full_backtest(args: argparse.Namespace) -> None:
    import pandas as pd

    from trading_platform.kalshi.backtest import KalshiBacktester
    from trading_platform.kalshi.signals import (
        KALSHI_CALIBRATION_DRIFT,
        KALSHI_TIME_DECAY,
        KALSHI_VOLUME_SPIKE,
    )
    from trading_platform.kalshi.signals_base_rate import KALSHI_BASE_RATE
    from trading_platform.kalshi.signals_metaculus import KALSHI_METACULUS_DIVERGENCE
    from trading_platform.kalshi.signals_informed_flow import (
        KALSHI_LARGE_ORDER,
        KALSHI_TAKER_IMBALANCE,
        KALSHI_UNEXPLAINED_MOVE,
    )

    # ── Resolve paths ────────────────────────────────────────────────────────
    feature_dir = Path(getattr(args, "feature_dir", None) or "data/kalshi/features")
    resolution_path = Path(
        getattr(args, "resolution_data", None) or "data/kalshi/raw/resolution.csv"
    )
    output_dir = Path(getattr(args, "output_dir", None) or "artifacts/kalshi_research")
    output_dir.mkdir(parents=True, exist_ok=True)

    entry_threshold = float(getattr(args, "entry_threshold", None) or 0.5)
    long_only = bool(getattr(args, "long_only", False))

    print("Kalshi Full Backtest")
    print(f"  feature dir       : {feature_dir}")
    print(f"  resolution data   : {resolution_path}")
    print(f"  output dir        : {output_dir}")
    print(f"  entry threshold   : {entry_threshold}")
    print(f"  long only         : {long_only}")

    # ── Load resolution data ──────────────────────────────────────────────────
    resolution_data = pd.DataFrame()
    if resolution_path.exists():
        try:
            resolution_data = pd.read_csv(resolution_path)
            print(f"  resolution records: {len(resolution_data)}")
        except Exception as exc:
            print(f"  [WARN] Could not load resolution data: {exc}")
    else:
        print(f"  [WARN] Resolution data not found at {resolution_path}. Run historical-ingest first.")

    # ── Check feature directory ───────────────────────────────────────────────
    if not feature_dir.exists():
        print(f"\n[ERROR] Feature directory not found: {feature_dir}")
        print("Run 'trading-cli data kalshi historical-ingest' first.")
        return

    feature_files = list(feature_dir.glob("*.parquet"))
    print(f"  feature files     : {len(feature_files)}")

    if not feature_files:
        print("\n[ERROR] No feature parquet files found. Run historical-ingest first.")
        return

    # ── Run backtest across all 5 signals ────────────────────────────────────
    all_families = [
        KALSHI_CALIBRATION_DRIFT,
        KALSHI_VOLUME_SPIKE,
        KALSHI_TIME_DECAY,
        KALSHI_BASE_RATE,
        KALSHI_METACULUS_DIVERGENCE,
        KALSHI_TAKER_IMBALANCE,
        KALSHI_LARGE_ORDER,
        KALSHI_UNEXPLAINED_MOVE,
    ]

    backtester = KalshiBacktester(
        entry_threshold=entry_threshold,
        long_only=long_only,
    )

    print("\nRunning backtest...")
    results = backtester.run(
        feature_dir=feature_dir,
        resolution_data=resolution_data,
        signal_families=all_families,
        output_dir=output_dir,
    )

    # The backtester writes backtest_results.csv; rename/copy to full_backtest_results.csv
    std_path = output_dir / "backtest_results.csv"
    full_path = output_dir / "full_backtest_results.csv"
    if std_path.exists() and std_path != full_path:
        import shutil
        shutil.copy(std_path, full_path)

    # ── Build summary markdown ────────────────────────────────────────────────
    rows_df = pd.read_csv(full_path) if full_path.exists() else pd.DataFrame(
        [{"signal_family": r.signal_family, "n_trades": r.n_trades,
          "win_rate": r.win_rate, "mean_edge": r.mean_edge,
          "sharpe": r.sharpe, "max_drawdown": r.max_drawdown, "ic": r.ic}
         for r in results]
    )

    summary_path = output_dir / "full_backtest_summary.md"
    _write_summary_markdown(rows_df, summary_path, feature_dir, resolution_path)

    # ── Print results table ───────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print("BACKTEST RESULTS — ALL 5 SIGNALS")
    print("=" * 80)
    _fmt = "{:<35}  {:>8}  {:>8}  {:>10}  {:>8}  {:>10}  {:>7}"
    print(_fmt.format("Signal", "n_trades", "IC", "win_rate", "mean_edge", "max_dd", "sharpe"))
    print("-" * 80)
    for result in sorted(results, key=lambda r: _sort_key(r.sharpe)):
        print(_fmt.format(
            result.signal_family,
            result.n_trades,
            _f(result.ic, ".4f"),
            _f(result.win_rate, ".1%"),
            _f(result.mean_edge, ".3f"),
            _f(result.max_drawdown, ".3f"),
            _f(result.sharpe, ".2f"),
        ))
    print("=" * 80)
    print(f"\nArtifacts written:")
    print(f"  CSV     : {full_path}")
    print(f"  Summary : {summary_path}")


def _sort_key(val: float) -> float:
    """Sort by sharpe descending, NaN last."""
    if math.isnan(val):
        return float("-inf")
    return val


def _f(val: float, fmt: str) -> str:
    """Format a float, returning 'n/a' for NaN."""
    if val != val:  # NaN check
        return "n/a"
    return format(val, fmt)


def _write_summary_markdown(
    df: "pd.DataFrame",
    path: Path,
    feature_dir: Path,
    resolution_path: Path,
) -> None:
    """Write a structured summary markdown file."""
    import pandas as pd

    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    n_markets = len(list(feature_dir.glob("*.parquet"))) if feature_dir.exists() else 0
    n_resolved = len(pd.read_csv(resolution_path)) if resolution_path.exists() else 0

    lines = [
        "# Kalshi Full Backtest Summary",
        "",
        f"Generated: {now}",
        f"Feature files: {n_markets}",
        f"Resolved markets: {n_resolved}",
        "",
        "## Per-Signal Results",
        "",
        "| Signal | n_trades | IC | Win Rate | Mean Edge | Sharpe | Max Drawdown |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]

    # Sort by Sharpe descending (NaN last)
    if not df.empty:
        df_sorted = df.copy()
        df_sorted["_sharpe_sort"] = df_sorted["sharpe"].apply(
            lambda x: x if (x == x and not math.isnan(float(x))) else float("-inf")
        )
        df_sorted = df_sorted.sort_values("_sharpe_sort", ascending=False)

        for _, row in df_sorted.iterrows():
            def _fv(val, fmt):
                try:
                    v = float(val)
                    if math.isnan(v):
                        return "n/a"
                    return format(v, fmt)
                except (TypeError, ValueError):
                    return "n/a"

            lines.append(
                f"| {row['signal_family']} "
                f"| {int(row['n_trades']) if row['n_trades'] == row['n_trades'] else 0} "
                f"| {_fv(row['ic'], '.4f')} "
                f"| {_fv(row['win_rate'], '.1%')} "
                f"| {_fv(row['mean_edge'], '.3f')} "
                f"| {_fv(row['sharpe'], '.2f')} "
                f"| {_fv(row['max_drawdown'], '.3f')} |"
            )

    lines += [
        "",
        "## Honest Assessment",
        "",
        _build_honest_assessment(df),
        "",
        "## Signal Rankings (by risk-adjusted edge)",
        "",
        _build_rankings(df),
        "",
        "## Data Notes",
        "",
        "- `base_rate_edge` and `metaculus_divergence` columns require the historical ingest",
        "  pipeline to have run with `run_base_rate=true` / `run_metaculus=true`.",
        "  Markets without these columns will show n/a for those signals.",
        "- IC (Information Coefficient) is the Pearson correlation between signal value",
        "  and forward resolution edge.  A meaningful IC is typically |IC| > 0.02.",
        "- Win rate and mean edge are computed on trades where |signal| > entry_threshold.",
        "- Sample sizes < 30 should be treated as noise, not signal.",
    ]

    path.write_text("\n".join(lines), encoding="utf-8")


def _build_honest_assessment(df: "pd.DataFrame") -> str:
    """Build an honest, data-driven assessment of signal quality."""
    import pandas as pd

    if df.empty:
        return "No data available — run historical-ingest first to populate feature parquets."

    lines = []
    for _, row in df.iterrows():
        name = row.get("signal_family", "")
        n = int(row.get("n_trades", 0) or 0)
        try:
            ic = float(row.get("ic", float("nan")))
            sharpe = float(row.get("sharpe", float("nan")))
            mean_edge = float(row.get("mean_edge", float("nan")))
        except (TypeError, ValueError):
            ic = sharpe = mean_edge = float("nan")

        if n < 10:
            verdict = f"**{name}**: INSUFFICIENT DATA ({n} trades) — cannot draw conclusions."
        elif math.isnan(ic):
            verdict = f"**{name}**: No IC computed — check that required feature columns exist in parquets."
        elif abs(ic) < 0.02:
            verdict = f"**{name}**: NO EDGE detected (IC={ic:.4f}, n={n}). Signal is indistinguishable from noise."
        elif abs(ic) < 0.05:
            verdict = f"**{name}**: WEAK EDGE (IC={ic:.4f}, Sharpe={sharpe:.2f}, n={n}). Marginal — needs larger sample."
        else:
            direction = "positive" if ic > 0 else "negative"
            verdict = (
                f"**{name}**: EDGE DETECTED (IC={ic:.4f}, Sharpe={sharpe:.2f}, mean_edge={mean_edge:.3f}, n={n}). "
                f"Signal shows {direction} predictive power. Worth further investigation."
            )
        lines.append(f"- {verdict}")

    return "\n".join(lines) if lines else "No signals to assess."


def _build_rankings(df: "pd.DataFrame") -> str:
    """Build a ranked list of signals by Sharpe ratio."""
    if df.empty:
        return "No data."

    ranked = []
    for _, row in df.iterrows():
        try:
            sharpe = float(row.get("sharpe", float("nan")))
        except (TypeError, ValueError):
            sharpe = float("nan")
        ranked.append((row.get("signal_family", ""), sharpe))

    ranked.sort(key=lambda x: x[1] if not math.isnan(x[1]) else float("-inf"), reverse=True)
    lines = []
    for i, (name, sharpe) in enumerate(ranked, 1):
        s_str = f"{sharpe:.2f}" if not math.isnan(sharpe) else "n/a"
        lines.append(f"{i}. **{name}** — Sharpe: {s_str}")
    return "\n".join(lines)
