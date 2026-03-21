from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def save_walkforward_return_plot(df: pd.DataFrame, output_path: Path) -> Path:
    plot_path = output_path.with_name(output_path.stem + "_returns.png")

    working = df.copy()
    working["test_start"] = pd.to_datetime(working["test_start"])
    working = working.sort_values("test_start")

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(working["test_start"], working["test_return_pct"], label="Strategy OOS Return %")
    ax.plot(working["test_start"], working["benchmark_return_pct"], label="Benchmark Return %")
    ax.plot(working["test_start"], working["excess_return_pct"], label="Excess Return %")

    ax.set_title("Walk-Forward Out-of-Sample Returns")
    ax.set_xlabel("Test Window Start")
    ax.set_ylabel("Return (%)")
    ax.legend()
    ax.grid(True)

    fig.tight_layout()
    fig.savefig(plot_path)
    plt.close(fig)

    return plot_path


def save_walkforward_param_plot(df: pd.DataFrame, output_path: Path) -> Path | None:
    plot_path = output_path.with_name(output_path.stem + "_params.png")

    working = df.copy()
    working["test_start"] = pd.to_datetime(working["test_start"])
    working = working.sort_values("test_start")

    has_fast = "fast" in working.columns and working["fast"].notna().any()
    has_slow = "slow" in working.columns and working["slow"].notna().any()
    has_lookback = "lookback" in working.columns and working["lookback"].notna().any()
    has_lookback_bars = "lookback_bars" in working.columns and working["lookback_bars"].notna().any()
    has_skip_bars = "skip_bars" in working.columns and working["skip_bars"].notna().any()
    has_top_n = "top_n" in working.columns and working["top_n"].notna().any()
    has_rebalance_bars = "rebalance_bars" in working.columns and working["rebalance_bars"].notna().any()

    if not (has_fast or has_slow or has_lookback or has_lookback_bars or has_skip_bars or has_top_n or has_rebalance_bars):
        return None

    fig, ax = plt.subplots(figsize=(10, 6))

    if has_fast:
        ax.plot(working["test_start"], working["fast"], label="fast")
    if has_slow:
        ax.plot(working["test_start"], working["slow"], label="slow")
    if has_lookback:
        ax.plot(working["test_start"], working["lookback"], label="lookback")
    if has_lookback_bars:
        ax.plot(working["test_start"], working["lookback_bars"], label="lookback_bars")
    if has_skip_bars:
        ax.plot(working["test_start"], working["skip_bars"], label="skip_bars")
    if has_top_n:
        ax.plot(working["test_start"], working["top_n"], label="top_n")
    if has_rebalance_bars:
        ax.plot(working["test_start"], working["rebalance_bars"], label="rebalance_bars")

    ax.set_title("Selected Parameters Across Walk-Forward Windows")
    ax.set_xlabel("Test Window Start")
    ax.set_ylabel("Parameter Value")
    ax.legend()
    ax.grid(True)

    fig.tight_layout()
    fig.savefig(plot_path)
    plt.close(fig)

    return plot_path


def save_walkforward_html_report(
    window_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    output_path: Path,
    returns_plot_path: Path | None,
    params_plot_path: Path | None,
) -> Path:
    report_path = output_path.with_name(output_path.stem + "_report.html")

    returns_plot_name = returns_plot_path.name if returns_plot_path else None
    params_plot_name = params_plot_path.name if params_plot_path else None

    summary_html = summary_df.to_html(index=False, border=0)
    windows_html = window_df.to_html(index=False, border=0)

    parts = [
        "<html>",
        "<head>",
        "<title>Walk-Forward Report</title>",
        "<style>",
        "body { font-family: Arial, sans-serif; margin: 24px; }",
        "h1, h2 { margin-top: 24px; }",
        "table { border-collapse: collapse; width: 100%; margin-top: 12px; }",
        "th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }",
        "th { background: #f5f5f5; }",
        "img { max-width: 100%; height: auto; margin-top: 12px; border: 1px solid #ddd; }",
        "</style>",
        "</head>",
        "<body>",
        "<h1>Walk-Forward Report</h1>",
        "<h2>Aggregate Summary</h2>",
        summary_html,
    ]

    if returns_plot_name:
        parts.extend([
            "<h2>Out-of-Sample Returns</h2>",
            f'<img src="{returns_plot_name}" alt="Walk-forward returns plot">',
        ])

    if params_plot_name:
        parts.extend([
            "<h2>Selected Parameters Over Time</h2>",
            f'<img src="{params_plot_name}" alt="Walk-forward parameter plot">',
        ])

    parts.extend([
        "<h2>Window Results</h2>",
        windows_html,
        "</body>",
        "</html>",
    ])

    report_path.write_text("\n".join(parts), encoding="utf-8")
    return report_path
