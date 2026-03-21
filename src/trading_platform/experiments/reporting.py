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


def save_xsec_construction_comparison_html_report(
    *,
    comparison_summary_df: pd.DataFrame,
    comparison_window_df: pd.DataFrame,
    output_path: Path,
    config_items: list[tuple[str, object]],
) -> Path:
    report_path = output_path.with_name(output_path.stem + "_report.html")

    summary_html = comparison_summary_df.to_html(index=False, border=0)
    windows_html = comparison_window_df.to_html(index=False, border=0)
    config_html = "".join(
        f"<li><strong>{key}</strong>: {value}</li>"
        for key, value in config_items
    )

    def _winner(metric: str, *, higher_is_better: bool = True) -> str:
        if metric not in comparison_summary_df.columns:
            return "n/a"
        working = comparison_summary_df[["portfolio_construction_mode", metric]].dropna()
        if working.empty:
            return "n/a"
        idx = working[metric].idxmax() if higher_is_better else working[metric].idxmin()
        row = working.loc[idx]
        return f"{row['portfolio_construction_mode']} ({row[metric]})"

    interpretation = [
        f"<li>Better average excess return: {_winner('avg_excess_return_pct', higher_is_better=True)}</li>",
        f"<li>Lower turnover: {_winner('mean_turnover', higher_is_better=False)}</li>",
        f"<li>Lower drawdown: {_winner('worst_test_max_drawdown_pct', higher_is_better=False)}</li>",
        f"<li>Lower realized holdings drift: {_winner('mean_average_realized_holdings_count', higher_is_better=False)}</li>",
    ]

    parts = [
        "<html>",
        "<head>",
        "<title>Xsec Construction Comparison Report</title>",
        "<style>",
        "body { font-family: Arial, sans-serif; margin: 24px; }",
        "h1, h2 { margin-top: 24px; }",
        "table { border-collapse: collapse; width: 100%; margin-top: 12px; }",
        "th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }",
        "th { background: #f5f5f5; }",
        ".metric-win { background: #e6f4ea; }",
        ".metric-loss { background: #fce8e6; }",
        "</style>",
        "</head>",
        "<body>",
        "<h1>Xsec Portfolio Construction Comparison</h1>",
        "<h2>Config</h2>",
        f"<ul>{config_html}</ul>",
        "<h2>Aggregate Comparison</h2>",
        summary_html,
        "<h2>Interpretation</h2>",
        f"<ul>{''.join(interpretation)}</ul>",
        "<h2>Per-Window Comparison</h2>",
        windows_html,
        "</body>",
        "</html>",
    ]

    report_path.write_text("\n".join(parts), encoding="utf-8")
    return report_path
