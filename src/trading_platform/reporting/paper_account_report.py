from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class PaperAccountReport:
    as_of: str | None
    latest_equity: float
    cumulative_return: float
    max_drawdown: float
    avg_daily_return: float
    daily_volatility: float
    sharpe_ratio: float
    total_fills: int
    buy_fill_count: int
    sell_fill_count: int
    gross_traded_notional: float
    total_commissions: float
    open_position_count: int
    gross_market_value: float
    top_position_symbol: str | None
    top_position_weight: float
    metrics: dict[str, float] = field(default_factory=dict)


def _read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _safe_float(value: float | int | None) -> float:
    if value is None or pd.isna(value):
        return 0.0
    return float(value)


def load_paper_ledgers(account_dir: str | Path) -> dict[str, pd.DataFrame]:
    base = Path(account_dir)
    ledger_dir = base / "ledgers"
    return {
        "fills": _read_csv_if_exists(ledger_dir / "fills.csv"),
        "equity_curve": _read_csv_if_exists(ledger_dir / "equity_curve.csv"),
        "positions_history": _read_csv_if_exists(ledger_dir / "positions_history.csv"),
        "orders_history": _read_csv_if_exists(ledger_dir / "orders_history.csv"),
    }


def _compute_equity_metrics(equity_df: pd.DataFrame) -> dict[str, float | str | None]:
    if equity_df.empty:
        return {
            "as_of": None,
            "latest_equity": 0.0,
            "cumulative_return": 0.0,
            "max_drawdown": 0.0,
            "avg_daily_return": 0.0,
            "daily_volatility": 0.0,
            "sharpe_ratio": 0.0,
            "gross_market_value": 0.0,
        }

    df = equity_df.copy()
    df["as_of"] = pd.to_datetime(df["as_of"], errors="coerce")
    df = df.dropna(subset=["as_of"]).sort_values("as_of")

    if df.empty:
        return {
            "as_of": None,
            "latest_equity": 0.0,
            "cumulative_return": 0.0,
            "max_drawdown": 0.0,
            "avg_daily_return": 0.0,
            "daily_volatility": 0.0,
            "sharpe_ratio": 0.0,
            "gross_market_value": 0.0,
        }

    equity = df["equity"].astype(float)
    latest_equity = _safe_float(equity.iloc[-1])
    first_equity = _safe_float(equity.iloc[0])

    daily_returns = equity.pct_change().replace([float("inf"), float("-inf")], pd.NA).dropna()
    avg_daily_return = _safe_float(daily_returns.mean()) if not daily_returns.empty else 0.0
    daily_volatility = _safe_float(daily_returns.std(ddof=0)) if not daily_returns.empty else 0.0
    sharpe_ratio = (
        (avg_daily_return / daily_volatility) * (252 ** 0.5)
        if daily_volatility > 0
        else 0.0
    )

    running_peak = equity.cummax()
    drawdowns = (equity / running_peak) - 1.0
    max_drawdown = _safe_float(drawdowns.min()) if not drawdowns.empty else 0.0

    cumulative_return = ((latest_equity / first_equity) - 1.0) if first_equity > 0 else 0.0
    gross_market_value = _safe_float(df["gross_market_value"].astype(float).iloc[-1])

    return {
        "as_of": df["as_of"].iloc[-1].strftime("%Y-%m-%d"),
        "latest_equity": latest_equity,
        "cumulative_return": float(cumulative_return),
        "max_drawdown": float(max_drawdown),
        "avg_daily_return": float(avg_daily_return),
        "daily_volatility": float(daily_volatility),
        "sharpe_ratio": float(sharpe_ratio),
        "gross_market_value": gross_market_value,
    }


def _compute_fill_metrics(fills_df: pd.DataFrame) -> dict[str, float]:
    if fills_df.empty:
        return {
            "total_fills": 0,
            "buy_fill_count": 0,
            "sell_fill_count": 0,
            "gross_traded_notional": 0.0,
            "total_commissions": 0.0,
        }

    df = fills_df.copy()
    total_fills = int(len(df))
    buy_fill_count = int((df["side"] == "BUY").sum()) if "side" in df.columns else 0
    sell_fill_count = int((df["side"] == "SELL").sum()) if "side" in df.columns else 0
    gross_traded_notional = _safe_float(df["notional"].astype(float).abs().sum()) if "notional" in df.columns else 0.0
    total_commissions = _safe_float(df["commission"].astype(float).sum()) if "commission" in df.columns else 0.0

    return {
        "total_fills": total_fills,
        "buy_fill_count": buy_fill_count,
        "sell_fill_count": sell_fill_count,
        "gross_traded_notional": gross_traded_notional,
        "total_commissions": total_commissions,
    }


def _compute_position_metrics(positions_df: pd.DataFrame, latest_equity: float) -> dict[str, float | str | None]:
    if positions_df.empty:
        return {
            "open_position_count": 0,
            "top_position_symbol": None,
            "top_position_weight": 0.0,
        }

    df = positions_df.copy()
    df["as_of"] = pd.to_datetime(df["as_of"], errors="coerce")
    df = df.dropna(subset=["as_of"]).sort_values("as_of")

    if df.empty:
        return {
            "open_position_count": 0,
            "top_position_symbol": None,
            "top_position_weight": 0.0,
        }

    latest_as_of = df["as_of"].max()
    latest_positions = df[df["as_of"] == latest_as_of].copy()

    if latest_positions.empty:
        return {
            "open_position_count": 0,
            "top_position_symbol": None,
            "top_position_weight": 0.0,
        }

    latest_positions["market_value"] = latest_positions["market_value"].astype(float)
    latest_positions = latest_positions.sort_values("market_value", ascending=False)

    top_symbol = str(latest_positions.iloc[0]["symbol"])
    top_value = _safe_float(latest_positions.iloc[0]["market_value"])
    top_weight = (top_value / latest_equity) if latest_equity > 0 else 0.0

    return {
        "open_position_count": int(len(latest_positions)),
        "top_position_symbol": top_symbol,
        "top_position_weight": float(top_weight),
    }


def build_paper_account_report(account_dir: str | Path) -> PaperAccountReport:
    ledgers = load_paper_ledgers(account_dir)

    equity_metrics = _compute_equity_metrics(ledgers["equity_curve"])
    fill_metrics = _compute_fill_metrics(ledgers["fills"])
    position_metrics = _compute_position_metrics(
        ledgers["positions_history"],
        latest_equity=float(equity_metrics["latest_equity"]),
    )

    metrics = {
        "cumulative_return_pct": float(equity_metrics["cumulative_return"]) * 100.0,
        "max_drawdown_pct": float(equity_metrics["max_drawdown"]) * 100.0,
        "avg_daily_return_pct": float(equity_metrics["avg_daily_return"]) * 100.0,
        "daily_volatility_pct": float(equity_metrics["daily_volatility"]) * 100.0,
        "top_position_weight_pct": float(position_metrics["top_position_weight"]) * 100.0,
    }

    return PaperAccountReport(
        as_of=equity_metrics["as_of"],
        latest_equity=float(equity_metrics["latest_equity"]),
        cumulative_return=float(equity_metrics["cumulative_return"]),
        max_drawdown=float(equity_metrics["max_drawdown"]),
        avg_daily_return=float(equity_metrics["avg_daily_return"]),
        daily_volatility=float(equity_metrics["daily_volatility"]),
        sharpe_ratio=float(equity_metrics["sharpe_ratio"]),
        total_fills=int(fill_metrics["total_fills"]),
        buy_fill_count=int(fill_metrics["buy_fill_count"]),
        sell_fill_count=int(fill_metrics["sell_fill_count"]),
        gross_traded_notional=float(fill_metrics["gross_traded_notional"]),
        total_commissions=float(fill_metrics["total_commissions"]),
        open_position_count=int(position_metrics["open_position_count"]),
        gross_market_value=float(equity_metrics["gross_market_value"]),
        top_position_symbol=position_metrics["top_position_symbol"],
        top_position_weight=float(position_metrics["top_position_weight"]),
        metrics=metrics,
    )


def write_paper_account_report(
    *,
    report: PaperAccountReport,
    output_dir: str | Path,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    json_path = output_path / "paper_account_report.json"
    csv_path = output_path / "paper_account_summary.csv"

    json_path.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")
    pd.DataFrame([asdict(report)]).to_csv(csv_path, index=False)

    return {
        "json_path": json_path,
        "csv_path": csv_path,
    }