"""
Resolved-market backtester for Kalshi historical research.

This runner evaluates one or more Kalshi signal families on locally ingested,
resolved markets and writes structured research artifacts:

- ``backtest_results.csv`` for lightweight compatibility with existing flows
- ``kalshi_backtest_summary.json``
- ``kalshi_signal_diagnostics.json``
- ``kalshi_trade_log.jsonl``
- ``kalshi_backtest_report.md``

Trading assumptions are explicit and serialized into the output artifacts.
"""
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from trading_platform.kalshi.signals import KalshiSignalFamily


_SETTLED_STATUSES = {"settled", "determined", "closed", "finalized"}
_PROB_CLIP_LOW = 1e-4
_PROB_CLIP_HIGH = 1.0 - _PROB_CLIP_LOW
_CONFIDENCE_BUCKETS: tuple[tuple[float, float, str], ...] = (
    (0.00, 0.05, "0-5%"),
    (0.05, 0.10, "5-10%"),
    (0.10, 0.20, "10-20%"),
    (0.20, 1.01, "20%+"),
)


@dataclass(frozen=True)
class KalshiExecutionAssumptions:
    entry_timing_mode: str = "hours_before_close"
    entry_offset_hours: float = 24.0
    holding_window_hours: float | None = None
    entry_slippage_points: float = 0.0
    exit_slippage_points: float = 0.0
    signal_probability_scale: float = 8.0

    def __post_init__(self) -> None:
        if self.entry_timing_mode not in {"hours_before_close", "last_bar"}:
            raise ValueError("entry_timing_mode must be 'hours_before_close' or 'last_bar'.")
        if self.entry_offset_hours < 0:
            raise ValueError("entry_offset_hours must be >= 0.")
        if self.holding_window_hours is not None and self.holding_window_hours <= 0:
            raise ValueError("holding_window_hours must be > 0 when provided.")
        if self.entry_slippage_points < 0 or self.exit_slippage_points < 0:
            raise ValueError("slippage points must be >= 0.")
        if self.signal_probability_scale <= 0:
            raise ValueError("signal_probability_scale must be > 0.")


@dataclass(frozen=True)
class KalshiBacktestTrade:
    ticker: str
    signal_family: str
    category: str
    market_title: str
    side: str
    entry_time: str
    exit_time: str
    close_time: str | None
    exit_reason: str
    entry_signal: float
    entry_threshold: float
    entry_price_yes: float
    entry_fill_yes: float
    exit_price_yes: float
    predicted_yes_probability: float
    predicted_edge: float
    confidence: float
    resolution_price: float
    actual_outcome: int
    realized_pnl_points: float
    realized_return: float
    win: bool
    brier_score: float
    supporting_features: dict[str, Any]


@dataclass(frozen=True)
class KalshiBacktestResult:
    signal_family: str
    markets_evaluated: int
    candidate_signals: int
    n_trades: int
    win_rate: float
    mean_edge: float
    sharpe: float
    max_drawdown: float
    ic: float
    avg_predicted_edge: float
    avg_confidence: float
    realized_avg_return: float
    brier_score: float
    avg_signal_value: float


def _compute_max_drawdown(equity_curve: pd.Series) -> float:
    if equity_curve.empty:
        return 0.0
    running_max = equity_curve.cummax()
    drawdown = (equity_curve - running_max) / running_max.replace(0.0, float("nan"))
    return float(drawdown.min()) if not drawdown.isna().all() else 0.0


def _compute_sharpe(returns: pd.Series) -> float:
    if len(returns) < 2:
        return 0.0
    std = float(returns.std())
    if std == 0.0 or math.isnan(std):
        return 0.0
    return float(returns.mean()) / std * math.sqrt(min(len(returns), 252))


def _compute_ic(signal: pd.Series, forward_edge: pd.Series) -> float:
    valid = pd.concat([signal, forward_edge], axis=1).dropna()
    if len(valid) < 5:
        return float("nan")
    return float(valid.iloc[:, 0].corr(valid.iloc[:, 1]))


def _safe_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result):
        return None
    return result


def _safe_mean(values: Sequence[float]) -> float:
    clean = [v for v in values if not math.isnan(v)]
    return float(sum(clean) / len(clean)) if clean else float("nan")


def _safe_rate(numerator: int, denominator: int) -> float:
    return float(numerator) / float(denominator) if denominator > 0 else float("nan")


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _load_market_metadata(raw_markets_dir: Path | None) -> dict[str, dict[str, Any]]:
    if raw_markets_dir is None or not raw_markets_dir.exists():
        return {}
    metadata: dict[str, dict[str, Any]] = {}
    for path in raw_markets_dir.glob("*.json"):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        ticker = str(raw.get("ticker") or path.stem)
        metadata[ticker] = raw
    return metadata


def _infer_raw_markets_dir(feature_dir: Path, raw_markets_dir: Path | None) -> Path | None:
    if raw_markets_dir is not None:
        return raw_markets_dir
    candidate = feature_dir.parent / "raw" / "markets"
    return candidate if candidate.exists() else None


def _row_close_time(metadata: dict[str, Any]) -> datetime | None:
    return _parse_timestamp(metadata.get("close_time"))


def _market_is_resolved(metadata: dict[str, Any], resolution_price: float | None) -> bool:
    if resolution_price is None:
        return False
    status = str(metadata.get("status") or "").lower()
    if not status:
        return True
    return status in _SETTLED_STATUSES


def _prepare_frame(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    if "timestamp" in frame.columns:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    else:
        frame["timestamp"] = pd.NaT
    if "close" in frame.columns:
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    return frame.sort_values("timestamp").reset_index(drop=True)


def _select_entry_row(
    df: pd.DataFrame,
    *,
    close_time: datetime | None,
    assumptions: KalshiExecutionAssumptions,
) -> pd.Series | None:
    valid = df.dropna(subset=["close"]).copy()
    if valid.empty:
        return None
    if assumptions.entry_timing_mode == "last_bar" or close_time is None:
        return valid.iloc[-1]
    if "timestamp" not in valid.columns:
        return None
    eligible = valid[valid["timestamp"].notna()]
    if eligible.empty:
        return None
    target_entry = close_time - timedelta(hours=assumptions.entry_offset_hours)
    eligible = eligible[eligible["timestamp"] <= target_entry]
    if eligible.empty:
        return None
    return eligible.iloc[-1]


def _resolve_exit(
    df: pd.DataFrame,
    *,
    entry_time: datetime,
    close_time: datetime | None,
    side: str,
    resolution_price: float,
    assumptions: KalshiExecutionAssumptions,
) -> tuple[float, datetime, str]:
    if assumptions.holding_window_hours is None:
        return resolution_price, close_time or entry_time, "resolution"

    target_exit = entry_time + timedelta(hours=assumptions.holding_window_hours)
    if close_time is not None and target_exit >= close_time:
        return resolution_price, close_time, "resolution"

    eligible = df[df["timestamp"].notna() & (df["timestamp"] >= target_exit) & df["close"].notna()]
    if eligible.empty:
        return resolution_price, close_time or target_exit, "resolution"

    exit_market_yes = float(eligible.iloc[0]["close"])
    if side == "yes":
        exit_fill = max(0.0, exit_market_yes - assumptions.exit_slippage_points)
    else:
        exit_fill = min(100.0, exit_market_yes + assumptions.exit_slippage_points)
    return exit_fill, eligible.iloc[0]["timestamp"].to_pydatetime(), "holding_window"


def _apply_entry_fill(entry_price_yes: float, side: str, assumptions: KalshiExecutionAssumptions) -> float:
    if side == "yes":
        return min(100.0, entry_price_yes + assumptions.entry_slippage_points)
    return max(0.0, entry_price_yes - assumptions.entry_slippage_points)


def _predicted_yes_probability(entry_price_yes: float, signal_value: float, scale: float) -> float:
    base_prob = min(max(entry_price_yes / 100.0, _PROB_CLIP_LOW), _PROB_CLIP_HIGH)
    logit = math.log(base_prob / (1.0 - base_prob))
    adjusted = logit + (signal_value / scale)
    prob = 1.0 / (1.0 + math.exp(-adjusted))
    return min(max(prob, _PROB_CLIP_LOW), _PROB_CLIP_HIGH)


def _confidence_bucket(confidence: float) -> str:
    for low, high, label in _CONFIDENCE_BUCKETS:
        if low <= confidence < high:
            return label
    return _CONFIDENCE_BUCKETS[-1][2]


def _trade_to_row(trade: KalshiBacktestTrade) -> dict[str, Any]:
    row = asdict(trade)
    row["confidence_bucket"] = _confidence_bucket(trade.confidence)
    return row


def _result_to_row(result: KalshiBacktestResult) -> dict[str, Any]:
    return asdict(result)


def _aggregate_trade_rows(rows: list[dict[str, Any]], group_field: str) -> list[dict[str, Any]]:
    if not rows:
        return []
    df = pd.DataFrame(rows)
    grouped_rows: list[dict[str, Any]] = []
    for group_value, group_df in df.groupby(group_field, dropna=False):
        realized_returns = pd.to_numeric(group_df["realized_return"], errors="coerce").dropna()
        grouped_rows.append(
            {
                group_field: "unknown" if pd.isna(group_value) else group_value,
                "trade_count": int(len(group_df)),
                "win_rate": float((group_df["win"] == True).mean()),  # noqa: E712
                "avg_predicted_edge": _safe_mean(group_df["predicted_edge"].astype(float).tolist()),
                "avg_confidence": _safe_mean(group_df["confidence"].astype(float).tolist()),
                "realized_avg_return": _safe_mean(group_df["realized_return"].astype(float).tolist()),
                "brier_score": _safe_mean(group_df["brier_score"].astype(float).tolist()),
                "mean_realized_pnl_points": _safe_mean(group_df["realized_pnl_points"].astype(float).tolist()),
                "sharpe": _compute_sharpe(realized_returns) if not realized_returns.empty else 0.0,
            }
        )
    grouped_rows.sort(key=lambda row: str(row[group_field]))
    return grouped_rows


def _safe_json_value(value: Any) -> float | str | bool | None:
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, str):
        return value
    safe = _safe_float(value)
    if safe is not None:
        return safe
    return str(value)


def _extract_supporting_features(signal_row: pd.Series) -> dict[str, Any]:
    ignored = {"signal_value", "direction", "confidence", "signal_probability", "signal_family"}
    features: dict[str, Any] = {}
    for key, value in signal_row.items():
        if key in ignored:
            continue
        features[str(key)] = _safe_json_value(value)
    return features


def _aggregate_supporting_features(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    feature_buckets: dict[str, list[Any]] = {}
    for row in rows:
        supporting = row.get("supporting_features") or {}
        if not isinstance(supporting, dict):
            continue
        for key, value in supporting.items():
            feature_buckets.setdefault(str(key), []).append(value)

    summary: dict[str, Any] = {}
    for key, values in feature_buckets.items():
        numeric_values = [_safe_float(value) for value in values]
        numeric_clean = [value for value in numeric_values if value is not None]
        if numeric_clean:
            summary[key] = {
                "mean": _safe_mean(numeric_clean),
                "min": min(numeric_clean),
                "max": max(numeric_clean),
            }
            continue
        string_values = [str(value) for value in values if value is not None]
        if string_values:
            counts = pd.Series(string_values).value_counts().head(5)
            summary[key] = {"top_values": counts.to_dict()}
    return summary


def _aggregate_candidate_rows(rows: list[dict[str, Any]], group_field: str) -> list[dict[str, Any]]:
    if not rows:
        return []
    df = pd.DataFrame(rows)
    grouped_rows: list[dict[str, Any]] = []
    for group_value, group_df in df.groupby(group_field, dropna=False):
        grouped_rows.append(
            {
                group_field: "unknown" if pd.isna(group_value) else group_value,
                "signal_count": int(len(group_df)),
                "avg_signal_value": _safe_mean(pd.to_numeric(group_df["signal_value"], errors="coerce").dropna().tolist()),
                "avg_confidence": _safe_mean(pd.to_numeric(group_df["confidence"], errors="coerce").dropna().tolist()),
                "threshold_pass_rate": float(pd.to_numeric(group_df["threshold_passed"], errors="coerce").fillna(0.0).mean()),
            }
        )
    grouped_rows.sort(key=lambda row: str(row[group_field]))
    return grouped_rows


def _build_report(
    *,
    generated_at: str,
    assumptions: KalshiExecutionAssumptions,
    summary: dict[str, Any],
    family_rows: list[dict[str, Any]],
    category_rows: list[dict[str, Any]],
    confidence_rows: list[dict[str, Any]],
) -> str:
    lines = [
        "# Kalshi Resolved-Market Backtest Report",
        "",
        f"Generated: {generated_at}",
        "",
        "## Overall Summary",
        "",
        f"- Markets evaluated: {summary['total_markets_evaluated']}",
        f"- Candidate signals: {summary['total_candidate_signals']}",
        f"- Executed trades: {summary['total_executed_trades']}",
        f"- Win rate: {_fmt(summary['win_rate'], '.1%')}",
        f"- Average predicted edge: {_fmt(summary['average_predicted_edge'], '.4f')}",
        f"- Average confidence: {_fmt(summary['average_confidence'], '.4f')}",
        f"- Realized average return: {_fmt(summary['realized_average_return'], '.4f')}",
        f"- Brier score: {_fmt(summary['brier_score'], '.4f')}",
        "",
        "## Execution Assumptions",
        "",
        f"- Entry timing mode: `{assumptions.entry_timing_mode}`",
        f"- Entry offset hours: {assumptions.entry_offset_hours}",
        f"- Holding window hours: {assumptions.holding_window_hours if assumptions.holding_window_hours is not None else 'resolution'}",
        f"- Entry slippage points: {assumptions.entry_slippage_points}",
        f"- Exit slippage points: {assumptions.exit_slippage_points}",
        f"- Signal probability scale: {assumptions.signal_probability_scale}",
        "",
        "## Breakdown By Signal Family",
        "",
        "| Signal Family | Markets | Candidates | Trades | Avg Signal | Win Rate | Avg Pred Edge | Avg Conf | Avg Return | Brier | IC | Sharpe |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in family_rows:
        lines.append(
            f"| {row['signal_family']} | {row['markets_evaluated']} | {row['candidate_signals']} | {row['n_trades']} | {_fmt(row['avg_signal_value'], '.4f')} "
            f"| {_fmt(row['win_rate'], '.1%')} | {_fmt(row['avg_predicted_edge'], '.4f')} "
            f"| {_fmt(row['avg_confidence'], '.4f')} | {_fmt(row['realized_avg_return'], '.4f')} "
            f"| {_fmt(row['brier_score'], '.4f')} | {_fmt(row['ic'], '.4f')} | {_fmt(row['sharpe'], '.2f')} |"
        )
    lines += [
        "",
        "## Breakdown By Category",
        "",
        "| Category | Trades | Win Rate | Avg Pred Edge | Avg Conf | Avg Return | Brier |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in category_rows:
        lines.append(
            f"| {row['category']} | {row['trade_count']} | {_fmt(row['win_rate'], '.1%')} "
            f"| {_fmt(row['avg_predicted_edge'], '.4f')} | {_fmt(row['avg_confidence'], '.4f')} "
            f"| {_fmt(row['realized_avg_return'], '.4f')} | {_fmt(row['brier_score'], '.4f')} |"
        )
    lines += [
        "",
        "## Breakdown By Confidence Bucket",
        "",
        "| Confidence Bucket | Trades | Win Rate | Avg Pred Edge | Avg Return | Brier |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in confidence_rows:
        lines.append(
            f"| {row['confidence_bucket']} | {row['trade_count']} | {_fmt(row['win_rate'], '.1%')} "
            f"| {_fmt(row['avg_predicted_edge'], '.4f')} | {_fmt(row['realized_avg_return'], '.4f')} "
            f"| {_fmt(row['brier_score'], '.4f')} |"
        )
    return "\n".join(lines)


def _fmt(value: Any, spec: str) -> str:
    safe = _safe_float(value)
    if safe is None:
        return "n/a"
    return format(safe, spec)


class KalshiBacktester:
    """
    Evaluate Kalshi signal families against locally stored resolved markets.

    The existing ``run(...)`` contract is preserved, but the runner now writes
    richer structured artifacts and makes execution assumptions explicit.
    """

    def __init__(
        self,
        *,
        entry_threshold: float = 0.5,
        long_only: bool = False,
        entry_timing_mode: str = "hours_before_close",
        entry_offset_hours: float = 24.0,
        holding_window_hours: float | None = None,
        entry_slippage_points: float = 0.0,
        exit_slippage_points: float = 0.0,
        signal_probability_scale: float = 8.0,
    ) -> None:
        self.entry_threshold = entry_threshold
        self.long_only = long_only
        self.execution_assumptions = KalshiExecutionAssumptions(
            entry_timing_mode=entry_timing_mode,
            entry_offset_hours=entry_offset_hours,
            holding_window_hours=holding_window_hours,
            entry_slippage_points=entry_slippage_points,
            exit_slippage_points=exit_slippage_points,
            signal_probability_scale=signal_probability_scale,
        )

    def run(
        self,
        feature_dir: Path,
        resolution_data: pd.DataFrame,
        signal_families: Sequence[KalshiSignalFamily],
        output_dir: Path,
        *,
        raw_markets_dir: Path | None = None,
    ) -> list[KalshiBacktestResult]:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        resolution_map: dict[str, float] = {}
        if (
            not resolution_data.empty
            and "ticker" in resolution_data.columns
            and "resolution_price" in resolution_data.columns
        ):
            for _, row in resolution_data.iterrows():
                resolution_price = _safe_float(row.get("resolution_price"))
                if resolution_price is None:
                    continue
                resolution_map[str(row["ticker"])] = resolution_price

        feature_dir = Path(feature_dir)
        feature_files = sorted(feature_dir.glob("*.parquet"))
        metadata_map = _load_market_metadata(_infer_raw_markets_dir(feature_dir, raw_markets_dir))

        trades: list[KalshiBacktestTrade] = []
        candidate_rows: list[dict[str, Any]] = []
        family_candidate_signals: dict[str, int] = {family.name: 0 for family in signal_families}
        family_markets_evaluated: dict[str, set[str]] = {family.name: set() for family in signal_families}
        family_signal_values: dict[str, list[float]] = {family.name: [] for family in signal_families}
        family_forward_edges: dict[str, list[float]] = {family.name: [] for family in signal_families}
        overall_markets_evaluated: set[str] = set()

        for fpath in feature_files:
            ticker = fpath.stem
            resolution_price = resolution_map.get(ticker)
            metadata = metadata_map.get(ticker, {})
            if not _market_is_resolved(metadata, resolution_price):
                continue

            try:
                df = pd.read_parquet(fpath)
            except Exception:
                continue
            if df.empty:
                continue

            frame = _prepare_frame(df)
            close_time = _row_close_time(metadata)
            entry_row = _select_entry_row(
                frame,
                close_time=close_time,
                assumptions=self.execution_assumptions,
            )
            if entry_row is None:
                continue

            entry_time = entry_row.get("timestamp")
            if pd.isna(entry_time):
                continue
            entry_time = pd.Timestamp(entry_time).to_pydatetime()
            entry_price_yes = _safe_float(entry_row.get("close"))
            if entry_price_yes is None:
                continue

            overall_markets_evaluated.add(ticker)
            category = str(metadata.get("category") or "unknown")
            market_title = str(metadata.get("title") or ticker)
            actual_outcome = 1 if float(resolution_price or 0.0) >= 50.0 else 0

            for family in signal_families:
                signal_frame = family.build_signal_frame(frame)
                signal = pd.to_numeric(signal_frame["signal_value"], errors="coerce")
                if signal.isna().all():
                    continue
                signal_value_raw = signal.loc[entry_row.name]
                signal_value = _safe_float(signal_value_raw)
                if signal_value is None:
                    continue
                signal_row = signal_frame.loc[entry_row.name]
                signal_confidence = _safe_float(signal_row.get("confidence"))
                signal_probability = _safe_float(signal_row.get("signal_probability"))
                supporting_features = _extract_supporting_features(signal_row)

                family_markets_evaluated[family.name].add(ticker)
                family_candidate_signals[family.name] += 1
                family_signal_values[family.name].append(signal_value)
                family_forward_edges[family.name].append(float(resolution_price or 0.0) - entry_price_yes)

                candidate_rows.append(
                    {
                        "ticker": ticker,
                        "signal_family": family.name,
                        "category": category,
                        "signal_value": signal_value,
                        "confidence": signal_confidence,
                        "signal_probability": signal_probability,
                        "threshold_passed": abs(signal_value) >= self.entry_threshold,
                        "supporting_features": supporting_features,
                    }
                )

                if abs(signal_value) < self.entry_threshold:
                    continue

                if signal_value > 0:
                    side = "yes"
                elif self.long_only:
                    continue
                else:
                    side = "no"

                predicted_yes_prob = signal_probability if signal_probability is not None else _predicted_yes_probability(
                    entry_price_yes,
                    signal_value,
                    self.execution_assumptions.signal_probability_scale,
                )
                market_yes_prob = entry_price_yes / 100.0
                confidence = signal_confidence if signal_confidence is not None else abs(predicted_yes_prob - market_yes_prob)
                if side == "yes":
                    predicted_edge = predicted_yes_prob - market_yes_prob
                else:
                    predicted_edge = market_yes_prob - predicted_yes_prob

                entry_fill_yes = _apply_entry_fill(entry_price_yes, side, self.execution_assumptions)
                exit_price_yes, exit_time, exit_reason = _resolve_exit(
                    frame,
                    entry_time=entry_time,
                    close_time=close_time,
                    side=side,
                    resolution_price=float(resolution_price or 0.0),
                    assumptions=self.execution_assumptions,
                )

                if side == "yes":
                    realized_pnl_points = exit_price_yes - entry_fill_yes
                    capital_at_risk = max(entry_fill_yes, 1.0)
                else:
                    realized_pnl_points = entry_fill_yes - exit_price_yes
                    capital_at_risk = max(100.0 - entry_fill_yes, 1.0)
                realized_return = realized_pnl_points / capital_at_risk
                brier_score = (predicted_yes_prob - actual_outcome) ** 2

                trades.append(
                    KalshiBacktestTrade(
                        ticker=ticker,
                        signal_family=family.name,
                        category=category,
                        market_title=market_title,
                        side=side,
                        entry_time=entry_time.isoformat(),
                        exit_time=exit_time.isoformat(),
                        close_time=close_time.isoformat() if close_time else None,
                        exit_reason=exit_reason,
                        entry_signal=signal_value,
                        entry_threshold=self.entry_threshold,
                        entry_price_yes=entry_price_yes,
                        entry_fill_yes=entry_fill_yes,
                        exit_price_yes=exit_price_yes,
                        predicted_yes_probability=predicted_yes_prob,
                        predicted_edge=predicted_edge,
                        confidence=confidence,
                        resolution_price=float(resolution_price or 0.0),
                        actual_outcome=actual_outcome,
                        realized_pnl_points=realized_pnl_points,
                        realized_return=realized_return,
                        win=realized_pnl_points > 0.0,
                        brier_score=brier_score,
                        supporting_features=supporting_features,
                    )
                )

        family_results: list[KalshiBacktestResult] = []
        trade_rows = [_trade_to_row(trade) for trade in trades]

        for family in signal_families:
            family_trade_rows = [row for row in trade_rows if row["signal_family"] == family.name]
            family_returns = pd.Series([row["realized_return"] for row in family_trade_rows], dtype=float)
            family_equity = family_returns.cumsum()
            family_signal_series = pd.Series(family_signal_values[family.name], dtype=float)
            family_edge_series = pd.Series(family_forward_edges[family.name], dtype=float)
            result = KalshiBacktestResult(
                signal_family=family.name,
                markets_evaluated=len(family_markets_evaluated[family.name]),
                candidate_signals=family_candidate_signals[family.name],
                n_trades=len(family_trade_rows),
                win_rate=_safe_rate(sum(1 for row in family_trade_rows if row["win"]), len(family_trade_rows)),
                mean_edge=_safe_mean([row["realized_pnl_points"] for row in family_trade_rows]),
                sharpe=_compute_sharpe(family_returns) if not family_returns.empty else 0.0,
                max_drawdown=_compute_max_drawdown(family_equity) if not family_equity.empty else 0.0,
                ic=_compute_ic(family_signal_series, family_edge_series),
                avg_predicted_edge=_safe_mean([row["predicted_edge"] for row in family_trade_rows]),
                avg_confidence=_safe_mean([row["confidence"] for row in family_trade_rows]),
                realized_avg_return=_safe_mean([row["realized_return"] for row in family_trade_rows]),
                brier_score=_safe_mean([row["brier_score"] for row in family_trade_rows]),
                avg_signal_value=_safe_mean([row["signal_value"] for row in candidate_rows if row["signal_family"] == family.name]),
            )
            family_results.append(result)

        summary_rows = [_result_to_row(result) for result in family_results]
        results_df = pd.DataFrame(summary_rows)
        results_df.to_csv(output_dir / "backtest_results.csv", index=False)

        category_rows = _aggregate_trade_rows(trade_rows, "category")
        confidence_rows = _aggregate_trade_rows(trade_rows, "confidence_bucket")
        family_diagnostic_rows = _aggregate_trade_rows(trade_rows, "signal_family")
        candidate_df = pd.DataFrame(candidate_rows)
        candidate_summary_rows: list[dict[str, Any]] = []
        candidate_category_rows: list[dict[str, Any]] = []
        candidate_confidence_rows: list[dict[str, Any]] = []
        supporting_feature_summary: dict[str, Any] = {}
        if not candidate_df.empty:
            candidate_df["confidence_bucket"] = candidate_df["confidence"].apply(
                lambda value: _confidence_bucket(float(value)) if _safe_float(value) is not None else "unknown"
            )
            for family_name, family_df in candidate_df.groupby("signal_family"):
                candidate_summary_rows.append(
                    {
                        "signal_family": str(family_name),
                        "signal_count": int(len(family_df)),
                        "avg_signal_value": _safe_mean(pd.to_numeric(family_df["signal_value"], errors="coerce").dropna().tolist()),
                        "avg_confidence": _safe_mean(pd.to_numeric(family_df["confidence"], errors="coerce").dropna().tolist()),
                        "threshold_pass_rate": float(pd.to_numeric(family_df["threshold_passed"], errors="coerce").fillna(0.0).mean()),
                    }
                )
                supporting_feature_summary[str(family_name)] = _aggregate_supporting_features(family_df.to_dict("records"))
            candidate_category_rows = _aggregate_candidate_rows(candidate_df.to_dict("records"), "category")
            candidate_confidence_rows = _aggregate_candidate_rows(candidate_df.to_dict("records"), "confidence_bucket")
            candidate_summary_rows.sort(key=lambda row: row["signal_family"])

        overall_summary = {
            "generated_at": datetime.now(UTC).isoformat(),
            "total_markets_evaluated": len(overall_markets_evaluated),
            "total_candidate_signals": int(sum(family_candidate_signals.values())),
            "total_executed_trades": len(trade_rows),
            "win_rate": _safe_rate(sum(1 for row in trade_rows if row["win"]), len(trade_rows)),
            "average_predicted_edge": _safe_mean([row["predicted_edge"] for row in trade_rows]),
            "average_confidence": _safe_mean([row["confidence"] for row in trade_rows]),
            "realized_average_return": _safe_mean([row["realized_return"] for row in trade_rows]),
            "brier_score": _safe_mean([row["brier_score"] for row in trade_rows]),
            "entry_threshold": self.entry_threshold,
            "long_only": self.long_only,
            "execution_assumptions": asdict(self.execution_assumptions),
            "candidate_signal_summary": candidate_summary_rows,
        }

        diagnostics = {
            "generated_at": overall_summary["generated_at"],
            "execution_assumptions": asdict(self.execution_assumptions),
            "by_signal_family": summary_rows,
            "by_category": category_rows,
            "by_confidence_bucket": confidence_rows,
            "signal_family_trade_breakdown": family_diagnostic_rows,
            "candidate_signal_summary": candidate_summary_rows,
            "candidate_by_category": candidate_category_rows,
            "candidate_by_confidence_bucket": candidate_confidence_rows,
            "supporting_feature_summaries": supporting_feature_summary,
        }

        (output_dir / "kalshi_backtest_summary.json").write_text(
            json.dumps(overall_summary, indent=2, default=str),
            encoding="utf-8",
        )
        (output_dir / "kalshi_signal_diagnostics.json").write_text(
            json.dumps(diagnostics, indent=2, default=str),
            encoding="utf-8",
        )
        trade_log_path = output_dir / "kalshi_trade_log.jsonl"
        with trade_log_path.open("w", encoding="utf-8") as handle:
            for row in trade_rows:
                handle.write(json.dumps(row, default=str) + "\n")

        report = _build_report(
            generated_at=overall_summary["generated_at"],
            assumptions=self.execution_assumptions,
            summary=overall_summary,
            family_rows=summary_rows,
            category_rows=category_rows,
            confidence_rows=confidence_rows,
        )
        (output_dir / "kalshi_backtest_report.md").write_text(report, encoding="utf-8")

        return family_results
