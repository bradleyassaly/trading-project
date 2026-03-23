from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.execution.transforms import build_executed_weights
from trading_platform.paper.models import PaperTradingConfig, PaperSignalSnapshot
from trading_platform.research.approved_model_state import load_approved_model_state
from trading_platform.research.alpha_lab.composite import (
    DEFAULT_COMPOSITE_CONFIG,
    build_component_weights,
    build_composite_scores,
    candidate_id,
    normalize_signal_by_date,
    select_low_redundancy_signals,
)
from trading_platform.research.alpha_lab.composite_portfolio import (
    CompositePortfolioConfig,
    apply_liquidity_filters,
    build_composite_portfolio_weights,
    build_liquidity_panel,
)
from trading_platform.research.alpha_lab.signals import build_signal
from trading_platform.signals.loaders import load_feature_frame


@dataclass(frozen=True)
class CompositePaperTargetResult:
    as_of: str
    latest_scores: dict[str, float]
    scheduled_target_weights: dict[str, float]
    effective_target_weights: dict[str, float]
    latest_prices: dict[str, float]
    skipped_symbols: list[str]
    diagnostics: dict[str, Any]


def _read_artifact_csv(artifact_dir: Path, filename: str) -> pd.DataFrame:
    path = artifact_dir / filename
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _read_artifact_json(artifact_dir: Path, filename: str) -> dict[str, Any]:
    path = artifact_dir / filename
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _load_feature_history(symbol: str) -> pd.DataFrame:
    feature_df = load_feature_frame(symbol).copy()
    if "timestamp" in feature_df.columns:
        feature_df["timestamp"] = pd.to_datetime(feature_df["timestamp"], errors="coerce")
        feature_df = feature_df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    else:
        raise ValueError("Feature frame must include a timestamp column for composite paper trading")
    if "symbol" not in feature_df.columns:
        feature_df["symbol"] = symbol
    return feature_df


def _ensure_promoted_signal_columns(
    promoted_signals_df: pd.DataFrame,
    *,
    artifact_dir: Path | None,
    composite_horizon: int,
) -> pd.DataFrame:
    if promoted_signals_df.empty:
        return promoted_signals_df
    normalized = promoted_signals_df.copy()
    if {"signal_family", "lookback", "horizon"}.issubset(normalized.columns):
        return normalized
    manifest = _read_artifact_json(artifact_dir, "research_run.json") if artifact_dir is not None else {}
    top_candidate = manifest.get("top_candidate", {}) if isinstance(manifest, dict) else {}
    evaluation_periods = manifest.get("evaluation_periods", {}) if isinstance(manifest, dict) else {}
    default_lookback = top_candidate.get("lookback")
    if default_lookback is None:
        lookbacks = evaluation_periods.get("lookbacks", []) if isinstance(evaluation_periods, dict) else []
        default_lookback = lookbacks[0] if lookbacks else 20
    default_horizon = top_candidate.get("horizon")
    if default_horizon is None:
        horizons = evaluation_periods.get("horizons", []) if isinstance(evaluation_periods, dict) else []
        default_horizon = horizons[0] if horizons else composite_horizon
    if "lookback" not in normalized.columns:
        normalized["lookback"] = int(default_lookback)
    if "horizon" not in normalized.columns:
        normalized["horizon"] = int(default_horizon)
    return normalized


def _build_component_panel(
    selected_signals_df: pd.DataFrame,
    *,
    feature_data_by_symbol: dict[str, pd.DataFrame],
) -> tuple[dict[tuple[str, int, int], pd.DataFrame], list[dict[str, Any]]]:
    score_panel_by_candidate: dict[tuple[str, int, int], pd.DataFrame] = {}
    component_rows: list[dict[str, Any]] = []
    for _, row in selected_signals_df.iterrows():
        candidate_key = (
            str(row["signal_family"]),
            int(row["lookback"]),
            int(row["horizon"]),
        )
        panel_frames: list[pd.DataFrame] = []
        for symbol, feature_df in feature_data_by_symbol.items():
            signal = build_signal(
                feature_df,
                signal_family=candidate_key[0],
                lookback=candidate_key[1],
            )
            signal_frame = feature_df[["timestamp", "symbol"]].copy()
            signal_frame["signal"] = signal
            signal_frame = signal_frame.dropna(subset=["signal"])
            if signal_frame.empty:
                continue
            panel_frames.append(signal_frame)
        if not panel_frames:
            continue
        candidate_panel = pd.concat(panel_frames, ignore_index=True).sort_values(
            ["timestamp", "symbol"]
        ).reset_index(drop=True)
        score_panel_by_candidate[candidate_key] = candidate_panel
        component_rows.append(
            {
                "candidate_id": candidate_id(*candidate_key),
                "signal_family": candidate_key[0],
                "lookback": candidate_key[1],
                "horizon": candidate_key[2],
            }
        )
    return score_panel_by_candidate, component_rows


def _latest_timestamp_from_features(feature_data_by_symbol: dict[str, pd.DataFrame]) -> pd.Timestamp | None:
    timestamps = [
        pd.to_datetime(df["timestamp"]).max()
        for df in feature_data_by_symbol.values()
        if not df.empty and "timestamp" in df.columns
    ]
    return max(timestamps) if timestamps else None


def _build_latest_component_diagnostics(
    selected_signals_df: pd.DataFrame,
    *,
    score_panel_by_candidate: dict[tuple[str, int, int], pd.DataFrame],
    latest_timestamp: pd.Timestamp,
    weighting_scheme: str,
) -> list[dict[str, Any]]:
    if selected_signals_df.empty:
        return []

    weighted_components = build_component_weights(
        selected_signals_df,
        weighting_scheme=weighting_scheme,
        quality_metric=DEFAULT_COMPOSITE_CONFIG.quality_metric,
    )
    rows: list[dict[str, Any]] = []
    for _, row in weighted_components.iterrows():
        candidate_key = (
            str(row["signal_family"]),
            int(row["lookback"]),
            int(row["horizon"]),
        )
        score_panel = score_panel_by_candidate.get(candidate_key, pd.DataFrame())
        if score_panel.empty:
            continue
        normalized = normalize_signal_by_date(score_panel)
        latest_slice = normalized.loc[normalized["timestamp"] == latest_timestamp].copy()
        if latest_slice.empty:
            continue
        latest_slice["candidate_id"] = candidate_id(*candidate_key)
        latest_slice["component_weight"] = float(row["component_weight"])
        latest_slice["weighted_score"] = (
            pd.to_numeric(latest_slice["normalized_signal"], errors="coerce")
            * float(row["component_weight"])
        )
        latest_slice["signal_family"] = candidate_key[0]
        latest_slice["lookback"] = candidate_key[1]
        latest_slice["horizon"] = candidate_key[2]
        rows.extend(latest_slice.to_dict(orient="records"))
    return rows


def build_composite_paper_snapshot(
    *,
    config: PaperTradingConfig,
) -> tuple[PaperSignalSnapshot, dict[str, Any]]:
    artifact_dir = Path(config.composite_artifact_dir) if config.composite_artifact_dir else None
    approved_model_state: dict[str, Any] = {}
    approved_model_state_path = config.approved_model_state_path

    if approved_model_state_path:
        approved_model_state = load_approved_model_state(approved_model_state_path)
        if artifact_dir is None and approved_model_state.get("source_artifact_dir"):
            artifact_dir = Path(str(approved_model_state["source_artifact_dir"]))
    elif artifact_dir is not None:
        try:
            approved_model_state = load_approved_model_state(artifact_dir)
            approved_model_state_path = str((artifact_dir / "approved" / "approved_model_state.json"))
        except FileNotFoundError:
            approved_model_state = {}

    if artifact_dir is None and not approved_model_state:
        raise ValueError("Composite paper trading requires approved_model_state or composite_artifact_dir")

    promoted_signals_df = pd.DataFrame(approved_model_state.get("promoted_signals", []))
    if promoted_signals_df.empty and artifact_dir is not None:
        promoted_signals_df = _read_artifact_csv(artifact_dir, "promoted_signals.csv")
    promoted_signals_df = _ensure_promoted_signal_columns(
        promoted_signals_df,
        artifact_dir=artifact_dir,
        composite_horizon=int(config.composite_horizon),
    )

    redundancy_df = pd.DataFrame(approved_model_state.get("redundancy_report", []))
    if redundancy_df.empty and artifact_dir is not None:
        redundancy_df = _read_artifact_csv(artifact_dir, "redundancy_report.csv")
        if redundancy_df.empty:
            redundancy_df = _read_artifact_csv(artifact_dir, "redundancy_diagnostics.csv")

    feature_data_by_symbol: dict[str, pd.DataFrame] = {}
    skipped_symbols: list[str] = []
    skipped_reasons: dict[str, str] = {}
    for symbol in config.symbols:
        try:
            feature_data_by_symbol[symbol] = _load_feature_history(symbol)
        except Exception as exc:
            skipped_symbols.append(symbol)
            skipped_reasons[symbol] = repr(exc)

    if not feature_data_by_symbol:
        raise ValueError(
            f"No valid symbol frames available for composite paper trading. Reasons: {skipped_reasons}"
        )

    latest_timestamp = _latest_timestamp_from_features(feature_data_by_symbol)
    if latest_timestamp is None:
        raise ValueError("No timestamps available for composite paper trading")

    if promoted_signals_df.empty:
        empty_index = pd.DatetimeIndex([latest_timestamp])
        empty_scores = pd.DataFrame(index=empty_index)
        closes = pd.concat(
            [
                feature_df.set_index("timestamp")["close"].rename(symbol)
                for symbol, feature_df in feature_data_by_symbol.items()
                if "close" in feature_df.columns
            ],
            axis=1,
        ).sort_index().ffill()
        asset_returns = closes.pct_change().fillna(0.0)
        diagnostics = {
            "signal_source": "composite",
            "reason": "no_approved_signals",
            "artifact_dir": str(artifact_dir),
            "selected_signals": [],
            "excluded_signals": [],
            "latest_component_scores": [],
            "latest_composite_scores": [],
            "skipped_symbols": skipped_symbols,
            "skipped_reasons": skipped_reasons,
        }
        return (
            PaperSignalSnapshot(
                asset_returns=asset_returns,
                scores=empty_scores,
                closes=closes,
                skipped_symbols=skipped_symbols,
                metadata={"feature_data_by_symbol": feature_data_by_symbol},
            ),
            diagnostics,
        )

    composite_inputs = approved_model_state.get("composite_inputs", {})
    selected_signals_records = (
        composite_inputs.get("horizons", {})
        .get(str(int(config.composite_horizon)), {})
        .get("selected_signals", [])
        if isinstance(composite_inputs, dict)
        else []
    )
    excluded_rows = (
        composite_inputs.get("horizons", {})
        .get(str(int(config.composite_horizon)), {})
        .get("excluded_signals", [])
        if isinstance(composite_inputs, dict)
        else []
    )
    if selected_signals_records:
        selected_signals_df = pd.DataFrame(selected_signals_records)
    else:
        selected_signals_df, excluded_rows = select_low_redundancy_signals(
            promoted_signals_df,
            redundancy_df,
            horizon=int(config.composite_horizon),
            redundancy_corr_threshold=DEFAULT_COMPOSITE_CONFIG.redundancy_corr_threshold,
        )
    score_panel_by_candidate, _ = _build_component_panel(
        selected_signals_df,
        feature_data_by_symbol=feature_data_by_symbol,
    )
    composite_scores_df = build_composite_scores(
        selected_signals_df,
        score_panel_by_candidate=score_panel_by_candidate,
        weighting_scheme=config.composite_weighting_scheme,
        quality_metric=DEFAULT_COMPOSITE_CONFIG.quality_metric,
    )
    closes = pd.concat(
        [
            feature_df.set_index("timestamp")["close"].rename(symbol)
            for symbol, feature_df in feature_data_by_symbol.items()
            if "close" in feature_df.columns
        ],
        axis=1,
    ).sort_index().ffill()
    asset_returns = closes.pct_change().fillna(0.0)
    score_matrix = (
        composite_scores_df.pivot(index="timestamp", columns="symbol", values="composite_score")
        .sort_index()
        .sort_index(axis=1)
        if not composite_scores_df.empty
        else pd.DataFrame(index=closes.index)
    )
    component_rows = _build_latest_component_diagnostics(
        selected_signals_df,
        score_panel_by_candidate=score_panel_by_candidate,
        latest_timestamp=latest_timestamp,
        weighting_scheme=config.composite_weighting_scheme,
    )
    latest_composite_scores = composite_scores_df.loc[
        composite_scores_df["timestamp"] == latest_timestamp
    ].copy()
    diagnostics = {
        "signal_source": "composite",
        "artifact_dir": str(artifact_dir) if artifact_dir is not None else "",
        "approved_model_state_path": approved_model_state_path or "",
        "selected_signals": selected_signals_df[
            ["signal_family", "lookback", "horizon"]
        ].to_dict(orient="records")
        if not selected_signals_df.empty
        else [],
        "excluded_signals": excluded_rows,
        "latest_component_scores": component_rows,
        "latest_composite_scores": latest_composite_scores.to_dict(orient="records"),
        "skipped_symbols": skipped_symbols,
        "skipped_reasons": skipped_reasons,
        "weighting_scheme": config.composite_weighting_scheme,
        "portfolio_mode": config.composite_portfolio_mode,
        "horizon": int(config.composite_horizon),
    }
    return (
        PaperSignalSnapshot(
            asset_returns=asset_returns,
            scores=score_matrix,
            closes=closes,
            skipped_symbols=skipped_symbols,
            metadata={"feature_data_by_symbol": feature_data_by_symbol},
        ),
        diagnostics,
    )


def compute_latest_composite_target_weights(
    *,
    config: PaperTradingConfig,
    snapshot: PaperSignalSnapshot,
    snapshot_diagnostics: dict[str, Any],
) -> CompositePaperTargetResult:
    closes = snapshot.closes.sort_index().ffill()
    latest_timestamp = closes.index.max()
    as_of = str(pd.Timestamp(latest_timestamp).date())
    latest_prices = {
        symbol: float(price)
        for symbol, price in closes.loc[latest_timestamp].fillna(0.0).items()
        if float(price) > 0.0
    }
    latest_scores = (
        {
            symbol: float(score)
            for symbol, score in snapshot.scores.loc[snapshot.scores.index.max()].fillna(0.0).items()
        }
        if not snapshot.scores.empty
        else {}
    )

    if snapshot.scores.empty:
        diagnostics = dict(snapshot_diagnostics)
        diagnostics["target_construction"] = {
            "selected_symbols": [],
            "selection_count": 0,
            "raw_total_weight": 0.0,
            "scheduled_total_weight": 0.0,
            "effective_total_weight": 0.0,
            "reason": snapshot_diagnostics.get("reason", "no_composite_scores"),
        }
        diagnostics["liquidity_exclusions"] = []
        diagnostics["approved_target_weights"] = []
        return CompositePaperTargetResult(
            as_of=as_of,
            latest_scores=latest_scores,
            scheduled_target_weights={},
            effective_target_weights={},
            latest_prices=latest_prices,
            skipped_symbols=snapshot.skipped_symbols,
            diagnostics=diagnostics,
        )

    composite_scores_df = (
        snapshot.scores.stack()
        .reset_index(name="composite_score")
        .rename(columns={"level_1": "symbol"})
    )
    composite_scores_df["horizon"] = int(config.composite_horizon)
    composite_scores_df["weighting_scheme"] = config.composite_weighting_scheme
    composite_scores_df["component_count"] = pd.NA
    composite_scores_df["selected_signal_count"] = pd.NA

    portfolio_config = CompositePortfolioConfig(
        top_n=config.top_n,
        long_quantile=config.composite_long_quantile,
        short_quantile=config.composite_short_quantile,
        max_weight=config.max_weight,
        min_price=config.min_price,
        min_volume=config.min_volume,
        min_avg_dollar_volume=config.min_avg_dollar_volume,
        max_adv_participation=config.max_adv_participation,
        max_position_pct_of_adv=config.max_position_pct_of_adv,
        max_notional_per_name=config.max_notional_per_name,
        rebalance_frequency=config.rebalance_frequency,
        timing=config.timing,
        modes=(config.composite_portfolio_mode,),
    )
    weights_df = build_composite_portfolio_weights(composite_scores_df, config=portfolio_config)
    if not weights_df.empty:
        weights_df = weights_df.loc[
            (weights_df["horizon"] == int(config.composite_horizon))
            & (weights_df["weighting_scheme"] == config.composite_weighting_scheme)
            & (weights_df["portfolio_mode"] == config.composite_portfolio_mode)
        ].copy()

    feature_data_by_symbol = snapshot.metadata.get("feature_data_by_symbol", {})
    symbol_data = {
        symbol: frame[["timestamp", "close", "volume"]].copy()
        if "volume" in frame.columns
        else frame[["timestamp", "close"]].copy()
        for symbol, frame in feature_data_by_symbol.items()
    }
    liquidity_panel = build_liquidity_panel(symbol_data)
    filtered_weights_df, exclusions_df, _ = apply_liquidity_filters(
        weights_df,
        liquidity_panel=liquidity_panel,
        config=portfolio_config,
    )
    if filtered_weights_df.empty:
        diagnostics = dict(snapshot_diagnostics)
        diagnostics["target_construction"] = {
            "selected_symbols": [],
            "selection_count": 0,
            "raw_total_weight": 0.0,
            "scheduled_total_weight": 0.0,
            "effective_total_weight": 0.0,
            "reason": "no_eligible_names",
        }
        diagnostics["liquidity_exclusions"] = exclusions_df.to_dict(orient="records")
        diagnostics["approved_target_weights"] = []
        return CompositePaperTargetResult(
            as_of=as_of,
            latest_scores=latest_scores,
            scheduled_target_weights={},
            effective_target_weights={},
            latest_prices=latest_prices,
            skipped_symbols=snapshot.skipped_symbols,
            diagnostics=diagnostics,
        )

    weight_matrix = (
        filtered_weights_df.pivot(index="timestamp", columns="symbol", values="weight")
        .sort_index()
        .sort_index(axis=1)
        .fillna(0.0)
    )
    scheduled_weights_df, effective_weights_df = build_executed_weights(
        weight_matrix,
        policy=ExecutionPolicy(
            timing=config.timing,
            rebalance_frequency=config.rebalance_frequency,
        ),
    )
    latest_scheduled_series = scheduled_weights_df.loc[scheduled_weights_df.index.max()].fillna(0.0)
    latest_effective_series = effective_weights_df.loc[effective_weights_df.index.max()].fillna(0.0)
    latest_scheduled = {
        symbol: float(weight)
        for symbol, weight in latest_scheduled_series.items()
        if abs(float(weight)) > 0.0
    }
    latest_effective = {
        symbol: float(weight)
        for symbol, weight in latest_effective_series.items()
        if abs(float(weight)) > 0.0
    }
    approved_target_weights = [
        {
            "symbol": symbol,
            "scheduled_target_weight": float(latest_scheduled_series.get(symbol, 0.0)),
            "effective_target_weight": float(latest_effective_series.get(symbol, 0.0)),
            "latest_price": latest_prices.get(symbol),
            "latest_score": latest_scores.get(symbol),
        }
        for symbol in sorted(set(latest_scheduled) | set(latest_effective))
    ]
    diagnostics = dict(snapshot_diagnostics)
    diagnostics["target_construction"] = {
        "selected_symbols": sorted(latest_effective.keys()),
        "selection_count": int(len(latest_effective)),
        "raw_total_weight": float(weight_matrix.iloc[-1].fillna(0.0).sum()) if not weight_matrix.empty else 0.0,
        "scheduled_total_weight": float(latest_scheduled_series.sum()),
        "effective_total_weight": float(latest_effective_series.sum()),
    }
    diagnostics["liquidity_exclusions"] = exclusions_df.loc[
        exclusions_df["timestamp"] == pd.Timestamp(latest_timestamp)
    ].to_dict(orient="records")
    diagnostics["approved_target_weights"] = approved_target_weights
    return CompositePaperTargetResult(
        as_of=as_of,
        latest_scores=latest_scores,
        scheduled_target_weights=latest_scheduled,
        effective_target_weights=latest_effective,
        latest_prices=latest_prices,
        skipped_symbols=snapshot.skipped_symbols,
        diagnostics=diagnostics,
    )
