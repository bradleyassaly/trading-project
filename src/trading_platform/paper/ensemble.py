from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.paper.composite import (
    _apply_latest_market_data,
    _ensure_promoted_signal_columns,
    _filter_feature_history_to_as_of,
    _historical_price_source,
    _latest_price_source,
    _load_feature_history,
    _read_artifact_csv,
    _read_artifact_json,
)
from trading_platform.paper.models import PaperTradingConfig, PaperSignalSnapshot
from trading_platform.paper.price_diagnostics import (
    build_execution_price_snapshots,
    summarize_execution_price_snapshots,
)
from trading_platform.research.approved_model_state import load_approved_model_state
from trading_platform.research.alpha_lab.composite import candidate_id
from trading_platform.research.alpha_lab.signals import build_signal
from trading_platform.research.ensemble import EnsembleConfig, assign_member_weights, build_ensemble_scores, select_ensemble_members


def build_ensemble_paper_snapshot(
    *,
    config: PaperTradingConfig,
) -> tuple[PaperSignalSnapshot, dict[str, Any]]:
    artifact_dir = Path(config.composite_artifact_dir) if config.composite_artifact_dir else None
    approved_model_state: dict[str, Any] = {}
    if config.approved_model_state_path:
        approved_model_state = load_approved_model_state(config.approved_model_state_path)
        if artifact_dir is None and approved_model_state.get("source_artifact_dir"):
            artifact_dir = Path(str(approved_model_state["source_artifact_dir"]))
    elif artifact_dir is not None:
        try:
            approved_model_state = load_approved_model_state(artifact_dir)
        except FileNotFoundError:
            approved_model_state = {}

    promoted_signals_df = pd.DataFrame(approved_model_state.get("promoted_signals", []))
    if promoted_signals_df.empty and artifact_dir is not None:
        promoted_signals_df = _read_artifact_csv(artifact_dir, "promoted_signals.csv")
    promoted_signals_df = _ensure_promoted_signal_columns(
        promoted_signals_df,
        artifact_dir=artifact_dir,
        composite_horizon=int(config.composite_horizon),
    )
    if promoted_signals_df.empty:
        raise ValueError("Ensemble paper trading requires promoted_signals data")

    ensemble_config = EnsembleConfig(
        enabled=bool(config.ensemble_enabled),
        mode=str(config.ensemble_mode),
        weight_method=str(config.ensemble_weight_method),
        normalize_scores=str(config.ensemble_normalize_scores),
        max_members=int(config.ensemble_max_members),
        require_promoted_only=bool(config.ensemble_require_promoted_only),
        max_members_per_family=config.ensemble_max_members_per_family,
        minimum_member_observations=int(config.ensemble_minimum_member_observations),
        minimum_member_metric=config.ensemble_minimum_member_metric,
    )
    member_source = promoted_signals_df.copy()
    if "candidate_id" not in member_source.columns:
        member_source["candidate_id"] = member_source.apply(
            lambda row: candidate_id(
                str(row["signal_family"]),
                int(row["lookback"]),
                int(row["horizon"]),
            ),
            axis=1,
        )
    member_summary = assign_member_weights(
        select_ensemble_members(member_source, ensemble_config),
        ensemble_config,
    )
    included_members = member_summary.loc[member_summary["included_in_ensemble"]].copy()
    if included_members.empty:
        raise ValueError("No eligible ensemble members were selected for paper trading")

    feature_data_by_symbol: dict[str, pd.DataFrame] = {}
    historical_feature_data_by_symbol: dict[str, pd.DataFrame] = {}
    skipped_symbols: list[str] = []
    skipped_reasons: dict[str, str] = {}
    for symbol in config.symbols:
        try:
            loaded_frame = _filter_feature_history_to_as_of(
                _load_feature_history(symbol),
                config=config,
            )
            if loaded_frame.empty:
                raise ValueError("no rows available on or before replay_as_of_date")
            feature_data_by_symbol[symbol] = loaded_frame
            historical_feature_data_by_symbol[symbol] = loaded_frame.copy()
        except Exception as exc:
            skipped_symbols.append(symbol)
            skipped_reasons[symbol] = repr(exc)
    if not feature_data_by_symbol:
        raise ValueError(f"No valid symbol frames available for ensemble paper trading. Reasons: {skipped_reasons}")

    feature_data_by_symbol, latest_fallback_used = _apply_latest_market_data(
        config=config,
        feature_data_by_symbol=feature_data_by_symbol,
    )
    effective_latest_source = _latest_price_source(config) if not latest_fallback_used else _historical_price_source(config)
    price_snapshots = build_execution_price_snapshots(
        historical_frames=historical_feature_data_by_symbol,
        final_frames=feature_data_by_symbol,
        historical_source=_historical_price_source(config),
        latest_data_source=effective_latest_source,
        fallback_used=latest_fallback_used,
        latest_data_max_age_seconds=int(config.latest_data_max_age_seconds),
    )
    freshness_summary = summarize_execution_price_snapshots(
        price_snapshots,
        latest_data_source=effective_latest_source,
        fallback_used=latest_fallback_used,
    )

    horizon = int(config.composite_horizon)
    horizon_members = included_members.loc[included_members["horizon"].astype(int) == horizon].copy()
    if horizon_members.empty:
        horizon_members = included_members.copy()
    signal_frames: dict[str, pd.DataFrame] = {}
    for _, member in horizon_members.iterrows():
        member_id = str(member["member_id"])
        lookback = int(member["lookback"])
        signal_family = str(member["signal_family"])
        panel_frames: list[pd.DataFrame] = []
        for symbol, feature_df in feature_data_by_symbol.items():
            signal = build_signal(feature_df, signal_family=signal_family, lookback=lookback)
            panel = feature_df[["timestamp", "symbol"]].copy()
            panel["signal"] = signal
            panel = panel.dropna(subset=["signal"])
            if not panel.empty:
                panel_frames.append(panel)
        signal_frames[member_id] = (
            pd.concat(panel_frames, ignore_index=True).sort_values(["timestamp", "symbol"]).reset_index(drop=True)
            if panel_frames
            else pd.DataFrame(columns=["timestamp", "symbol", "signal"])
        )

    ensemble_scores_df = build_ensemble_scores(signal_frames, ensemble_config, horizon_members)
    if ensemble_scores_df.empty:
        raise ValueError("No ensemble score panel could be constructed for paper trading")

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
        ensemble_scores_df.pivot(index="timestamp", columns="symbol", values="ensemble_score")
        .sort_index()
        .sort_index(axis=1)
    )
    diagnostics: dict[str, Any] = {
        "signal_source": "ensemble",
        "artifact_dir": str(artifact_dir) if artifact_dir is not None else "",
        "ensemble_enabled": True,
        "ensemble_mode": ensemble_config.mode,
        "weighting_method": ensemble_config.weight_method,
        "normalize_scores": ensemble_config.normalize_scores,
        "member_count": int(horizon_members["included_in_ensemble"].sum()),
        "selected_members": horizon_members[
            ["member_id", "family", "normalized_weight", "raw_metric"]
        ].rename(columns={"family": "signal_family"}).to_dict(orient="records"),
        "final_signal_source": "ensemble",
        "historical_price_source": _historical_price_source(config),
        "latest_price_source": effective_latest_source,
        "latest_price_fallback_used": latest_fallback_used,
        **freshness_summary,
    }
    return (
        PaperSignalSnapshot(
            asset_returns=asset_returns,
            scores=score_matrix,
            closes=closes,
            skipped_symbols=skipped_symbols,
            metadata={
                "feature_data_by_symbol": feature_data_by_symbol,
                "price_snapshots": price_snapshots,
                "freshness_summary": freshness_summary,
                "ensemble_snapshot": ensemble_scores_df,
                "ensemble_member_summary": horizon_members,
            },
        ),
        diagnostics,
    )
