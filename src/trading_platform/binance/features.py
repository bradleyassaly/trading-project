from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.binance.models import BinanceFeatureConfig, BinanceFeatureResult
from trading_platform.features.store import LocalFeatureStore


BINANCE_CRYPTO_FEATURE_SCHEMA_VERSION = "binance_crypto_market_features_v1"


def _read_parquet_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _interval_to_floor_alias(interval: str) -> str:
    match = re.fullmatch(r"(\d+)([mhdw])", str(interval).strip().lower())
    if match is None:
        raise ValueError(f"Unsupported Binance interval for feature projection: {interval}")
    value = int(match.group(1))
    unit = match.group(2)
    unit_map = {"m": "min", "h": "h", "d": "d", "w": "W"}
    return f"{value}{unit_map[unit]}"


def _aggregate_trades(trades: pd.DataFrame, interval: str) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "timestamp",
                "agg_trade_count",
                "agg_trade_quantity",
                "agg_trade_notional",
                "signed_flow_quantity",
                "agg_trade_first_event_time",
                "agg_trade_last_event_time",
                "signed_flow_ratio",
            ]
        )
    frame = trades.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True).dt.floor(_interval_to_floor_alias(interval))
    frame["notional"] = pd.to_numeric(frame["price"], errors="coerce") * pd.to_numeric(frame["quantity"], errors="coerce")
    frame["signed_quantity"] = pd.to_numeric(frame["quantity"], errors="coerce").where(~frame["is_buyer_maker"], -pd.to_numeric(frame["quantity"], errors="coerce"))
    grouped = (
        frame.groupby(["symbol", "timestamp"], as_index=False)
        .agg(
            agg_trade_count=("aggregate_trade_id", "count"),
            agg_trade_quantity=("quantity", "sum"),
            agg_trade_notional=("notional", "sum"),
            signed_flow_quantity=("signed_quantity", "sum"),
            agg_trade_first_event_time=("event_time", "min"),
            agg_trade_last_event_time=("event_time", "max"),
        )
        .sort_values(["symbol", "timestamp"])
        .reset_index(drop=True)
    )
    grouped["signed_flow_ratio"] = grouped["signed_flow_quantity"].div(grouped["agg_trade_quantity"].replace(0.0, pd.NA)).fillna(0.0)
    return grouped


def _aggregate_top_of_book(book: pd.DataFrame, interval: str) -> pd.DataFrame:
    if book.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "timestamp",
                "top_of_book_event_time",
                "bid_price",
                "bid_quantity",
                "ask_price",
                "ask_quantity",
                "mid_price",
                "spread",
                "spread_bps",
                "book_imbalance",
                "top_of_book_snapshot_count",
            ]
        )
    frame = book.copy()
    frame["event_time"] = pd.to_datetime(frame["event_time"], utc=True)
    frame["timestamp"] = frame["event_time"].dt.floor(_interval_to_floor_alias(interval))
    frame["mid_price"] = (pd.to_numeric(frame["bid_price"], errors="coerce") + pd.to_numeric(frame["ask_price"], errors="coerce")) / 2.0
    frame["spread"] = pd.to_numeric(frame["ask_price"], errors="coerce") - pd.to_numeric(frame["bid_price"], errors="coerce")
    frame["spread_bps"] = frame["spread"].div(frame["mid_price"].replace(0.0, pd.NA)).fillna(0.0) * 10_000.0
    frame["book_imbalance"] = (
        (pd.to_numeric(frame["bid_quantity"], errors="coerce") - pd.to_numeric(frame["ask_quantity"], errors="coerce"))
        .div((pd.to_numeric(frame["bid_quantity"], errors="coerce") + pd.to_numeric(frame["ask_quantity"], errors="coerce")).replace(0.0, pd.NA))
        .fillna(0.0)
    )
    counts = (
        frame.groupby(["symbol", "timestamp"], as_index=False)
        .size()
        .rename(columns={"size": "top_of_book_snapshot_count"})
    )
    latest = (
        frame.sort_values(["symbol", "timestamp", "event_time"])
        .groupby(["symbol", "timestamp"], as_index=False)
        .tail(1)
        .rename(columns={"event_time": "top_of_book_event_time"})
    )
    columns = [
        "symbol",
        "timestamp",
        "top_of_book_event_time",
        "bid_price",
        "bid_quantity",
        "ask_price",
        "ask_quantity",
        "mid_price",
        "spread",
        "spread_bps",
        "book_imbalance",
    ]
    return latest.loc[:, columns].merge(counts, on=["symbol", "timestamp"], how="left").reset_index(drop=True)


def _apply_group_features(frame: pd.DataFrame, config: BinanceFeatureConfig) -> pd.DataFrame:
    ordered = frame.sort_values("timestamp").reset_index(drop=True).copy()
    ordered["return_1"] = ordered["close"].pct_change()
    ordered["dollar_volume"] = ordered["close"] * ordered["volume"]
    for horizon in config.return_horizons:
        ordered[f"return_{horizon}"] = ordered["close"].pct_change(periods=horizon)
    for window in config.volatility_windows:
        ordered[f"rolling_volatility_{window}"] = ordered["return_1"].rolling(window=window, min_periods=window).std(ddof=0)
    for window in config.volume_windows:
        ordered[f"rolling_volume_mean_{window}"] = ordered["volume"].rolling(window=window, min_periods=window).mean()
        ordered[f"rolling_dollar_volume_mean_{window}"] = ordered["dollar_volume"].rolling(window=window, min_periods=window).mean()
    ordered["agg_trade_count"] = ordered["agg_trade_count"].fillna(0).astype(int)
    ordered["agg_trade_quantity"] = ordered["agg_trade_quantity"].fillna(0.0)
    ordered["agg_trade_notional"] = ordered["agg_trade_notional"].fillna(0.0)
    ordered["signed_flow_quantity"] = ordered["signed_flow_quantity"].fillna(0.0)
    ordered["signed_flow_ratio"] = ordered["signed_flow_ratio"].fillna(0.0)
    ordered["top_of_book_snapshot_count"] = ordered["top_of_book_snapshot_count"].fillna(0).astype(int)
    ordered["spread"] = ordered["spread"].fillna(0.0)
    ordered["spread_bps"] = ordered["spread_bps"].fillna(0.0)
    ordered["book_imbalance"] = ordered["book_imbalance"].fillna(0.0)
    ordered["mid_price"] = ordered["mid_price"].fillna(ordered["close"])
    for window in config.trade_intensity_windows:
        ordered[f"rolling_trade_count_mean_{window}"] = ordered["agg_trade_count"].rolling(window=window, min_periods=1).mean()
        ordered[f"rolling_signed_flow_ratio_mean_{window}"] = ordered["signed_flow_ratio"].rolling(window=window, min_periods=1).mean()
    for window in config.order_book_windows:
        ordered[f"rolling_spread_bps_mean_{window}"] = ordered["spread_bps"].rolling(window=window, min_periods=1).mean()
        ordered[f"rolling_book_imbalance_mean_{window}"] = ordered["book_imbalance"].rolling(window=window, min_periods=1).mean()
    return ordered


def _slice_feature_paths(features_root: Path, symbol: str, interval: str) -> Path:
    return features_root / "crypto_market_features" / symbol.upper() / f"{interval}.parquet"


def _materialize_symbol_interval_features(
    bars: pd.DataFrame,
    trades: pd.DataFrame,
    book: pd.DataFrame,
    *,
    config: BinanceFeatureConfig,
    symbol: str,
    interval: str,
    existing: pd.DataFrame,
    full_rebuild: bool,
) -> pd.DataFrame:
    scoped_bars = bars.loc[
        (bars["symbol"].str.upper() == symbol.upper()) & (bars["interval"] == interval)
    ].sort_values("timestamp").reset_index(drop=True)
    if scoped_bars.empty:
        return pd.DataFrame()
    recompute_bars = scoped_bars.copy()
    if config.incremental_refresh and not full_rebuild and not existing.empty:
        latest_existing = pd.to_datetime(existing["timestamp"], utc=True).max()
        first_new_mask = pd.to_datetime(scoped_bars["timestamp"], utc=True) > latest_existing
        if first_new_mask.any():
            first_new_index = int(first_new_mask.idxmax())
            start_index = max(0, first_new_index - config.rebuild_lookback_rows)
            recompute_bars = scoped_bars.iloc[start_index:].copy()
        else:
            start_index = max(0, len(scoped_bars.index) - config.rebuild_lookback_rows)
            recompute_bars = scoped_bars.iloc[start_index:].copy()
    feature_frame = recompute_bars.copy()
    trade_features = _aggregate_trades(trades.loc[trades["symbol"].str.upper() == symbol.upper()], interval)
    book_features = _aggregate_top_of_book(book.loc[book["symbol"].str.upper() == symbol.upper()], interval)
    feature_frame = feature_frame.merge(trade_features, on=["symbol", "timestamp"], how="left")
    feature_frame = feature_frame.merge(book_features, on=["symbol", "timestamp"], how="left")
    feature_frame["feature_time"] = pd.to_datetime(feature_frame["close_timestamp"], utc=True)
    feature_frame["event_time"] = pd.to_datetime(feature_frame["event_time"], utc=True)
    feature_frame["source"] = "binance_projection"
    feature_frame["provider"] = "binance"
    feature_frame["asset_class"] = "crypto"
    feature_frame["schema_version"] = BINANCE_CRYPTO_FEATURE_SCHEMA_VERSION
    feature_frame["feature_set"] = "crypto_market_features"
    feature_frame["dedupe_key"] = feature_frame["symbol"].astype(str) + "|" + feature_frame["interval"].astype(str) + "|" + feature_frame["timestamp"].astype(str)
    feature_frame = _apply_group_features(feature_frame, config)
    if existing.empty or full_rebuild:
        return feature_frame.reset_index(drop=True)
    retained = existing.loc[~existing["dedupe_key"].isin(set(feature_frame["dedupe_key"]))].copy()
    combined = pd.concat([retained, feature_frame], ignore_index=True)
    combined = combined.sort_values(["symbol", "interval", "timestamp", "feature_time"]).drop_duplicates(
        subset=["symbol", "interval", "timestamp"],
        keep="last",
    )
    return combined.reset_index(drop=True)


def build_binance_market_features(
    config: BinanceFeatureConfig,
    *,
    full_rebuild: bool = False,
) -> BinanceFeatureResult:
    projection_root = Path(config.projection_root)
    features_root = Path(config.features_root)
    features_root.mkdir(parents=True, exist_ok=True)
    feature_store = LocalFeatureStore(config.feature_store_root)
    bars = _read_parquet_if_exists(projection_root / "crypto_ohlcv_bars.parquet")
    trades = _read_parquet_if_exists(projection_root / "crypto_agg_trades.parquet")
    book = _read_parquet_if_exists(projection_root / "crypto_top_of_book.parquet")
    if bars.empty:
        raise FileNotFoundError(f"Projected Binance bars not found at {projection_root / 'crypto_ohlcv_bars.parquet'}")

    symbol_filter = {symbol.upper() for symbol in config.symbols} if config.symbols else set(bars["symbol"].astype(str).str.upper())
    interval_filter = set(config.intervals) if config.intervals else set(bars["interval"].astype(str))
    slice_paths: list[str] = []
    manifest_paths: list[str] = []
    combined_frames: list[pd.DataFrame] = []
    for symbol in sorted(symbol_filter):
        for interval in sorted(interval_filter):
            existing_path = _slice_feature_paths(features_root, symbol, interval)
            existing = _read_parquet_if_exists(existing_path)
            output = _materialize_symbol_interval_features(
                bars,
                trades,
                book,
                config=config,
                symbol=symbol,
                interval=interval,
                existing=existing,
                full_rebuild=full_rebuild,
            )
            if output.empty:
                continue
            existing_path.parent.mkdir(parents=True, exist_ok=True)
            output.to_parquet(existing_path, index=False)
            slice_paths.append(str(existing_path))
            combined_frames.append(output)
            artifact = feature_store.write_from_parquet(
                source_path=existing_path,
                symbol=symbol,
                timeframe=interval,
                feature_groups=["crypto", "binance", "market_features"],
                metadata={
                    "provider": "binance",
                    "asset_class": "crypto",
                    "schema_version": BINANCE_CRYPTO_FEATURE_SCHEMA_VERSION,
                    "projection_root": str(projection_root),
                    "feature_set": "crypto_market_features",
                    "incremental_refresh": config.incremental_refresh,
                },
            )
            if artifact.manifest_path:
                manifest_paths.append(str(artifact.manifest_path))

    if combined_frames:
        combined = (
            pd.concat(combined_frames, ignore_index=True)
            .sort_values(["symbol", "interval", "timestamp", "feature_time"])
            .drop_duplicates(subset=["symbol", "interval", "timestamp"], keep="last")
            .reset_index(drop=True)
        )
    else:
        combined = pd.DataFrame()
    combined_path = features_root / "crypto_market_features.parquet"
    combined.to_parquet(combined_path, index=False)

    summary = {
        "features_path": str(combined_path),
        "slice_paths": slice_paths,
        "feature_store_manifest_paths": manifest_paths,
        "rows_written": int(len(combined.index)),
        "artifacts_written": len(slice_paths),
        "symbols": sorted(symbol_filter),
        "intervals": sorted(interval_filter),
        "full_rebuild": full_rebuild,
        "incremental_refresh": config.incremental_refresh,
        "schema_version": BINANCE_CRYPTO_FEATURE_SCHEMA_VERSION,
        "projection_inputs": {
            "crypto_ohlcv_bars": str(projection_root / "crypto_ohlcv_bars.parquet"),
            "crypto_agg_trades": str(projection_root / "crypto_agg_trades.parquet"),
            "crypto_top_of_book": str(projection_root / "crypto_top_of_book.parquet"),
        },
        "uniqueness_rules": ["symbol", "interval", "timestamp"],
        "feature_time_semantics": "feature_time equals bar close timestamp; timestamp is the bar open timestamp.",
    }
    summary_path = Path(config.summary_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return BinanceFeatureResult(
        summary_path=str(summary_path),
        features_path=str(combined_path),
        slice_paths=slice_paths,
        feature_store_manifest_paths=manifest_paths,
        rows_written=int(len(combined.index)),
        artifacts_written=len(slice_paths),
        symbols=sorted(symbol_filter),
        intervals=sorted(interval_filter),
    )
