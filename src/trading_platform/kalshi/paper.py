from __future__ import annotations

import json
import math
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd
import polars as pl

from trading_platform.kalshi.client import KalshiClient
from trading_platform.kalshi.features import build_kalshi_features
from trading_platform.kalshi.models import KalshiMarket, KalshiOrderBook, KalshiTrade, price_to_float
from trading_platform.kalshi.signals import KalshiSignalFamily
from trading_platform.kalshi.signals_base_rate import KalshiBaseRateSignal

_STATE_SCHEMA_VERSION = 1
_SETTLED_STATUSES = {"settled", "determined", "closed", "finalized"}


def _utc_now() -> datetime:
    return datetime.now(UTC)


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


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number):
        return None
    return number


def _safe_mean(values: Iterable[float]) -> float:
    clean = [float(value) for value in values if not math.isnan(float(value))]
    return float(sum(clean) / len(clean)) if clean else 0.0


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


@dataclass(frozen=True)
class KalshiPaperExecutionConfig:
    orderbook_depth: int = 10
    signal_lookback_hours: int = 48
    feature_period: str = "1h"
    min_recent_trades: int = 20
    stale_trade_seconds: int = 900
    min_market_volume: int = 25
    min_market_liquidity_dollars: float = 50.0
    max_spread: float = 0.08
    max_contracts_per_trade: int = 10
    max_fraction_top_level_liquidity: float = 0.5
    max_fraction_market_volume: float = 0.05
    min_confidence: float = 0.10
    entry_threshold: float = 0.5
    exit_threshold: float = 0.5
    no_entry_before_close_minutes: int = 60
    max_holding_hours: float | None = 24.0
    fill_penalty_factor: float = 0.5
    max_markets_per_run: int | None = None

    def __post_init__(self) -> None:
        if self.orderbook_depth <= 0:
            raise ValueError("orderbook_depth must be > 0.")
        if self.signal_lookback_hours <= 0 or self.min_recent_trades < 0 or self.stale_trade_seconds < 0:
            raise ValueError("Lookback/trade freshness settings must be non-negative and positive where required.")
        if self.min_market_volume < 0 or self.min_market_liquidity_dollars < 0 or self.max_spread < 0:
            raise ValueError("Liquidity thresholds must be >= 0.")
        if self.max_contracts_per_trade <= 0:
            raise ValueError("max_contracts_per_trade must be > 0.")
        if not 0.0 < self.max_fraction_top_level_liquidity <= 1.0:
            raise ValueError("max_fraction_top_level_liquidity must be in (0, 1].")
        if not 0.0 < self.max_fraction_market_volume <= 1.0:
            raise ValueError("max_fraction_market_volume must be in (0, 1].")
        if self.min_confidence < 0 or self.entry_threshold < 0 or self.exit_threshold < 0:
            raise ValueError("Confidence and threshold settings must be >= 0.")
        if self.no_entry_before_close_minutes < 0:
            raise ValueError("no_entry_before_close_minutes must be >= 0.")
        if self.max_holding_hours is not None and self.max_holding_hours <= 0:
            raise ValueError("max_holding_hours must be > 0 when provided.")
        if self.fill_penalty_factor < 0:
            raise ValueError("fill_penalty_factor must be >= 0.")


@dataclass(frozen=True)
class KalshiPaperRiskConfig:
    max_exposure_per_market: float = 100.0
    max_exposure_per_category: float = 250.0
    max_simultaneous_positions: int = 5
    max_drawdown_pct: float = 0.20

    def __post_init__(self) -> None:
        if self.max_exposure_per_market <= 0 or self.max_exposure_per_category <= 0:
            raise ValueError("Exposure limits must be > 0.")
        if self.max_simultaneous_positions <= 0:
            raise ValueError("max_simultaneous_positions must be > 0.")
        if not 0.0 <= self.max_drawdown_pct <= 1.0:
            raise ValueError("max_drawdown_pct must be between 0 and 1.")


@dataclass(frozen=True)
class KalshiPaperTradingConfig:
    state_path: str
    output_dir: str
    initial_cash: float = 1_000.0
    poll_interval_seconds: float = 30.0
    max_iterations: int = 1
    default_market_status: str = "open"
    tracked_series: tuple[str, ...] = ()
    tracked_tickers: tuple[str, ...] = ()
    signal_family_names: tuple[str, ...] = ()
    base_rate_db_path: str | None = None
    execution: KalshiPaperExecutionConfig = field(default_factory=KalshiPaperExecutionConfig)
    risk: KalshiPaperRiskConfig = field(default_factory=KalshiPaperRiskConfig)

    def __post_init__(self) -> None:
        if self.initial_cash <= 0:
            raise ValueError("initial_cash must be > 0.")
        if self.poll_interval_seconds < 0:
            raise ValueError("poll_interval_seconds must be >= 0.")
        if self.max_iterations <= 0:
            raise ValueError("max_iterations must be > 0.")


@dataclass
class KalshiPaperPosition:
    trade_id: str
    ticker: str
    market_title: str
    category: str
    side: str
    quantity: int
    entry_time: str
    entry_price: float
    entry_yes_price: float
    current_mark_price: float
    current_yes_price: float
    confidence: float
    predicted_probability: float
    predicted_edge: float
    signal_family: str
    supporting_features: dict[str, Any] = field(default_factory=dict)
    entry_rationale: str = ""
    execution_assumptions: dict[str, Any] = field(default_factory=dict)
    market_close_time: str | None = None
    last_update_time: str | None = None

    @property
    def market_value(self) -> float:
        return float(self.current_mark_price * self.quantity)

    @property
    def max_loss(self) -> float:
        return float(self.entry_price * self.quantity)

    @property
    def unrealized_pnl(self) -> float:
        return float((self.current_mark_price - self.entry_price) * self.quantity)


@dataclass
class KalshiPaperTradeRecord:
    trade_id: str
    ticker: str
    market_title: str
    category: str
    signal_family: str
    side: str
    quantity: int
    confidence: float
    predicted_probability: float
    predicted_edge: float
    supporting_features: dict[str, Any]
    entry_rationale: str
    execution_assumptions: dict[str, Any]
    entry_time: str
    entry_price: float
    entry_yes_price: float
    entry_reference_yes_price: float
    exit_time: str | None = None
    exit_price: float | None = None
    exit_yes_price: float | None = None
    exit_reason: str | None = None
    realized_pnl: float | None = None
    status: str = "open"


@dataclass
class KalshiPaperState:
    schema_version: int = _STATE_SCHEMA_VERSION
    as_of: str | None = None
    cash: float = 0.0
    peak_equity: float = 0.0
    cumulative_realized_pnl: float = 0.0
    halted: bool = False
    halt_reason: str | None = None
    open_positions: dict[str, KalshiPaperPosition] = field(default_factory=dict)
    trades: list[KalshiPaperTradeRecord] = field(default_factory=list)
    next_trade_id: int = 1

    @property
    def unrealized_pnl(self) -> float:
        return float(sum(position.unrealized_pnl for position in self.open_positions.values()))

    @property
    def equity(self) -> float:
        return float(self.cash + sum(position.market_value for position in self.open_positions.values()))

    @classmethod
    def new(cls, *, initial_cash: float) -> "KalshiPaperState":
        return cls(cash=initial_cash, peak_equity=initial_cash)

    @classmethod
    def from_dict(cls, payload: dict[str, Any], *, initial_cash: float) -> "KalshiPaperState":
        state = cls.new(initial_cash=initial_cash)
        state.schema_version = int(payload.get("schema_version", _STATE_SCHEMA_VERSION))
        state.as_of = payload.get("as_of")
        state.cash = float(payload.get("cash", initial_cash))
        state.peak_equity = float(payload.get("peak_equity", max(state.cash, initial_cash)))
        state.cumulative_realized_pnl = float(payload.get("cumulative_realized_pnl", 0.0))
        state.halted = bool(payload.get("halted", False))
        state.halt_reason = payload.get("halt_reason")
        state.next_trade_id = int(payload.get("next_trade_id", 1))
        state.open_positions = {
            str(ticker): KalshiPaperPosition(**raw)
            for ticker, raw in (payload.get("open_positions") or {}).items()
        }
        state.trades = [KalshiPaperTradeRecord(**raw) for raw in payload.get("trades", [])]
        return state


@dataclass(frozen=True)
class KalshiPaperSignalCandidate:
    ticker: str
    market_title: str
    category: str
    side: str
    confidence: float
    predicted_probability: float
    predicted_edge: float
    signal_family: str
    signal_value: float
    supporting_features: dict[str, Any]
    entry_rationale: str
    market_close_time: str | None


@dataclass(frozen=True)
class KalshiExecutionQuote:
    contract_price: float
    yes_price: float
    spread: float
    top_level_quantity: int
    tradable_quantity: int
    quote_source: str


@dataclass(frozen=True)
class KalshiPaperSessionSummary:
    generated_at: str
    markets_polled: int
    candidate_signals: int
    executed_entries: int
    executed_exits: int
    rejected_by_reason: dict[str, int]
    open_positions: int
    closed_trades: int
    realized_pnl: float
    unrealized_pnl: float
    equity: float
    cash: float
    peak_equity: float
    current_drawdown_pct: float
    halted: bool
    halt_reason: str | None
    signal_family_summary: list[dict[str, Any]]
    category_summary: list[dict[str, Any]]
    execution_assumptions: dict[str, Any]
    risk_limits: dict[str, Any]


class JsonKalshiPaperStateStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def load(self, *, initial_cash: float) -> KalshiPaperState:
        if not self.path.exists():
            return KalshiPaperState.new(initial_cash=initial_cash)
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        return KalshiPaperState.from_dict(payload, initial_cash=initial_cash)

    def save(self, state: KalshiPaperState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(asdict(state), indent=2, default=_json_default), encoding="utf-8")


class KalshiPaperTrader:
    def __init__(
        self,
        *,
        client: KalshiClient,
        config: KalshiPaperTradingConfig,
        signal_families: Sequence[KalshiSignalFamily],
    ) -> None:
        self.client = client
        self.config = config
        self.signal_families = list(signal_families)
        self.state_store = JsonKalshiPaperStateStore(Path(config.state_path))
        self.base_rate_signal = KalshiBaseRateSignal(config.base_rate_db_path) if config.base_rate_db_path else None

    def run(self) -> KalshiPaperSessionSummary:
        state = self.state_store.load(initial_cash=self.config.initial_cash)
        latest_summary: KalshiPaperSessionSummary | None = None
        for iteration in range(self.config.max_iterations):
            latest_summary = self.run_once(state=state)
            self.state_store.save(state)
            if iteration + 1 < self.config.max_iterations and self.config.poll_interval_seconds > 0:
                time.sleep(self.config.poll_interval_seconds)
        if latest_summary is None:
            raise RuntimeError("Kalshi paper trader did not execute any session.")
        return latest_summary

    def run_once(self, *, state: KalshiPaperState) -> KalshiPaperSessionSummary:
        now = _utc_now()
        state.as_of = now.isoformat()
        state.peak_equity = max(state.peak_equity, state.equity)

        rejected_by_reason: dict[str, int] = {}
        candidate_rows: list[dict[str, Any]] = []
        executed_entries = 0
        executed_exits = 0

        markets = self._fetch_markets()
        market_map = {market.ticker: market for market in markets}

        executed_exits += self._settle_resolved_positions(state=state, now=now, market_map=market_map)

        current_drawdown = self._current_drawdown_pct(state)
        if current_drawdown >= self.config.risk.max_drawdown_pct:
            state.halted = True
            state.halt_reason = (
                f"drawdown {current_drawdown:.2%} breached limit {self.config.risk.max_drawdown_pct:.2%}"
            )

        candidate_map: dict[str, KalshiPaperSignalCandidate] = {}
        for market in markets:
            candidate, reason = self._build_candidate_for_market(market=market, now=now)
            if candidate is None:
                if reason:
                    rejected_by_reason[reason] = rejected_by_reason.get(reason, 0) + 1
                continue
            candidate_map[candidate.ticker] = candidate
            candidate_rows.append(
                {
                    "ticker": candidate.ticker,
                    "category": candidate.category,
                    "signal_family": candidate.signal_family,
                    "signal_value": candidate.signal_value,
                    "confidence": candidate.confidence,
                    "predicted_edge": candidate.predicted_edge,
                }
            )

        executed_exits += self._process_exit_candidates(
            state=state,
            candidate_map=candidate_map,
            market_map=market_map,
            now=now,
            rejected_by_reason=rejected_by_reason,
        )
        executed_entries += self._process_entry_candidates(
            state=state,
            candidate_map=candidate_map,
            market_map=market_map,
            now=now,
            rejected_by_reason=rejected_by_reason,
        )

        for ticker, position in state.open_positions.items():
            market = market_map.get(ticker)
            if market is None:
                continue
            orderbook = self.client.get_orderbook(ticker, depth=self.config.execution.orderbook_depth)
            position.current_mark_price = self._current_mark_price(side=position.side, market=market, orderbook=orderbook)
            position.current_yes_price = self._current_yes_reference_price(market=market, orderbook=orderbook)
            position.last_update_time = now.isoformat()

        state.peak_equity = max(state.peak_equity, state.equity)
        summary = KalshiPaperSessionSummary(
            generated_at=now.isoformat(),
            markets_polled=len(markets),
            candidate_signals=len(candidate_rows),
            executed_entries=executed_entries,
            executed_exits=executed_exits,
            rejected_by_reason=dict(sorted(rejected_by_reason.items())),
            open_positions=len(state.open_positions),
            closed_trades=sum(1 for trade in state.trades if trade.status == "closed"),
            realized_pnl=state.cumulative_realized_pnl,
            unrealized_pnl=state.unrealized_pnl,
            equity=state.equity,
            cash=state.cash,
            peak_equity=state.peak_equity,
            current_drawdown_pct=self._current_drawdown_pct(state),
            halted=state.halted,
            halt_reason=state.halt_reason,
            signal_family_summary=self._aggregate_candidates(candidate_rows, "signal_family"),
            category_summary=self._aggregate_candidates(candidate_rows, "category"),
            execution_assumptions=asdict(self.config.execution),
            risk_limits=asdict(self.config.risk),
        )
        self._write_artifacts(state=state, summary=summary)
        return summary

    def _fetch_markets(self) -> list[KalshiMarket]:
        seen: dict[str, KalshiMarket] = {}
        if self.config.tracked_tickers:
            for ticker in self.config.tracked_tickers:
                market = self.client.get_market(ticker)
                seen[market.ticker] = market
        elif self.config.tracked_series:
            for series_ticker in self.config.tracked_series:
                for market in self.client.get_all_markets(
                    status=self.config.default_market_status,
                    series_ticker=series_ticker,
                ):
                    seen[market.ticker] = market
        else:
            for market in self.client.get_all_markets(status=self.config.default_market_status):
                seen[market.ticker] = market
        markets = sorted(seen.values(), key=lambda market: market.ticker)
        if self.config.execution.max_markets_per_run is not None:
            return markets[: self.config.execution.max_markets_per_run]
        return markets

    def _build_candidate_for_market(
        self,
        *,
        market: KalshiMarket,
        now: datetime,
    ) -> tuple[KalshiPaperSignalCandidate | None, str | None]:
        if str(market.status or "").lower() != "open":
            return None, "market_not_open"
        close_time = _parse_timestamp(market.close_time)
        if close_time is not None:
            minutes_to_close = (close_time - now).total_seconds() / 60.0
            if minutes_to_close <= self.config.execution.no_entry_before_close_minutes:
                return None, "near_settlement"

        volume = market.volume or 0
        if volume < self.config.execution.min_market_volume:
            return None, "low_volume"
        liquidity = price_to_float(market.liquidity) or 0.0
        if liquidity < self.config.execution.min_market_liquidity_dollars:
            return None, "low_liquidity"

        orderbook = self.client.get_orderbook(market.ticker, depth=self.config.execution.orderbook_depth)
        yes_bid = self._best_yes_bid(market, orderbook)
        yes_ask = self._best_yes_ask(market, orderbook)
        if yes_bid is None or yes_ask is None or yes_ask <= yes_bid:
            return None, "missing_quote"
        spread = yes_ask - yes_bid
        if spread > self.config.execution.max_spread:
            return None, "wide_spread"

        min_ts = int((now - timedelta(hours=self.config.execution.signal_lookback_hours)).timestamp())
        max_ts = int(now.timestamp())
        trades = self.client.get_all_trades(market.ticker, min_ts=min_ts, max_ts=max_ts)
        if len(trades) < self.config.execution.min_recent_trades:
            return None, "sparse_trades"
        latest_trade_time = max((_parse_timestamp(trade.created_time) for trade in trades), default=None)
        if latest_trade_time is None:
            return None, "missing_trade_timestamps"
        age_seconds = (now - latest_trade_time).total_seconds()
        if age_seconds > self.config.execution.stale_trade_seconds:
            return None, "stale_market"

        feature_frame = self._build_live_feature_frame(market=market, trades=trades, close_time=close_time)
        if feature_frame.empty:
            return None, "empty_features"

        best_candidate: KalshiPaperSignalCandidate | None = None
        market_yes_probability = self._current_yes_reference_price(market=market, orderbook=orderbook)
        for family in self.signal_families:
            signal_frame = family.build_signal_frame(feature_frame)
            signal_row = signal_frame.iloc[-1]
            signal_value = _safe_float(signal_row.get("signal_value"))
            confidence = _safe_float(signal_row.get("confidence")) or 0.0
            predicted_probability = _safe_float(signal_row.get("signal_probability"))
            if signal_value is None or predicted_probability is None:
                continue
            if abs(signal_value) < self.config.execution.entry_threshold or confidence < self.config.execution.min_confidence:
                continue
            side = "yes" if signal_value > 0 else "no"
            predicted_edge = (
                predicted_probability - market_yes_probability
                if side == "yes"
                else market_yes_probability - predicted_probability
            )
            supporting_features = self._extract_supporting_features(signal_row)
            rationale = (
                f"{family.name} signaled BUY {side.upper()} with confidence {confidence:.2f} "
                f"and predicted edge {predicted_edge:.4f}."
            )
            candidate = KalshiPaperSignalCandidate(
                ticker=market.ticker,
                market_title=market.title,
                category=str(market.category or "unknown"),
                side=side,
                confidence=confidence,
                predicted_probability=predicted_probability,
                predicted_edge=predicted_edge,
                signal_family=family.name,
                signal_value=signal_value,
                supporting_features=supporting_features,
                entry_rationale=rationale,
                market_close_time=close_time.isoformat() if close_time else None,
            )
            if best_candidate is None or (candidate.confidence, abs(candidate.signal_value)) > (
                best_candidate.confidence,
                abs(best_candidate.signal_value),
            ):
                best_candidate = candidate
        if best_candidate is None:
            return None, "no_signal"
        return best_candidate, None

    def _build_live_feature_frame(
        self,
        *,
        market: KalshiMarket,
        trades: list[KalshiTrade],
        close_time: datetime | None,
    ) -> pd.DataFrame:
        rows = []
        for trade in trades:
            traded_at = _parse_timestamp(trade.created_time)
            if traded_at is None:
                continue
            rows.append(
                {
                    "trade_id": trade.trade_id,
                    "ticker": trade.ticker,
                    "side": str(trade.side or "").lower(),
                    "yes_price": _safe_float(trade.yes_price),
                    "count": int(trade.count or 0),
                    "traded_at": traded_at,
                }
            )
        if not rows:
            return pd.DataFrame()
        trades_df = pl.from_dicts(rows)
        extra_scalar_features: dict[str, float] = {}
        if self.base_rate_signal is not None:
            last_trade_yes = _safe_float(rows[-1]["yes_price"])
            if last_trade_yes is not None:
                extra_scalar_features.update(
                    self.base_rate_signal.compute_for_market(
                        market.title,
                        market.series_ticker or "",
                        last_trade_yes * 100.0 if last_trade_yes <= 1.0 else last_trade_yes,
                    )
                )
        feature_df = build_kalshi_features(
            trades_df,
            ticker=market.ticker,
            period=self.config.execution.feature_period,
            close_time=close_time,
            extra_scalar_features=extra_scalar_features or None,
            market_context={
                "title": market.title,
                "series_ticker": market.series_ticker,
                "base_rate_db_path": self.config.base_rate_db_path,
                "side_col": "side",
            },
        )
        return feature_df.to_pandas()

    def _process_exit_candidates(
        self,
        *,
        state: KalshiPaperState,
        candidate_map: dict[str, KalshiPaperSignalCandidate],
        market_map: dict[str, KalshiMarket],
        now: datetime,
        rejected_by_reason: dict[str, int],
    ) -> int:
        exits = 0
        for ticker in list(state.open_positions):
            position = state.open_positions.get(ticker)
            if position is None:
                continue
            market = market_map.get(ticker)
            if market is None:
                continue
            candidate = candidate_map.get(ticker)
            reason: str | None = None
            if candidate is not None and candidate.side != position.side and abs(candidate.signal_value) >= self.config.execution.exit_threshold:
                reason = "signal_reversal"
            elif self._holding_hours_exceeded(position=position, now=now):
                reason = "max_holding_window"
            if reason is None:
                continue
            orderbook = self.client.get_orderbook(ticker, depth=self.config.execution.orderbook_depth)
            quote = self._execution_quote_for_side(
                side=position.side,
                action="sell",
                market=market,
                orderbook=orderbook,
                requested_quantity=position.quantity,
            )
            if quote is None:
                rejected_by_reason["exit_missing_liquidity"] = rejected_by_reason.get("exit_missing_liquidity", 0) + 1
                continue
            self._close_position(
                state=state,
                ticker=ticker,
                exit_price=quote.contract_price,
                exit_yes_price=quote.yes_price,
                exit_reason=reason,
                exit_time=now,
            )
            exits += 1
        return exits

    def _process_entry_candidates(
        self,
        *,
        state: KalshiPaperState,
        candidate_map: dict[str, KalshiPaperSignalCandidate],
        market_map: dict[str, KalshiMarket],
        now: datetime,
        rejected_by_reason: dict[str, int],
    ) -> int:
        if state.halted:
            rejected_by_reason["halted"] = rejected_by_reason.get("halted", 0) + len(candidate_map)
            return 0
        entries = 0
        for ticker, candidate in sorted(candidate_map.items()):
            if ticker in state.open_positions:
                rejected_by_reason["existing_position"] = rejected_by_reason.get("existing_position", 0) + 1
                continue
            market = market_map[ticker]
            risk_reason = self._entry_risk_reason(state=state, market=market)
            if risk_reason is not None:
                rejected_by_reason[risk_reason] = rejected_by_reason.get(risk_reason, 0) + 1
                continue
            orderbook = self.client.get_orderbook(ticker, depth=self.config.execution.orderbook_depth)
            quote = self._execution_quote_for_side(
                side=candidate.side,
                action="buy",
                market=market,
                orderbook=orderbook,
                requested_quantity=self.config.execution.max_contracts_per_trade,
            )
            if quote is None:
                rejected_by_reason["entry_missing_liquidity"] = rejected_by_reason.get("entry_missing_liquidity", 0) + 1
                continue
            if quote.tradable_quantity <= 0:
                rejected_by_reason["size_capped_to_zero"] = rejected_by_reason.get("size_capped_to_zero", 0) + 1
                continue
            projected_cost = quote.contract_price * quote.tradable_quantity
            if projected_cost > state.cash:
                affordable = int(state.cash / max(quote.contract_price, 1e-9))
                if affordable <= 0:
                    rejected_by_reason["insufficient_cash"] = rejected_by_reason.get("insufficient_cash", 0) + 1
                    continue
                quote = KalshiExecutionQuote(
                    contract_price=quote.contract_price,
                    yes_price=quote.yes_price,
                    spread=quote.spread,
                    top_level_quantity=quote.top_level_quantity,
                    tradable_quantity=min(quote.tradable_quantity, affordable),
                    quote_source=quote.quote_source,
                )
            self._open_position(
                state=state,
                market=market,
                candidate=candidate,
                quote=quote,
                now=now,
            )
            entries += 1
        return entries

    def _entry_risk_reason(self, *, state: KalshiPaperState, market: KalshiMarket) -> str | None:
        if len(state.open_positions) >= self.config.risk.max_simultaneous_positions:
            return "max_positions"
        market_exposure = sum(
            position.max_loss for position in state.open_positions.values() if position.ticker == market.ticker
        )
        if market_exposure >= self.config.risk.max_exposure_per_market:
            return "market_exposure_limit"
        category = str(market.category or "unknown")
        category_exposure = sum(
            position.max_loss for position in state.open_positions.values() if position.category == category
        )
        if category_exposure >= self.config.risk.max_exposure_per_category:
            return "category_exposure_limit"
        return None

    def _open_position(
        self,
        *,
        state: KalshiPaperState,
        market: KalshiMarket,
        candidate: KalshiPaperSignalCandidate,
        quote: KalshiExecutionQuote,
        now: datetime,
    ) -> None:
        trade_id = f"kalshi-paper-{state.next_trade_id:06d}"
        state.next_trade_id += 1
        state.cash -= quote.contract_price * quote.tradable_quantity
        execution_assumptions = {
            "quote_source": quote.quote_source,
            "spread": quote.spread,
            "top_level_quantity": quote.top_level_quantity,
            "tradable_quantity": quote.tradable_quantity,
            "fill_penalty_factor": self.config.execution.fill_penalty_factor,
        }
        position = KalshiPaperPosition(
            trade_id=trade_id,
            ticker=market.ticker,
            market_title=market.title,
            category=str(market.category or "unknown"),
            side=candidate.side,
            quantity=quote.tradable_quantity,
            entry_time=now.isoformat(),
            entry_price=quote.contract_price,
            entry_yes_price=quote.yes_price,
            current_mark_price=quote.contract_price,
            current_yes_price=quote.yes_price,
            confidence=candidate.confidence,
            predicted_probability=candidate.predicted_probability,
            predicted_edge=candidate.predicted_edge,
            signal_family=candidate.signal_family,
            supporting_features=candidate.supporting_features,
            entry_rationale=candidate.entry_rationale,
            execution_assumptions=execution_assumptions,
            market_close_time=candidate.market_close_time,
            last_update_time=now.isoformat(),
        )
        state.open_positions[market.ticker] = position
        state.trades.append(
            KalshiPaperTradeRecord(
                trade_id=trade_id,
                ticker=market.ticker,
                market_title=market.title,
                category=str(market.category or "unknown"),
                signal_family=candidate.signal_family,
                side=candidate.side,
                quantity=quote.tradable_quantity,
                confidence=candidate.confidence,
                predicted_probability=candidate.predicted_probability,
                predicted_edge=candidate.predicted_edge,
                supporting_features=candidate.supporting_features,
                entry_rationale=candidate.entry_rationale,
                execution_assumptions=execution_assumptions,
                entry_time=now.isoformat(),
                entry_price=quote.contract_price,
                entry_yes_price=quote.yes_price,
                entry_reference_yes_price=quote.yes_price,
            )
        )

    def _close_position(
        self,
        *,
        state: KalshiPaperState,
        ticker: str,
        exit_price: float,
        exit_yes_price: float,
        exit_reason: str,
        exit_time: datetime,
    ) -> None:
        position = state.open_positions.pop(ticker)
        pnl = (exit_price - position.entry_price) * position.quantity
        state.cash += exit_price * position.quantity
        state.cumulative_realized_pnl += pnl
        for trade in reversed(state.trades):
            if trade.trade_id == position.trade_id:
                trade.exit_time = exit_time.isoformat()
                trade.exit_price = exit_price
                trade.exit_yes_price = exit_yes_price
                trade.exit_reason = exit_reason
                trade.realized_pnl = pnl
                trade.status = "closed"
                break

    def _settle_resolved_positions(
        self,
        *,
        state: KalshiPaperState,
        now: datetime,
        market_map: dict[str, KalshiMarket],
    ) -> int:
        exits = 0
        for ticker in list(state.open_positions):
            position = state.open_positions.get(ticker)
            if position is None:
                continue
            market = market_map.get(ticker)
            status = str(market.status).lower() if market else ""
            if status not in _SETTLED_STATUSES:
                continue
            historical_market = self.client.get_historical_market(ticker)
            resolution_yes = self._resolution_yes_price(historical_market)
            exit_price = resolution_yes if position.side == "yes" else 1.0 - resolution_yes
            self._close_position(
                state=state,
                ticker=ticker,
                exit_price=exit_price,
                exit_yes_price=resolution_yes,
                exit_reason="settlement",
                exit_time=now,
            )
            exits += 1
        return exits

    def _holding_hours_exceeded(self, *, position: KalshiPaperPosition, now: datetime) -> bool:
        if self.config.execution.max_holding_hours is None:
            return False
        entry_time = _parse_timestamp(position.entry_time)
        if entry_time is None:
            return False
        return (now - entry_time).total_seconds() / 3600.0 >= self.config.execution.max_holding_hours

    def _resolution_yes_price(self, historical_market: dict[str, Any]) -> float:
        result = str(historical_market.get("result") or "").lower()
        if result == "yes":
            return 1.0
        if result == "no":
            return 0.0
        resolution_price = _safe_float(historical_market.get("resolution_price"))
        if resolution_price is not None:
            return resolution_price / 100.0 if resolution_price > 1.0 else resolution_price
        yes_bid = _safe_float(historical_market.get("yes_bid")) or _safe_float(historical_market.get("yes_bid_dollars"))
        if yes_bid is not None:
            return yes_bid / 100.0 if yes_bid > 1.0 else yes_bid
        return 0.0

    def _execution_quote_for_side(
        self,
        *,
        side: str,
        action: str,
        market: KalshiMarket,
        orderbook: KalshiOrderBook,
        requested_quantity: int,
    ) -> KalshiExecutionQuote | None:
        yes_bid = self._best_yes_bid(market, orderbook)
        yes_ask = self._best_yes_ask(market, orderbook)
        no_bid = self._best_no_bid(market, orderbook)
        no_ask = self._best_no_ask(market, orderbook)
        if yes_bid is None or yes_ask is None or no_bid is None or no_ask is None:
            return None

        if side == "yes":
            bid_price = yes_bid
            ask_price = yes_ask
            if action == "buy":
                top_level_quantity = orderbook.no_bids[0].quantity if orderbook.no_bids else 0
            else:
                top_level_quantity = orderbook.yes_bids[0].quantity if orderbook.yes_bids else 0
        else:
            bid_price = no_bid
            ask_price = no_ask
            if action == "buy":
                top_level_quantity = orderbook.yes_bids[0].quantity if orderbook.yes_bids else 0
            else:
                top_level_quantity = orderbook.no_bids[0].quantity if orderbook.no_bids else 0
        if top_level_quantity <= 0:
            return None

        market_volume = max(int(market.volume or 0), 1)
        tradable_quantity = min(
            requested_quantity,
            max(1, int(top_level_quantity * self.config.execution.max_fraction_top_level_liquidity)),
            max(1, int(market_volume * self.config.execution.max_fraction_market_volume)),
        )
        spread = ask_price - bid_price
        participation = min(1.0, tradable_quantity / max(top_level_quantity, 1))
        penalty = spread * participation * self.config.execution.fill_penalty_factor
        if action == "buy":
            contract_price = min(0.9999, ask_price + penalty)
        else:
            contract_price = max(0.0001, bid_price - penalty)
        yes_price = contract_price if side == "yes" else 1.0 - contract_price
        return KalshiExecutionQuote(
            contract_price=contract_price,
            yes_price=yes_price,
            spread=spread,
            top_level_quantity=top_level_quantity,
            tradable_quantity=tradable_quantity,
            quote_source="orderbook",
        )

    def _best_yes_bid(self, market: KalshiMarket, orderbook: KalshiOrderBook) -> float | None:
        return orderbook.best_yes_bid or price_to_float(market.yes_bid)

    def _best_no_bid(self, market: KalshiMarket, orderbook: KalshiOrderBook) -> float | None:
        return orderbook.best_no_bid or price_to_float(market.no_bid)

    def _best_yes_ask(self, market: KalshiMarket, orderbook: KalshiOrderBook) -> float | None:
        market_yes_ask = price_to_float(market.yes_ask)
        implied_yes_ask = None if orderbook.best_no_bid is None else 1.0 - orderbook.best_no_bid
        if market_yes_ask is None:
            return implied_yes_ask
        if implied_yes_ask is None:
            return market_yes_ask
        return min(market_yes_ask, implied_yes_ask)

    def _best_no_ask(self, market: KalshiMarket, orderbook: KalshiOrderBook) -> float | None:
        market_no_ask = price_to_float(market.no_ask)
        implied_no_ask = None if orderbook.best_yes_bid is None else 1.0 - orderbook.best_yes_bid
        if market_no_ask is None:
            return implied_no_ask
        if implied_no_ask is None:
            return market_no_ask
        return min(market_no_ask, implied_no_ask)

    def _current_mark_price(self, *, side: str, market: KalshiMarket, orderbook: KalshiOrderBook) -> float:
        if side == "yes":
            return self._best_yes_bid(market, orderbook) or 0.0
        return self._best_no_bid(market, orderbook) or 0.0

    def _current_yes_reference_price(self, *, market: KalshiMarket, orderbook: KalshiOrderBook) -> float:
        yes_bid = self._best_yes_bid(market, orderbook)
        yes_ask = self._best_yes_ask(market, orderbook)
        if yes_bid is not None and yes_ask is not None:
            return (yes_bid + yes_ask) / 2.0
        market_yes = price_to_float(market.yes_bid) or price_to_float(market.yes_ask)
        return market_yes or 0.5

    def _current_drawdown_pct(self, state: KalshiPaperState) -> float:
        if state.peak_equity <= 0:
            return 0.0
        return max(0.0, (state.peak_equity - state.equity) / state.peak_equity)

    def _extract_supporting_features(self, signal_row: pd.Series) -> dict[str, Any]:
        ignored = {"signal_value", "direction", "confidence", "signal_probability", "signal_family"}
        payload: dict[str, Any] = {}
        for key, value in signal_row.items():
            if key in ignored:
                continue
            safe = _safe_float(value)
            payload[str(key)] = safe if safe is not None else (None if pd.isna(value) else value)
        return payload

    def _aggregate_candidates(self, rows: list[dict[str, Any]], field_name: str) -> list[dict[str, Any]]:
        if not rows:
            return []
        frame = pd.DataFrame(rows)
        result: list[dict[str, Any]] = []
        for field_value, group in frame.groupby(field_name, dropna=False):
            result.append(
                {
                    field_name: "unknown" if pd.isna(field_value) else field_value,
                    "signal_count": int(len(group)),
                    "average_confidence": _safe_mean(pd.to_numeric(group["confidence"], errors="coerce").fillna(0.0)),
                    "average_predicted_edge": _safe_mean(
                        pd.to_numeric(group["predicted_edge"], errors="coerce").fillna(0.0)
                    ),
                    "average_signal_value": _safe_mean(
                        pd.to_numeric(group["signal_value"], errors="coerce").fillna(0.0)
                    ),
                }
            )
        return sorted(result, key=lambda row: str(row[field_name]))

    def _write_artifacts(self, *, state: KalshiPaperState, summary: KalshiPaperSessionSummary) -> None:
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        positions_payload = {
            "generated_at": summary.generated_at,
            "open_positions": [asdict(position) for position in state.open_positions.values()],
            "cash": state.cash,
            "equity": state.equity,
            "unrealized_pnl": state.unrealized_pnl,
            "cumulative_realized_pnl": state.cumulative_realized_pnl,
        }
        (output_dir / "kalshi_paper_positions.json").write_text(
            json.dumps(positions_payload, indent=2, default=_json_default),
            encoding="utf-8",
        )
        with (output_dir / "kalshi_paper_trade_log.jsonl").open("w", encoding="utf-8") as handle:
            for trade in state.trades:
                handle.write(json.dumps(asdict(trade), default=_json_default) + "\n")
        (output_dir / "kalshi_paper_session_summary.json").write_text(
            json.dumps(asdict(summary), indent=2, default=_json_default),
            encoding="utf-8",
        )
        (output_dir / "kalshi_paper_report.md").write_text(
            self._build_report(state=state, summary=summary),
            encoding="utf-8",
        )

    def _build_report(self, *, state: KalshiPaperState, summary: KalshiPaperSessionSummary) -> str:
        lines = [
            "# Kalshi Paper Trading Report",
            "",
            f"Generated: {summary.generated_at}",
            "",
            "## Session Summary",
            "",
            f"- Markets polled: {summary.markets_polled}",
            f"- Candidate signals: {summary.candidate_signals}",
            f"- Executed entries: {summary.executed_entries}",
            f"- Executed exits: {summary.executed_exits}",
            f"- Open positions: {summary.open_positions}",
            f"- Closed trades: {summary.closed_trades}",
            f"- Cash: {summary.cash:.2f}",
            f"- Equity: {summary.equity:.2f}",
            f"- Realized P&L: {summary.realized_pnl:.2f}",
            f"- Unrealized P&L: {summary.unrealized_pnl:.2f}",
            f"- Drawdown: {summary.current_drawdown_pct:.2%}",
            f"- Halted: {summary.halted}",
        ]
        if summary.halt_reason:
            lines.append(f"- Halt reason: {summary.halt_reason}")
        lines += [
            "",
            "## Open Positions",
            "",
            "| Ticker | Side | Qty | Entry | Mark | Unrealized P&L | Signal Family | Confidence |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for position in sorted(state.open_positions.values(), key=lambda item: item.ticker):
            lines.append(
                f"| {position.ticker} | {position.side.upper()} | {position.quantity} | {position.entry_price:.4f} "
                f"| {position.current_mark_price:.4f} | {position.unrealized_pnl:.4f} | {position.signal_family} | {position.confidence:.2f} |"
            )
        if not state.open_positions:
            lines.append("| none | - | - | - | - | - | - | - |")
        lines += [
            "",
            "## Rejections",
            "",
        ]
        if summary.rejected_by_reason:
            for reason, count in summary.rejected_by_reason.items():
                lines.append(f"- {reason}: {count}")
        else:
            lines.append("- none")
        return "\n".join(lines)
