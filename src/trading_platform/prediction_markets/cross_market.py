from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from difflib import SequenceMatcher
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, Sequence

from trading_platform.kalshi.models import KalshiMarket, price_to_float

_STOPWORDS = {
    "a",
    "an",
    "and",
    "be",
    "by",
    "for",
    "if",
    "in",
    "is",
    "of",
    "on",
    "the",
    "to",
    "will",
    "with",
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return str(value)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed):
        return None
    return parsed


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _round_or_none(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(float(value), digits)


def _coerce_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return [segment.strip() for segment in raw.split(",") if segment.strip()]
        return parsed if isinstance(parsed, list) else [parsed]
    return [value]


def normalize_prediction_text(text: str | None) -> str:
    raw = (text or "").lower()
    raw = raw.replace("?", " ").replace("%", " percent ")
    raw = re.sub(r"[^a-z0-9\s]", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def _tokenize_prediction_text(text: str | None) -> tuple[str, ...]:
    normalized = normalize_prediction_text(text)
    tokens = [token for token in normalized.split() if token and token not in _STOPWORDS]
    return tuple(tokens)


def _extract_numeric_tokens(tokens: Sequence[str]) -> tuple[str, ...]:
    return tuple(token for token in tokens if any(char.isdigit() for char in token))


def _jaccard_similarity(left: Sequence[str], right: Sequence[str]) -> float:
    left_set = set(left)
    right_set = set(right)
    if not left_set and not right_set:
        return 1.0
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


class PredictionMarketVenue(str, Enum):
    KALSHI = "kalshi"
    POLYMARKET = "polymarket"


@dataclass(frozen=True)
class NormalizedPredictionMarket:
    venue: PredictionMarketVenue
    source_market_id: str
    title: str
    category: str | None
    expiration_time: str | None
    yes_probability: float | None
    liquidity_proxy: float | None
    volume_proxy: float | None
    equivalence_key: str
    normalized_title: str
    matchable_tokens: tuple[str, ...]
    settlement_rules: str | None = None
    match_score_hint: float | None = None
    settlement_mismatch_flags: tuple[str, ...] = ()
    raw: dict[str, Any] = field(default_factory=dict, compare=False)

    @property
    def expiration_dt(self) -> datetime | None:
        return _parse_timestamp(self.expiration_time)


@dataclass(frozen=True)
class CrossMarketMatchRecord:
    observed_at: str
    run_id: str
    snapshot_tag: str | None
    kalshi_market_id: str
    polymarket_market_id: str | None
    kalshi_title: str
    polymarket_title: str | None
    category: str | None
    equivalence_key: str
    match_score: float
    title_similarity: float
    token_overlap: float
    expiration_diff_hours: float | None
    probability_spread: float | None
    accepted: bool
    ambiguous: bool
    rejection_reasons: tuple[str, ...] = ()
    settlement_mismatch_flags: tuple[str, ...] = ()


@dataclass(frozen=True)
class CrossMarketOpportunityRecord:
    observed_at: str
    run_id: str
    snapshot_tag: str | None
    equivalence_key: str
    category: str | None
    kalshi_market_id: str
    polymarket_market_id: str
    kalshi_title: str
    polymarket_title: str
    kalshi_yes_probability: float
    polymarket_yes_probability: float
    probability_spread: float
    absolute_spread: float
    match_score: float
    title_similarity: float
    token_overlap: float
    expiration_diff_hours: float | None
    kalshi_liquidity_proxy: float | None
    polymarket_liquidity_proxy: float | None


@dataclass(frozen=True)
class CrossMarketMonitorConfig:
    output_dir: str
    min_probability_spread: float = 0.03
    match_threshold: float = 0.84
    ambiguity_margin: float = 0.03
    max_expiration_diff_hours: float = 24.0
    min_title_similarity: float = 0.72
    min_token_overlap: float = 0.60
    kalshi_max_markets: int | None = 250
    polymarket_max_markets: int | None = 250
    append_history: bool = True
    snapshot_tag: str | None = None

    def __post_init__(self) -> None:
        if self.min_probability_spread < 0:
            raise ValueError("min_probability_spread must be >= 0.")
        if not 0 <= self.match_threshold <= 1:
            raise ValueError("match_threshold must be between 0 and 1.")
        if self.ambiguity_margin < 0:
            raise ValueError("ambiguity_margin must be >= 0.")
        if self.max_expiration_diff_hours < 0:
            raise ValueError("max_expiration_diff_hours must be >= 0.")
        if not 0 <= self.min_title_similarity <= 1:
            raise ValueError("min_title_similarity must be between 0 and 1.")
        if not 0 <= self.min_token_overlap <= 1:
            raise ValueError("min_token_overlap must be between 0 and 1.")


@dataclass(frozen=True)
class CrossMarketRunSummary:
    generated_at: str
    run_id: str
    snapshot_tag: str | None
    total_kalshi_markets: int
    total_polymarket_markets: int
    total_candidate_matches: int
    total_accepted_matches: int
    total_opportunities: int
    average_spread: float
    max_spread: float
    category_breakdown: list[dict[str, Any]]
    persistence_summary: list[dict[str, Any]]
    strongest_opportunities: list[dict[str, Any]]
    rejected_examples: list[dict[str, Any]]
    matching_assumptions: dict[str, Any]


class MarketAdapter(Protocol):
    def fetch_markets(self, *, max_markets: int | None = None) -> list[NormalizedPredictionMarket]:
        ...


def _build_equivalence_key(title: str, expiration_time: str | None) -> str:
    normalized_title = normalize_prediction_text(title)
    expiration_dt = _parse_timestamp(expiration_time)
    if expiration_dt is None:
        return normalized_title
    return f"{normalized_title}|{expiration_dt.date().isoformat()}"


def _mid_probability_from_quotes(*, yes_bid: float | None, yes_ask: float | None) -> float | None:
    if yes_bid is not None and yes_ask is not None:
        if yes_ask < yes_bid:
            yes_ask = yes_bid
        return (yes_bid + yes_ask) / 2.0
    if yes_bid is not None:
        return yes_bid
    return yes_ask


class KalshiMarketAdapter:
    def __init__(self, client: Any) -> None:
        self.client = client

    def fetch_markets(self, *, max_markets: int | None = None) -> list[NormalizedPredictionMarket]:
        markets = self.client.get_all_markets(status="open")
        if max_markets is not None:
            markets = markets[:max_markets]
        return [self._normalize_market(market) for market in markets]

    def _normalize_market(self, market: KalshiMarket) -> NormalizedPredictionMarket:
        yes_bid = price_to_float(market.yes_bid)
        yes_ask = price_to_float(market.yes_ask)
        no_bid = price_to_float(market.no_bid)
        if yes_ask is None and no_bid is not None:
            yes_ask = 1.0 - no_bid
        yes_probability = _mid_probability_from_quotes(yes_bid=yes_bid, yes_ask=yes_ask)
        title = market.title or market.ticker
        category = str(market.category or "unknown")
        settlement_rules = str(market.subtitle or market.raw.get("rules_primary") or market.raw.get("rules") or "")
        tokens = _tokenize_prediction_text(f"{title} {settlement_rules}")
        return NormalizedPredictionMarket(
            venue=PredictionMarketVenue.KALSHI,
            source_market_id=market.ticker,
            title=title,
            category=category,
            expiration_time=market.close_time,
            yes_probability=_round_or_none(yes_probability),
            liquidity_proxy=_round_or_none(price_to_float(market.liquidity)),
            volume_proxy=_round_or_none(_safe_float(market.volume)),
            equivalence_key=_build_equivalence_key(title, market.close_time),
            normalized_title=normalize_prediction_text(title),
            matchable_tokens=tokens,
            settlement_rules=settlement_rules or None,
            raw=market.raw,
        )


class PolymarketMarketAdapter:
    def __init__(self, client: Any) -> None:
        self.client = client

    def fetch_markets(self, *, max_markets: int | None = None) -> list[NormalizedPredictionMarket]:
        markets = self.client.get_all_markets(max_markets=max_markets, active=True, closed=False, archived=False)
        normalized: list[NormalizedPredictionMarket] = []
        for market in markets:
            normalized_market = self._normalize_market(market)
            if normalized_market is not None:
                normalized.append(normalized_market)
        return normalized

    def _normalize_market(self, market: dict[str, Any]) -> NormalizedPredictionMarket | None:
        outcome_names = [str(item).strip().lower() for item in _coerce_list(market.get("outcomes"))]
        outcome_prices = [_safe_float(item) for item in _coerce_list(market.get("outcomePrices"))]
        if len(outcome_names) != 2 or len(outcome_prices) != 2:
            return None
        if {"yes", "no"} != set(outcome_names):
            return None
        yes_index = outcome_names.index("yes")
        yes_probability = outcome_prices[yes_index]
        if yes_probability is None:
            return None
        title = str(
            market.get("question")
            or market.get("title")
            or market.get("description")
            or market.get("slug")
            or market.get("id")
        )
        expiration = (
            market.get("endDate")
            or market.get("end_date_iso")
            or market.get("end_time")
            or market.get("closeTime")
            or market.get("closedTime")
        )
        category = str(market.get("category") or market.get("tag") or "unknown")
        settlement_rules = str(
            market.get("description")
            or market.get("rules")
            or market.get("resolutionSource")
            or ""
        )
        source_market_id = str(market.get("id") or market.get("conditionId") or market.get("slug") or title)
        liquidity_proxy = _safe_float(market.get("liquidityNum"))
        if liquidity_proxy is None:
            liquidity_proxy = _safe_float(market.get("liquidity"))
        volume_proxy = _safe_float(market.get("volumeNum"))
        if volume_proxy is None:
            volume_proxy = _safe_float(market.get("volume"))
        tokens = _tokenize_prediction_text(f"{title} {settlement_rules}")
        return NormalizedPredictionMarket(
            venue=PredictionMarketVenue.POLYMARKET,
            source_market_id=source_market_id,
            title=title,
            category=category,
            expiration_time=None if expiration is None else str(expiration),
            yes_probability=_round_or_none(yes_probability),
            liquidity_proxy=_round_or_none(liquidity_proxy),
            volume_proxy=_round_or_none(volume_proxy),
            equivalence_key=_build_equivalence_key(title, None if expiration is None else str(expiration)),
            normalized_title=normalize_prediction_text(title),
            matchable_tokens=tokens,
            settlement_rules=settlement_rules or None,
            raw=market,
        )


class CrossMarketMonitor:
    def __init__(
        self,
        *,
        kalshi_adapter: MarketAdapter,
        polymarket_adapter: MarketAdapter,
        config: CrossMarketMonitorConfig,
    ) -> None:
        self.kalshi_adapter = kalshi_adapter
        self.polymarket_adapter = polymarket_adapter
        self.config = config

    def run(self) -> CrossMarketRunSummary:
        observed_at = _utc_now()
        run_id = observed_at.strftime("%Y%m%dT%H%M%SZ")
        kalshi_markets = self.kalshi_adapter.fetch_markets(max_markets=self.config.kalshi_max_markets)
        polymarket_markets = self.polymarket_adapter.fetch_markets(max_markets=self.config.polymarket_max_markets)

        matches, opportunities = self._build_matches(
            kalshi_markets=kalshi_markets,
            polymarket_markets=polymarket_markets,
            observed_at=observed_at,
            run_id=run_id,
        )
        summary = self._build_summary(
            observed_at=observed_at,
            run_id=run_id,
            kalshi_markets=kalshi_markets,
            polymarket_markets=polymarket_markets,
            matches=matches,
            opportunities=opportunities,
        )
        self._write_artifacts(summary=summary, matches=matches, opportunities=opportunities)
        return summary

    def _build_matches(
        self,
        *,
        kalshi_markets: Sequence[NormalizedPredictionMarket],
        polymarket_markets: Sequence[NormalizedPredictionMarket],
        observed_at: datetime,
        run_id: str,
    ) -> tuple[list[CrossMarketMatchRecord], list[CrossMarketOpportunityRecord]]:
        matches: list[CrossMarketMatchRecord] = []
        opportunities: list[CrossMarketOpportunityRecord] = []

        for kalshi_market in kalshi_markets:
            scored_candidates: list[tuple[CrossMarketMatchRecord, NormalizedPredictionMarket]] = []
            for polymarket_market in polymarket_markets:
                record = self._score_pair(
                    kalshi_market=kalshi_market,
                    polymarket_market=polymarket_market,
                    observed_at=observed_at,
                    run_id=run_id,
                )
                scored_candidates.append((record, polymarket_market))
            if not scored_candidates:
                continue
            scored_candidates.sort(key=lambda item: item[0].match_score, reverse=True)
            best_record, best_market = scored_candidates[0]
            second_score = scored_candidates[1][0].match_score if len(scored_candidates) > 1 else None
            ambiguous = (
                second_score is not None
                and best_record.match_score >= self.config.match_threshold
                and second_score >= self.config.match_threshold - self.config.ambiguity_margin
                and abs(best_record.match_score - second_score) <= self.config.ambiguity_margin
            )

            rejection_reasons = list(best_record.rejection_reasons)
            if ambiguous:
                rejection_reasons.append("ambiguous_match")
            accepted = best_record.accepted and not ambiguous
            match_record = CrossMarketMatchRecord(
                observed_at=best_record.observed_at,
                run_id=best_record.run_id,
                snapshot_tag=best_record.snapshot_tag,
                kalshi_market_id=best_record.kalshi_market_id,
                polymarket_market_id=best_record.polymarket_market_id,
                kalshi_title=best_record.kalshi_title,
                polymarket_title=best_record.polymarket_title,
                category=best_record.category,
                equivalence_key=best_record.equivalence_key,
                match_score=best_record.match_score,
                title_similarity=best_record.title_similarity,
                token_overlap=best_record.token_overlap,
                expiration_diff_hours=best_record.expiration_diff_hours,
                probability_spread=best_record.probability_spread,
                accepted=accepted,
                ambiguous=ambiguous,
                rejection_reasons=tuple(dict.fromkeys(rejection_reasons)),
                settlement_mismatch_flags=best_record.settlement_mismatch_flags,
            )
            matches.append(match_record)

            if (
                accepted
                and match_record.probability_spread is not None
                and abs(match_record.probability_spread) >= self.config.min_probability_spread
                and kalshi_market.yes_probability is not None
                and best_market.yes_probability is not None
            ):
                opportunities.append(
                    CrossMarketOpportunityRecord(
                        observed_at=observed_at.isoformat(),
                        run_id=run_id,
                        snapshot_tag=self.config.snapshot_tag,
                        equivalence_key=kalshi_market.equivalence_key,
                        category=kalshi_market.category,
                        kalshi_market_id=kalshi_market.source_market_id,
                        polymarket_market_id=best_market.source_market_id,
                        kalshi_title=kalshi_market.title,
                        polymarket_title=best_market.title,
                        kalshi_yes_probability=float(kalshi_market.yes_probability),
                        polymarket_yes_probability=float(best_market.yes_probability),
                        probability_spread=float(kalshi_market.yes_probability - best_market.yes_probability),
                        absolute_spread=abs(float(kalshi_market.yes_probability - best_market.yes_probability)),
                        match_score=match_record.match_score,
                        title_similarity=match_record.title_similarity,
                        token_overlap=match_record.token_overlap,
                        expiration_diff_hours=match_record.expiration_diff_hours,
                        kalshi_liquidity_proxy=kalshi_market.liquidity_proxy,
                        polymarket_liquidity_proxy=best_market.liquidity_proxy,
                    )
                )

        return matches, opportunities

    def _score_pair(
        self,
        *,
        kalshi_market: NormalizedPredictionMarket,
        polymarket_market: NormalizedPredictionMarket,
        observed_at: datetime,
        run_id: str,
    ) -> CrossMarketMatchRecord:
        title_similarity = SequenceMatcher(
            None,
            kalshi_market.normalized_title,
            polymarket_market.normalized_title,
        ).ratio()
        token_overlap = _jaccard_similarity(kalshi_market.matchable_tokens, polymarket_market.matchable_tokens)
        category_match = (
            1.0
            if kalshi_market.category and polymarket_market.category and kalshi_market.category == polymarket_market.category
            else 0.5
            if not kalshi_market.category or not polymarket_market.category
            else 0.0
        )
        expiration_diff_hours = self._expiration_diff_hours(kalshi_market, polymarket_market)
        expiration_score = 0.5
        if expiration_diff_hours is not None:
            capped = min(expiration_diff_hours, self.config.max_expiration_diff_hours)
            expiration_score = max(0.0, 1.0 - (capped / max(self.config.max_expiration_diff_hours, 1e-9)))

        score = (
            0.45 * title_similarity
            + 0.30 * token_overlap
            + 0.15 * category_match
            + 0.10 * expiration_score
        )
        if kalshi_market.equivalence_key == polymarket_market.equivalence_key:
            score = min(1.0, score + 0.10)

        mismatch_flags: list[str] = []
        rejection_reasons: list[str] = []
        if title_similarity < self.config.min_title_similarity:
            rejection_reasons.append("title_similarity_below_threshold")
        if token_overlap < self.config.min_token_overlap:
            rejection_reasons.append("token_overlap_below_threshold")

        kalshi_numeric = set(_extract_numeric_tokens(kalshi_market.matchable_tokens))
        polymarket_numeric = set(_extract_numeric_tokens(polymarket_market.matchable_tokens))
        if kalshi_numeric and polymarket_numeric and kalshi_numeric != polymarket_numeric:
            mismatch_flags.append("numeric_token_mismatch")

        settlement_similarity = _jaccard_similarity(
            _tokenize_prediction_text(kalshi_market.settlement_rules),
            _tokenize_prediction_text(polymarket_market.settlement_rules),
        )
        if kalshi_market.settlement_rules and polymarket_market.settlement_rules and settlement_similarity < 0.35:
            mismatch_flags.append("settlement_rule_mismatch")
        if expiration_diff_hours is not None and expiration_diff_hours > self.config.max_expiration_diff_hours:
            mismatch_flags.append("expiration_mismatch")

        probability_spread = None
        if kalshi_market.yes_probability is not None and polymarket_market.yes_probability is not None:
            probability_spread = float(kalshi_market.yes_probability - polymarket_market.yes_probability)

        accepted = (
            score >= self.config.match_threshold
            and not mismatch_flags
            and title_similarity >= self.config.min_title_similarity
            and token_overlap >= self.config.min_token_overlap
        )
        if score < self.config.match_threshold:
            rejection_reasons.append("match_score_below_threshold")

        return CrossMarketMatchRecord(
            observed_at=observed_at.isoformat(),
            run_id=run_id,
            snapshot_tag=self.config.snapshot_tag,
            kalshi_market_id=kalshi_market.source_market_id,
            polymarket_market_id=polymarket_market.source_market_id,
            kalshi_title=kalshi_market.title,
            polymarket_title=polymarket_market.title,
            category=kalshi_market.category,
            equivalence_key=kalshi_market.equivalence_key,
            match_score=round(score, 6),
            title_similarity=round(title_similarity, 6),
            token_overlap=round(token_overlap, 6),
            expiration_diff_hours=_round_or_none(expiration_diff_hours),
            probability_spread=_round_or_none(probability_spread),
            accepted=accepted,
            ambiguous=False,
            rejection_reasons=tuple(dict.fromkeys(rejection_reasons)),
            settlement_mismatch_flags=tuple(dict.fromkeys(mismatch_flags)),
        )

    def _expiration_diff_hours(
        self,
        left: NormalizedPredictionMarket,
        right: NormalizedPredictionMarket,
    ) -> float | None:
        left_expiry = left.expiration_dt
        right_expiry = right.expiration_dt
        if left_expiry is None or right_expiry is None:
            return None
        return abs((left_expiry - right_expiry).total_seconds()) / 3600.0

    def _build_summary(
        self,
        *,
        observed_at: datetime,
        run_id: str,
        kalshi_markets: Sequence[NormalizedPredictionMarket],
        polymarket_markets: Sequence[NormalizedPredictionMarket],
        matches: Sequence[CrossMarketMatchRecord],
        opportunities: Sequence[CrossMarketOpportunityRecord],
    ) -> CrossMarketRunSummary:
        spreads = [opportunity.absolute_spread for opportunity in opportunities]
        category_breakdown = self._category_breakdown(opportunities)
        persistence_summary = self._build_persistence_summary(opportunities)
        strongest_opportunities = [
            asdict(opportunity)
            for opportunity in sorted(opportunities, key=lambda item: item.absolute_spread, reverse=True)[:5]
        ]
        rejected_examples = [asdict(match) for match in matches if not match.accepted][:5]

        return CrossMarketRunSummary(
            generated_at=observed_at.isoformat(),
            run_id=run_id,
            snapshot_tag=self.config.snapshot_tag,
            total_kalshi_markets=len(kalshi_markets),
            total_polymarket_markets=len(polymarket_markets),
            total_candidate_matches=len(matches),
            total_accepted_matches=sum(1 for match in matches if match.accepted),
            total_opportunities=len(opportunities),
            average_spread=round(sum(spreads) / len(spreads), 6) if spreads else 0.0,
            max_spread=round(max(spreads), 6) if spreads else 0.0,
            category_breakdown=category_breakdown,
            persistence_summary=persistence_summary,
            strongest_opportunities=strongest_opportunities,
            rejected_examples=rejected_examples,
            matching_assumptions={
                "min_probability_spread": self.config.min_probability_spread,
                "match_threshold": self.config.match_threshold,
                "ambiguity_margin": self.config.ambiguity_margin,
                "max_expiration_diff_hours": self.config.max_expiration_diff_hours,
                "min_title_similarity": self.config.min_title_similarity,
                "min_token_overlap": self.config.min_token_overlap,
            },
        )

    def _category_breakdown(
        self,
        opportunities: Sequence[CrossMarketOpportunityRecord],
    ) -> list[dict[str, Any]]:
        grouped: dict[str, list[CrossMarketOpportunityRecord]] = {}
        for opportunity in opportunities:
            grouped.setdefault(str(opportunity.category or "unknown"), []).append(opportunity)
        rows: list[dict[str, Any]] = []
        for category, items in sorted(grouped.items()):
            spreads = [item.absolute_spread for item in items]
            rows.append(
                {
                    "category": category,
                    "opportunity_count": len(items),
                    "average_spread": round(sum(spreads) / len(spreads), 6),
                    "max_spread": round(max(spreads), 6),
                }
            )
        return rows

    def _build_persistence_summary(
        self,
        opportunities: Sequence[CrossMarketOpportunityRecord],
    ) -> list[dict[str, Any]]:
        history_path = Path(self.config.output_dir) / "cross_market_opportunities.jsonl"
        history_rows: list[dict[str, Any]] = []
        if self.config.append_history and history_path.exists():
            history_rows.extend(self._read_jsonl(history_path))
        history_rows.extend(asdict(opportunity) for opportunity in opportunities)

        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in history_rows:
            grouped.setdefault(str(row.get("equivalence_key", "")), []).append(row)

        summaries: list[dict[str, Any]] = []
        for key, rows in grouped.items():
            if not key:
                continue
            sorted_rows = sorted(rows, key=lambda item: str(item.get("observed_at", "")))
            spreads = [abs(float(item.get("probability_spread", 0.0))) for item in sorted_rows]
            summaries.append(
                {
                    "equivalence_key": key,
                    "observation_count": len(sorted_rows),
                    "first_seen": sorted_rows[0].get("observed_at"),
                    "last_seen": sorted_rows[-1].get("observed_at"),
                    "average_spread": round(sum(spreads) / len(spreads), 6),
                    "max_spread": round(max(spreads), 6),
                }
            )
        summaries.sort(key=lambda item: (item["observation_count"], item["max_spread"]), reverse=True)
        return summaries[:10]

    def _write_artifacts(
        self,
        *,
        summary: CrossMarketRunSummary,
        matches: Sequence[CrossMarketMatchRecord],
        opportunities: Sequence[CrossMarketOpportunityRecord],
    ) -> None:
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        self._write_jsonl(
            output_dir / "cross_market_matches.jsonl",
            [asdict(match) for match in matches],
            append=self.config.append_history,
        )
        self._write_jsonl(
            output_dir / "cross_market_opportunities.jsonl",
            [asdict(opportunity) for opportunity in opportunities],
            append=self.config.append_history,
        )
        (output_dir / "cross_market_summary.json").write_text(
            json.dumps(asdict(summary), indent=2, default=_json_default),
            encoding="utf-8",
        )
        (output_dir / "cross_market_report.md").write_text(
            self._build_report(summary=summary),
            encoding="utf-8",
        )

    def _write_jsonl(self, path: Path, rows: Sequence[dict[str, Any]], *, append: bool) -> None:
        mode = "a" if append else "w"
        with path.open(mode, encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, default=_json_default) + "\n")

    def _read_jsonl(self, path: Path) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return rows

    def _build_report(self, *, summary: CrossMarketRunSummary) -> str:
        lines = [
            "# Cross-Market Monitor Report",
            "",
            f"Generated: {summary.generated_at}",
            "",
            "## Scan Summary",
            "",
            f"- Kalshi markets scanned: {summary.total_kalshi_markets}",
            f"- Polymarket markets scanned: {summary.total_polymarket_markets}",
            f"- Candidate matches: {summary.total_candidate_matches}",
            f"- Accepted matches: {summary.total_accepted_matches}",
            f"- Opportunities above spread threshold: {summary.total_opportunities}",
            f"- Average spread: {summary.average_spread:.4f}",
            f"- Max spread: {summary.max_spread:.4f}",
            "",
            "## Strongest Opportunities",
            "",
            "| Kalshi | Polymarket | Category | Spread | Match Score |",
            "| --- | --- | --- | --- | --- |",
        ]
        if summary.strongest_opportunities:
            for row in summary.strongest_opportunities:
                lines.append(
                    f"| {row['kalshi_market_id']} | {row['polymarket_market_id']} | {row.get('category') or 'unknown'} "
                    f"| {float(row['probability_spread']):.4f} | {float(row['match_score']):.4f} |"
                )
        else:
            lines.append("| none | none | - | - | - |")

        lines.extend(["", "## Rejected Examples", ""])
        if summary.rejected_examples:
            for row in summary.rejected_examples:
                reasons = ", ".join(row.get("rejection_reasons") or [])
                flags = ", ".join(row.get("settlement_mismatch_flags") or [])
                lines.append(
                    f"- {row['kalshi_market_id']} -> {row.get('polymarket_market_id') or 'none'}: "
                    f"score={float(row['match_score']):.4f}; reasons={reasons or 'none'}; mismatches={flags or 'none'}"
                )
        else:
            lines.append("- none")

        lines.extend(["", "## Persistence", ""])
        if summary.persistence_summary:
            for row in summary.persistence_summary:
                lines.append(
                    f"- {row['equivalence_key']}: observations={row['observation_count']}, "
                    f"avg_spread={float(row['average_spread']):.4f}, max_spread={float(row['max_spread']):.4f}"
                )
        else:
            lines.append("- none")
        return "\n".join(lines)
