from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from trading_platform.data.fundamentals.models import (
    CANONICAL_FUNDAMENTAL_METRICS,
    CompanyMasterRecord,
    FilingMetadataRecord,
    FUNDAMENTAL_FILING_COLUMNS,
    FundamentalValueRecord,
)
from trading_platform.data.fundamentals.providers.base import (
    FundamentalsProvider,
    ProviderFetchResult,
)


SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions"
SEC_COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts"
SEC_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
DEFAULT_SEC_USER_AGENT = os.getenv(
    "TRADING_PLATFORM_SEC_USER_AGENT",
    "trading-platform/1.0 (local fundamentals snapshot)",
)
DEFAULT_SEC_CACHE_TTL_DAYS = 30.0
DEFAULT_SEC_REQUEST_DELAY_SECONDS = 0.2
DEFAULT_SEC_MAX_RETRIES = 4
DEFAULT_SEC_MAX_BACKOFF_SECONDS = 30.0
SEC_TIMEOUT_SECONDS = 30

SEC_CONCEPT_MAP: dict[str, tuple[str, ...]] = {
    "revenue": ("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"),
    "gross_profit": ("GrossProfit",),
    "operating_income": ("OperatingIncomeLoss",),
    "net_income": ("NetIncomeLoss", "ProfitLoss"),
    "total_assets": ("Assets",),
    "total_liabilities": ("Liabilities",),
    "shareholders_equity": ("StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
    "cash_and_equivalents": ("CashAndCashEquivalentsAtCarryingValue",),
    "current_assets": ("AssetsCurrent",),
    "current_liabilities": ("LiabilitiesCurrent",),
    "long_term_debt": ("LongTermDebt", "LongTermDebtNoncurrent"),
    "operating_cash_flow": ("NetCashProvidedByUsedInOperatingActivities",),
    "capital_expenditures": ("PaymentsToAcquirePropertyPlantAndEquipment", "CapitalExpendituresIncurredButNotYetPaid"),
    "shares_outstanding": ("CommonStockSharesOutstanding", "EntityCommonStockSharesOutstanding"),
}

SEC_FACT_NAMESPACES = ("us-gaap", "dei")


def _now_utc_iso() -> str:
    return datetime.now(UTC).isoformat()


def _normalize_cik(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(10) if digits else None


def _load_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _coerce_date(value: Any) -> pd.Timestamp | None:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return None
    return pd.Timestamp(timestamp).normalize()


def _format_date(value: pd.Timestamp | None) -> str | None:
    return value.strftime("%Y-%m-%d") if value is not None else None


def _empty_result(provider_name: str, **diagnostics: Any) -> ProviderFetchResult:
    return ProviderFetchResult(
        company_master_df=pd.DataFrame(),
        filing_metadata_df=pd.DataFrame(),
        fundamental_values_df=pd.DataFrame(),
        diagnostics={"provider": provider_name, **diagnostics},
    )


def _empty_filing_frame(provider_name: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            FilingMetadataRecord(
                symbol="",
                cik=None,
                fiscal_year=None,
                fiscal_period=None,
                period_type=None,
                period_end_date=None,
                filing_date=None,
                available_date=None,
                form_type=None,
                accession_number=None,
                source=provider_name,
            ).to_dict()
        ]
    ).iloc[0:0].copy()


def _empty_value_frame(provider_name: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            FundamentalValueRecord(
                symbol="",
                cik=None,
                fiscal_year=None,
                fiscal_period=None,
                period_type=None,
                period_end_date=None,
                filing_date=None,
                available_date=None,
                form_type=None,
                accession_number=None,
                source=provider_name,
                metric_name="",
            ).to_dict()
        ]
    ).iloc[0:0].copy()


class SECClient:
    def __init__(
        self,
        *,
        raw_cache_root: Path,
        user_agent: str,
        cache_enabled: bool = True,
        cache_ttl_days: float = DEFAULT_SEC_CACHE_TTL_DAYS,
        force_refresh: bool = False,
        request_delay_seconds: float = DEFAULT_SEC_REQUEST_DELAY_SECONDS,
        max_retries: int = DEFAULT_SEC_MAX_RETRIES,
        max_backoff_seconds: float = DEFAULT_SEC_MAX_BACKOFF_SECONDS,
        max_requests_per_run: int | None = None,
        offline: bool = False,
        sleep_fn: Any = None,
        monotonic_fn: Any = None,
    ) -> None:
        self.raw_cache_root = Path(raw_cache_root)
        self.user_agent = user_agent
        self.cache_enabled = cache_enabled
        self.cache_ttl_days = float(cache_ttl_days)
        self.force_refresh = force_refresh
        self.request_delay_seconds = max(float(request_delay_seconds), 0.0)
        self.max_retries = max(int(max_retries), 0)
        self.max_backoff_seconds = max(float(max_backoff_seconds), 0.0)
        self.max_requests_per_run = max_requests_per_run
        self.offline = offline
        self.sleep_fn = sleep_fn or time.sleep
        self.monotonic_fn = monotonic_fn or time.monotonic
        self._last_request_started_at: float | None = None
        self.cache_hits = 0
        self.cache_misses = 0
        self.retry_count = 0
        self.rate_limit_error_count = 0
        self.requests_made = 0

    def _cache_file(self, *segments: str) -> Path:
        cleaned = [segment.strip("/\\") for segment in segments if str(segment).strip()]
        return self.raw_cache_root.joinpath(*cleaned)

    def _is_cache_fresh(self, cache_path: Path) -> bool:
        if not self.cache_enabled or not cache_path.exists() or self.force_refresh:
            return False
        ttl_seconds = max(self.cache_ttl_days, 0.0) * 86400.0
        if ttl_seconds == 0.0:
            return False
        age_seconds = max(time.time() - cache_path.stat().st_mtime, 0.0)
        return age_seconds <= ttl_seconds

    def _load_cached_json(self, cache_path: Path) -> dict[str, Any]:
        return _load_json(cache_path)

    def _write_cached_json(self, cache_path: Path, payload: dict[str, Any]) -> None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    def _throttle(self) -> None:
        if self.request_delay_seconds <= 0.0:
            return
        now = self.monotonic_fn()
        if self._last_request_started_at is None:
            self._last_request_started_at = now
            return
        elapsed = now - self._last_request_started_at
        remaining = self.request_delay_seconds - elapsed
        if remaining > 0.0:
            self.sleep_fn(remaining)
        self._last_request_started_at = self.monotonic_fn()

    def _retry_delay_seconds(self, attempt_number: int) -> float:
        base = min(float(2 ** max(attempt_number - 1, 0)), self.max_backoff_seconds)
        deterministic_jitter = min(0.101 * attempt_number, 0.4)
        return min(base + deterministic_jitter, self.max_backoff_seconds)

    def _is_retryable_exception(self, exc: Exception) -> bool:
        if isinstance(exc, HTTPError):
            return exc.code in {429, 500, 502, 503, 504}
        return isinstance(exc, (TimeoutError, URLError))

    def _fetch_json(self, *, url: str, cache_path: Path) -> dict[str, Any]:
        if self._is_cache_fresh(cache_path):
            self.cache_hits += 1
            return self._load_cached_json(cache_path)
        self.cache_misses += 1
        if self.offline:
            return self._load_cached_json(cache_path)
        if self.max_requests_per_run is not None and self.requests_made >= self.max_requests_per_run:
            raise RuntimeError(f"SEC request budget exhausted before fetching {url}")
        request = Request(url, headers={"User-Agent": self.user_agent})
        last_error: Exception | None = None
        for attempt_number in range(1, self.max_retries + 2):
            try:
                self._throttle()
                self.requests_made += 1
                with urlopen(request, timeout=SEC_TIMEOUT_SECONDS) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                if isinstance(payload, dict):
                    self._write_cached_json(cache_path, payload)
                    return payload
                self._write_cached_json(cache_path, {"payload": payload})
                return {"payload": payload}
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if isinstance(exc, HTTPError) and exc.code == 429:
                    self.rate_limit_error_count += 1
                if attempt_number > self.max_retries or not self._is_retryable_exception(exc):
                    break
                self.retry_count += 1
                self.sleep_fn(self._retry_delay_seconds(attempt_number))
        raise RuntimeError(f"SEC request failed for {url}: {last_error}") from last_error

    def fetch_ticker_map(self) -> pd.DataFrame:
        cache_path = self._cache_file("company_tickers.json")
        payload = self._fetch_json(url=SEC_TICKER_MAP_URL, cache_path=cache_path)
        rows: list[dict[str, Any]] = []
        if isinstance(payload, dict):
            for value in payload.values():
                if not isinstance(value, dict):
                    continue
                symbol = str(value.get("ticker") or "").strip().upper()
                cik = _normalize_cik(value.get("cik_str"))
                if not symbol or cik is None:
                    continue
                rows.append(
                    {
                        "symbol": symbol,
                        "cik": cik,
                        "company_name": value.get("title"),
                        "source": "sec:ticker_map",
                    }
                )
        return pd.DataFrame(rows)

    def fetch_submissions(self, cik: str) -> dict[str, Any]:
        normalized_cik = _normalize_cik(cik)
        if normalized_cik is None:
            return {}
        cache_path = self._cache_file("submissions", f"CIK{normalized_cik}.json")
        return self._fetch_json(
            url=f"{SEC_SUBMISSIONS_URL}/CIK{normalized_cik}.json",
            cache_path=cache_path,
        )

    def fetch_companyfacts(self, cik: str) -> dict[str, Any]:
        normalized_cik = _normalize_cik(cik)
        if normalized_cik is None:
            return {}
        cache_path = self._cache_file("companyfacts", f"CIK{normalized_cik}.json")
        return self._fetch_json(
            url=f"{SEC_COMPANYFACTS_URL}/CIK{normalized_cik}.json",
            cache_path=cache_path,
        )

    def diagnostics_summary(self) -> dict[str, Any]:
        return {
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "retry_count": self.retry_count,
            "rate_limit_error_count": self.rate_limit_error_count,
            "requests_made": self.requests_made,
        }


class SECFundamentalsProvider(FundamentalsProvider):
    provider_name = "sec"

    def __init__(
        self,
        *,
        companyfacts_root: str | Path | None = None,
        submissions_root: str | Path | None = None,
        symbol_cik_map_path: str | Path | None = None,
        cache_enabled: bool = True,
        cache_root: str | Path | None = None,
        cache_ttl_days: float = DEFAULT_SEC_CACHE_TTL_DAYS,
        force_refresh: bool = False,
        request_delay_seconds: float = DEFAULT_SEC_REQUEST_DELAY_SECONDS,
        max_retries: int = DEFAULT_SEC_MAX_RETRIES,
        max_symbols_per_run: int | None = None,
        max_requests_per_run: int | None = None,
        user_agent: str | None = None,
        offline: bool = False,
    ) -> None:
        self.cache_root = Path(cache_root) if cache_root else None
        self.submissions_root = Path(submissions_root) if submissions_root else (self.cache_root / "submissions" if self.cache_root else None)
        self.companyfacts_root = Path(companyfacts_root) if companyfacts_root else (self.cache_root / "companyfacts" if self.cache_root else None)
        self.symbol_cik_map_path = Path(symbol_cik_map_path) if symbol_cik_map_path else (
            (self.cache_root.parent / "sec_symbol_cik_map.parquet") if self.cache_root else None
        )
        self.cache_enabled = cache_enabled
        self.cache_ttl_days = cache_ttl_days
        self.force_refresh = force_refresh
        self.request_delay_seconds = request_delay_seconds
        self.max_retries = max_retries
        self.max_symbols_per_run = max_symbols_per_run
        self.max_requests_per_run = max_requests_per_run
        self.user_agent = user_agent or DEFAULT_SEC_USER_AGENT
        self.offline = offline

    def is_configured(self) -> bool:
        return bool(self.submissions_root or self.companyfacts_root or self.cache_root)

    def _resolve_local_path(self, root: Path | None, *, symbol: str | None = None, cik: str | None = None) -> Path | None:
        if root is None:
            return None
        candidates: list[Path] = []
        normalized_cik = _normalize_cik(cik)
        if normalized_cik is not None:
            candidates.append(root / f"CIK{normalized_cik}.json")
        if symbol:
            candidates.append(root / f"{str(symbol).upper()}.json")
        return next((candidate for candidate in candidates if candidate.exists()), None)

    def _load_symbol_cik_map(self) -> pd.DataFrame:
        candidates: list[pd.DataFrame] = []
        if self.symbol_cik_map_path and self.symbol_cik_map_path.exists():
            if self.symbol_cik_map_path.suffix.lower() == ".parquet":
                candidates.append(pd.read_parquet(self.symbol_cik_map_path))
            else:
                candidates.append(pd.read_csv(self.symbol_cik_map_path))
        if candidates:
            combined = pd.concat(candidates, ignore_index=True)
            if "symbol" in combined.columns:
                combined["symbol"] = combined["symbol"].astype(str).str.upper()
            if "cik" in combined.columns:
                combined["cik"] = combined["cik"].map(_normalize_cik)
            return combined.dropna(subset=["symbol", "cik"]).drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
        return pd.DataFrame(columns=["symbol", "cik", "company_name", "source"])

    def _resolve_local_symbol_mapping_rows(self, symbols: list[str]) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for symbol in symbols:
            submissions_payload = _load_json(self._resolve_local_path(self.submissions_root, symbol=symbol))
            companyfacts_payload = _load_json(self._resolve_local_path(self.companyfacts_root, symbol=symbol))
            cik = _normalize_cik(submissions_payload.get("cik") or companyfacts_payload.get("cik"))
            if cik is None:
                continue
            rows.append(
                {
                    "symbol": str(symbol).upper(),
                    "cik": cik,
                    "company_name": submissions_payload.get("name"),
                    "source": "sec:local_cache",
                }
            )
        return pd.DataFrame(rows)

    def _write_symbol_cik_map(self, frame: pd.DataFrame) -> None:
        if self.symbol_cik_map_path is None:
            return
        self.symbol_cik_map_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(self.symbol_cik_map_path, index=False)

    def _resolve_symbol_cik_map(
        self,
        *,
        symbols: list[str],
        client: SECClient,
    ) -> tuple[pd.DataFrame, list[str], list[str]]:
        requested_symbols = [str(symbol).upper() for symbol in symbols]
        existing = self._load_symbol_cik_map()
        existing_symbols = set(existing["symbol"].tolist()) if not existing.empty else set()
        missing_symbols = [symbol for symbol in requested_symbols if symbol not in existing_symbols]

        if missing_symbols:
            local_rows = self._resolve_local_symbol_mapping_rows(missing_symbols)
            if not local_rows.empty:
                combined = pd.concat([existing, local_rows], ignore_index=True) if not existing.empty else local_rows
                combined["symbol"] = combined["symbol"].astype(str).str.upper()
                combined["cik"] = combined["cik"].map(_normalize_cik)
                combined = combined.dropna(subset=["symbol", "cik"]).drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
                self._write_symbol_cik_map(combined)
                existing = combined
                existing_symbols = set(existing["symbol"].tolist())
                missing_symbols = [symbol for symbol in requested_symbols if symbol not in existing_symbols]

        fetched_symbols: list[str] = []
        if missing_symbols and not self.offline:
            ticker_map = client.fetch_ticker_map()
            if not ticker_map.empty:
                fetched = ticker_map.loc[ticker_map["symbol"].isin(missing_symbols)].copy()
                fetched_symbols = sorted(fetched["symbol"].dropna().astype(str).str.upper().unique().tolist())
                combined = pd.concat([existing, fetched], ignore_index=True) if not existing.empty else fetched
                combined["symbol"] = combined["symbol"].astype(str).str.upper()
                combined["cik"] = combined["cik"].map(_normalize_cik)
                combined = combined.dropna(subset=["symbol", "cik"]).drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
                self._write_symbol_cik_map(combined)
                existing = combined

        resolved = existing.loc[existing["symbol"].isin(requested_symbols)].copy()
        unresolved = sorted(set(requested_symbols) - set(resolved["symbol"].astype(str).tolist()))
        return resolved.reset_index(drop=True), fetched_symbols, unresolved

    def _normalize_company_master(
        self,
        *,
        symbol: str,
        cik: str | None,
        submissions_payload: dict[str, Any],
        mapping_row: dict[str, Any] | None,
    ) -> dict[str, Any]:
        exchanges = submissions_payload.get("exchanges") or []
        return CompanyMasterRecord(
            symbol=symbol.upper(),
            cik=_normalize_cik(cik),
            company_name=submissions_payload.get("name") or (mapping_row or {}).get("company_name"),
            exchange=exchanges[0] if exchanges else None,
            sector=None,
            industry=submissions_payload.get("sicDescription"),
            is_active=True,
            source=self.provider_name,
        ).to_dict()

    def _iter_metric_entries(self, companyfacts_payload: dict[str, Any], metric_name: str) -> list[dict[str, Any]]:
        facts_root = companyfacts_payload.get("facts", {})
        for namespace in SEC_FACT_NAMESPACES:
            facts = facts_root.get(namespace, {})
            for concept in SEC_CONCEPT_MAP.get(metric_name, ()):
                concept_payload = facts.get(concept, {})
                for entries in concept_payload.get("units", {}).values():
                    if isinstance(entries, list):
                        return [entry for entry in entries if isinstance(entry, dict)]
        return []

    def _submission_accession_map(self, submissions_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
        recent = (submissions_payload.get("filings") or {}).get("recent") or {}
        accession_numbers = recent.get("accessionNumber") or []
        forms = recent.get("form") or []
        filing_dates = recent.get("filingDate") or []
        acceptance_datetimes = recent.get("acceptanceDateTime") or []
        report_dates = recent.get("reportDate") or []
        mapping: dict[str, dict[str, Any]] = {}
        row_count = len(accession_numbers)
        for index in range(row_count):
            accession = accession_numbers[index]
            if not accession:
                continue
            mapping[str(accession)] = {
                "form_type": forms[index] if index < len(forms) else None,
                "filing_date": filing_dates[index] if index < len(filing_dates) else None,
                "available_date": _format_date(_coerce_date(acceptance_datetimes[index] if index < len(acceptance_datetimes) else None))
                or (filing_dates[index] if index < len(filing_dates) else None),
                "period_end_date": report_dates[index] if index < len(report_dates) else None,
            }
        return mapping

    def _normalize_values(
        self,
        *,
        symbol: str,
        cik: str | None,
        companyfacts_payload: dict[str, Any],
        submissions_payload: dict[str, Any],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        accession_map = self._submission_accession_map(submissions_payload)
        filing_rows: dict[tuple[str | None, int | None, str | None, str | None, str | None], dict[str, Any]] = {}
        value_rows: list[dict[str, Any]] = []
        for metric_name in CANONICAL_FUNDAMENTAL_METRICS:
            if metric_name == "free_cash_flow":
                continue
            for entry in self._iter_metric_entries(companyfacts_payload, metric_name):
                accession_number = str(entry.get("accn") or "").strip() or None
                fiscal_year = entry.get("fy")
                fiscal_period = entry.get("fp")
                period_end_date = entry.get("end")
                accession_metadata = accession_map.get(accession_number or "", {})
                filing_date = accession_metadata.get("filing_date") or entry.get("filed")
                available_date = accession_metadata.get("available_date") or filing_date
                form_type = accession_metadata.get("form_type") or entry.get("form")
                if not any((accession_number, filing_date, period_end_date)):
                    continue
                inferred_period_type = "annual" if str(form_type).upper() == "10-K" or str(fiscal_period).upper() == "FY" else "quarterly"
                key = (accession_number, fiscal_year, fiscal_period, period_end_date, filing_date)
                filing_rows.setdefault(
                    key,
                    {
                        "symbol": symbol.upper(),
                        "cik": _normalize_cik(cik),
                        "fiscal_year": fiscal_year,
                        "fiscal_period": fiscal_period,
                        "period_type": inferred_period_type,
                        "period_end_date": accession_metadata.get("period_end_date") or period_end_date,
                        "filing_date": filing_date,
                        "available_date": available_date,
                        "form_type": form_type,
                        "accession_number": accession_number,
                        "source": self.provider_name,
                    },
                )
                value_rows.append(
                    FundamentalValueRecord(
                        symbol=symbol.upper(),
                        cik=_normalize_cik(cik),
                        fiscal_year=fiscal_year,
                        fiscal_period=fiscal_period,
                        period_type=inferred_period_type,
                        period_end_date=accession_metadata.get("period_end_date") or period_end_date,
                        filing_date=filing_date,
                        available_date=available_date,
                        form_type=form_type,
                        accession_number=accession_number,
                        source=self.provider_name,
                        metric_name=metric_name,
                        metric_value=entry.get("val"),
                    ).to_dict()
                )

        values_df = pd.DataFrame(value_rows)
        if values_df.empty:
            return pd.DataFrame(columns=FUNDAMENTAL_FILING_COLUMNS), _empty_value_frame(self.provider_name)
        filing_df = pd.DataFrame(filing_rows.values(), columns=FUNDAMENTAL_FILING_COLUMNS).drop_duplicates().reset_index(drop=True)
        values_df = values_df.drop_duplicates(
            subset=["symbol", "accession_number", "metric_name", "period_end_date", "fiscal_year", "fiscal_period"],
            keep="last",
        ).reset_index(drop=True)
        return filing_df, values_df

    def fetch(self, *, symbols: list[str]) -> ProviderFetchResult:
        if not self.is_configured():
            return _empty_result(self.provider_name, configured=False, status="not_configured")

        raw_cache_root = self.cache_root or (self.submissions_root.parent if self.submissions_root is not None else None)
        if raw_cache_root is None:
            return _empty_result(self.provider_name, configured=False, status="not_configured")

        client = SECClient(
            raw_cache_root=raw_cache_root,
            user_agent=self.user_agent,
            cache_enabled=self.cache_enabled,
            cache_ttl_days=self.cache_ttl_days,
            force_refresh=self.force_refresh,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            max_requests_per_run=self.max_requests_per_run,
            offline=self.offline,
        )
        requested_symbols = [str(symbol).upper() for symbol in symbols]
        resolved_map_df, mapping_fetched_symbols, unresolved_symbols = self._resolve_symbol_cik_map(
            symbols=requested_symbols,
            client=client,
        )
        resolved_symbols = resolved_map_df["symbol"].tolist() if not resolved_map_df.empty else []
        if self.max_symbols_per_run is not None:
            target_symbols = resolved_symbols[: self.max_symbols_per_run]
            skipped_due_limit = resolved_symbols[self.max_symbols_per_run :]
        else:
            target_symbols = resolved_symbols
            skipped_due_limit = []

        mapping_by_symbol = {
            str(row["symbol"]).upper(): row
            for row in resolved_map_df.to_dict(orient="records")
        }

        company_rows: list[dict[str, Any]] = []
        filing_frames: list[pd.DataFrame] = []
        value_frames: list[pd.DataFrame] = []
        symbols_fetched_from_network: list[str] = []
        symbols_reused_from_cache: list[str] = []
        symbols_failed: list[str] = []
        symbol_errors: list[dict[str, str]] = []

        for symbol in target_symbols:
            mapping_row = mapping_by_symbol.get(symbol, {})
            cik = _normalize_cik(mapping_row.get("cik"))
            if cik is None:
                continue
            requests_before = client.requests_made
            try:
                submissions_path = self._resolve_local_path(self.submissions_root, symbol=symbol, cik=cik)
                companyfacts_path = self._resolve_local_path(self.companyfacts_root, symbol=symbol, cik=cik)
                submissions_payload = _load_json(submissions_path)
                companyfacts_payload = _load_json(companyfacts_path)
                if submissions_payload and submissions_path is not None:
                    client.cache_hits += 1
                else:
                    submissions_payload = client.fetch_submissions(cik)
                if companyfacts_payload and companyfacts_path is not None:
                    client.cache_hits += 1
                else:
                    companyfacts_payload = client.fetch_companyfacts(cik)
            except Exception as exc:  # noqa: BLE001
                symbols_failed.append(symbol)
                symbol_errors.append(
                    {
                        "symbol": symbol,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    }
                )
                continue
            if client.requests_made == requests_before:
                symbols_reused_from_cache.append(symbol)
            else:
                symbols_fetched_from_network.append(symbol)
            if not submissions_payload and not companyfacts_payload:
                symbols_failed.append(symbol)
                continue
            company_rows.append(
                self._normalize_company_master(
                    symbol=symbol,
                    cik=cik,
                    submissions_payload=submissions_payload,
                    mapping_row=mapping_row,
                )
            )
            filing_df, values_df = self._normalize_values(
                symbol=symbol,
                cik=cik,
                companyfacts_payload=companyfacts_payload,
                submissions_payload=submissions_payload,
            )
            if not filing_df.empty:
                filing_frames.append(filing_df)
            if not values_df.empty:
                value_frames.append(values_df)

        company_df = pd.DataFrame(company_rows)
        filing_df = pd.concat(filing_frames, ignore_index=True) if filing_frames else _empty_filing_frame(self.provider_name)
        values_df = pd.concat(value_frames, ignore_index=True) if value_frames else _empty_value_frame(self.provider_name)
        if not values_df.empty:
            values_df = values_df.sort_values(
                ["symbol", "available_date", "period_end_date", "metric_name"],
                na_position="last",
            ).drop_duplicates(
                subset=["symbol", "accession_number", "metric_name", "period_end_date", "fiscal_year", "fiscal_period"],
                keep="last",
            ).reset_index(drop=True)
            filing_df = filing_df.sort_values(
                ["symbol", "available_date", "period_end_date"],
                na_position="last",
            ).drop_duplicates(
                subset=["symbol", "accession_number", "period_end_date", "fiscal_year", "fiscal_period"],
                keep="last",
            ).reset_index(drop=True)

        status = "ok"
        if symbol_errors and values_df.empty:
            status = "error"
        elif symbol_errors or unresolved_symbols:
            status = "partial_success"

        return ProviderFetchResult(
            company_master_df=company_df,
            filing_metadata_df=filing_df,
            fundamental_values_df=values_df,
            diagnostics={
                "provider": self.provider_name,
                "configured": True,
                "status": status,
                "mode": "sec_snapshot",
                "user_agent": self.user_agent,
                "symbols_requested": len(requested_symbols),
                "symbols_resolved_to_cik": resolved_symbols,
                "symbols_unresolved": unresolved_symbols,
                "mapping_fetched_symbols": mapping_fetched_symbols,
                "symbols_fetched": sorted(set(symbols_fetched_from_network)),
                "symbols_skipped_from_cache": sorted(set(symbols_reused_from_cache)),
                "symbols_failed": sorted(set(symbols_failed)),
                "skipped_due_limit": skipped_due_limit,
                "company_count": int(len(company_df)),
                "filing_count": int(len(filing_df)),
                "value_count": int(len(values_df)),
                "submissions_root": str(self.submissions_root) if self.submissions_root is not None else None,
                "companyfacts_root": str(self.companyfacts_root) if self.companyfacts_root is not None else None,
                "symbol_cik_map_path": str(self.symbol_cik_map_path) if self.symbol_cik_map_path is not None else None,
                "cache_enabled": self.cache_enabled,
                "cache_ttl_days": self.cache_ttl_days,
                "force_refresh": self.force_refresh,
                "request_delay_seconds": self.request_delay_seconds,
                "max_retries": self.max_retries,
                "max_symbols_per_run": self.max_symbols_per_run,
                "max_requests_per_run": self.max_requests_per_run,
                "offline": self.offline,
                "symbol_errors": symbol_errors,
                "message": (
                    f"SEC snapshot fetch failed for {len(symbol_errors)} symbol(s) and produced no canonical values."
                    if status == "error"
                    else None
                ),
                **client.diagnostics_summary(),
            },
        )
