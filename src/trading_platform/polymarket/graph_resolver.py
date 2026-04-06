"""
Polymarket Graph resolution fetcher.

Queries the Goldsky-hosted positions subgraph to determine market
resolution outcomes (YES/NO) from on-chain payout data.

Usage::

    from trading_platform.polymarket.graph_resolver import GraphResolver
    resolver = GraphResolver()
    resolutions = resolver.get_resolutions(["0xabc...", "0xdef..."])
    # {"0xabc...": True, "0xdef...": False}  (True=YES won)
"""
from __future__ import annotations

import logging
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

_GOLDSKY_URL = (
    "https://api.goldsky.com/api/public/"
    "project_cl6mb8i9h0003e201j6li0diw/subgraphs/"
    "positions-subgraph/0.0.7/gn"
)


class GraphResolver:
    """Fetch market resolutions from The Graph positions subgraph."""

    def __init__(self, *, endpoint_url: str = _GOLDSKY_URL, sleep_sec: float = 0.2) -> None:
        self._url = endpoint_url
        self._sleep = sleep_sec
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    def get_resolutions(self, condition_ids: list[str]) -> dict[str, bool]:
        """Fetch resolution outcomes for condition IDs.

        Returns dict of ``{condition_id: resolves_yes}`` for resolved markets only.
        Unresolved markets (null/empty payouts) are omitted.
        """
        if not condition_ids:
            return {}

        results: dict[str, bool] = {}
        # Batch in groups of 100
        for i in range(0, len(condition_ids), 100):
            batch = condition_ids[i:i + 100]
            batch_results = self._query_conditions(batch)
            results.update(batch_results)
            if i + 100 < len(condition_ids):
                time.sleep(self._sleep)

        logger.info("Resolved %d of %d condition_ids via Graph", len(results), len(condition_ids))
        return results

    def get_token_conditions(self, token_ids: list[str]) -> dict[str, str]:
        """Map token_id → condition_id via TokenIdCondition entities.

        Returns dict of ``{token_id: condition_id}``.
        """
        if not token_ids:
            return {}

        results: dict[str, str] = {}
        for i in range(0, len(token_ids), 100):
            batch = token_ids[i:i + 100]
            batch_results = self._query_token_conditions(batch)
            results.update(batch_results)
            if i + 100 < len(token_ids):
                time.sleep(self._sleep)

        return results

    def _query_conditions(self, condition_ids: list[str]) -> dict[str, bool]:
        ids_str = ", ".join(f'"{cid}"' for cid in condition_ids)
        query = f"""
        {{
          conditions(where: {{id_in: [{ids_str}]}}, first: 100) {{
            id
            payouts
          }}
        }}
        """
        try:
            resp = self._session.post(self._url, json={"query": query}, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Graph conditions query failed: %s", exc)
            return {}

        conditions = data.get("data", {}).get("conditions", [])
        results: dict[str, bool] = {}
        for cond in conditions:
            cid = cond.get("id", "")
            payouts = cond.get("payouts")
            if not payouts or not isinstance(payouts, list) or len(payouts) < 2:
                continue
            try:
                yes_payout = float(payouts[0])
                no_payout = float(payouts[1])
            except (TypeError, ValueError, IndexError):
                continue
            if yes_payout > no_payout:
                results[cid] = True  # YES won
            elif no_payout > yes_payout:
                results[cid] = False  # NO won
            # Equal payouts = ambiguous, skip
        return results

    def _query_token_conditions(self, token_ids: list[str]) -> dict[str, str]:
        ids_str = ", ".join(f'"{tid}"' for tid in token_ids)
        query = f"""
        {{
          tokenIdConditions(where: {{id_in: [{ids_str}]}}, first: 100) {{
            id
            condition {{ id }}
          }}
        }}
        """
        try:
            resp = self._session.post(self._url, json={"query": query}, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Graph tokenIdConditions query failed: %s", exc)
            return {}

        items = data.get("data", {}).get("tokenIdConditions", [])
        results: dict[str, str] = {}
        for item in items:
            tid = item.get("id", "")
            cond = item.get("condition", {})
            cid = cond.get("id", "") if isinstance(cond, dict) else ""
            if tid and cid:
                results[tid] = cid
        return results
