"""
Resolve Polymarket condition_id → CLOB token_id.

The CLOB API requires token_id (the specific YES/NO token) to place
orders. Markets have two tokens; this module maps condition_id + side
to the correct token_id via the Gamma API, with caching.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

# In-memory cache: condition_id → {YES: token_id, NO: token_id, expires: ts}
_CACHE: dict[str, dict[str, Any]] = {}
_CACHE_TTL = 86400  # 24 hours


def get_token_id(condition_id: str, side: str) -> str | None:
    """Get the CLOB token_id for a condition_id + side (YES/NO)."""
    if not condition_id:
        return None

    side_key = side.upper()
    if side_key in ("BUY", "YES"):
        side_key = "YES"
    elif side_key in ("SELL", "NO"):
        side_key = "NO"

    # Check cache
    cached = _CACHE.get(condition_id)
    if cached and cached.get("expires", 0) > time.time():
        return cached.get(side_key)

    # Fetch from Gamma API
    try:
        import requests
        r = requests.get(
            "https://gamma-api.polymarket.com/markets",
            params={"condition_ids": condition_id},
            timeout=10,
        )
        if not r.ok:
            return None

        markets = r.json()
        if not markets or not isinstance(markets, list):
            return None

        m = markets[0]
        tokens = m.get("tokens", [])
        result = {"expires": time.time() + _CACHE_TTL}

        # Parse tokens array
        if tokens and isinstance(tokens, list):
            for tok in tokens:
                outcome = (tok.get("outcome") or "").upper()
                tid = tok.get("token_id")
                if outcome and tid:
                    result[outcome] = tid

        # Fallback: clobTokenIds (JSON string or list)
        if "YES" not in result:
            clob_ids = m.get("clobTokenIds")
            if clob_ids:
                if isinstance(clob_ids, str):
                    try:
                        clob_ids = json.loads(clob_ids)
                    except Exception:
                        clob_ids = []
                if isinstance(clob_ids, list) and len(clob_ids) >= 2:
                    result["YES"] = clob_ids[0]
                    result["NO"] = clob_ids[1]

        _CACHE[condition_id] = result
        return result.get(side_key)

    except Exception as exc:
        logger.debug("Token resolution failed for %s: %s", condition_id[:16], exc)
        return None
