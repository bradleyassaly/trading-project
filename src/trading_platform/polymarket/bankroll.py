"""Live Polymarket bankroll source.

Centralizes the one number every risk/sizing module needs: "how much USDC
is currently in the account?". Previously hardcoded as $350 in
`kill_switch.py` and `kelly_sizer.py`. This module fetches live values
where possible and caches them so queries are free.

Fetch priority (each returns USD or None):

  1. **py-clob-client** ``get_balance_allowance(COLLATERAL)`` — returns
     the CLOB's view of USDC in the funding proxy wallet. When the
     proxy is correctly wired this gives a live reading.
  2. **Polymarket Data API** ``/value`` — returns open-position
     notional value. Used as a sanity check + added to cash for
     total portfolio if both are available.
  3. **Environment override** ``POLYMARKET_LIVE_BANKROLL_USD`` —
     operator-supplied hard value. Wins only if (1) and (2) return
     zero or fail (they will while API proxy linkage is unclear).

Cached to ``data/polymarket/bankroll.json`` with a timestamp so we
can show freshness in the GUI and detect stale caches.

Scheduler: ``refresh_bankroll`` task runs every 15 min.
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_CACHE_PATH = _PROJECT_ROOT / "data" / "polymarket" / "bankroll.json"
_STALE_AFTER_SECONDS = 3600  # warn if cache older than 1h

# Fallback when nothing else works — used during phase 1 before the
# CLOB balance lookup is wired correctly. Operator-set via .env.
_FALLBACK_ENV = "POLYMARKET_LIVE_BANKROLL_USD"
_FALLBACK_DEFAULT = 350.0


def get_bankroll() -> float:
    """Return the current known bankroll in USD. Never raises.

    Reads cache first; falls back to the env var; last resort returns
    the conservative default so callers always get a positive number.
    """
    # 1. cache
    try:
        if _CACHE_PATH.exists():
            data = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
            val = float(data.get("bankroll_usd") or 0)
            if val > 0:
                return val
    except Exception as exc:
        logger.debug("bankroll cache read failed: %s", exc)
    # 2. env override
    env_val = os.environ.get(_FALLBACK_ENV, "").strip()
    if env_val:
        try:
            v = float(env_val)
            if v > 0:
                return v
        except ValueError:
            pass
    # 3. hard default
    return _FALLBACK_DEFAULT


def get_bankroll_info() -> dict[str, Any]:
    """Return the full cached snapshot (value + freshness + source)."""
    info = {
        "bankroll_usd": _FALLBACK_DEFAULT,
        "source": "default",
        "refreshed_at": None,
        "age_seconds": None,
        "stale": True,
    }
    try:
        if _CACHE_PATH.exists():
            data = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
            info["bankroll_usd"] = float(data.get("bankroll_usd") or _FALLBACK_DEFAULT)
            info["source"] = data.get("source") or "cache"
            ts = data.get("refreshed_at")
            if ts:
                info["refreshed_at"] = int(ts)
                info["age_seconds"] = int(time.time() - ts)
                info["stale"] = info["age_seconds"] > _STALE_AFTER_SECONDS
            info["cash_usd"] = data.get("cash_usd")
            info["positions_value_usd"] = data.get("positions_value_usd")
            info["wallet"] = data.get("wallet")
        else:
            # No cache — check env override
            env_val = os.environ.get(_FALLBACK_ENV, "").strip()
            if env_val:
                try:
                    info["bankroll_usd"] = float(env_val)
                    info["source"] = "env_override"
                except ValueError:
                    pass
    except Exception as exc:
        logger.debug("bankroll info read failed: %s", exc)
    return info


def refresh_bankroll() -> dict[str, Any]:
    """Fetch fresh values from CLOB + Polymarket APIs, persist to cache.

    Always writes SOME cache (even if APIs return 0) so downstream
    consumers can see the freshness. If APIs fail, falls back to env
    override.
    """
    result: dict[str, Any] = {
        "bankroll_usd": None,
        "cash_usd": None,
        "positions_value_usd": None,
        "wallet": os.environ.get("POLYMARKET_WALLET_ADDRESS", "").lower(),
        "source": "unknown",
        "refreshed_at": int(time.time()),
    }

    cash_usd = _fetch_clob_cash()
    pos_value = _fetch_positions_notional(result["wallet"])

    if cash_usd is not None:
        result["cash_usd"] = cash_usd
    if pos_value is not None:
        result["positions_value_usd"] = pos_value

    if cash_usd is not None and cash_usd > 0:
        total = cash_usd + (pos_value or 0)
        result["bankroll_usd"] = total
        result["source"] = "clob_balance+positions"
    else:
        # APIs returned 0 or failed — use env override
        env_val = os.environ.get(_FALLBACK_ENV, "").strip()
        if env_val:
            try:
                v = float(env_val)
                if v > 0:
                    result["bankroll_usd"] = v + (pos_value or 0)
                    result["source"] = "env_override+positions"
            except ValueError:
                pass

    if result["bankroll_usd"] is None or result["bankroll_usd"] <= 0:
        result["bankroll_usd"] = _FALLBACK_DEFAULT
        result["source"] = "default"

    # Persist
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("bankroll cache write failed: %s", exc)

    return result


def _fetch_clob_cash() -> float | None:
    """USDC balance from CLOB's funding proxy. Returns USD float or None."""
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.constants import POLYGON
        from py_clob_client.clob_types import (
            ApiCreds, BalanceAllowanceParams, AssetType,
        )
    except ImportError:
        return None
    api_key = os.environ.get("POLYMARKET_API_KEY", "").strip()
    api_secret = os.environ.get("POLYMARKET_API_SECRET", "").strip()
    api_pass = (
        os.environ.get("POLYMARKET_API_PASSPHRASE")
        or os.environ.get("POLYMARKET_PASSPHRASE") or ""
    ).strip()
    pk = os.environ.get("POLYMARKET_PRIVATE_KEY", "").strip()
    if not (api_key and api_secret and api_pass and pk):
        return None
    try:
        client = ClobClient(
            host="https://clob.polymarket.com", chain_id=POLYGON,
            key=pk,
            creds=ApiCreds(
                api_key=api_key, api_secret=api_secret, api_passphrase=api_pass,
            ),
        )
        b = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        raw = b.get("balance") if isinstance(b, dict) else None
        if raw is None:
            return None
        # USDC has 6 decimals on Polygon; convert raw → USD
        return float(raw) / 1_000_000.0
    except Exception as exc:
        logger.debug("CLOB balance fetch failed: %s", exc)
        return None


def _fetch_positions_notional(wallet: str) -> float | None:
    """Open-position notional from Polymarket Data API. Returns USD or None."""
    if not wallet:
        return None
    try:
        import requests
        r = requests.get(
            "https://data-api.polymarket.com/value",
            params={"user": wallet}, timeout=8,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        if isinstance(data, list) and data:
            val = data[0].get("value")
            if val is not None:
                return float(val)
    except Exception as exc:
        logger.debug("positions-value fetch failed: %s", exc)
    return None


def main() -> int:
    from dotenv import load_dotenv
    load_dotenv(str(_PROJECT_ROOT / ".env"))
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    result = refresh_bankroll()
    print(f"bankroll refresh: {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
