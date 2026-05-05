"""
Polymarket CLOB API client.

Thin REST client for the Polymarket CLOB. Public read endpoints
(``/book``, ``/midpoint``, ``/last-trade-price``) work without
credentials. Order placement requires API credentials and prefers
``py-clob-client`` if installed.

This module is a CLIENT — it does not decide what to trade. The live
executor (:mod:`polymarket_live_executor`) is the only caller that
should ever invoke ``place_market_order``, and only after the
:class:`KillSwitch` has cleared the trade.

Setup
-----
1. Get API credentials from Polymarket (Profile → API → Derive Key).
2. Add to ``.env``::

       POLYMARKET_API_KEY=...
       POLYMARKET_API_SECRET=...
       POLYMARKET_API_PASSPHRASE=...
       POLYMARKET_PRIVATE_KEY=0x...
       POLYMARKET_WALLET_ADDRESS=0x...

3. Optionally install py-clob-client-v2::

       pip install py-clob-client-v2
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any

import requests

logger = logging.getLogger(__name__)

CLOB_BASE = "https://clob.polymarket.com"


@dataclass
class OrderResult:
    success: bool
    order_id: str | None
    status: str
    filled_price: float | None
    filled_size: float | None
    error_msg: str | None
    raw: dict


class ClobClient:
    """Thin Polymarket CLOB REST client."""

    def __init__(self) -> None:
        self._api_key = os.getenv("POLYMARKET_API_KEY", "").strip()
        self._api_secret = os.getenv("POLYMARKET_API_SECRET", "").strip()
        self._passphrase = (os.getenv("POLYMARKET_API_PASSPHRASE") or os.getenv("POLYMARKET_PASSPHRASE") or "").strip()
        self._private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
        self._wallet = os.getenv("POLYMARKET_WALLET_ADDRESS", "").strip()
        self._configured = bool(
            self._api_key and self._api_secret and self._passphrase and self._private_key
        )
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    @property
    def is_configured(self) -> bool:
        return self._configured

    # ── Public read endpoints (no auth) ────────────────────────────────────

    def get_order_book(self, token_id: str) -> dict[str, Any]:
        """Fetch the current CLOB order book for a token."""
        try:
            r = self._session.get(
                f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=5,
            )
            if r.status_code != 200:
                return {"error": f"HTTP {r.status_code}", "bids": [], "asks": []}
            return r.json()
        except Exception as exc:
            return {"error": str(exc), "bids": [], "asks": []}

    def get_mid_price(self, token_id: str) -> float | None:
        """Best bid/ask mid price (probability 0..1)."""
        try:
            r = self._session.get(
                f"{CLOB_BASE}/midpoint", params={"token_id": token_id}, timeout=5,
            )
            if r.status_code == 200:
                mid = r.json().get("mid")
                if mid is not None:
                    return float(mid)
        except Exception:
            pass
        # Fallback: compute from book
        book = self.get_order_book(token_id)
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        if bids and asks:
            try:
                return (float(bids[0]["price"]) + float(asks[0]["price"])) / 2
            except (TypeError, ValueError, KeyError):
                return None
        return None

    def get_last_price(self, token_id: str) -> float | None:
        """Last traded price (probability 0..1)."""
        try:
            r = self._session.get(
                f"{CLOB_BASE}/last-trade-price", params={"token_id": token_id}, timeout=5,
            )
            if r.status_code == 200:
                price = r.json().get("price")
                if price is not None:
                    return float(price)
        except Exception:
            return None
        return None

    # ── Order placement (requires credentials) ─────────────────────────────

    def place_market_order(
        self,
        token_id: str,
        side: str,
        size_usdc: float,
        max_slippage: float = 0.02,
        exact_shares: int | None = None,
    ) -> OrderResult:
        """Submit a market order to the CLOB. Requires py-clob-client."""
        if not self._configured:
            return OrderResult(
                success=False, order_id=None, status="error",
                filled_price=None, filled_size=None,
                error_msg="CLOB not configured — set POLYMARKET_API_KEY etc. in .env",
                raw={},
            )

        try:
            from py_clob_client_v2 import ClobClient as PyClobClient
            from py_clob_client_v2 import ApiCreds, OrderArgs, OrderType, PartialCreateOrderOptions
            from py_clob_client_v2.constants import POLYGON
        except ImportError:
            return OrderResult(
                success=False, order_id=None, status="error",
                filled_price=None, filled_size=None,
                error_msg=(
                    "py-clob-client-v2 not installed. "
                    "Run: pip install py-clob-client-v2"
                ),
                raw={},
            )

        try:
            # Polymarket uses a proxy-wallet custody model: users deposit
            # into a shared CTF-compatible proxy, and Polymarket tracks
            # per-user balances internally. For orders placed from a
            # proxy-backed account we MUST set signature_type=2 and pass
            # the funder (the proxy address) — otherwise py_clob_client
            # signs as an EOA and the CLOB reports "balance: 0" because
            # it's checking EOA-side USDC, not proxy-side.
            import os as _os
            funder = _os.environ.get("POLYMARKET_FUNDER_ADDRESS") or ""
            client_kwargs = dict(
                host=CLOB_BASE,
                chain_id=POLYGON,
                key=self._private_key,
                creds=ApiCreds(
                    api_key=self._api_key,
                    api_secret=self._api_secret,
                    api_passphrase=self._passphrase,
                ),
            )
            if funder:
                # signature_type 1 = Polymarket proxy wallet
                # signature_type 2 = Polymarket Gnosis Safe
                # Verified: our test account is type 1 (balance=$345 at proxy).
                client_kwargs["signature_type"] = int(
                    _os.environ.get("POLYMARKET_SIGNATURE_TYPE", "1")
                )
                client_kwargs["funder"] = funder
                logger.info(
                    "[clob] proxy-wallet mode: sig_type=%d funder=%s",
                    client_kwargs["signature_type"], funder[:12] + "...",
                )
            client = PyClobClient(**client_kwargs)

            current_price = self.get_mid_price(token_id) or 0.5

            tick = 0.01
            book = self.get_order_book(token_id)
            if side.upper() == "BUY":
                asks = book.get("asks") or []
                # asks are sorted ascending (lowest = best for buyer)
                best_ask = float(asks[0]["price"]) if asks else current_price
                target = round(best_ask + tick, 2)
            else:
                bids = book.get("bids") or []
                # bids are sorted ascending; take the highest (best for seller)
                best_bid = max((float(b["price"]) for b in bids), default=current_price)
                target = round(best_bid - tick, 2)
            target = max(0.01, min(0.99, target))
            if exact_shares is not None:
                # Caller-supplied exact count (exit sells use this to avoid
                # exceeding the token balance we actually hold).
                shares_int = max(1, int(exact_shares))
                actual_usdc = round(shares_int * target, 2)
            elif side.upper() == "BUY":
                _CLOB_MIN_SHARES = 5
                shares_int = max(_CLOB_MIN_SHARES, int(float(size_usdc) / target))
                actual_usdc = round(shares_int * target, 2)
                if actual_usdc > float(size_usdc) * 3.0:
                    return OrderResult(
                        success=False, order_id=None, status="error",
                        filled_price=None, filled_size=None,
                        error_msg=(
                            f"stake ${size_usdc:.2f} too small: CLOB min {_CLOB_MIN_SHARES} shares "
                            f"@ {target:.3f} = ${actual_usdc:.2f} ({actual_usdc / size_usdc:.1f}x)"
                        ),
                        raw={},
                    )
            else:
                shares_int = max(1, int(float(size_usdc) / target))
                actual_usdc = round(shares_int * target, 2)
            order_args = OrderArgs(
                token_id=token_id,
                price=target,
                size=float(shares_int),
                side=side.upper(),
            )
            neg_risk_flag = False
            try:
                neg_risk_flag = bool(client.get_neg_risk(token_id))
                logger.info("[clob] neg_risk=%s for %s...", neg_risk_flag, token_id[:12])
            except Exception as _nr_exc:
                logger.warning("[clob] get_neg_risk lookup failed (%s), defaulting to False", _nr_exc)
            opts = PartialCreateOrderOptions(tick_size="0.01", neg_risk=neg_risk_flag)
            resp = client.create_and_post_order(order_args, options=opts, order_type=OrderType.FOK)
            logger.info(
                "[clob] market-as-FOK %s %d shares @ %.2f = $%.2f neg_risk=%s",
                side.upper(), shares_int, target, actual_usdc, neg_risk_flag,
            )

            order_id = resp.get("orderID") or resp.get("id")
            status = resp.get("status", "unknown")
            filled_raw = resp.get("avgFilledPrice")
            filled_price = float(filled_raw) if filled_raw else current_price

            return OrderResult(
                success=status in ("matched", "live", "delayed"),
                order_id=order_id,
                status=status,
                filled_price=filled_price,
                filled_size=float(actual_usdc),
                error_msg=None if status != "error" else str(resp),
                raw=resp,
            )
        except Exception as exc:
            logger.warning("place_market_order failed: %s", exc)
            return OrderResult(
                success=False, order_id=None, status="error",
                filled_price=None, filled_size=None,
                error_msg=str(exc), raw={},
            )

    def place_limit_order(
        self,
        token_id: str,
        side: str,
        size_usdc: float,
        timeout_sec: float = 30.0,
        aggression: str = "passive",
    ) -> OrderResult:
        """Place a limit order with fill monitoring.

        Aggression levels:
        - "passive": at the bid (maker) — earns spread, may not fill
        - "mid": at midpoint — balanced fill speed vs. price improvement
        - "aggressive": at ask+tick — equivalent to market, guaranteed fill

        Posts as GTC (good-till-cancel), polls for fill up to timeout_sec,
        cancels if unfilled.
        """
        if not self._configured:
            return OrderResult(
                success=False, order_id=None, status="error",
                filled_price=None, filled_size=None,
                error_msg="CLOB not configured", raw={},
            )

        try:
            from py_clob_client_v2 import ClobClient as PyClobClient
            from py_clob_client_v2 import ApiCreds, OrderArgs, OrderType, PartialCreateOrderOptions
            from py_clob_client_v2.constants import POLYGON
            import os as _os, time as _time

            funder = _os.environ.get("POLYMARKET_FUNDER_ADDRESS") or ""
            client_kwargs = dict(
                host=CLOB_BASE, chain_id=POLYGON, key=self._private_key,
                creds=ApiCreds(
                    api_key=self._api_key, api_secret=self._api_secret,
                    api_passphrase=self._passphrase,
                ),
            )
            if funder:
                client_kwargs["signature_type"] = int(
                    _os.environ.get("POLYMARKET_SIGNATURE_TYPE", "1")
                )
                client_kwargs["funder"] = funder
            client = PyClobClient(**client_kwargs)

            book = self.get_order_book(token_id)
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            best_bid = float(bids[0]["price"]) if bids else 0.50
            best_ask = float(asks[0]["price"]) if asks else 0.50
            mid = (best_bid + best_ask) / 2 if best_bid and best_ask else 0.50
            tick = 0.01

            if side.upper() == "BUY":
                if aggression == "passive":
                    price = round(best_bid, 2)
                elif aggression == "mid":
                    price = round(mid, 2)
                else:
                    price = round(best_ask + tick, 2)
            else:
                if aggression == "passive":
                    price = round(best_ask, 2)
                elif aggression == "mid":
                    price = round(mid, 2)
                else:
                    price = round(best_bid - tick, 2)
            price = max(0.01, min(0.99, price))

            _CLOB_MIN_SHARES = 5
            shares_int = max(_CLOB_MIN_SHARES, int(float(size_usdc) / price))
            _limit_usdc = round(shares_int * price, 2)
            if _limit_usdc > float(size_usdc) * 3.0:
                return OrderResult(
                    success=False, order_id=None, status="error",
                    filled_price=None, filled_size=None,
                    error_msg=(
                        f"stake ${size_usdc:.2f} too small: CLOB min {_CLOB_MIN_SHARES} shares "
                        f"@ {price:.3f} = ${_limit_usdc:.2f} ({_limit_usdc / size_usdc:.1f}x)"
                    ),
                    raw={},
                )
            order_args = OrderArgs(
                token_id=token_id, price=price,
                size=float(shares_int), side=side.upper(),
            )
            neg_risk_flag = False
            try:
                neg_risk_flag = bool(client.get_neg_risk(token_id))
                logger.info("[clob] neg_risk=%s for %s...", neg_risk_flag, token_id[:12])
            except Exception as _nr_exc:
                logger.warning("[clob] get_neg_risk lookup failed (%s), defaulting to False", _nr_exc)
            _opts = PartialCreateOrderOptions(tick_size="0.01", neg_risk=neg_risk_flag)

            order_type = OrderType.FOK if aggression == "aggressive" else OrderType.GTC
            resp = client.create_and_post_order(order_args, options=_opts, order_type=order_type)
            order_id = resp.get("orderID") or resp.get("id")
            status = resp.get("status", "unknown")

            if order_type == OrderType.FOK or status == "matched":
                filled = resp.get("avgFilledPrice")
                return OrderResult(
                    success=status in ("matched", "live", "delayed"),
                    order_id=order_id, status=status,
                    filled_price=float(filled) if filled else price,
                    filled_size=float(size_usdc), error_msg=None, raw=resp,
                )

            # GTC: poll for fill
            deadline = _time.time() + timeout_sec
            while _time.time() < deadline:
                _time.sleep(2.0)
                try:
                    check = client.get_order(order_id)
                    st = check.get("status", "")
                    if st == "matched":
                        fp = check.get("avgFilledPrice") or check.get("associate_trades", [{}])[0].get("price")
                        return OrderResult(
                            success=True, order_id=order_id, status="matched",
                            filled_price=float(fp) if fp else price,
                            filled_size=float(size_usdc), error_msg=None,
                            raw=check,
                        )
                    if st in ("cancelled", "expired"):
                        break
                except Exception:
                    pass

            # Unfilled — cancel and fall back to aggressive
            try:
                client.cancel_order(order_id)
            except Exception:
                pass

            if aggression == "passive":
                logger.info("[clob] passive order unfilled after %.0fs, escalating to mid", timeout_sec)
                return self.place_limit_order(
                    token_id, side, size_usdc,
                    timeout_sec=timeout_sec, aggression="mid",
                )
            elif aggression == "mid":
                logger.info("[clob] mid order unfilled, escalating to aggressive (FOK)")
                return self.place_limit_order(
                    token_id, side, size_usdc,
                    timeout_sec=0, aggression="aggressive",
                )

            return OrderResult(
                success=False, order_id=order_id, status="unfilled",
                filled_price=None, filled_size=None,
                error_msg="order timed out", raw={},
            )

        except Exception as exc:
            logger.warning("place_limit_order failed: %s", exc)
            return OrderResult(
                success=False, order_id=None, status="error",
                filled_price=None, filled_size=None,
                error_msg=str(exc), raw={},
            )

    # ── Diagnostics ────────────────────────────────────────────────────────

    def test_connection(self) -> dict[str, Any]:
        """Smoke-test the public endpoint and report credential state."""
        results: dict[str, Any] = {}
        try:
            r = self._session.get(f"{CLOB_BASE}/markets", timeout=5)
            results["public_endpoint"] = r.status_code in (200, 404)
        except Exception:
            results["public_endpoint"] = False

        results["credentials_configured"] = self._configured
        results["wallet_set"] = bool(self._wallet)
        results["private_key_set"] = bool(self._private_key)
        try:
            import py_clob_client_v2  # noqa: F401
            results["py_clob_client"] = True
        except ImportError:
            results["py_clob_client"] = False
        return results
