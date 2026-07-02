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
    fill_time_ms: int | None = None


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
        price_hint: float | None = None,
        max_price: float | None = None,
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
            from py_clob_client_v2 import ApiCreds, OrderType, PartialCreateOrderOptions
            from py_clob_client_v2.clob_types import OrderArgsV2 as OrderArgs
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

        _submit_ts = time.time()
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

            # price_hint is the caller's direction-corrected token price.
            # get_mid_price always returns the YES-space price regardless of
            # token queried, so for NO token exits the fallback must come from
            # the caller (1 - yes_mid), not a fresh fetch.
            current_price = price_hint if price_hint is not None else (self.get_mid_price(token_id) or 0.5)

            tick = 0.01
            book = self.get_order_book(token_id)
            if side.upper() == "BUY":
                asks = book.get("asks") or []
                # asks are sorted ascending (lowest = best for buyer)
                best_ask = float(asks[0]["price"]) if asks else current_price
                # 2026-07-02: hard price ceiling. Entries used to post at
                # best_ask+tick with NO bound relative to the signal's price
                # — on thin books that produced 0.815→0.99 chases (22 cents
                # of pure spread payment, larger than any whale edge).
                # If the book is beyond the ceiling, REFUSE rather than rest
                # a stale order that could fill days later.
                if max_price is not None and best_ask > max_price:
                    return OrderResult(
                        success=False, order_id=None, status="blocked_chase",
                        filled_price=None, filled_size=None,
                        error_msg=(f"best ask {best_ask:.3f} > max_price "
                                   f"{max_price:.3f} — refusing to chase"),
                        raw={"best_ask": best_ask, "max_price": max_price},
                    )
                target = round(best_ask + tick, 2)
                if max_price is not None:
                    target = min(target, round(max_price, 2))
            else:
                bids = book.get("bids") or []
                # bids are sorted ascending; take the highest (best for seller)
                best_bid = max((float(b["price"]) for b in bids), default=current_price)
                target = round(best_bid - tick, 2)
            target = max(0.01, min(0.99, target))
            # Guard: Polymarket's order book API returns YES-space prices for
            # ALL token queries, including NO tokens. For a NO token at 0.01,
            # the book's asks come back at ~0.99 (YES space), making target=0.99
            # instead of 0.02. Detected when target deviates >0.30 from the
            # caller's direction-corrected price_hint; fall back to hint±tick.
            if current_price != 0.5 and abs(target - current_price) > 0.30:
                target = round(current_price + tick, 2) if side.upper() == "BUY" \
                    else round(current_price - tick, 2)
                target = max(0.01, min(0.99, target))
            if exact_shares is not None:
                # Caller-supplied exact count (exit sells use this to avoid
                # exceeding the token balance we actually hold).
                shares_int = max(1, int(exact_shares))
                actual_usdc = round(shares_int * target, 2)
            elif side.upper() == "BUY":
                if float(size_usdc) < 1.0:
                    return OrderResult(
                        success=False, order_id=None, status="error",
                        filled_price=None, filled_size=None,
                        error_msg=f"CLOB minimum BUY is $1.00; requested ${size_usdc:.2f}",
                        raw={},
                    )
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
            _order = client.create_order(order_args, opts)
            # Polymarket treats marketable GTC orders as FOK server-side and
            # rejects when book depth at our target < order size. Retry as FAK
            # (Fill And Kill) so we accept whatever the book can fill instead
            # of losing the entry entirely. Was costing ~14 SELL entries/week.
            #
            # 2026-06-03: Take-profit exits were 100% blocked for weeks because
            # a prior status='live' SELL on the same token locks the entire
            # CONDITIONAL balance ("sum of active orders == balance"). Detect
            # that error, cancel stale orders on this asset, retry once.
            def _post():
                try:
                    return client.post_order(_order, order_type=OrderType.GTC)
                except Exception as _gtc_exc:
                    if "FOK" in str(_gtc_exc) or "couldn't be fully filled" in str(_gtc_exc):
                        logger.info(
                            "[clob] GTC rejected as FOK (thin book) — retrying as FAK for partial fill"
                        )
                        return client.post_order(_order, order_type=OrderType.FAK)
                    raise
            try:
                resp = _post()
            except Exception as _post_exc:
                err = str(_post_exc).lower()
                if (
                    side.upper() == "SELL"
                    and "not enough balance" in err
                    and "active orders" in err
                ):
                    try:
                        from py_clob_client_v2.clob_types import OrderMarketCancelParams
                        cancel_resp = client.cancel_market_orders(
                            OrderMarketCancelParams(asset_id=token_id)
                        )
                        n_cancelled = len(cancel_resp.get("canceled", [])) if isinstance(cancel_resp, dict) else 0
                        logger.warning(
                            "[clob] balance locked by stale orders on %s... — cancelled %d, retrying SELL",
                            token_id[:12], n_cancelled,
                        )
                    except Exception as _cancel_exc:
                        logger.error("[clob] cancel_market_orders failed: %s", _cancel_exc)
                        raise _post_exc
                    # Brief settle pause so the cancel propagates server-side
                    time.sleep(0.5)
                    # The CLOB rejects identical retries as "Duplicated"
                    # because order hashes are deterministic from
                    # (token_id, price, size, side, expiration). Perturb
                    # price by 1 tick to change the hash. For SELL this
                    # is slightly more aggressive (drop bid); for BUY
                    # slightly worse (raise ask) — accepted tradeoff vs
                    # the position staying stuck forever.
                    _retry_price = max(0.01, min(0.99,
                        target - 0.01 if side.upper() == "SELL" else target + 0.01
                    ))
                    _order = client.create_order(
                        OrderArgs(
                            token_id=token_id, price=_retry_price,
                            size=float(shares_int), side=side.upper(),
                        ),
                        opts,
                    )
                    logger.info(
                        "[clob] retry order rebuilt at %.2f (was %.2f) to dodge hash collision",
                        _retry_price, target,
                    )
                    resp = _post()
                else:
                    raise
            logger.info(
                "[clob] market-as-GTC %s %d shares @ %.2f = $%.2f neg_risk=%s",
                side.upper(), shares_int, target, actual_usdc, neg_risk_flag,
            )

            order_id = resp.get("orderID") or resp.get("id")
            status = resp.get("status", "unknown")
            filled_raw = resp.get("avgFilledPrice")
            # CLOB returns avgFilledPrice=0/"0" synchronously even on matched
            # orders (the trade-detail enrichment lands later). Distinguish:
            #   - matched: terminally filled — fall back to `target` (limit
            #     price). The order matched at <= target by construction, so
            #     this is a conservative cost-basis estimate. Same pattern
            #     as place_limit_order's `filled_price=float(fp) if fp else price`.
            #   - live: still resting in book — leave NULL so the position
            #     monitor backfills via balance check.
            if filled_raw and float(filled_raw) > 0:
                filled_price = float(filled_raw)
            elif status == "matched":
                filled_price = float(target)
            else:
                filled_price = None
            success = status in ("matched", "live", "delayed")

            # 2026-06-03: regression guard. The YES/NO token-space confusion
            # that drained ~$40 of BUY signals through May 7 (commit 9d8a019
            # fixed the underlying order-format issue) leaves a forensic
            # signature: filled_price diverges from price_hint by >0.30 in
            # the wrong direction. If price_hint was supplied and the fill
            # came back in a clearly different space, that's the bug coming
            # back — surface as ERROR so it can't slip in silently again.
            if (
                filled_price is not None
                and price_hint is not None
                and abs(filled_price - price_hint) > 0.30
            ):
                logger.error(
                    "[clob] YES/NO REGRESSION GUARD tripped: filled_price=%.3f "
                    "diverged >0.30 from price_hint=%.3f on side=%s token=%s... "
                    "target=%.3f. Investigate token_id derivation.",
                    filled_price, price_hint, side.upper(), token_id[:12], target,
                )
            # Capture non-success CLOB status so callers can log what happened.
            if not success:
                err_str = f"CLOB status={status}" if status != "error" else str(resp)
            else:
                err_str = None
            return OrderResult(
                success=success,
                order_id=order_id,
                status=status,
                filled_price=filled_price,
                filled_size=float(actual_usdc),
                error_msg=err_str,
                raw=resp,
                fill_time_ms=int((time.time() - _submit_ts) * 1000),
            )
        except Exception as exc:
            logger.warning("place_market_order failed: %s", exc)
            return OrderResult(
                success=False, order_id=None, status="error",
                filled_price=None, filled_size=None,
                error_msg=str(exc), raw={},
                fill_time_ms=int((time.time() - _submit_ts) * 1000),
            )

    def place_limit_order(
        self,
        token_id: str,
        side: str,
        size_usdc: float,
        timeout_sec: float = 30.0,
        aggression: str = "passive",
        price_hint: float | None = None,
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
            from py_clob_client_v2 import ApiCreds, OrderType, PartialCreateOrderOptions
            from py_clob_client_v2.clob_types import OrderArgsV2 as OrderArgs, OrderPayload
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

            # price_hint is the caller's already-fetched, direction-corrected
            # token price. Prefer it over re-fetching mid (which always returns
            # the YES price even for NO token queries).
            mid_fallback = price_hint if price_hint is not None else (self.get_mid_price(token_id) or 0.5)
            book = self.get_order_book(token_id)
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            best_bid = float(bids[0]["price"]) if bids else mid_fallback
            best_ask = float(asks[0]["price"]) if asks else mid_fallback
            mid = (best_bid + best_ask) / 2
            tick = 0.01
            _order_submit_ts = _time.time()

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
            # Guard: order book returns YES-space prices for NO token queries;
            # fall back to price_hint ± tick when computed price is far off.
            if mid_fallback != 0.5 and abs(price - mid_fallback) > 0.30:
                price = round(mid_fallback + tick, 2) if side.upper() == "BUY" \
                    else round(mid_fallback - tick, 2)
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
            _order = client.create_order(order_args, _opts)
            # Same FOK-retry-as-FAK fallback as place_market_order — server
            # rejects marketable GTC when book depth at target < order size.
            try:
                resp = client.post_order(_order, order_type=OrderType.GTC)
            except Exception as _gtc_exc:
                if "FOK" in str(_gtc_exc) or "couldn't be fully filled" in str(_gtc_exc):
                    logger.info(
                        "[clob] GTC rejected as FOK (thin book) — retrying as FAK for partial fill"
                    )
                    resp = client.post_order(_order, order_type=OrderType.FAK)
                else:
                    raise
            order_id = resp.get("orderID") or resp.get("id")
            status = resp.get("status", "unknown")

            if status == "matched":
                filled = resp.get("avgFilledPrice")
                return OrderResult(
                    success=True,
                    order_id=order_id, status=status,
                    filled_price=float(filled) if filled else price,
                    filled_size=float(size_usdc), error_msg=None, raw=resp,
                    fill_time_ms=int((_time.time() - _order_submit_ts) * 1000),
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
                            fill_time_ms=int((_time.time() - _order_submit_ts) * 1000),
                        )
                    if st in ("cancelled", "expired"):
                        break
                except Exception:
                    pass

            # Unfilled — cancel and fall back to next aggression level
            try:
                client.cancel_order(OrderPayload(orderID=order_id))
            except Exception:
                pass

            if aggression == "passive":
                logger.info("[clob] passive order unfilled after %.0fs, escalating to mid", timeout_sec)
                return self.place_limit_order(
                    token_id, side, size_usdc,
                    timeout_sec=timeout_sec, aggression="mid",
                    price_hint=price_hint,
                )
            elif aggression == "mid":
                logger.info("[clob] mid order unfilled, escalating to aggressive (GTC, 15s)")
                return self.place_limit_order(
                    token_id, side, size_usdc,
                    timeout_sec=15.0, aggression="aggressive",
                    price_hint=price_hint,
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

    # ── Balance queries ────────────────────────────────────────────────────

    def get_conditional_balance(self, token_id: str) -> float | None:
        """Return actual CONDITIONAL token balance in full tokens (1 = 1_000_000 units).

        GTC partial fills on thin near-resolution books result in only 1–N tokens
        being filled even when the intended order size implied hundreds. Using
        size_usd / no_price for the sell quantity then hits "not enough balance".
        This method returns the ground-truth share count to use for exit orders.
        """
        if not self._configured or not token_id:
            return None
        try:
            from py_clob_client_v2 import ClobClient as PyClobClient, ApiCreds
            from py_clob_client_v2.constants import POLYGON
            from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
            import os as _os
            funder = _os.environ.get("POLYMARKET_FUNDER_ADDRESS") or ""
            kwargs: dict = dict(
                host=CLOB_BASE, chain_id=POLYGON, key=self._private_key,
                creds=ApiCreds(
                    api_key=self._api_key,
                    api_secret=self._api_secret,
                    api_passphrase=self._passphrase,
                ),
            )
            if funder:
                kwargs["signature_type"] = int(_os.environ.get("POLYMARKET_SIGNATURE_TYPE", "1"))
                kwargs["funder"] = funder
            client = PyClobClient(**kwargs)
            result = client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
            )
            return int(result.get("balance", 0)) / 1_000_000
        except Exception as exc:
            logger.debug("[clob] conditional balance check failed for %s: %s", token_id[:16], exc)
            return None

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
            results["py_clob_client_v2"] = True
        except ImportError:
            results["py_clob_client_v2"] = False
        return results
