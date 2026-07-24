"""Auto-redeem reconciler — book winners Polymarket settled without a SELL.

Pins the 2026-07-23 fix: our three live_trades closers all miss auto-redeemed
positions (absent from /positions, no SELL fill), so redeemed winners sat
status='matched'/open forever with realized_pnl unbooked. See
trading_platform/polymarket/redeem_reconciler.py.

Mirrors tests/polymarket/test_dust_settlement.py: the settlement decision is a
pure function fed pre-fetched wallet cash-flow, so win/loss/leave-open is
testable without network or DB. The key regression is the FULL cash-flow
model — the real ghosts were partially SOLD then partially REDEEMED, so
booking only the redeem leg against the whole-position basis is wrong.
"""
from __future__ import annotations

import pytest

from trading_platform.polymarket.redeem_reconciler import (
    RedeemBooking,
    reconcile_redeemed_positions,
    settle_redeem_from_activity,
)


def _buy(usdc, size=None, price=None):
    d = {"usdcSize": usdc}
    if size is not None:
        d["size"] = size
    if price is not None:
        d["price"] = price
    return d


def _call(**overrides):
    kwargs = dict(
        direction="BUY",
        fill_price=0.39,
        entry_price=0.385,
        shares=5.0,
        size_usd=1.925,
        in_positions=False,
        market_ended=True,
        buys=[],
        sells=[],
        redeems=[],
    )
    kwargs.update(overrides)
    return settle_redeem_from_activity(**kwargs)


# ── truth gate: a still-held token is not ours to close ────────────────────

def test_in_positions_leaves_open():
    # Present in /positions → still held on-chain; the /positions-based closer
    # owns it, not the redeem reconciler.
    assert _call(in_positions=True, redeems=[_buy(5, 5)]) is None


def test_absent_but_unresolved_no_cashflow_leaves_open():
    # Absent from /positions, market NOT ended, no redeem, no sells → we
    # cannot tell win/loss/still-settling. Never guess.
    assert _call(market_ended=False, buys=[_buy(2.0)]) is None


# ── the real 2026-07-23 ghosts (full lifecycle: buy → partial sell → redeem)

def test_full_redeem_no_sell_books_win():
    # #365948: bought 5 @ cost $2.1609, no sell, all 5 auto-redeemed for $5.
    b = _call(buys=[_buy(2.1609)], redeems=[_buy(5, 5)])
    assert b.outcome == "win"
    assert b.exit_reason == "reconciled_redeem"
    assert b.realized_pnl == 2.8391  # 5.0 − 2.1609
    assert b.exit_price == 1.0       # payout/shares = 5/5


def test_partial_sell_plus_redeem_net_win():
    # #335506: bought 5 ($2.00947), SOLD 4 → $1.3545, redeemed 1 → $1.
    # Booking only the redeem leg against the 5-share basis would misprice it;
    # the full cash-flow is the truth.
    b = _call(buys=[_buy(2.00947)], sells=[_buy(1.3545, 4, 0.35)],
              redeems=[_buy(1, 1)])
    assert b.outcome == "win"
    assert b.exit_reason == "reconciled_redeem"
    assert b.realized_pnl == 0.345   # (1.3545 + 1.0) − 2.00947
    assert b.buy_cost == 2.0095
    assert b.sell_proceeds == 1.3545
    assert b.redeem_payout == 1.0


def test_partial_sell_at_loss_plus_redeem_is_net_loss():
    # #389616: bought 5 ($2.11047), dumped 4 @ 0.15 → $0.5745, redeemed 1 → $1.
    # The last share "won" but the position NET LOST — outcome must follow the
    # sign of realized PnL, not the presence of a redeem.
    b = _call(buys=[_buy(2.11047)], sells=[_buy(0.5745, 4, 0.15)],
              redeems=[_buy(1, 1)])
    assert b.outcome == "loss"
    assert b.exit_reason == "reconciled_redeem"  # still a redeem close
    assert b.realized_pnl == pytest.approx(-0.536, abs=1e-4)
    assert b.exit_price == 1.0                    # the redeemed share paid $1


# ── sell-out with no redeem → the declared-but-unwritten reason ────────────

def test_full_selloff_no_redeem_books_reconciled_dataapi_sell():
    # Token gone from /positions, no redeem, but SELL proceeds exist → a full
    # exit that was never booked. Uses exit_counterfactual's declared reason.
    b = _call(market_ended=False, buys=[_buy(2.0)], sells=[_buy(2.5)])
    assert b.outcome == "win"
    assert b.exit_reason == "reconciled_dataapi_sell"
    assert b.realized_pnl == 0.5
    assert b.exit_price == 0.0


# ── expired worthless (loss) — buy activity vs DB-reconstructed basis ───────

def test_expired_worthless_books_loss_from_buy_activity():
    # Absent + market ended + no redeem + no sells → token expired worthless.
    # Loss at exit_price 0, basis from the actual on-chain BUY spend.
    b = _call(buys=[_buy(2.0)])
    assert b.outcome == "loss"
    assert b.exit_reason == "reconciled_expired"
    assert b.realized_pnl == -2.0
    assert b.exit_price == 0.0


def test_expired_worthless_reconstructs_basis_when_no_buy_activity():
    # /activity didn't return the BUY (pagination/age). Reconstruct entry cost
    # from the DB row (shares × entry), NOT dust — the test_dust_settlement
    # precedent. BUY 38 @ 0.15 → $5.70 basis.
    b = _call(fill_price=0.15, shares=38.0, size_usd=5.70, buys=[])
    assert b.outcome == "loss"
    assert b.realized_pnl == -5.70
    assert b.exit_reason == "reconciled_expired"


def test_expired_worthless_no_basis_leaves_open():
    # No buy activity AND no priceable DB row → can't book a number; leave open.
    assert _call(fill_price=None, entry_price=None, shares=0.0, size_usd=0.0,
                 buys=[]) is None


# ── SELL-direction (long NO) redeem: cost basis is (1 − fill)/share ─────────

def test_sell_direction_redeem_win_uses_no_side_basis():
    # A SELL row (long NO at 1−0.90 = $0.10/share). NO won and auto-redeemed
    # 5 → $5. No buy activity, so basis reconstructs from the NO price.
    b = _call(direction="SELL", fill_price=0.90, shares=5.0, size_usd=5.0,
              buys=[], redeems=[_buy(5, 5)])
    assert b.outcome == "win"
    assert b.realized_pnl == 4.5   # 5.0 − (5 × 0.10)
    assert b.exit_price == 1.0


# ── usdcSize is authoritative; fall back to size×price only when absent ─────

def test_usdc_falls_back_to_size_times_price():
    # A BUY activity row lacking usdcSize → cost = size × price = 10 × 0.20.
    b = _call(buys=[{"size": 10, "price": 0.20}], redeems=[_buy(10, 10)])
    assert b.buy_cost == 2.0
    assert b.realized_pnl == 8.0   # 10.0 − 2.0
    assert b.outcome == "win"


def test_exit_price_is_payout_per_share_even_when_fractional():
    # Redeem of 3 shares for $2.97 (0.99/share resolved) → exit_price 0.99.
    b = _call(buys=[_buy(1.0)], redeems=[_buy(2.97, 3)])
    assert b.exit_price == 0.99


# ── orchestrator: attribution, truth-gate, redeem consumption, delta ───────

class _FakeConn:
    """Minimal sqlite3/pg wrapper stand-in for the reconciler orchestrator."""

    def __init__(self, select_rows):
        self._select_rows = select_rows
        self._result: list = []
        self.updates: list = []
        self.committed = False

    def execute(self, sql, params=None):
        if sql.lstrip().upper().startswith("SELECT"):
            self._result = self._select_rows
        else:
            self.updates.append(params)
            self._result = []
        return self

    def fetchall(self):
        return self._result

    def commit(self):
        self.committed = True

    def close(self):
        pass


def test_orchestrator_books_ghost_skips_held_and_conserves(monkeypatch):
    monkeypatch.setenv("POLYMARKET_FUNDER_ADDRESS", "0xfunder")
    # Two open rows: A is redeemed+absent (books), B is still held (skip).
    # Columns: id, token_id, condition_id, direction, side, fill_price,
    #          entry_price, shares, size_usd, resolution_date, q
    rows = [
        (335506, "TOKA", "CIDA", "BUY", "BUY", 0.39, 0.385, 5.0, 1.925, 1784433599, "UFC..."),
        (999, "TOKB", "CIDB", "BUY", "BUY", 0.40, 0.40, 5.0, 2.0, None, "keep..."),
    ]
    fake = _FakeConn(rows)

    import trading_platform.polymarket.redeem_reconciler as rr
    import trading_platform.polymarket.db_connection as dbc
    import trading_platform.polymarket.position_fetcher as pf

    monkeypatch.setattr(dbc, "get_connection", lambda *a, **k: fake)

    class _FakePF:
        def fetch_wallet_positions(self, wallet):
            # Only B is still held; A is gone (auto-redeemed).
            return [{"asset": "TOKB", "conditionId": "CIDB"}]

    monkeypatch.setattr(pf, "PositionFetcher", _FakePF)

    def _fake_activity(wallet, kind, max_pages=12):
        if kind == "TRADE":
            return [
                {"asset": "TOKA", "side": "BUY", "usdcSize": 2.00947, "size": 5},
                {"asset": "TOKA", "side": "SELL", "usdcSize": 1.3545, "size": 4},
            ]
        return [{"conditionId": "CIDA", "usdcSize": 1.0, "size": 1,
                 "timestamp": 1784419084}]

    monkeypatch.setattr(rr, "_fetch_activity", _fake_activity)

    report = reconcile_redeemed_positions(apply=True)

    assert report["checked"] == 2
    assert report["booked"] == 1
    assert report["still_held"] == 1
    assert report["realized_delta"] == 0.345
    assert fake.committed is True
    # Exactly one UPDATE, for the ghost row, carrying the right realized_pnl.
    assert len(fake.updates) == 1
    params = fake.updates[0]
    assert 335506 in params
    assert round(params[3], 4) == 0.345  # realized_pnl in the UPDATE tuple


def test_orchestrator_fail_safe_on_empty_positions(monkeypatch):
    # An empty /positions response must NOT be read as "everything settled" —
    # that would mass-book every open row. Skip and book nothing.
    monkeypatch.setenv("POLYMARKET_FUNDER_ADDRESS", "0xfunder")
    import trading_platform.polymarket.position_fetcher as pf

    class _EmptyPF:
        def fetch_wallet_positions(self, wallet):
            return []

    monkeypatch.setattr(pf, "PositionFetcher", _EmptyPF)
    report = reconcile_redeemed_positions(apply=True)
    assert report["booked"] == 0
    assert "empty /positions" in (report["error"] or "")
