"""Book-state capture at paper-entry fire time + honest_fill_sim taker grade.

DB-free where possible (pure functions, bare instances via __new__); the
end-to-end pieces run on throwaway SQLite files — never the prod Postgres
(2026-07-16 lesson: pytest suites silently writing PROD DB).
"""
from __future__ import annotations

import importlib.util
import json
import sqlite3
import threading
import time
from pathlib import Path

import pytest

from trading_platform.polymarket.polymarket_paper_executor import (
    PolymarketPaperExecutor,
    compute_book_state,
)

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"


def _load_honest_fill_sim():
    spec = importlib.util.spec_from_file_location(
        "honest_fill_sim_under_test", _SCRIPTS / "honest_fill_sim.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ── compute_book_state (pure) ────────────────────────────────────────────

class TestComputeBookState:
    BOOK = {
        "asks": [{"price": "0.12", "size": "100"},
                 {"price": "0.15", "size": "200"},
                 {"price": "0.30", "size": "50"}],
        "bids": [{"price": "0.10", "size": "80"},
                 {"price": "0.08", "size": "500"}],
    }

    def test_normal_book(self):
        bid, ask, depth = compute_book_state(self.BOOK, claimed_price=0.15)
        assert bid == 0.10
        assert ask == 0.12
        # asks at <= 0.15: 0.12*100 + 0.15*200 = 12 + 30 = 42
        assert depth == pytest.approx(42.0)

    def test_descending_raw_order_is_harmless(self):
        # The raw CLOB API returns both sides sorted DESCENDING (the
        # 2026-07-06 asks[0]-is-worst trap). min/max must not care.
        flipped = {"asks": list(reversed(self.BOOK["asks"])),
                   "bids": list(reversed(self.BOOK["bids"]))}
        assert compute_book_state(flipped, 0.15) == \
            compute_book_state(self.BOOK, 0.15)

    def test_cutoff_includes_exact_claimed_price(self):
        _, _, depth = compute_book_state(self.BOOK, claimed_price=0.12)
        assert depth == pytest.approx(12.0)

    def test_claimed_below_best_ask_gives_zero_depth(self):
        _, _, depth = compute_book_state(self.BOOK, claimed_price=0.05)
        assert depth == 0.0

    def test_claimed_none_gives_none_depth_but_keeps_tops(self):
        bid, ask, depth = compute_book_state(self.BOOK, claimed_price=None)
        assert (bid, ask) == (0.10, 0.12)
        assert depth is None

    def test_empty_and_malformed(self):
        assert compute_book_state(None, 0.5) == (None, None, None)
        # Transport failure must NOT read as depth 0.0 ("no liquidity at
        # our price") — that would be a claim about the market.
        assert compute_book_state({"error": "HTTP 500", "bids": [], "asks": []},
                                  0.5) == (None, None, None)
        # A genuinely empty book IS zero depth — real information.
        assert compute_book_state({"bids": [], "asks": []},
                                  0.5) == (None, None, 0.0)
        junk = {"asks": [{"price": "abc"}, {"size": "5"},
                         {"price": "0.20", "size": "10"}],
                "bids": "not-a-list"}
        bid, ask, depth = compute_book_state(junk, 0.25)
        assert bid is None
        assert ask == 0.20
        assert depth == pytest.approx(2.0)


# ── _capture_book_at_fire (bare instance, no DB, fake client) ────────────

class _FakeClob:
    def __init__(self, book=None, exc=None):
        self.book = book
        self.exc = exc
        self.calls: list[str] = []

    def get_order_book(self, token_id: str):
        self.calls.append(token_id)
        if self.exc:
            raise self.exc
        return self.book


def _bare_executor(**attrs) -> PolymarketPaperExecutor:
    ex = PolymarketPaperExecutor.__new__(PolymarketPaperExecutor)
    for k, v in attrs.items():
        setattr(ex, k, v)
    return ex


class TestCaptureBookAtFire:
    BOOK = {"asks": [{"price": "0.14", "size": "100"},
                     {"price": "0.16", "size": "50"}],
            "bids": [{"price": "0.13", "size": "40"}]}

    def test_yes_side_uses_yes_token_and_yes_space(self):
        fake = _FakeClob(book=self.BOOK)
        ex = _bare_executor(_book_client=fake)
        sig = {"yes_token_id": "ytok", "no_token_id": "ntok"}
        bid, ask, depth = ex._capture_book_at_fire(sig, "BUY", 0.14)
        assert fake.calls == ["ytok"]
        assert (bid, ask) == (0.13, 0.14)
        assert depth == pytest.approx(14.0)

    def test_no_side_uses_no_token_and_converts_claimed(self):
        fake = _FakeClob(book=self.BOOK)
        ex = _bare_executor(_book_client=fake)
        sig = {"yes_token_id": "ytok", "no_token_id": "ntok"}
        # claimed YES price 0.85 → NO-space cutoff 0.15 → only the 0.14 ask
        bid, ask, depth = ex._capture_book_at_fire(sig, "SELL", 0.85)
        assert fake.calls == ["ntok"]
        assert (bid, ask) == (0.13, 0.14)
        assert depth == pytest.approx(14.0)

    def test_fetch_failure_returns_nones(self):
        ex = _bare_executor(_book_client=_FakeClob(exc=RuntimeError("down")))
        sig = {"yes_token_id": "ytok"}
        assert ex._capture_book_at_fire(sig, "BUY", 0.5) == (None, None, None)

    def test_no_side_correct_token_returns_nones(self):
        # Generic token_id must NOT be used as a fallback — it can be
        # either outcome token; a wrong-book capture is silent garbage.
        fake = _FakeClob(book=self.BOOK)
        ex = _bare_executor(_book_client=fake)
        sig = {"token_id": "whichever", "condition_id": "c1"}
        assert ex._capture_book_at_fire(sig, "BUY", 0.5) == (None, None, None)
        assert fake.calls == []

    def test_markets_table_fallback(self, tmp_path):
        conn = sqlite3.connect(tmp_path / "w.db", check_same_thread=False)
        conn.execute("CREATE TABLE markets "
                     "(condition_id TEXT PRIMARY KEY, yes_token_id TEXT, "
                     "no_token_id TEXT)")
        conn.execute("INSERT INTO markets VALUES ('c1', 'ymk', 'nmk')")
        conn.commit()
        fake = _FakeClob(book=self.BOOK)
        ex = _bare_executor(_book_client=fake, _wallet_conn=conn,
                            _wallet_lock=threading.Lock())
        sig = {"condition_id": "c1"}
        _, ask, _ = ex._capture_book_at_fire(sig, "BUY", 0.5)
        assert fake.calls == ["ymk"]
        assert ask == 0.14
        conn.close()

    def test_zero_claimed_price_treated_as_missing(self):
        fake = _FakeClob(book=self.BOOK)
        ex = _bare_executor(_book_client=fake)
        bid, ask, depth = ex._capture_book_at_fire(
            {"yes_token_id": "ytok"}, "BUY", 0)
        assert (bid, ask) == (0.13, 0.14)
        assert depth is None


# ── discovery INSERT end-to-end (throwaway sqlite, fail-safe contract) ───

_DISCOVERY_TABLE = """
CREATE TABLE polymarket_paper_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT, question TEXT, category TEXT, side TEXT,
    entry_price REAL, size_usd REAL, signal_type TEXT,
    confidence REAL, confidence_raw REAL, wallet TEXT, entry_ts INTEGER,
    exit_ts INTEGER, archived INTEGER DEFAULT 0,
    wallet_tier_at_fire TEXT, source_wallet TEXT,
    detection_lag_seconds INTEGER, whale_entry_price REAL,
    alpha_score_at_fire REAL, features_at_fire TEXT,
    best_bid_at_fire REAL, best_ask_at_fire REAL, ask_depth_usd_at_fire REAL
)
"""


def _discovery_signal(cid: str) -> dict:
    return {
        "signal_type": "resolution_decay",
        "condition_id": cid,
        "direction": "BUY",
        "price": 0.14,
        "confidence": 0.5,
        "category": "crypto",
        # scanner wallet → hypothesis/alert branch skipped (no DB reach-out)
        "wallet": "velocity_detector",
        "question": "q?",
        "yes_token_id": "ytok",
        "no_token_id": "ntok",
    }


class TestDiscoveryInsertCarriesBookState:
    def _executor(self, book_client) -> PolymarketPaperExecutor:
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        conn.execute(_DISCOVERY_TABLE)
        conn.commit()
        return _bare_executor(_wallet_conn=conn,
                              _wallet_lock=threading.Lock(),
                              _book_client=book_client)

    def test_populated_when_fetch_works(self):
        ex = self._executor(_FakeClob(book=TestCaptureBookAtFire.BOOK))
        out = ex._execute_discovery(_discovery_signal("c-ok"),
                                    "resolution_decay", 0)
        assert out and out["discovery"] is True
        row = ex._wallet_conn.execute(
            "SELECT best_bid_at_fire, best_ask_at_fire, ask_depth_usd_at_fire,"
            " entry_price FROM polymarket_paper_trades").fetchone()
        assert row[0] == 0.13
        assert row[1] == 0.14
        assert row[2] == pytest.approx(14.0)
        assert row[3] == 0.14

    def test_fetch_failure_never_blocks_entry(self):
        # THE contract: a dead CLOB stores NULLs but the paper entry lands.
        ex = self._executor(_FakeClob(exc=ConnectionError("clob down")))
        out = ex._execute_discovery(_discovery_signal("c-fail"),
                                    "resolution_decay", 0)
        assert out is not None
        row = ex._wallet_conn.execute(
            "SELECT best_bid_at_fire, best_ask_at_fire, ask_depth_usd_at_fire"
            " FROM polymarket_paper_trades").fetchone()
        assert row == (None, None, None)


# ── honest_fill_sim: taker grade ─────────────────────────────────────────

class TestTakerFillable:
    def setup_method(self):
        self.sim = _load_honest_fill_sim()

    def test_boundaries(self):
        tf = self.sim.taker_fillable
        assert tf(0.14, 0.14, 5.0, 5.0) is True          # equality both legs
        assert tf(0.15, 0.14, 100.0, 5.0) is True
        assert tf(0.13, 0.14, 100.0, 5.0) is False       # claimed < ask
        assert tf(0.15, 0.14, 4.99, 5.0) is False        # depth < stake
        assert tf(None, 0.14, 100.0, 5.0) is False
        assert tf(0.15, None, 100.0, 5.0) is False       # no capture → resting
        assert tf(0.15, 0.14, None, 5.0) is False


class TestRunSimTakerBranch:
    def _seed(self, db: Path, now: int) -> None:
        conn = sqlite3.connect(db)
        conn.executescript("""
        CREATE TABLE polymarket_paper_trades (
            id INTEGER PRIMARY KEY, signal_type TEXT, condition_id TEXT,
            side TEXT, entry_price REAL, raw_entry_price REAL,
            exit_price REAL, raw_exit_price REAL, size_usd REAL,
            realized_pnl REAL, exit_reason TEXT,
            entry_ts INTEGER, exit_ts INTEGER, archived INTEGER DEFAULT 0,
            best_ask_at_fire REAL, ask_depth_usd_at_fire REAL,
            best_bid_at_fire REAL
        );
        CREATE TABLE markets (condition_id TEXT PRIMARY KEY,
                              yes_token_id TEXT);
        CREATE TABLE market_resolutions (condition_id TEXT PRIMARY KEY,
                                         resolves_yes INTEGER,
                                         payout_yes REAL);
        CREATE TABLE wallet_trades (asset TEXT, side TEXT, timestamp INTEGER,
                                    price REAL, size REAL);
        """)
        t = now - 3600
        rows = [
            # (id, cid, entry, size, pnl, reason, ask_at_fire, depth) —
            # 1: taker-honest (claimed 0.15 >= ask 0.14, depth 20 >= $5),
            #    zero tape prints → only the taker grade saves it.
            (1, "c1", 0.15, 5.0, 4.0, "resolved", 0.14, 20.0),
            # 2: book covered but depth $2 < $5 stake, no prints → drops.
            (2, "c2", 0.15, 5.0, 3.0, "resolved", 0.14, 2.0),
            # 3: pre-capture row (NULL book), prints cover → resting grade.
            (3, "c3", 0.20, 5.0, 2.0, "resolved", None, None),
            # 4: pre-capture row, no prints → drops.
            (4, "c4", 0.20, 5.0, 1.0, "resolved", None, None),
        ]
        for (rid, cid, ep, sz, pnl, reason, ask, depth) in rows:
            conn.execute(
                "INSERT INTO polymarket_paper_trades (id, signal_type,"
                " condition_id, side, entry_price, size_usd, realized_pnl,"
                " exit_reason, entry_ts, exit_ts, best_ask_at_fire,"
                " ask_depth_usd_at_fire) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (rid, "resolution_decay", cid, "YES", ep, sz, pnl, reason,
                 t, t + 600, ask, depth))
            conn.execute("INSERT INTO markets VALUES (?, ?)",
                         (cid, f"tok-{cid}"))
        # prints for row 3 only: 30 shares @ 0.19 <= 0.20 (needs 25 shares)
        conn.execute("INSERT INTO wallet_trades VALUES ('tok-c3', 'BUY', ?,"
                     " 0.19, 30)", (t + 60,))
        conn.commit()
        conn.close()

    def test_taker_or_resting_entry(self, tmp_path, monkeypatch):
        sim = _load_honest_fill_sim()
        from trading_platform.polymarket import db_connection
        monkeypatch.setattr(db_connection, "DB_BACKEND", "sqlite")
        monkeypatch.setattr(sim, "_write_report", lambda r: None)
        db = tmp_path / "sim.db"
        self._seed(db, int(time.time()))

        report = sim.run_sim(days=2, entry_window_s=300, db_path=str(db))
        v = {r["signal_type"]: r for r in report["verdicts"]}
        decay = v["resolution_decay"]
        assert decay["n_paper"] == 4
        assert decay["entry_fill"] == "2/4"   # rows 1 (taker) + 3 (resting)
        assert decay["taker"] == "1/2"        # 2 book-covered, 1 taker-filled
        # honest P&L keeps paper booking for the two entered resolved rows
        assert decay["honest_pnl"] == pytest.approx(6.0)
        assert decay["paper_pnl"] == pytest.approx(10.0)

        # standing verdict row carries the new counters
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT n_entry_filled, n_book_covered, n_taker_filled"
            " FROM honest_fill_verdicts WHERE signal_type='resolution_decay'"
        ).fetchone()
        conn.close()
        assert row == (2, 2, 1)

    def test_features_json_roundtrip_guard(self):
        # compute_book_state output must be JSON-serializable (rides into
        # INSERT params); trivially true for floats/None but pin it.
        vals = compute_book_state(TestCaptureBookAtFire.BOOK, 0.14)
        assert json.loads(json.dumps(vals)) == [0.13, 0.14, 14.0]
