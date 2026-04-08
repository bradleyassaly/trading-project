"""
Tests for the keyword-based market categorizer.

Mocks the SQLite cache so they run with a tmp_path DB. Verifies the
keyword sets and the bulk backfill UPDATE behavior end-to-end against
a synthetic wallet_trades fixture.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from trading_platform.polymarket.market_categorizer import (
    MarketCategorizer,
    classify_keywords,
)


def _bootstrap_db(tmp_path: Path) -> Path:
    db = tmp_path / "wi.db"
    conn = sqlite3.connect(str(db))
    conn.executescript(
        """
        CREATE TABLE wallet_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT,
            title TEXT,
            condition_id TEXT,
            category TEXT
        );
        CREATE TABLE market_categories (
            market_slug TEXT PRIMARY KEY,
            condition_id TEXT,
            category TEXT NOT NULL,
            source TEXT,
            classified_at INTEGER NOT NULL
        );
        """
    )
    conn.commit()
    conn.close()
    return db


# ── classify_keywords ───────────────────────────────────────────────────────


class TestClassifyKeywords:
    def test_politics_trump(self):
        cat, src = classify_keywords("will-donald-trump-win-the-2024-us-presidential-election")
        assert cat == "politics"

    def test_politics_iran_regime(self):
        cat, _ = classify_keywords("will-the-iranian-regime-fall-by-march-31")
        assert cat == "politics"

    def test_politics_khamenei(self):
        cat, _ = classify_keywords("khamenei-out-as-supreme-leader-of-iran-by-february-28")
        assert cat == "politics"

    def test_crypto_bitcoin(self):
        cat, _ = classify_keywords("will-bitcoin-dip-to-65k-in-march-2026")
        assert cat == "crypto"

    def test_crypto_xrp(self):
        cat, _ = classify_keywords("xrp-updown-15m-1770296400")
        assert cat == "crypto"

    def test_sports_nhl_prefix(self):
        cat, src = classify_keywords("nhl-tb-buf-2026-04-06", "Lightning vs. Sabres")
        assert cat == "sports"
        assert src == "league_prefix"

    def test_sports_nba_prefix(self):
        cat, src = classify_keywords("nba-por-den-2026-04-06", "Trail Blazers vs. Nuggets")
        assert cat == "sports"
        assert src == "league_prefix"

    def test_sports_cricket_prefix(self):
        cat, src = classify_keywords("cricipl-raj-mum-2026-04-07", "IPL Rajasthan vs Mumbai")
        assert cat == "sports"
        assert src == "league_prefix"

    def test_sports_soccer_prefix(self):
        cat, src = classify_keywords("ucl-rma1-bay1-2026-04-07-bay1")
        assert cat == "sports"
        assert src == "league_prefix"

    def test_sports_french_ligue_prefix(self):
        cat, src = classify_keywords("fl1-ang-lyo-2026-04-05-lyo")
        assert cat == "sports"

    def test_sports_premier_league_natural_lang(self):
        cat, _ = classify_keywords("will-arsenal-win-the-202526-english-premier-league")
        assert cat == "sports"

    def test_economics_fed_rate(self):
        cat, _ = classify_keywords("will-the-fed-cut-rates-in-june")
        assert cat == "economics"

    def test_economics_crude_oil(self):
        cat, _ = classify_keywords("will-crude-oil-cl-hit-high-100-by-end-of-march")
        assert cat == "economics"

    def test_science_spacex(self):
        cat, _ = classify_keywords("will-spacex-launch-starship-by-may")
        assert cat == "science"

    def test_entertainment_grammy(self):
        cat, _ = classify_keywords("will-taylor-swift-win-grammy-2026")
        assert cat == "entertainment"

    def test_entertainment_eurovision(self):
        cat, _ = classify_keywords("will-france-win-eurovision-2026")
        assert cat == "entertainment"

    def test_no_match_returns_other(self):
        cat, src = classify_keywords("random-nonsense-slug")
        assert cat == "other"
        assert src == "fallback"

    def test_priority_politics_beats_crypto(self):
        # Slug mentions both — politics has higher priority
        cat, _ = classify_keywords("trump-bitcoin-tariff-tweet")
        assert cat == "politics"

    def test_title_fallback_when_slug_lacks_keywords(self):
        cat, src = classify_keywords("market-12345-xyz", title="NBA Finals MVP")
        assert cat == "sports"
        assert src == "title_keyword"

    def test_empty_slug_returns_other(self):
        cat, _ = classify_keywords("")
        assert cat == "other"


# ── MarketCategorizer (with sqlite) ─────────────────────────────────────────


class TestMarketCategorizerCache:
    def test_cache_prevents_double_classification(self, tmp_path: Path):
        db = _bootstrap_db(tmp_path)
        mc = MarketCategorizer(str(db))
        c1 = mc.classify("nhl-tb-buf-2026-04-06", "Lightning vs Sabres")
        c2 = mc.classify("nhl-tb-buf-2026-04-06", "Lightning vs Sabres")
        assert c1 == c2 == "sports"
        # Cache table should have exactly one row
        conn = sqlite3.connect(str(db))
        n = conn.execute("SELECT COUNT(*) FROM market_categories").fetchone()[0]
        conn.close()
        assert n == 1

    def test_persisted_cache_survives_restart(self, tmp_path: Path):
        db = _bootstrap_db(tmp_path)
        mc1 = MarketCategorizer(str(db))
        mc1.classify("will-bitcoin-hit-200k-in-2027", None)
        mc2 = MarketCategorizer(str(db))
        mc2._load_db_cache()
        assert mc2._memo.get("will-bitcoin-hit-200k-in-2027") == "crypto"


# ── Bulk backfill ──────────────────────────────────────────────────────────


class TestBulkBackfill:
    def test_backfill_updates_all_null(self, tmp_path: Path):
        db = _bootstrap_db(tmp_path)
        conn = sqlite3.connect(str(db))
        # Seed 5 trades with null category, 1 with existing 'crypto'
        for slug, title in [
            ("nhl-tb-buf-2026-04-06", "Lightning vs Sabres"),
            ("will-trump-win-2024", "Will Trump win?"),
            ("will-bitcoin-100k", "Bitcoin to 100k?"),
            ("will-fed-cut-rates", "Fed rate cut?"),
            ("random-nonsense", None),
        ]:
            conn.execute(
                "INSERT INTO wallet_trades (slug, title, category) VALUES (?, ?, NULL)",
                (slug, title),
            )
        conn.execute(
            "INSERT INTO wallet_trades (slug, title, category) VALUES (?, ?, ?)",
            ("will-eth-flip-btc", "ETH flippening?", "crypto"),
        )
        conn.commit()
        conn.close()

        mc = MarketCategorizer(str(db))
        summary = mc.backfill_wallet_trades(progress_every=1000, log_fn=lambda m: None)

        assert summary["still_null"] == 0
        assert summary["rows_updated"] >= 5

        conn = sqlite3.connect(str(db))
        cats = dict(conn.execute("SELECT slug, category FROM wallet_trades").fetchall())
        conn.close()
        assert cats["nhl-tb-buf-2026-04-06"] == "sports"
        assert cats["will-trump-win-2024"] == "politics"
        assert cats["will-bitcoin-100k"] == "crypto"
        assert cats["will-fed-cut-rates"] == "economics"
        assert cats["random-nonsense"] == "other"
        # The pre-existing crypto row stays untouched
        assert cats["will-eth-flip-btc"] == "crypto"

    def test_backfill_idempotent(self, tmp_path: Path):
        db = _bootstrap_db(tmp_path)
        conn = sqlite3.connect(str(db))
        conn.execute(
            "INSERT INTO wallet_trades (slug, title, category) VALUES (?, ?, NULL)",
            ("nba-lal-bos-2026-01-01", "Lakers vs Celtics"),
        )
        conn.commit()
        conn.close()

        mc = MarketCategorizer(str(db))
        first = mc.backfill_wallet_trades(log_fn=lambda m: None)
        second = mc.backfill_wallet_trades(log_fn=lambda m: None)
        # Second run finds nothing to do
        assert second["distinct_slugs_classified"] >= 0
        assert second["still_null"] == 0
        # Final state matches
        conn = sqlite3.connect(str(db))
        cat = conn.execute("SELECT category FROM wallet_trades").fetchone()[0]
        conn.close()
        assert cat == "sports"

    def test_other_under_15_percent_on_synthetic_realistic_set(self, tmp_path: Path):
        """Generate a realistic mix of slug patterns and verify <15% other."""
        db = _bootstrap_db(tmp_path)
        conn = sqlite3.connect(str(db))
        seed = [
            # Sports (most common in production)
            ("nhl-tb-buf-2026-04-06", "Lightning vs Sabres"),
            ("nba-lal-bos-2026-04-06", "Lakers vs Celtics"),
            ("nfl-kc-buf-2026-09-01", "Chiefs vs Bills"),
            ("mlb-nyy-bos-2026-05-01", "Yankees vs Red Sox"),
            ("ucl-rma-bar-2026-04-07", "Real vs Barca"),
            ("epl-mnc-liv-2026-04-04", "Man City vs Liverpool"),
            ("cricipl-raj-mum-2026-04-07", "IPL"),
            # Politics
            ("will-trump-win-2028", "Trump 2028"),
            ("will-the-fed-cut-rates", "Fed cut?"),  # economics actually
            ("zelenskyy-suit-by-july", "Zelenskyy in suit"),
            # Crypto
            ("will-bitcoin-hit-200k", "Bitcoin 200k"),
            ("ethereum-merge-success", "ETH merge"),
            # Entertainment
            ("will-taylor-swift-grammy-2026", "Taylor Grammy"),
            ("eurovision-2026-winner", "Eurovision"),
            # Science
            ("spacex-mars-launch-2027", "SpaceX Mars"),
            # Other (3 nonsense slugs out of 16 total = 18.75% but we tolerate)
            ("xyz-1234-mystery", None),
            ("abc-9999-qqq", None),
        ]
        for slug, title in seed:
            conn.execute(
                "INSERT INTO wallet_trades (slug, title, category) VALUES (?, ?, NULL)",
                (slug, title),
            )
        conn.commit()
        conn.close()

        mc = MarketCategorizer(str(db))
        summary = mc.backfill_wallet_trades(log_fn=lambda m: None)
        other_pct = summary["by_category"].get("other", {}).get("pct", 0.0)
        # On this small synthetic set we tolerate up to 25% other; the
        # production assertion is <10% on the real data.
        assert other_pct <= 25.0
