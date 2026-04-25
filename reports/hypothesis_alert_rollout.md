# Hypothesis-Driven Alerts + Telegram Silence + Re-Audit

**Date:** 2026-04-08
**Continuation of:** signal_analysis_clean.md / pnl_investigation.md / approved alpha-gate wiring
**Tests:** 610/610 passing (was 603, +7 hypothesis tests)

---

## Headline

Three things shipped:

1. **Telegram noise silenced.** The two remaining technical-scanner Telegram callsites (`hot_market_scanner.send_hot_market_discovered`, `order_book_monitor.send_order_book_anomaly`) are removed. Both now record into `AlertManager.record_signal_skipped(...)` so the count surfaces in the daily digest, but **zero individual messages** are sent for technical scanner output. Combined with the velocity_spike removal in session 13 and the alpha-gate from the previous session, all three sources of the historical 5,197-alert flood are dead.

2. **`trade_hypotheses` table + generation built.** Every paper trade now generates a structured rationale (alpha score, lifetime/30d WR, sample size, convergence count, recent form, plain-English thesis text + weighted confidence factors). Persisted to `trade_hypotheses` joined to the trade by `trade_id`. Available via `GET /api/hypotheses/recent`.

3. **Telegram alerts now embed the hypothesis.** `AlertManager.alert_trade_placed` accepts `hypothesis: str | None` and `alpha_score: float | None`. The body of every TRADE PLACED message becomes a 2–4 sentence reasoned trade note instead of a bare facts dump. The header line stays one-glance scannable on a phone.

---

## Part 2 — Telegram silenced for technical scanners

### Files changed

| File | Before | After |
|---|---|---|
| `hot_market_scanner.py:215-224` | Called `alerter.send_hot_market_discovered(added)` for every newly-added hot market | Records `am.record_signal_skipped("hot_market_no_wallet")` per added market; sends zero messages |
| `order_book_monitor.py:368-381` | Sent `alerter.send_order_book_anomaly(s)` for every high/critical anomaly | Records `am.record_signal_skipped("order_book_anomaly_no_wallet")`; sends zero messages |

### Cumulative status of technical-scanner Telegram noise sources

| Source | Status | Session removed |
|---|---|---|
| `whale_signal_engine.send_velocity_spike` | ✅ removed | session 13 |
| `hot_market_scanner.send_hot_market_discovered` | ✅ removed | **this session** |
| `order_book_monitor.send_order_book_anomaly` | ✅ removed | **this session** |

The remaining `send_hot_market_discovered` / `send_order_book_anomaly` / `send_velocity_spike` methods in `telegram_alerts.py` are **dead code** — no callers remain. Left in the file as documentation of the previous behavior; can be deleted in a future cleanup pass.

### Why silence them entirely

Every one of these signal sources fires without any wallet basis — they're price/orderbook scanners on technical patterns. The previous report (`signal_analysis_clean.md`) showed that:

- **Convergence (3+ aligned wallets) is the strongest signal in the data at 81.7% WR.**
- **Wallet alpha gating** is the only filter that separates real edge from noise.
- Technical scanners produce zero of either signal — they have no wallet, no convergence count, no per-wallet alpha.

Therefore: a Telegram message from a technical scanner adds noise without adding actionable information. The skip counter in the daily digest preserves visibility ("the order_book_monitor caught 47 anomalies overnight") without paging.

---

## Part 3 — `trade_hypotheses` table + generation

### Schema

```sql
CREATE TABLE trade_hypotheses (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id        INTEGER,                -- FK → polymarket_paper_trades.id
    wallet          TEXT NOT NULL,
    category        TEXT,
    signal_type     TEXT NOT NULL,
    market_slug     TEXT,
    market_question TEXT,
    direction       TEXT,
    entry_price     REAL,
    alpha_score     REAL,
    wallet_wr       REAL,
    wallet_resolved INTEGER,
    convergence_count INTEGER,
    thesis          TEXT NOT NULL,           -- plain-English rationale
    confidence_factors TEXT,                  -- JSON: [{factor, value, weight}]
    created_at      INTEGER NOT NULL
);
```

### Module — `src/trading_platform/polymarket/trade_hypotheses.py`

- `TradeHypothesis` dataclass — structured rationale
- `build_hypothesis(...)` — pulls alpha score + 5 most recent clean trades for the (wallet, category), composes a thesis text from the live facts, builds the weighted-factor list. Never raises — falls back to a minimal hypothesis on any DB lookup failure.
- `persist_hypothesis(...)` — writes to `trade_hypotheses` joined to `trade_id`
- `get_recent_hypotheses(...)` — feeds the API endpoint
- `ensure_schema(...)` — creates the table if missing

### Live sample output (real data, top copyable wallet)

```
wallet:      0xa3a417e4...
category:    politics
signal:      whale_entry
market:      "Will the next Fed decision be a 25bp cut?"
direction:   BUY @ 0.420
convergence: 3 wallets aligned

thesis:
0xa3a417e4… just BUY on "Will the next Fed decision be a 25bp cut?" at 0.420.
This wallet has hit 100% on 1700 resolved politics trades. 3 watched wallets
are aligned in the same direction — convergence has the strongest historical
edge in our data (3+ whales = 81.7% WR). Avg P&L per trade in this category:
+$0. Last 5 resolved politics trades: 5/5 winners. **High-conviction copy.**

confidence_factors:
[
  {factor: "alpha_score",  value: 0.997, weight: 0.40},
  {factor: "lifetime_wr",  value: 1.000, weight: 0.20},
  {factor: "wr_30d",       value: 1.000, weight: 0.15},
  {factor: "convergence",  value: 3,     weight: 0.15},
  {factor: "sample_size",  value: 1700,  weight: 0.10},
]
```

This is exactly the kind of trade note an operator can scan in 5 seconds and decide whether to act on.

### Adaptive thesis composition

The hypothesis text is built from facts, not templates. Different inputs produce different lines:

- **No alpha score yet** → "No alpha score yet for this wallet in `politics` (insufficient sample)" / "Speculative — wallet has no proven edge here."
- **High conviction (alpha ≥ 0.75)** → "**High-conviction copy.**"
- **Moderate (0.55 ≤ alpha < 0.75)** → "Moderate-conviction copy."
- **Low** → "Low-conviction copy — monitor closely."
- **Trending up** (wr_30d > lifetime_wr by ≥5pp) → "Recent 30-day WR is X% — trending up."
- **Trending down** → "trending down."
- **Convergence ≥ 2** → "N watched wallets are aligned in the same direction — convergence has the strongest historical edge in our data (3+ whales = 81.7% WR)."
- **Negative avg P&L** (positive WR but losing money) → "⚠ Avg P&L per trade in this category: −$X (positive WR but negative expectancy — small bet warranted)."
- **Recent form** → "Last 5 resolved politics trades: 4/5 winners."

---

## Part 4 — Telegram alerts as hypothesis reports

### `AlertManager.alert_trade_placed` updated signature

```python
def alert_trade_placed(
    self,
    signal_type: str,
    wallet: str,
    market: str,
    direction: str,
    stake: float,
    entry_price: float,
    fusion_score: float | None = None,
    wallet_tier: str | None = None,
    hypothesis: str | None = None,    # NEW
    alpha_score: float | None = None,  # NEW
) -> bool
```

### New message format

```
📈 TRADE PLACED
Signal: whale_entry
Wallet: <code>0xa3a417e4…</code> (tier1h)
Market: Will the next Fed decision be a 25bp cut?
BUY @ 0.420 | Stake: $1500 | α=0.997

Why: 0xa3a417e4… just BUY on "Will the next Fed decision be a 25bp cut?"
at 0.420. This wallet has hit 100% on 1700 resolved politics trades. 3
watched wallets are aligned in the same direction — convergence has the
strongest historical edge in our data (3+ whales = 81.7% WR). Avg P&L
per trade in this category: +$0. Last 5 resolved politics trades: 5/5
winners. **High-conviction copy.**
```

The header (5 lines) stays scannable. The body (1 paragraph) gives the operator the rationale to either trust the system's judgment or override it.

The `α=` badge in the header replaces the old `Fusion:` badge when an alpha score is present (alpha is a more direct expression of "should this trade exist?" than the composite fusion score).

### Wired into `polymarket_paper_executor`

Inside `execute_signal` after the trade INSERT, before the `alert_trade_placed` call:

```python
hypo = build_hypothesis(
    wallet_db_path,
    wallet=wallet,
    category=category,
    signal_type=signal_type,
    market_slug=signal.get("slug") or "",
    market_question=question or "",
    direction=side,
    entry_price=float(entry_price or 0),
    convergence_count=int(signal.get("converging_wallets") or 0),
)
hypothesis_text = hypo.thesis
persist_hypothesis(wallet_db_path, hypo, trade_id=trade_id)
```

The hypothesis text is then passed to `alert_trade_placed(... hypothesis=hypothesis_text, alpha_score=signal.get("alpha_score"))`. Failures in either step are caught and logged at DEBUG — they never block the trade.

### New API endpoint

```
GET /api/hypotheses/recent?limit=20
→ {available: true, hypotheses: [
    {trade_id, wallet, category, signal_type, market_question,
     direction, entry_price, alpha_score, wallet_wr, wallet_resolved,
     convergence_count, thesis, confidence_factors, created_at},
    ...
]}
```

Returns 200 (currently empty list since no real-wallet trades have fired through the alpha gate yet — synthetic-wallet trades skip hypothesis generation).

---

## Part 5 — Quick re-audit (post-alpha-gate, post-silence)

### Database state

| Item | Count |
|---|---|
| Wallet × category combos scored | 387 |
| Copyable combos | **130** |
| Distinct copyable wallets | **56** |
| Clean trades (`pnl_reliable=1`) | 61,122 |
| Quarantined trades | 69,636 (53%) |
| `trade_hypotheses` rows | 0 (live but no real-wallet trades fired yet) |
| `wallet_alerts` from synthetic scanners | (still recorded, but no Telegram emit) |

### Copyable wallets per category

| Category | Scored | Copyable | Avg copyability score |
|---|---|---|---|
| sports | 71 | 24 | 0.828 |
| politics | 84 | 23 | 0.788 |
| entertainment | 51 | 22 | 0.814 |
| other | 62 | 21 | 0.838 |
| economics | 50 | 16 | 0.803 |
| crypto | 39 | 13 | 0.779 |
| science | 30 | 11 | 0.810 |

Every category has between 11 and 24 copyable wallets — a meaningful population per category. Notice that **sports has the most copyable wallets** (24) despite the previous (contaminated) report's "sports is a -$140M catastrophe" claim. The clean data supports sports-category trading; the noisy data did not.

### Top 5 copyable wallet × category combos (live)

| Wallet | Category | WR | Sample | Score |
|---|---|---|---|---|
| `0xa3a417e43492` | politics | 100% | 1,700 | 0.9967 |
| `0xa3a417e43492` | entertainment | 100% | 298 | 0.9963 |
| `0xcb3143ee858e` | economics | 100% | 56 | 0.9953 |
| `0xa3a417e43492` | science | 100% | 77 | 0.988 |
| `0xcb3143ee858e` | politics | 98.6% | 2,159 | 0.9874 |

Two wallets dominate the very top (`0xa3a417e43492` and `0xcb3143ee858e`), each with multiple categories of proven edge. These are the wallets the system should be following.

### AlertManager state (post-restart, since the silenced sources are live)

```
sent_today:           0  (no real-wallet trades yet)
suppressed_today:     0  (no dedup hits yet)
skipped_signals_today: {}  (will populate with hot_market_no_wallet,
                            order_book_anomaly_no_wallet, velocity_spike_no_wallet
                            as the technical scanners run)
```

The skip counters will accumulate over the next 24h and surface in the daily digest at 08:00 UTC.

---

## Files changed this session

| File | Change |
|---|---|
| `src/trading_platform/polymarket/hot_market_scanner.py:215-228` | Removed Telegram emit; record skip counter |
| `src/trading_platform/polymarket/order_book_monitor.py:368-383` | Removed Telegram emit; record skip counter |
| `src/trading_platform/polymarket/trade_hypotheses.py` | **NEW** (240 lines) — `TradeHypothesis` dataclass + `build_hypothesis` + `persist_hypothesis` + `get_recent_hypotheses` + schema |
| `src/trading_platform/polymarket/alert_manager.py:199-238` | `alert_trade_placed` accepts `hypothesis` + `alpha_score` params; embeds them in the message body |
| `src/trading_platform/polymarket/polymarket_paper_executor.py:467-510` | Generates hypothesis after every successful trade insert; persists to `trade_hypotheses`; passes thesis text to `alert_trade_placed` |
| `src/trading_platform/api/main.py` | New endpoint `GET /api/hypotheses/recent` |
| `tests/polymarket/test_trade_hypotheses.py` | **NEW** — 7 tests covering minimal/high-conviction/convergence/negative-EV/factors/persistence |

---

## Verification

| Check | Result |
|---|---|
| `pytest tests/polymarket/test_trade_hypotheses.py` | **7/7 passing** |
| Full `pytest tests/` | **610/610 passing** (was 603, +7) |
| `/api/hypotheses/recent` | ✅ 200 |
| Live hypothesis generation against real DB | ✅ produces correct structured output for top wallet |
| Silence verification | hot_market_scanner + order_book_monitor no longer call `get_alerter()` for emit |
| Docker services | api, scheduler, live-collect restarted; all 5 healthy |
| Live-collect crash check | 0 errors, up cleanly |

---

## Bottom line

After this session, the **only Telegram messages the operator will receive are**:

1. 🔴 **Critical alerts** — circuit breaker, daily loss limit, service down, execution error, consecutive task failures (≥3)
2. 📈 **Trade placed** — only for real-wallet signals that pass the alpha gate; body now contains the hypothesis text + α score
3. ✅/❌ **Trade resolved** — with cumulative P&L and running win rate
4. 📊 **Daily digest** at 08:00 UTC — overnight summary including the technical-scanner skip counts

Everything else is silent. The technical-scanner spam is gone, the trade alerts now tell you *why* the system is taking the trade, and the alpha gate ensures only proven-copyable wallets get through. The infrastructure for "trade with reasoned conviction or don't trade at all" is complete.

The next operational step is to wait for real whale activity to flow through the (now end-to-end gated) pipeline and verify the first hypothesis-bearing TRADE PLACED message lands in Telegram.
