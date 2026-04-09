# Polymarket Autonomous Trading Platform

## Thesis

> Some Polymarket wallets have **persistent, category-specific informational
> edge**. This system identifies those wallets, monitors their trades in
> real-time, and copies them when their historical edge in the relevant
> category exceeds a statistical threshold.

Every component of this codebase exists to test one piece of this claim.
See [`THESIS.md`](THESIS.md) for the canonical statement of sub-claims,
evidence, and the decision framework.

## Five Claims Under Test

1. **Persistent edge exists** — some wallets win >55% over 50+ trades and the rate persists across rolling windows
2. **Edge is category-specific** — the same wallet can have edge in politics but not crypto; the unit of analysis is `(wallet, category)`
3. **We can identify who has edge** — alpha scoring separates copyable from non-copyable wallets
4. **We can copy in real-time** — live-collect WebSocket + alpha gate detect and act on whale trades fast enough that the edge isn't already priced in
5. **Edge survives transaction costs** — only verifiable in live trading

## Current Status

| Claim | Status | Evidence |
|---|---|---|
| 1. Persistent edge | ✅ confirmed | 77.9% WR on 61,122 clean reliable trades |
| 2. Category-specific | ✅ confirmed | 7 categories with copyable wallets |
| 3. Identification | ✅ confirmed | 130 copyable / 387 scored = 34% selectivity, 56 distinct wallets |
| 4. Real-time copy | 🔄 testing | Alpha gate live, accumulating hypothesis-bearing trades |
| 5. Transaction costs | ⏳ not started | Gated on Phase 4 (paper accuracy ≥70% on 50+ trades) |

## Daily Validation

Hypothesis accuracy is the single number that summarizes everything. The
daily Telegram digest at 08:00 UTC and the Command Center hero card both
display the same scorecard pulled from `KPITracker._thesis_scorecard()`.

| Sample | Accuracy | Verdict |
|---|---|---|
| < 10 | n/a | ACCUMULATING |
| 10–49 | n/a | PRELIMINARY |
| ≥ 50 | ≥ 70% | ✅ GO LIVE |
| ≥ 50 | 55–69% | 🟡 PROMISING — keep paper trading |
| ≥ 50 | 50–54% | ⚠️ MARGINAL |
| ≥ 50 | < 50% | 🔴 REJECTED — stop trading |

## How It Works

```
Polymarket WebSocket → live_collector
   ↓ (whale trade detected)
whale_signal_engine → ALPHA GATE (per-wallet, per-category copyability)
   ↓ (only copyable wallets pass)
trade_hypotheses (rationale generated + persisted)
   ↓
polymarket_paper_executor → fillability + circuit breaker + fusion gates
   ↓
polymarket_paper_trades INSERT
   ↓ (24h+ later, market resolves)
check_and_resolve_open_trades → mark hypothesis correct/incorrect
   ↓
KPITracker.compute_all() → daily digest + dashboard scorecard
   ↓ (if accuracy ≥70% on 50+ trades)
Phase 4 unlocked: live executor with $500 starting capital
```

**Test suite:** 622 passing (1 environmentally-fragile test deselected on contended live DB).

## Two-Process Architecture

**Process 1 — Daily Intelligence Pipeline** (runs every 4 hours)
1. `data-api-fetch` — accumulate new trade CSVs
2. `wallet-profiles --from-db` — compute directional_win_rate per wallet
3. `classify-wallet-buckets` — behavioral bucket classification
4. `refresh-universe` — top 25 markets x 9 categories from Gamma API
5. WhaleTripwire reloads watched wallet set automatically

**Process 2 — Continuous Live Monitor** (always running)
- Loads watched wallets from DB at startup + every 4h
- Subscribes to 225 markets via CLOB WebSocket
- On whale detection: fire signal → paper trade immediately
- Writes to wallet_alerts + market_signals tables in real time

## Quick Start

```bash
# 1. Rebuild wallet profiles from DB (one-time after fresh clone)
trading-cli data polymarket wallet-profiles --from-db
trading-cli data polymarket classify-wallet-buckets

# 2. Refresh market universe
trading-cli data polymarket refresh-universe

# 3. Start live collector with whale detection
trading-cli data polymarket live-collect --config configs/polymarket.yaml

# 4. Start Kalshi candle collection
trading-cli data kalshi live-candles --lookback-days 30

# 5. Start API + frontend
uvicorn trading_platform.api.main:app --port 8001
cd src/trading_platform/frontend && npm run dev   # http://localhost:5173

# 6. Check paper trade status
trading-cli paper dashboard
```

## Data Sources

| Source | Type | Markets | Usage |
|--------|------|---------|-------|
| Kalshi API | Authenticated REST | 213 open Economics | Live signals + paper trading |
| Polymarket CLOB WebSocket | Public WS | 225 active | Live price + whale detection |
| Polymarket Data API | Public REST | All markets | Wallet trade accumulation |
| Polymarket Gamma API | Public REST | All markets | Market universe + resolution |

## Signal Library

| Signal | Platform | What it measures | Status |
|--------|----------|-----------------|--------|
| whale_entry | Polymarket | Tier1/tier2 wallet trade detected | Active |
| convergence | Polymarket | 2+ watched wallets same side within 2h | Active |
| calibration_drift | Kalshi | Mean-reversion on price overshoot | Active (56.2% WR) |
| volume_spike | Kalshi | Follow informed money on unusual volume | Active (55.2% WR) |
| time_decay | Kalshi | Fade uncertainty premium near close | Active (54.8% WR) |

## Go-Live Criteria

| Gate | Requirement | Status |
|------|------------|--------|
| 1 | 50+ paper trades resolved with > 55% win rate | Pending |
| 2 | At least 2 categories showing edge independently | Pending |
| 3 | Max drawdown < 20% of bankroll over 30 days | Pending |
| 4 | Human review and approval | Pending |
| 5 | Start with $500 real capital, 1 category only | Pending |

## Known Limitations

- Kalshi historical candlestick endpoint returns 404 on free API tier. Use live candle collector for open markets only.
- Paper executor does not account for spread/slippage. Real trades will have worse execution.
- Polymarket WebSocket occasionally sends messages as JSON arrays. The collector handles this.
- Whale detection depends on watched wallet set quality — requires regular profile rebuilds.
- Category classification uses keyword matching; some markets may be miscategorized.
