# Live Trading Checklist

Operational checklist aligned to the scaling ladder in `THESIS.md`.
Each level (L0 → L5) has promotion criteria, risk limits, and a review
cadence.

## Scaling Ladder

| Level | Bankroll | Max/trade | Max open | Target P&L/mo | Status |
|---|---:|---:|---:|---:|---|
| L0 Validate (paper) | $345 | $24 | 10 | n/a | ✅ DONE |
| **L1 Probate** | **~$300** | **$5 (Tier 1)** | **30** | **$50–100** | **🔄 ACTIVE** |
| L2 Confirm | $1,000 | $50 | 15 | $250–500 | ⏳ gated |
| L3 Growth | $5,000 | $150 | 20 | $1,250–2,500 | ⏳ gated |
| L4 Scale | $25,000 | $500 | 25 | $5,000–10,000 | ⏳ gated |
| L5 Target | $100–300K | $1,500–3,000 | 30 | $10,000–20,000 | ⏳ gated |

*L1 uses the stake-ladder system rather than a fixed cap. Current tier: Tier 1
($5/trade cap). Bankroll raise to $1,000 and max/trade $50 requires slippage
measurement + human approval.*

---

## L0 → L1 Promotion (Paper → First Real Capital)

### Pre-Launch
- [ ] ≥50 resolved hypotheses
- [ ] ≥70% overall hypothesis accuracy
- [ ] ≥2 signal types at ≥60% accuracy on ≥20 resolved each
- [ ] ≥2 categories positive PnL on ≥20 resolved each
- [ ] Max drawdown <20% over rolling 30d
- [ ] 2+ weeks ops stable (no silent failures, no data loss, no pool exhaustion)
- [ ] py-clob-client installed and configured
- [ ] CLOB API credentials derived (`client.create_or_derive_api_creds()`)
- [ ] Private key set in `.env`
- [ ] $1,000 deposited on Polymarket
- [ ] `POLYMARKET_LIVE_BANKROLL_USD` raised 345 → 1000
- [ ] Circuit breaker thresholds confirmed (20% max DD, 5% daily)
- [ ] Kill switch tested (emergency stop + clear)
- [ ] Exit strategy documented (stop loss -30%, take profit +80%, time decay 14d)
- [ ] Human review + written approval of all gates

### Dry-Run Phase (24h before real money)
- [ ] Set `POLYMARKET_LIVE_ENABLED=1` but `DRY_RUN=True`
- [ ] Verify dry-run trades appear in Telegram and `live_trades` table with `dry_run=1`
- [ ] Verify order book is readable for target markets
- [ ] Verify limit prices are computed correctly vs actual book depth
- [ ] Review `dry_run_trades` for signal quality
- [ ] Confirm no real orders placed
- [ ] Check `stale_signals` — how many were missed due to timing?

### Day 1 Live (L1)
- [ ] Set `DRY_RUN=False`
- [ ] $1,000 capital, $50 max per trade, 8 max open positions
- [ ] Monitor first 3 trades manually via Telegram
- [ ] Check fill quality: slippage <2%, fill time <5s
- [ ] Verify Telegram alerts arriving for: trade placed, trade resolved, whale exit
- [ ] Confirm execution-gate logging in `live-collect` logs

---

## L1 → L2 Promotion Criteria

Measured over ≥30 live resolved trades at L1.

- [ ] Slippage median ≤ 2%
- [ ] Fill time median ≤ 5s
- [ ] Live WR ≥ 55% (within ±5pp of paper WR on same signals)
- [ ] Zero circuit-breaker / kill-switch incidents
- [ ] No silent failures over the promotion window
- [ ] Paper-to-live PnL correlation ≥ 0.7
- [ ] Human review + written approval

On promotion:
- [ ] Raise `POLYMARKET_LIVE_BANKROLL_USD` 1000 → 5000
- [ ] Max/trade $50 → $150
- [ ] Max open 8 → 10
- [ ] Re-confirm circuit-breaker dollar thresholds (5% daily = $250)

---

## L2 → L3 Promotion Criteria

Measured over ≥50 additional live resolved trades at L2.

- [ ] ≥2 categories showing independent positive PnL on ≥20 resolved each
- [ ] Live WR ≥ 55% overall
- [ ] Max drawdown <10% on rolling 30d
- [ ] Category concentration ≤ 50% in any single category
- [ ] Slippage still ≤2% at $150/trade size
- [ ] Human review + written approval

On promotion:
- [ ] Raise bankroll 5000 → 25000
- [ ] Max/trade $150 → $500
- [ ] Max open 10 → 15
- [ ] Add category-level circuit breaker (halt individual category at -$500/day)

---

## L3 → L4 Promotion Criteria

Measured over ≥100 additional live resolved trades at L3.

- [ ] 30-day rolling Sharpe ≥ 1.0
- [ ] ≥3 categories with independent positive PnL on ≥30 resolved each
- [ ] Max drawdown <10% on rolling 30d
- [ ] Slippage ≤2% at $500/trade size (Claim 6 test)
- [ ] Signal-decay monitoring in place (rotate out signals whose 30d WR drops below 55%)
- [ ] Ops stable 60+ days (uptime, no data-loss incidents)
- [ ] Human review + written approval

On promotion:
- [ ] Raise bankroll 25000 → 100000 (stage in tranches: 50K, then 100K after 2 weeks)
- [ ] Max/trade $500 → $1500
- [ ] Max open 15 → 20
- [ ] Dedicated capital reserve = 25% of bankroll outside trading

---

## L4 → L5 Promotion Criteria

Measured over 3 consecutive months at L4.

- [ ] Each of the 3 months within target P&L range ($5K–10K)
- [ ] No month below $2.5K PnL
- [ ] Max drawdown <15% on any rolling 30d window
- [ ] Slippage ≤2% at $1500/trade (definitive Claim 6 test at large size)
- [ ] ≥3 categories sustaining edge
- [ ] Backup signal pipeline: at least one new signal family validated at L3+ to rotate in if existing signals decay
- [ ] Human review + written approval

On promotion:
- [ ] Raise bankroll 100K → 200K (half-tranche), then 200K → 300K after 2 months if stable
- [ ] Max/trade $1500 → $3000
- [ ] Max open 20 → 25

---

## L5 Steady State (TARGET)

- Target: $10K–20K P&L per month
- Review cadence: monthly
- Demotion rule: two consecutive months below $5K PnL, OR any max-DD breach >15%, triggers demotion to L4
- Monthly operations:
  - [ ] Review per-signal WR and retire any below 55% on 30d rolling
  - [ ] Review per-wallet alpha scores; rotate wallets whose 30d WR drops below 0.45
  - [ ] Review category allocation; rebalance if any category is >50% of total P&L
  - [ ] Audit of slippage, fill time, capacity (is our order size affecting book?)

---

## Ongoing (Every Level)

### Gate Thresholds (progressive tightening with scale)

| Gate | Paper | L1 | L3 | L5 |
|---|---|---|---|---|
| Max price move since whale | 5% | 4% | 3% | 2% |
| Max category exposure | 40% | 35% | 30% | 25% |
| Depth multiple required | 2× | 2.5× | 3× | 4× |
| Spread safety factor | 1.5× | 1.75× | 2× | 2.5× |
| Staleness cutoff | 15min | 10min | 8min | 5min |

### Kill-switch escalation

At every level:
- [ ] Circuit breaker cumulative DD halt: 20% max DD (NEVER auto-reset)
- [ ] Daily loss limit: 5% of bankroll (auto-resets at midnight)
- [ ] Per-trade cap: level-specific max
- [ ] Max open positions: level-specific

### Telegram commands (2-way comms required at every level)

- [ ] `/status`, `/positions`, `/readiness`, `/funnel`, `/insiders` read
- [ ] `/kill <reason>`, `/unkill` remote control
- [ ] Critical alerts: CB trigger, daily-loss hit, service down, exec fail

### Deployment gates (code → live at every level)

- [ ] Paper-tested for ≥3 days before any live-path code change
- [ ] PR review + approval before touching `polymarket_live_executor.py`,
  `kill_switch.py`, `circuit_breaker.py`, `LIVE_SIGNAL_TYPES`, or
  `LIVE_TRADE_CATEGORIES`
- [ ] No `--no-verify` on live-path commits
- [ ] Backups healthy before any schema migration

---

## Demotion Rules (symmetric)

- Two consecutive months below target P&L range → demote one level
- Any circuit-breaker trip → halt and investigate before any further trading
- Any data-loss incident → halt and restore from backup before resume
- Slippage persistently >3% at any level → halt and investigate market
  capacity before scaling further

Demotion is not failure — it is the mechanism that keeps the ladder
honest. A level that can't sustain itself has to give up capital back
to the prior level.
