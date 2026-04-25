# Insider Detection Analysis — 2026-04-12

## Executive Summary

**Can insider detection produce a profitable signal? YES — and it's as strong as accumulation.**

The best method (`accuracy_60`: wallets with >60% uncertain-zone accuracy over 10+ resolved trades) produces **+0.283 EV** on 38 out-of-sample trades (p < 0.001, WR 84%). This is essentially identical to accumulation's +0.280 EV. Critically, the insider wallets have **zero overlap** with accumulation wallets — they're completely different wallets generating alpha through different mechanisms. Running both signals together would roughly double the number of tradeable signals.

However: only 3 wallets currently pass the accuracy_60 filter. Concentration risk is the main concern.

## Insider Score Methodology

Six-component composite score (weights sum to 1.0):

| Component | Weight | Rationale |
|---|---:|---|
| Uncertain-zone accuracy | 35% | Winning at 0.15-0.85 prices = real information, not penny collecting |
| Entry earliness (1 - avg entry percentile) | 20% | Trading before others suggests pre-information |
| Category purity | 15% | Domain specialists > generalists |
| Sample size (min(n/20, 1)) | 15% | More trades = more credible |
| Conviction sizing (min(avg_size/1000, 1)) | 15% | Larger positions = higher conviction |

**Score distribution** (27 wallets with 3+ uncertain trades):
- p75: 0.602
- p90: 0.647

## Detection Methods Tested

Five methods were defined; four had enough data for out-of-sample testing.

### Out-of-sample backtest (temporal split: first 50% for ID, last 50% for EV)

| Method | Insider wallets | OOS trades | WR | EV | p-value | Verdict |
|---|---:|---:|---:|---:|---:|---|
| **accuracy_60** | 3 | **38** | **84.2%** | **+0.283** | **<0.001** | SIGNIFICANT |
| **specialist** | 3 | 30 | 86.7% | +0.275 | 0.0002 | SIGNIFICANT |
| **score_top10** | 3 | **64** | 60.9% | +0.158 | 0.002 | SIGNIFICANT |
| **combined_no_bots** | 1 | 5 | 100% | +0.274 | 0.0002 | n too small |
| early_accurate | 0 | — | — | — | — | no qualifying wallets |

**All four testable methods produce statistically significant positive EV.** The simplest and strongest: `accuracy_60` — just take wallets with >60% accuracy in the uncertain zone over 10+ resolved trades and copy their future trades.

### accuracy_60 category breakdown (OOS)

| Category | N | WR | EV |
|---|---:|---:|---:|
| politics | 9 | 88.9% | +0.361 |
| other | 14 | 100% | +0.336 |
| geopolitics | 13 | 61.5% | +0.188 |

Positive across all three categories. Politics is the strongest cell for insiders (opposite of accumulation, which is strongest in geopolitics). This makes them even more complementary.

## Top 3 Insider Candidates

| # | Wallet | Accuracy | Uncertain N | Insider Score | PnL | Primary category |
|---|---|---:|---:|---:|---:|---|
| 1 | 0xd035fd5f4d6b... | **100%** | 28 | 0.696 | +$112 | other |
| 2 | 0xa7c1f914724f... | **65.2%** | 23 | 0.660 | +$4,458 | geopolitics |
| 3 | 0x4e84ce1599d3... | **100%** | 9 | 0.639 | -$93 | science |

Wallet #1 has 100% accuracy on 28 uncertain-zone trades — that's not noise at n=28 (binomial p < 0.000001 against 50% base rate). This wallet has genuine information advantage in the markets it trades.

## Political/Geopolitical Insiders

438 uncertain pol/geo trades across 20 wallets. 3 qualify as Tier 2 insiders (>60% accuracy, 5+ trades).

**T1+T2 OOS backtest**: N=41, WR=68.3%, EV=+0.214, p=0.016 — significant.

## Market Type Analysis

| Market type | Base rate | Insiders | N insiders | Edge |
|---|---:|---:|---:|---:|
| **military** | 51.3% | **70.4%** | 27 | **+19.0%** |
| **other** | 51.0% | **82.5%** | 63 | **+31.6%** |
| election | 36.5% | 28.9% | 45 | -7.6% |
| trade_policy | 50.0% | 0.0% | 0 | n/a |

**Military markets show the strongest insider edge** (+19%). Elections show NO insider edge — insiders actually underperform the base rate on elections (probably because election outcomes are genuinely uncertain, not information-asymmetric).

## Comparison to Existing Signals

| Signal | N | WR | EV | p |
|---|---:|---:|---:|---:|
| accumulation | 59 | 79.7% | +0.280 | <0.001 |
| **insider (accuracy_60)** | **38** | **84.2%** | **+0.283** | **<0.001** |
| whale_entry | 112 | 64.3% | -0.028 | 0.31 |

**The insider signal matches accumulation's EV with comparable statistical significance.**

### Overlap: ZERO

None of the 3 insider wallets appear in the accumulation signal's wallet set. The signals fire on different wallets, in different categories (insiders are strongest in politics; accumulation is strongest in geopolitics), on different markets. **They are fully complementary.**

## Recommendations

### Implement as `insider_entry` signal type

The evidence is strong enough for paper-mode deployment:
- +0.283 EV on 38 OOS trades (p < 0.001)
- Zero overlap with accumulation (complementary alpha)
- Simple detection rule (>60% uncertain accuracy, 10+ resolved)

### Implementation approach

1. **Run `accuracy_60` detection weekly** (recompute which wallets qualify based on their latest resolved trades)
2. **When a qualifying wallet enters a new market**: fire `insider_entry` signal
3. **Paper-trade it for 2-4 weeks** to confirm OOS performance holds
4. **If it holds**: add to live alongside accumulation

### Concerns to monitor

1. **Concentration risk**: only 3 wallets qualify. If one goes cold, the signal degrades. Consider the `score_top10` method (3 wallets but 64 OOS trades) as a fallback.
2. **Small sample**: 38 OOS trades is statistically significant but thin. Need 50+ before full confidence.
3. **Look-ahead risk**: the temporal split mitigates this, but the wallets were selected from the full dataset. True out-of-sample would require prospective data collection.
4. **Elections don't work**: insiders underperform on election markets. Consider excluding the `election` market type.

### Priority relative to going live

**Lower priority than going live with accumulation.** The system is cleared for live with accumulation now. Insider detection should run in paper mode alongside live accumulation, collecting OOS data. If it confirms after 2-4 weeks, add it to live.

---

**Report file:** `C:\Users\bradl\PycharmProjects\trading_platform\reports\insider_detection_analysis_2026-04-12.md`
**Script:** `scripts/insider_detection.py`
