# Alpha-generation roadmap & open research items

Companion to the system audit on 2026-06-03. This file captures the
non-code design work that came out of the audit — the items that need
analysis or strategic decisions before code is written.

---

## 1. Edge mechanism hypothesis (#1, #4)

### What we observe
- SELL/fade signals: +$1.36 EV/trade, 53% WR — **all of cumulative P&L
  (+$193 over 47 days) comes from this lane.**
- BUY/copy signals: −$0.62 EV/trade ex-lottery — strictly negative
  outside one Polish-politics 33× outlier.
- Naive copy of high-WR cohort: +13.7% / 47d in backtest at perfect fills.

### Hypothesis: where the edge comes from

**SELL/fade alpha:** whales BUY YES at high prices (0.70–0.95) on thin,
near-resolution books. Their entry is information-poor (impulsive,
mean-reverting) and we capture the over-payment when probability
re-prices. Mechanically: we BUY NO at (1 − whale_price). Edge is the
spread between whale's payment and rational probability.

**Naive-copy alpha:** high-WR/stable-size wallets have genuine
information (research, networks). Slow-horizon (hours-to-days) holds
mean the information advantage persists through our fill delay.

### Decay watch
- Whales using sub-accounts to obscure entries → fade signal weakens.
- Mass arb tools commoditize the same signals.
- Mitigation: track per-wallet SELL/fade WR over rolling 30d.
  If a wallet's "fade-ability" drops below baseline for 14 days,
  remove from SELL-trigger list.

### Open research
- [ ] Backtest SELL EV in the 2024-Q4 election cycle (high-vol regime).
- [ ] Decompose SELL EV by entry price bucket — is the edge concentrated
      in extreme-favorite SELLs (0.85+) where mean reversion is biggest?
- [ ] Measure per-wallet fade-ability and correlation with wq_score
      (the BUY-side metric). If they correlate negatively the same wallet
      can't be in both cohorts.

---

## 2. Regime detection (#3, #16)

### Problem
The +33% ROI test window (Apr–Jun 2026) is low-vol, low-political-event.
The lifetime PnL of top wallets is dominated by the 2024 cycle. We
can't tell from current data whether our edge generalizes.

### Approach
- Define regime by event density × outcome dispersion:
  - **Quiet:** <5 markets/day with >$10K volume, <2 mean abs daily P&L
  - **Active:** election cycle, geopolitical event clusters
- Tag every resolved trade with the regime active at fill time.
- Per-regime EV by signal_type. Promote/demote signals per-regime.

### Open research
- [ ] Reconstruct regime labels for our paper data + 2024 wallet history.
- [ ] Build a regime classifier from Polymarket daily volume + spread + outcome dispersion.
- [ ] Add a `regime_at_fire` column to live_trades and live_trades.dr=1.

---

## 3. Multi-venue / multi-key (#5)

### Risk
Single Polymarket proxy + single private key + single region:
- One region 403 → 100% downtime (we already had 7 incidents in May).
- One key compromise → 100% loss.
- Polymarket policy change → 100% capacity wipe.

### Proposed architecture
- **Primary venue:** Polymarket (current).
- **Secondary venue:** Kalshi for politics + economics. Code partially
  exists but is not trading; needs CFTC-compliant onboarding.
- **Key segregation:** dedicated wallet per strategy lane
  (SELL/fade vs naive_copy) so a fade-strategy compromise doesn't
  burn the copy-strategy bankroll.
- **Region resilience:** add a fallback VPN/proxy region for
  CLOB orders when 403s start.

### Open decisions
- [ ] Onboard Kalshi accounts? (legal/compliance review)
- [ ] Acceptable risk: 1 key, 1 region (current) or multi-key, multi-region?

---

## 4. Staging environment + versioned releases (#25, #27)

### Current state
- All changes bind-mount into running containers. Mistakes hit prod.
- No tagged releases; can't bisect a regression.
- No reproducible build for a known-good version.

### Proposed
- **Staging stack:** `docker-compose.staging.yml`, separate postgres,
  separate proxy wallet (paper-only). Every PR-equivalent change runs
  there for ≥24 hours first.
- **Tagged releases:** `git tag v0.1.x` for every executor change;
  Docker image tag matches. Rollback = `docker compose up <prev_tag>`.
- **Migration discipline:** every schema change goes through a numbered
  migration file. Currently we use ad-hoc `ALTER TABLE` like the four
  I ran today (slippage_signed, slippage_cost_usd, wq_*, exit_attempts).

### Sequencing
1. Lift schema changes from this session into a numbered migration file.
2. Add a `Dockerfile` SHA tag + git tag at the next clean state.
3. Set up `docker-compose.staging.yml` with `data/staging/postgres_data`
   volume + a paper-only proxy.

---

## 5. Observability roadmap (#26)

### Current state
- Structured JSON logs to per-task files. Discovery is grep.
- Telegram alerts on hooks, but no aggregated dashboards.
- No metric retention — can't answer "what was the fill rate 2 weeks ago?".

### Proposed
- **Tier 1 (cheap):** A `metrics_daily` table — written nightly by
  daily_system_review with: realized PnL, n_trades, fill_rate, mean
  latency, Brier per signal, cohort_size. Query via SQL.
- **Tier 2:** Grafana on a Prometheus exporter that scrapes the same
  metrics endpoint. ~1 day of work; massive win for visibility.
- **Tier 3:** Trace IDs per trade. Stamp signal_fired_at →
  decision_trace.id → live_trades.id → on-chain tx so we can replay
  any trade end-to-end.

---

## 6. Causal attribution (#17)

### Problem
We say "whale_entry_filtered SELL made +$113." We can't say "without
the entry-price gate the same signal would have lost $X." Without
counterfactual numbers, we can't tell which gates earn their keep
and which are dead code.

### Approach
- Tag every gate decision in `decision_trace` (already partly done).
- Nightly job: for every blocked trade, simulate "what if we'd traded
  anyway?" using historical resolution. Compute the gate's saved or
  forgone $.
- Surface per-gate net-saved-$ in daily review.

### Open research
- [ ] Audit decision_trace schema; ensure we log enough to simulate the
      blocked counterfactual. Currently we have signal_type/side/value/threshold.
      Need to record condition_id + intended size at minimum.
- [ ] Build the nightly counterfactual job. Output: per-gate
      saved/forgone $, per-signal-type, last 30d.

---

## 7. YES/NO direction confusion (NEW — surfaced during slippage audit)

### Critical finding
Inspection of live BUY trades showed `entry_price=0.505` (target) and
`fill_price=0.99` (fill) — meaning we placed a BUY for the YES token
when the whale had bought the NO token. The system is silently
buying the wrong side on some BUY signals.

### Likely cause
`token_id` derivation in signal generation prefers `yes_token_id`
fallback when the whale's `asset` field is ambiguous. The whale buys
NO at 0.50 but we default to YES at 0.50.

### Fix path
- Audit `naive_copy_signal._find_candidate_trades`: we use the whale's
  `asset` directly. Good.
- Audit the whale-derived signal generators (whale_signal_engine,
  cascade, oversized_bet) — these are likely deriving the wrong
  token_id when the signal context is YES/NO-ambiguous.
- Add a runtime check: if our intended fill price differs from the
  signal's whale price by >0.2, refuse to place the order.

### Action item
- [ ] Add the 0.2-divergence pre-flight check to clob_client.place_market_order.
- [ ] Inventory which signal generators set token_id and confirm each
      one passes through the whale's actual asset, not a default.

---

## Status of code-deliverable items (Phase 1–4)

| # | Flaw | Status |
|---|---|---|
| 2 | Take-profit allowance bug | ✅ Fixed |
| 5 | Orphan scheduler tasks | ✅ Disabled (10 tasks) |
| 8 | Slippage not measured | ✅ Signed slippage + $ cost columns added |
| 11 | Zero-fill rate | Already 15% post-depth-fix; not regressing |
| 12 | No exit retry queue | ✅ exit_attempts + escalation + stuck-flag |
| 13 | No partial-fill mgmt | Already in place via sync_shares_from_onchain |
| 14 | Latency budget | ✅ fill_time_ms now populated for market orders |
| 15 | Brier 0.32 / per-slice calib | ✅ Activated (env=1) |
| 18 | Demote/promote asymmetry | Deferred — needs more cohort data |
| 19 | Single-day VaR uncapped | ✅ portfolio_risk.py + VAR gate (shadow) |
| 20 | No correlation tracking | ✅ topic clustering in portfolio_risk |
| 21 | Daily-loss circuit | ✅ vol_target_sizer.py (shadow) |
| 22 | No tail-risk hedge | Deferred — needs capital |
| 23 | 130 modules → 10 alpha | ✅ 10 orphan files deleted; 6 services still solid |
| 24 | Config scatter | ✅ flags.py centralization |
| 28 | Sparse tests | ✅ Full-loop integration test added |
| 1, 4, 6, 7, 9, 10, 16, 17, 25, 26, 27 | Research / design | ↑ This document |

---

## Phase 1 deployment summary (delivered 2026-06-03)

- **Per-slice calibration:** active in api + scheduler + live-collect.
  Brier 0.58→0.18 on top SELL signals.
- **Real slippage tracking:** `slippage_signed` and `slippage_cost_usd`
  columns added; 142 historical rows backfilled; 30 space-confused rows
  nulled out to avoid pollution.
- **Latency:** `fill_time_ms` populated by both `place_market_order`
  and `place_limit_order` going forward.
- **Exit retry queue:** failed exits increment `exit_attempts`, fire
  Telegram alerts at 5 and 20 attempts, auto-stuck flag stops looping
  on doomed positions after 20.
- **Wallet-quality score:** `wq_score` computed for 1,206 wallets.
  Top cohort radically different from prior alpha_score ranking
  (top-10 now includes consistent grinders, not lottery winners).
- **Portfolio risk:** topic clustering + 30d VaR(5%) reporter — current
  VaR = $20.58 (5.5% of equity).
- **Vol-target sizer:** computes scale (shadow mode); current scale
  = 1.12× given $9.97/day realized vol vs $11.20 target.
- **VaR gate:** wired into executor (shadow); enables with
  `ENABLE_VAR_GATE=1`.
- **Orphan deletion:** 10 module files removed (Phase C/D/E/F + helpers).
- **Config centralization:** `flags.py` is the canonical source for
  active feature flags.
- **Integration test:** `tests/polymarket/test_full_loop.py` covers
  cohort → candidate → shadow row → resolution P&L.

Realized impact won't be known until: (1) the take-profit fix resolves
the $105 unrealized; (2) per-slice calibration affects 4+ weeks of new
trades; (3) naive_copy shadow accumulates ~30 resolved samples.

Next review: 2026-06-17 (two weeks).
