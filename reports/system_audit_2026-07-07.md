# Full-System Adversarial Audit — 2026-07-07

Six parallel specialized audits (execution, exit/settlement, risk/calibration,
data pipeline, ops/infra, paper/evidence loop) plus a cross-cutting synthesis.
~120 raw findings, deduplicated below. Line numbers verified against the
working tree at commit 1cc3190; DB claims verified against production Postgres.

**Verdict:** the plumbing rebuilt over 7/5–7/6 (booking honesty, executable
marks, event caps, reconciliation) is sound, but it sits on four rotten
foundations: (1) the ledger's price-space for SELL-side trades is
indeterminate, (2) `confidence` is not a probability anywhere and calibration
trains on its own output, (3) there is no canonical resolution truth and the
evidence loops are mechanically starved, (4) eleven unco-ordinated writers
share one autocommit god-table with no locking. Everything the promotion
pipeline "knows" about signal quality passes through at least one of these.

---

## A. Systemic root causes (architecture flaws)

**A1. No canonical price space for `fill_price` / SELL-side math.**
`place_market_order` stores the NO-space target for SELL fills
(clob_client.py:387-392) while `_record_attempt` documents fill_price as
YES-space and computes `shares = size/(1-fill_price)`
(polymarket_live_executor.py:1700-1706). The 0.30-deviation guard makes the
stored space *price-dependent* (NO-space at extremes, YES-space mid-range).
Consumers assuming YES-space: monitor cost basis (:332), `_settle_dust_close`
(:92), live SELL-exit PnL (:677-683 — every NO exit below 0.50 books phantom
profit), paper NO-side resolution (paper_executor:1886-92 vs mark-exit :2230
using the opposite frame), the equity snapshot, both repair scripts. The 7/6
`backfill_actual_fills` (order_reconciler.py:82-113) writes data-api VWAP in
the *held token's* space into the same column every 10 minutes. One column,
two frames, ≥8 read sites disagreeing. **Every SELL/NO number in the ledger is
suspect until one frame is chosen and enforced.**

**A2. Confidence is a sizing dial being fit as a probability (closed loop).**
Per-signal `confidence` at generation is heterogeneous garbage: WR×1.3
(wallet_reversal), 0.60+0.08×count (accumulation), |Δprice|×3
(price_velocity), hardcoded 0.60 (naive_copy) — none is P(win). The paper
executor replaces it with an 8-component ensemble score, calibrates it, then
multiplies by stake boosts up to 1.6× and persists THAT
(paper_executor:1278→1291→1504→1664). The live executor calibrates then
multiplies by specialist ×1.25 / z ×1.4 / conviction ×1.15 (executor:719-787)
and persists that. `confidence_raw` is computed but never persisted to a
column. Both isotonic fitters (global fallback added 7/6,
isotonic_calibration.py:163-168; per-slice :323-333) train on these stored
post-calibration, post-boost values — each refit composes the previous map
into its own training data (recursive shrinkage), while the per-slice curve is
additionally *applied* to raw confidence (domain mismatch, :519-527).
Validation is in-sample (PAV always "improves" its own training Brier); the
`bp_max ≥ 0.65` persistence guard means a truthful curve can never be saved if
honest top-bin WR < 65% — zombie-optimistic curves win by construction.

**A3. No canonical resolution truth; the evidence loops are mechanically dead.**
Six+ outcome sources (markets.outcome_prices, gamma_resolution.csv, live Gamma
UMA status, wallet_trades.market_outcome, data-api cashPnl/redeemable,
on-chain balances) with no `resolutions` table; every consumer uses a
different subset in a different order. Verified starvation mechanics:
- `lastrowid` is hardcoded None under PG (db_connection.py:319-322); the paper
  executor and hypothesis persister still read it → **91% of trade_hypotheses
  have NULL trade_id; 552 of 565,766 signal_outcomes have paper_trade_id** —
  the hypothesis and outcome loops have been severed since the April cutover.
- `fetch_all_resolved` pages endDate-ASCENDING with max_pages=300 over a 90d
  window → the daily rebuild never gets past the oldest ~2 weeks. **CSV
  rebuilt 7/6 18:57; newest close_time in it: 2026-04-21.**
- signal_resolver appends 3-column rows into the 7-column CSV (schema
  mismatch, unreadable) and the daily fetcher truncates them anyway.
- resolution_resolver's condition_id fallback returns the NO-token payout
  (last-row-wins index, :83-88) — systematically inverted.
- enrich_resolution Pass 2 writes token-outcome as market-outcome
  (:224-226) — **6,496 condition_ids carry BOTH 'YES' and 'NO'
  market_outcome rows**; paper `check_resolutions_v2` settles from them with
  `LIMIT 1` no ORDER BY (row lottery).
- signal_resolver books side-blind PnL (`(res-entry)*size/entry` regardless of
  direction, :212-221) — NO-side winners booked as -100% losses with
  outcome='win'.
- **474,029 signal_outcomes pending (84%)**; hypotheses 612 created/week vs 14
  resolved/30d.

**A4. Eleven writers, one autocommit god-table, zero locks.**
live_trades is 99.4% gate-decision logs (136k blocked vs 236 matched) with 11
distinct exit-field writers (monitor live-fill/dust-SELL/dust-BUY, paper
exits, book_resolved_positions, reconcile_positions, shadow resolver,
chain-direct second process, 3 repair scripts). Pool is autocommit=True
(db_connection.py:126) so commit() is a no-op and read-decide-write is
unprotected; monitor bookings UPDATE `WHERE id=?` with no `exit_ts IS NULL`
guard (:444,:500,:689) so they overwrite other channels' bookings;
`check_live_exits` runs concurrently in scheduler AND wallet-stream processes
with no lock — the second's cancel-retry can cancel the first's live order.
Blocked rows are stamped `dry_run=self.DRY_RUN` (always True) so live-path
blocks are recorded as dry-run rows; several gates record nothing.

**A5. Evidence split-brain: three courts, three verdicts, no supremacy rule.**
KillSwitch treats polymarket_paper_trades as "ground truth" and blends stale
backtests: `effective_n = n_live + bt_n/2` with no recency filter lets a
months-old backtest alone authorize full-stake live trading (kill_switch:
211-228); the WR gate blends backtest FOREVER (:304-309) though the EV gate
stops at n_live≥15. The daily review's reconciled live verdicts (KILL etc.)
influence nothing except a hardcoded set in one module. signal_category_ev is
simultaneously distrusted ("stale resolution data", executor:588-591) and
trusted (slice bypass :615-655, stake-cap scaling :151-196). The paper
executor still 1.5×-boosts wallet_reversal (KILLed 7/5) from an April cohort.

**A6. Paper evidence is structurally unlike live execution.**
Paper fills at the whale's historical price with flat 2% spread (look-back
fill); depth gate runs at $25 MIN_STAKE then sizes up to $500 unchecked;
discovery lane bypasses ALL gates and its rows are indistinguishable evidence;
paper regime = 300 open positions vs live 5-30; paper trailing stop is dead
code (mfe never selected → never fires) and paper equity unrealized is
structurally 0; `pre_resolve_decay` unit bug (fraction÷dollars) force-exits
nearly everything within 8h of resolution at marks — systematically clipping
the resolution winners that resolution_decay's thesis depends on. The
naive-copy lane's slippage instrumentation is dead code (expected := whale
price). No consumer weights paper evidence by fill realism.

**A7. Gates accrete and contradict; policies conflict across modules.**
~25 gates in the live chain (docstring says 14). Leader budget and topic cap
count error/cancelled rows forever (the dedup bug class, fixed only for
dedup); order_reconciler cancels ALL >10-min GTC orders while the executor's
routing strategy deliberately rests GTC on thin books; discovery $1 cap is a
fiction ($5 CLOB floor, then sizing multiplier ×2 applied AFTER the probation
clamp); Kelly is decorative at this bankroll ($5 floor > 2% cap ⇒ flat $5-7.7
sizing); probation never enforces its documented WR floor; kill-switch
breaker fails OPEN on DB errors; horizon gate does an HTTP GET per signal
before the free emergency-stop check.

**A8. Ops: one Windows box, sequential scheduler, log-grep monitoring, leaked keys.**
`.kalshi/private_key.pem` committed to git; POSTGRES_PASSWORD in compose ×8;
POLYMARKET_PRIVATE_KEY injected into 7 containers (4 needless) and readable
via the repo mount in all; API :8001 on 0.0.0.0 with NO auth exposing
kill-switch reset; Postgres :5432 LAN-published. The dead-man's switch built
after the June 5.7-day outage is UNARMED (HEARTBEAT_PING_URL unset). The
scheduler is one serial thread: weekly tasks get 3.5-day timeouts ahead of the
5-min live exit monitor, whose own 150s timeout SIGKILLs it mid-pass (the
institutionalized "exit fills, booking dies" bug); shell=True timeouts orphan
grandchildren (zombie double-writers). Monitoring greps exact log strings with
no meta-alert; reconciler exits 2 on drift so "monitor found drift" arrives as
"task failed". Money paths have ~zero direct test coverage (all 9 of this
week's production bugs were in untested code).

---

## B. Critical findings (fix before trusting any number)

| # | Finding | Where |
|---|---------|-------|
| C1 | SELL fill_price space indeterminate; every SELL/NO ledger number suspect (family A1) | clob_client:221-260, executor:1696-1706, monitor:332/677-683 |
| C2 | 7/6 fill-backfill writes held-token-space VWAP into YES-space column every 10 min; also pools fills across rows (double-count) | order_reconciler:82-113 |
| C3 | Calibration closed loop: fitters train on stored calibrated×boosted confidence; confidence_raw never persisted | isotonic_calibration:163-168,323-333; paper:1504,1664; executor:719-787,1777 |
| C4 | signal_resolver books side-blind PnL; paper NO-side resolution uses wrong cost basis — EV tables & curve history invalidated for NO-side | signal_resolver:212-221; paper:1886-1892,2903-2911 |
| C5 | lastrowid=None severed hypothesis/outcome linkage since April (91% NULL) | db_connection:319-322; paper:1675; trade_hypotheses:584 |
| C6 | Resolution CSV permanently ~75d stale (ascending paging × 300-page cap); newest close_time 2026-04-21 after 7/6 rebuild | gamma_resolution_fetcher:67-215 |
| C7 | position_fetcher + market_universe write/read dead SQLite; wallet_positions frozen 2026-04-11; wallet-derived stream subscriptions computed from April data | position_fetcher:69-170; market_universe:342-364 |
| C8 | Stale backtest alone authorizes full-stake live (effective_n blend, no recency; WR blends forever) | kill_switch:211-228,304-309 |
| C9 | `_record_attempt` swallows failures at DEBUG and references ~15 columns ensure-table never creates → real fills can vanish from ledger | executor:1760-1810 |
| C10 | Committed Kalshi private key (git history); wallet key in 7 containers; unauthenticated kill-switch-reset on 0.0.0.0; PG on LAN with committed password | .kalshi/, compose, api/main.py:2077,2419 |
| C11 | Dead-man's switch unarmed (HEARTBEAT_PING_URL unset) — whole-host outages still silent | health_watchdog:278, .env |
| C12 | Exit monitor: 150s scheduler timeout kills it mid-pass; runs concurrently in 2 processes, no lock; bookings unguarded (last-writer-wins) | task_scheduler:1131; wallet_stream:390-396; monitor:444/500/689 |
| C13 | reconcile_positions.py re-introduces three fixed bugs (zero-balance⇒loss, zeroes shares/size, wrong-space math) — live landmine, no dry-run | scripts/reconcile_positions.py:80-134 |
| C14 | enrich_resolution Pass 2 writes token-outcome as market-outcome (6,496 self-contradicting markets); paper v2 settles from them via LIMIT 1 row-lottery | enrich_resolution:224-252; paper:2893-2903 |

## C. High (abridged; full detail in agent reports)

- Dust settle truth-1 lacks `redeemable` check → stale balance read books
  mark-to-market as realized and closes a live position (monitor:97-105).
- cashPnl fan-out double-booking across rows sharing token/condition
  (book_resolved_positions:67-89; dust path same).
- Own-sell-fills lookup unscoped (no time window/share cap; >500-trade history
  pages out → falls through to resolution booking at wrong price)
  (monitor:115-134).
- Never-filled `live` rows can be dust-closed while their resting GTC order
  survives, fills later, and owns tokens with no open DB row (monitor:256;
  order_reconciler:146).
- Leader budget + topic cap count error rows forever (executor:899-925,
  1485-1500); portfolio caps read-then-act with no lock (PSG-class race one
  level up).
- Sizing multiplier applied after probation/discovery clamp (2× the bounded
  stake); global+multiplier double correction where no per-slice curve.
- Depth gate frame contradiction on SELL (executor:1256-1279 vs clob_client
  YES-space claim); max-chase skipped entirely when signal lacks entry_price.
- Whale-mirror exit fires on any SELL row (no size/side/token check) and
  silently never fires for mixed-case wallets; whale_exit_detector emits
  sign-inverted paper evidence (side=NO, direction=BUY).
- wallet_trade_poller `outcomeIndex == "0"` int-vs-string → multi-outcome
  trades mislabeled NO; stale-trade skip also skips whale-EXIT detection after
  any >10-min stream outage.
- Stream: no missed-event recovery, no subscription-ack validation (a dead
  topic is silent), watermark not monotonic (stream vs poller regression),
  tier-1 fallback polls arbitrary 50 of 343 wallets.
- Paper: filter matrix (4 different denominators across kill_switch /
  EV slicers / evaluator — evaluator excludes every resolver-resolved row);
  archived>14d rows permanently unfalsifiable at pnl=0; resolution-within-
  window survivorship unlocks `_is_proven_slice` which bypasses
  EXCLUDE_SIGNAL_TYPES for killed signals; ex-top-3 trimming applied nowhere
  that matters.
- Bankroll denominators frozen at process start from a cache that goes stale
  (kill_switch %, Kelly, MAX_TRADE_USD all share it).
- `db()` transaction manager exists but money paths use raw autocommit
  connections; executescript() is a silent no-op under PG (schema drift trap).
- Scheduler: task "dependencies" are comments; state.json non-atomic; daily
  failures effectively silent for days (mute ladder); reconciler drift = task
  failure (alert conflation); exit-2 log currently shows real drift.

## D. Medium/Low themes (see agent outputs for full lists)

Slippage column mixes units and inverts sign on SELL; MFE dollars vs pct
mismatch breaks trail ladders on partial fills; `exact_shares` int-truncation
books PnL on unsold fractions; api_health "24h" counters are lifetime;
tombstones clobber cached uma_status (hurts the counterfactual); duplicate
dict keys silently override floors (specialist 0.30→0.10); `_aggression`
computed and never used; ~60-90 HTTP round-trips per monitor cycle (fresh
authed client per balance call, full-wallet positions fetch per dust row);
four independent data-api pollers with four watermarks (~2k calls/h
overlapping); parquet bars fabricate volume; hourly logs grow unbounded and
monitor_alerts loads whole files; SQLite backups stopped 5/03 while five
.CORRUPTED_ siblings sit on disk; two conflicting TRADING_MODE lines in .env;
Docker Desktop doesn't self-start on Windows boot.

## E. Honest note on this week's fixes

Three of the audited defects were introduced or amplified by the 7/5–7/6
repair work: the fill backfill's price-space/double-count flaws (C2), the
calibration fallback closing the feedback loop harder (C3), and the
`-more-markets` tombstone clobbering uma_status. The repairs fixed real bugs
but inherited the two root causes (A1 no canonical price space, A2 no raw
confidence) — supporting the audit's core conclusion that these must be fixed
at the schema level, not per-call-site.

## F. Remediation order (highest leverage first)

1. **Choose ONE price space** (store fill_price in held-token space + add
   `fill_price_space` column or normalize everything to YES) and sweep all
   ~10 read/write sites; recompute SELL-side ledger from data-api fills.
2. **Persist `confidence_raw`** in both trade tables; fit all curves ONLY on
   raw; move boosts to a separate `size_mult` column; add curve holdout +
   age check.
3. **One `market_resolutions` table**, single writer (UMA-checked), all six
   consumers pointed at it; fix fetcher paging (descending/watermark);
   RETURNING-id support in the wrapper (un-severs the loops).
4. **Serialize the ledger**: `exit_ts IS NULL` guards everywhere, per-row
   advisory locks or single-writer booking service, monitor process lock,
   dedicated container + sane timeout for the exit monitor.
5. **Security/ops day**: rotate+scrub Kalshi key, arm heartbeat, auth or
   localhost-bind the API, strip private key from non-trading containers,
   scope kill-switch evidence (drop stale-backtest blending).
6. Delete or quarantine `scripts/reconcile_positions.py` and the other
   unguarded repair scripts.
7. Paper-fidelity minimums: current-price fills, depth at final stake,
   provenance flag on discovery rows, resolve-don't-archive.
