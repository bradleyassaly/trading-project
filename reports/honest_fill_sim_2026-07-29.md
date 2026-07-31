# Honest fill simulation — 2026-07-29

Cohort: closed YES-side paper trades since 2026-07-23T18:18:01+00:00 (6d tape-complete window), entry patience 300s.

Entry honest ⇔ TAKER grade (fire-time book: claimed ≥ best ask AND ask depth ≥ stake; `taker` column = taker-filled/book-covered, coverage accrues from the 2026-07-28 capture deploy) OR RESTING grade (tape printed ≤ our claimed entry price at our size within the window). TP honest ⇔ the tape printed ≥ the claimed TP price before resolution; otherwise the trade rides to the resolution payout. Stops/expiries keep paper booking (honest EV is an UPPER bound for stop-heavy signals).

| signal | n | entry fill | taker | TP honest | unresolved | paper P&L | honest P&L | paper EV/$ | honest EV/$ |
|---|---|---|---|---|---|---|---|---|---|
| resolution_decay | 245 | 4/245 | 0/0 | 0/0 | 0 | $+15.18 | $-8.31 | +0.008 | -0.519 |
| synthetic | 1 | 0/1 | 0/0 | 0/0 | 0 | $-0.43 | $+0.00 | -0.860 | +0.000 |
| confluence_2plus | 4 | 1/4 | 0/0 | 0/0 | 0 | $-12.65 | $-2.01 | -0.515 | -0.402 |
