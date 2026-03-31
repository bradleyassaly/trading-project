# Multi-Frequency Alignment

The repository now includes a shared alignment helper for joining slower and faster market-data series without introducing forward-looking leakage.

## Supported Modes

### `event`
Use this when the right-hand series timestamp represents the moment the value became known. Alignment is a backward `merge_asof`, so each left-side row only sees right-side values at or before its timestamp.

### `period_end_effective_next`
Use this when the right-hand series is a bar or aggregate that should only become usable after that bar has fully completed. This is the safe default for daily-to-intraday alignment when daily bars represent end-of-day information.

In this mode, the right-side row becomes effective on the next observed right-side timestamp for the same symbol. That means an intraday row on `2025-01-02 10:00` will not see the `2025-01-02` daily close bar until the next daily period begins.

## Safety Rules

- Only backward alignment is supported.
- The helper requires explicit timestamp-mode selection.
- Exact-match behavior is configurable, but forward-looking directions are rejected.

## Current Scope

This utility is additive and does not yet replace existing subsystem-specific alignment code automatically. It is intended as the shared foundation for future ingestion, feature-store, and replay workflows that need mixed-frequency joins.
