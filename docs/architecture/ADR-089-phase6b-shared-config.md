# ADR-089 — Phase 6b: Canonical PAIR_CONFIG (Shared Config)

**Date:** 2026-05-30
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)
**Approved by:** Gemini (Staff Architect)

---

## Context

`PAIR_CONFIG` (26 FX pairs with sweep parameters) was defined inline in `sweep_validation_engine.py`. Phase 6b live executor work requires the same pair universe in `market_state_daemon.py` and currency matrix precompute — duplicate definitions risk drift.

---

## Decision

Extract canonical pair config to `scripts/shared_config.py`:

- `PAIR_CONFIG` — 26 pairs with `ma_pairs`, `timeouts`, `directions`, `sl_mode`, `enabled`
- `ALL_PAIRS` — `sorted(PAIR_CONFIG.keys())` for incidence matrix construction

`sweep_validation_engine.py` imports from `shared_config` — inline dict removed.

Future consumers (`market_state_daemon.py`, oanda-trading in Part B) import the same module.

---

## Consequences

| Aspect | Detail |
|--------|--------|
| Single source of truth | Pair universe changes in one file |
| Alphabetical order | `ALL_PAIRS` guarantees consistent matrix row order |
| Runtime mutation | `main()` still overwrites `PAIR_CONFIG[pair]["timeouts"]` from `TIMEFRAME_CONFIGS` — shared dict is mutated in place (unchanged behavior) |

---

## Negative space (Part A)

- oanda-trading not touched
- No changes to `TIMEFRAME_CONFIGS`, sweep logic, or export scripts
