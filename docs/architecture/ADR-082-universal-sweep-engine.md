# ADR-082 — Universal Sweep Engine (Prompt A: Shell)

**Date:** 2026-05-30
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)

---

## Context

CandleLab maintained three near-duplicate sweep scripts:

- `scripts/sweep_validation_ftmo_m30.py`
- `scripts/sweep_validation_ftmo_h1.py`
- `scripts/sweep_validation_ftmo_m15.py`

M30 accumulated Phase 2/3 features (IS Gate v2, IS cache, `sweep_leaderboard`). H1 and M15 diverged only in timeframe constants (`GRANULARITY`, window sizes, gaps, timeouts, output paths) while sharing identical signal detection and WFV logic.

Maintaining three files guaranteed drift and blocked unified deployment.

---

## Decision

**Prompt A:** Rename M30 to a single universal engine and introduce config-driven timeframe selection.

| Action | Detail |
|--------|--------|
| Rename | `sweep_validation_ftmo_m30.py` → `sweep_validation_engine.py` |
| Remove | `git rm` legacy `sweep_validation_ftmo_h1.py`, `sweep_validation_ftmo_m15.py` |
| Config | `TIMEFRAME_CONFIGS` dict — M30 and H1 keys |
| CLI | `--tf M30\|H1` via `_parse_args()` with `parse_known_args()` |
| Bridge | `main()` sets module-level globals from selected cfg |

M15 is not in `TIMEFRAME_CONFIGS` yet; can be added as a third key in a follow-up.

---

## TIMEFRAME_CONFIGS as single source of truth

Each timeframe key holds:

- Window geometry: `is_weeks`, `oos_weeks`, `n_windows`
- SQN gates: `sqn_min_trades_is`, `sqn_min_trades`, `sqn_promote_threshold`
- Simulation defaults: `timeout_bars_default`, `atr_period`, `ma_fast`, `ma_slow`
- IS Gate v2: `min_trades_eligible`, `min_trades_watchlist`
- Pattern axis: `continuation_gaps`, `dead_zone_hours`
- Outputs: `output_dir`, `roster_file`
- Per-pair sweep: `pair_timeouts` (overrides `PAIR_CONFIG`)

H1 example differences from M30: 16-week IS, gaps `[5,10,15]`, timeouts `[10,20,30,48]`, lower SQN floors.

---

## Temporary global bridge (Prompt A only)

`main()` resolves `cfg = TIMEFRAME_CONFIGS[_tf]` and assigns 16 module-level globals before any sweep work.

**Why safe for Prompt A:**

- All entry is through `main()` → `if __name__ == "__main__"` path parses CLI first.
- Import-as-module defaults to M30 without parsing argv (`_tf = "M30"`).
- No function signatures changed; helpers still read globals — behavior identical to pre-refactor for default M30 run.
- `parse_known_args()` avoids breaking Jupyter or wrapper scripts that pass extra flags.

**Prompt B** threads `cfg` through function signatures. Module-level globals remain as import-time defaults and `main()` bridge fallbacks (`cfg[...] if cfg else GLOBAL`).

---

## Prompt B — cfg threading (completed)

All sweep helpers accept optional `cfg: dict | None = None`. When `cfg` is passed, reads use `cfg[...]`; when `None`, fall back to module-level constants (supports import without running `main()`).

| Function | cfg keys used |
|----------|---------------|
| `build_combo_list(cfg)` | `continuation_gaps` |
| `detect_signals(..., cfg=cfg)` | `dead_zone_hours` |
| `compute_atr(..., cfg=cfg)` | `atr_period` |
| `compute_indicators(..., cfg=cfg)` | `ma_fast`, `ma_slow` |
| `_cache_store(..., cfg=cfg)` | `granularity` |
| `_write_is_results(..., cfg=cfg)` | `granularity` |
| `fetch_instrument_data(..., cfg=cfg)` | `granularity` |
| `compute_shadow_status(..., cfg=cfg)` | `sqn_min_trades` |
| `run_wfv(..., cfg=cfg)` | `n_windows`, `oos_weeks`, `is_weeks`, `ma_fast`, `ma_slow`, `min_trades_eligible`, `granularity`; passes `cfg` to all helpers |
| `write_leaderboard_csv(..., cfg=cfg)` | `output_dir` |
| `write_paper_roster(..., cfg=cfg)` | `output_dir` |
| `_write_leaderboard(..., cfg=cfg)` | `granularity` |

`main()` passes `cfg=cfg` to all top-level call sites. Global bridge assignments in `main()` are **unchanged** (backward compat for any code still reading module globals).

**Verification:** No bare global reads inside function bodies except `cfg`-guard fallbacks, `def` default args, docstrings, and `main()`.

---

## Dependency injection (Prompt B — completed)

`cfg` is now threaded through all functions listed above. Global bridge in `main()` retained for module-level constants used at import time (`_os.makedirs(OUTPUT_DIR)` etc.).

Future Prompt C may remove the global bridge entirely once import-time side effects are eliminated.

---

## Negative space — Prompt B does NOT

- Remove `main()` global bridge (retained)
- Change business logic or `TIMEFRAME_CONFIGS`
- Touch `_make_block_hash`, Postgres schemas, external repos

---

## Negative space — Prompt A did NOT

`PAIR_CONFIG` is initialized at module load with M30 timeouts `[20, 40, 60, 96]` on every pair. At runtime:

```python
pair_cfg.get("timeouts", [TIMEOUT_BARS])
```

**Never uses the fallback** when `"timeouts"` key exists — H1 would silently keep M30 values.

**Fix:** After setting globals, explicitly overwrite:

```python
for pair in PAIR_CONFIG:
    PAIR_CONFIG[pair]["timeouts"] = cfg["pair_timeouts"]
```

---

## PAIR_CONFIG timeout override

## git rm of legacy files

| Removed | Reason |
|---------|--------|
| `sweep_validation_ftmo_h1.py` | Superseded by `--tf H1` |
| `sweep_validation_ftmo_m15.py` | Superseded when M15 key added; file was stale duplicate |

Canonical engine: `scripts/sweep_validation_engine.py`.

---

## Negative space — Prompt A does NOT

- Thread `cfg` through function signatures (Prompt B)
- Change `run_wfv` logic or signatures
- Replace in-function `GRANULARITY` reads with `cfg[...]`
- Add M15 to `TIMEFRAME_CONFIGS`
- Touch Postgres schemas (`sweep_oos_cache`, `sweep_is_results`, `sweep_leaderboard`)
- Touch `candlelab-v2`, `pipshed`, `fx_candles`

---

## Usage

```bash
python scripts/sweep_validation_engine.py           # default M30
python scripts/sweep_validation_engine.py --tf H1
```

---

## Related

- ADR-078 — IS Gate v2
- ADR-080 — IS bulk cache
- ADR-081 — sweep_leaderboard
