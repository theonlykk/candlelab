# ADR-020: Wizard Continuation Mode (Option B) — Phase 3A Backend

## Status

Accepted

## Date

2026-05-16

## Context

The CandleLab wizard is moving to **Option B** continuation mode: traders can select up to two continuation patterns (e.g. Inside Bar Breakout, 1-Candle Flag) in addition to the existing Three Soldiers/Crows path. The SPA changes are deferred to Phase 3B (`templates/index.html`). Phase 3A must enforce server-side rules, extend backtest plumbing, and expose continuation patterns via existing APIs without modifying `candlelab_core` or installed packages.

Prior behaviour accepted only a single continuation string on `/api/finalise`, mapped one column in `detect_signal`, and listed only Three Soldiers/Crows under `_TREND_PATTERNS`. Multi-pattern continuation with OR semantics and a spatial MA alignment filter (`ma_alignment`) were not available on the Flask backtest path.

## Decision

1. **Activate continuation pattern names** in `_TREND_PATTERNS`: add `Inside Bar Breakout` and `1-Candle Flag` (keys already exist in `PATTERNS` from `candlelab_core`).

2. **Add `check_ma_alignment`** in `app.py` (not in `candlelab_core`): SMA10/SMA50 hierarchy on bid/ask close when present; returns `False` during warmup or invalid index without raising.

3. **Extend `_make_indicator_fn`** with `type: "ma_alignment"` → `check_ma_alignment`.

4. **Extend `_backtest_pattern.continuation`** to `str | list | None`:
   - Normalise single string to a one-element list internally.
   - Deduplicate and filter to columns present in `signals_df`.
   - One pattern: pass column name to `detect_signal`.
   - Two+ patterns: OR-combine into `__continuation_combined__` on a copied `signals_df` (confluence bars included).

5. **Harden `api_finalise`**:
   - Parse `continuation` as list or string; empty list → `None`.
   - Reject more than two continuation patterns with HTTP 400.
   - Read `mode` from body as `strategy_mode` (default `"reversal"`) for Phase 3B; no persistence or routing change in 3A.

Complement / connector / `pattern_2` derivation in `api_finalise` is unchanged per scope.

## Options considered

| Option | Summary | Why not (or why chosen) |
|--------|---------|-------------------------|
| **A — Frontend-only** | Send combined pattern name from wizard | Rejected: server must enforce max-2 and OR semantics; client cannot be trusted. |
| **B — Backend list + OR (chosen)** | `continuation` as list in JSON; combine in `_backtest_pattern` | Chosen: matches wizard UX, backward compatible with single string. |
| **C — Modify `candlelab_core`** | Move MA alignment and multi-continuation into package | Rejected for 3A scope: no installed-package changes. |
| **D — Postgres `mode` column** | Persist reversal vs continuation mode | Deferred: mode inferred from anchor null in later work; 3A only reads body field. |

## Consequences

### Positive

- `/api/patterns?strategy_type=continuation` returns Inside Bar Breakout and 1-Candle Flag alongside Three Soldiers/Crows.
- `/api/finalise` accepts `continuation: ["Inside Bar Breakout", "1-Candle Flag"]` with server-side dedup and max-2 validation.
- Legacy single-string `continuation` continues to work.
- `indicator_filter` with `{"type": "ma_alignment"}` is evaluable on the wizard backtest path.

### Negative / risks

- **`strategy_mode` is read but unused in 3A** — intentional placeholder for 3B; linters may flag unused variable until wired.
- **Complement fallback unchanged** — if only `continuation` is a list and no `pattern_2`, `complement` may still receive the list object in existing connector logic; 3B payload shape should align with Type 4 / reversal flows.
- **OR combination copies `signals_df`** — small extra memory per backtest when two continuations are used; acceptable for 30-day wizard windows.
- **Unknown pattern names in list** — silently dropped when not in `signals_df.columns` (no 400); documented failure mode.

### Verification (Phase 3A)

1. `POST /api/finalise` with `continuation: ["Inside Bar Breakout", "1-Candle Flag"]` → 200 with backtest results.
2. Three items in `continuation` → 400 `Maximum 2 continuation patterns allowed`.
3. Single string `continuation: "Three Soldiers/Crows"` → unchanged behaviour.
4. Duplicate entries in list → deduplicated server-side.
5. `GET /api/patterns?strategy_type=continuation` → includes new trend patterns.

## Files changed (Phase 3A)

- `app.py` — `_TREND_PATTERNS`, `check_ma_alignment`, `_make_indicator_fn`, `_backtest_pattern`, `api_finalise`
- `docs/architecture/ADR-020-wizard-continuation-mode.md` — this document

Not changed: `templates/index.html`, `sweep_validation.py`, `simulation_engine.py`, `strategy_runner.py`, `candlelab_core`.
