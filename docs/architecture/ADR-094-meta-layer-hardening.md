# ADR-094 — Meta-Layer Hardening (Discovery)

**Date:** 2026-06-03  
**Status:** Accepted  
**Repo:** theonlykk/candlelab  
**Author:** Red Team + Gemini (review) / Cursor (implementation)  
**Target:** `scripts/meta_sweep.py`

---

## Context

The meta sweep discovery layer assigns `meta_status` (GREEN / RED / RED_SPIKE / RED_THIN) to promoted combos in `sweep_leaderboard`. Prior logic used a **relative** per-instrument NQS P70 threshold combined with an absolute floor — allowing weak universes to promote marginal neighbors via percentile ranking alone. Three locked changes harden against curve-fitting and execution fragility.

---

## Decision — Three Changes

### 1. Global absolute NQS gate (replaces relative P70)

**Removed:**
- `DISCOVERY_PERCENTILE` (70)
- `NQS_ABS_FLOOR`
- Per-instrument `nqs_p70 = np.percentile(...)`
- `threshold = max(nqs_p70, NPR_ABS_FLOOR)` hybrid logic

**Added:**
```python
NQS_GLOBAL_FLOOR = 0.30  # absolute global standard — no relative ranking
```

**GREEN gate:**
```python
if nqs >= NQS_GLOBAL_FLOOR and nqs > 0 and sqn_gap <= SPIKE_TOLERANCE:
    status = "GREEN"
```

**Why relative P70 was removed**

In a weak instrument universe, P70 can fall below any meaningful quality bar — e.g. if all neighbors have NQS ≈ 0.15, the 70th percentile is still ~0.15 and GREEN promotes combos that would fail on any absolute standard. Relative ranking rewards being "best of a bad bunch," which is curve-fitting at the meta layer, not edge discovery.

An absolute floor of 0.30 means every GREEN combo must demonstrate neighborhood mean R quality regardless of peer weakness.

### 2. Slippage Asymmetry Penalty (SAP) via timeout proxy

Short-timeout combos exit faster and are more exposed to stop-hunts and spread blowouts at session edges. Before NQS aggregation, each neighbor's `oos_mean_r` is discounted:

| `timeout_bars` | Discount |
|----------------|----------|
| ≤ 20 | 15% |
| ≤ 40 | 10% |
| ≤ 60 | 5% |
| > 60 | 0% |

```python
r_neighbor_adjusted = apply_sap(neighbor["oos_mean_r"], neighbor["timeout_bars"])
```

**ADR-096 deferral:** Full `sl_mult`-based SAP requires combo-space expansion; timeout proxy is sufficient for M30 discovery until ADR-096.

### 3. Cross-Validation Stability Penalty (CVSP)

Replaces simple mean of clipped neighbor returns:

```
NQS_raw = mean(max(0, r)) for r in neighbor_mean_rs   # SAP-adjusted
variance = var(neighbor_mean_rs, ddof=1)
CVSP = 1 + sqrt(variance)
NQS = NQS_raw / CVSP
```

| Neighborhood shape | CVSP | Effect |
|--------------------|------|--------|
| Low variance (stable plateau) | ≈ 1.0 | Minimal penalty |
| High variance (one spike neighbor) | > 1.0 | Heavy penalty |

Requires `MIN_NEIGHBORS = 2` for sample variance (`ddof=1`). Pipeline order: **SAP first → CVSP on adjusted list**.

---

## Negative space

This ADR does **not**:

- Modify `sweep_validation_engine.py`, `shared_config.py`, or `d:\oanda-trading\`.
- Change `sweep_leaderboard` schema.
- Change Hamming distance weights (`PARAM_WEIGHTS`).
- Change `RED_THIN` or `RED_SPIKE` classification paths.
- Change `min_windows_promoted` filter or `SPIKE_TOLERANCE`.
- Change `MIN_NEIGHBORS` (remains 2).
- Implement stability mode (still deferred per ADR-088).
- Remove `NPR_ABS_FLOOR` (legacy constant; no longer used in discovery threshold — may be removed in cleanup ADR).
- Implement full `sl_mult`-based SAP (ADR-096).

---

## Related

- ADR-088 — Meta sweep v2 (`MIN_NEIGHBORS = 2`)
- ADR-084 — Meta sweep discovery/stability modes
- ADR-095 — ATR-scaled timeouts (engine layer; timeout_bars values unchanged in leaderboard)
- ADR-096 — Full SAP via `sl_mult` (planned)
