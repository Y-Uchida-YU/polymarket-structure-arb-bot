# Polymarket Structure Arb Bot (Shadow Paper Run v13)

Paper-only bot for Polymarket YES/NO complement arbitrage validation.

## Not Live-Ready

- Live order execution is not implemented.
- Auth/signing/balance/cancel-replace are not implemented.
- Use only for paper/shadow-paper diagnostics.

## v13 Goal

v13 focuses on keeping a **stable watched universe** while preserving strict readiness/eligibility gates.

Key targets:

- avoid `eligible_market_count = 0` lock-in
- prevent watched markets from collapsing to very small counts
- separate `watched`, `ready`, and `eligible` concepts
- keep low-quality penalties, but avoid self-destruction by staged exclusion
- keep strategy thresholds strict (no signal inflation by threshold loosening)

## Core v13 Changes

### Definitions

- `watched market`: monitored in runtime universe (can be warming/recovering).
- `ready market`: both legs have usable book state for freshness/readiness.
- `eligible market`: ready market that also passes eligibility gates (update count, freshness state).

### 1. Freshness State Machine

- Market freshness/readiness states are explicit:
  - `ready`
  - `not_ready`
  - `probation`
  - `recovering`
  - `stale_no_recent_quote`
  - `stale_quote_age`
- Strategy evaluation runs only after readiness + eligibility pass.
- `market_quote_stale*` now focuses on truly stale states, not probation/recovering side-effects.

### 2. Signal Eligibility Gate

- Added pre-strategy eligibility requirements:
  - both legs ready
  - minimum quote update count per leg (`runtime.market_eligibility_min_quote_updates_per_asset`)
  - market freshness state must be `ready`
- Ineligible markets are logged as readiness-side reasons (for example `book_not_ready_insufficient_updates`).

### 3. No-Signal Spam Suppression

- Repeated identical no-signal reasons per market are rate-limited by:
  - `runtime.market_no_signal_reason_cooldown_ms`
- This prevents high-frequency duplicate reason inflation while preserving first-hit diagnostics.

### 4. Stale/Resync Loop Suppression

- Stale-asset resync now skips probation/recovering/not-ready assets.
- Added stronger stale resync cooldown knobs:
  - `runtime.stale_asset_resync_additional_cooldown_ms`
  - `runtime.stale_asset_resync_ready_ratio_min`
- Recovering assets have max dwell via:
  - `runtime.market_recovering_max_ms`

### 5. Watched Floor + Selection/Eligibility Separation

- Runtime can keep a minimum watched universe:
  - `runtime.min_watched_markets_floor`
- If watched count falls below floor, refresh can backfill watched candidates while keeping strategy eligibility strict.
- `watched` does not imply `eligible`; not-ready markets can remain watched and continue warming/recovering.

### 6. Staged Low-Quality Penalty (Healthy -> Degraded -> Candidate -> Excluded)

- Low-quality penalty now uses staged progression instead of one-shot ejection.
- Exclusion requires sustained deterioration:
  - `runtime.low_quality_market_exclusion_consecutive_cycles`
- Stage thresholds are configurable:
  - `runtime.low_quality_market_degraded_penalty_ratio`
  - `runtime.low_quality_market_probation_penalty_ratio`
  - `runtime.low_quality_market_exclusion_candidate_penalty_ratio`
- Runtime exclusion can be relaxed while watched universe is below floor:
  - `runtime.watched_floor_relax_runtime_exclusion`
  - `runtime.watched_floor_relax_activity_filters`

### 7. Summary Count vs Reason Aggregate Alignment

- `market_universe_change_events`:
  - explicit universe-change events (metric event count).
- `resync_due_to_universe_change`:
  - resync event count whose reason is `market_universe_changed` (often per-asset).
- `ws_connected_events` / `ws_reconnect_events`: explicit websocket event counts.
- This avoids confusion such as:
  - one universe-change event causing many per-asset resync rows.

### 8. Dashboard / Report Additions

Daily report/dashboard now include market-state readiness counters:

- `watched_market_count_current`
- `ready_market_count`
- `recovering_market_count`
- `stale_market_count`
- `eligible_market_count`
- `blocked_market_count`
- `ready_market_ratio`
- `eligible_market_ratio`
- `min_watched_markets_floor`
- `low_quality_runtime_excluded_count`

`no_eligible_markets` now exposes cause hints:

- `watched_too_small`
- `all_markets_recovering`
- `all_markets_not_ready`
- `all_markets_stale`
- `quality_penalty_excessive`

Dashboard diagnostics keep strategy/readiness/transport grouping and remain read-only.

## CLI

### Shadow paper run

```powershell
python -m src.main run --shadow-paper
```

### Report

```powershell
python -m src.main report --last-hours 24
python -m src.main report --date 2026-03-12
python -m src.main report --last-hours 24 --run-id <run_id>
```

`--last-hours` means "from now back N hours" (UTC storage window).

### Dashboard (read-only)

```powershell
python -m streamlit run src/dashboard_app.py -- --db-path data/state/state.db
```

Dashboard defaults to JST display and can switch to UTC.

## Windows 11 Quick Setup

```powershell
cd C:\MyProjects\polymarket-structure-arb-bot
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -e .[dev,dashboard]
copy .env.example .env
```

Validate config:

```powershell
python -m src.main run --validate-config --settings-path config/settings.shadow.yaml
```

## Files

- SQLite: `data/state/state.db`
- logs: `data/logs/`
- exports: `data/exports/`

## Strategy-Evaluable Criteria (Practical)

Treat run as **not strategy-evaluable** if one or more remain dominant:

- `data_not_ready_for_evaluation`
- `book_state_unhealthy`
- frequent `ws_reconnect_events` / websocket instability warning
- high `resync_due_to_universe_change` without matching `market_universe_change_events`
- readiness no-signal dominance (`book_not_ready`, `market_not_ready`, `market_probation`, `book_recovering`)
- frequent market/asset blocks dominating no-signal reasons
- very low `eligible_market_count` relative to watched markets

Treat run as **close to strategy-evaluable** when:

- watched universe is stable (low churn, low cumulative drift)
- transport/readiness no-signal reasons trend down materially
- `watched_market_count_current >= min_watched_markets_floor`
- `ready_market_ratio` and `eligible_market_ratio` remain stably positive
- block events are intermittent and clear quickly
- no-signal distribution is mostly strategy-side (`edge_below_threshold`)

## Quality Checks

```powershell
python -m pytest -q
python -m ruff check .
python -m black --check .
```
