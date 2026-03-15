# Polymarket Structure Arb Bot (Shadow Paper Run v12)

Paper-only bot for Polymarket YES/NO complement arbitrage validation.

## Not Live-Ready

- Live order execution is not implemented.
- Auth/signing/balance/cancel-replace are not implemented.
- Use only for paper/shadow-paper diagnostics.

## v12 Goal

v12 focuses on reducing **freshness/readiness friction** so shadow runs can approach strategy-evaluable quality.

Key targets:

- reduce noisy `market_quote_stale`, `book_not_ready`, `book_recovering`
- suppress `stale_asset -> resync -> recovering -> block` loops
- evaluate only truly signal-eligible markets/assets
- gradually drop low-quality markets from watched universe
- keep strategy threshold strict (no signal inflation by threshold loosening)

## Core v12 Changes

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

### 5. Runtime Low-Quality Market Exclusion

- Markets with sustained poor readiness quality accumulate penalty.
- Refresh can exclude those markets via runtime-only filter (`low_quality_runtime`).
- Key knobs:
  - `runtime.low_quality_market_penalty_threshold`
  - `runtime.low_quality_market_penalty_increment`
  - `runtime.low_quality_market_penalty_decay`
  - `runtime.low_quality_market_min_observations`

### 6. Summary Count vs Reason Aggregate Alignment

- `market_universe_change_events`:
  - explicit universe-change events (metric event count).
- `resync_due_to_universe_change`:
  - resync event count whose reason is `market_universe_changed` (often per-asset).
- `ws_connected_events` / `ws_reconnect_events`: explicit websocket event counts.
- This avoids confusion such as:
  - one universe-change event causing many per-asset resync rows.

### 7. Dashboard / Report Additions

Daily report/dashboard now include market-state readiness counters:

- `ready_market_count`
- `recovering_market_count`
- `stale_market_count`
- `eligible_market_count`
- `blocked_market_count`

No-signal reasons now distinguish readiness preconditions more explicitly (for example `book_not_ready_insufficient_updates`).

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
- `eligible_market_count` remains stably positive
- block events are intermittent and clear quickly
- no-signal distribution is mostly strategy-side (`edge_below_threshold`)

## Quality Checks

```powershell
python -m pytest -q
python -m ruff check .
python -m black --check .
```
