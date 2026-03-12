# Polymarket Structure Arb Bot (Shadow Paper Run v10)

Paper-only bot for Polymarket YES/NO complement arbitrage validation.

## Not Live-Ready

- Live order execution is not implemented.
- Auth/signing/balance/cancel-replace are not implemented.
- Use only for paper/shadow-paper diagnostics.

## v10 Goal

v10 focuses on reducing **data-readiness friction** so shadow runs can approach strategy-evaluable quality.

Key targets:

- suppress market universe churn
- add probation for newly watched markets/assets
- add market-level readiness gate before strategy evaluation
- reduce noisy `book_not_ready`, `quote_too_old`, `book_recovering`
- reduce unnecessary market/asset block events

## Core v10 Changes

### 1. Market Universe Hysteresis

- Existing watched markets are preferred during top-N selection.
- New fields:
  - `market_filters.prefer_existing_watched_markets`
  - `market_filters.existing_market_hysteresis_score_ratio`
  - `market_filters.max_market_replacements_per_refresh`
- Refresh-side hysteresis:
  - dynamic confirmation is stricter when current universe is healthy
  - large rotations are skipped when healthy and non-forced
- `market_universe_changed` now records richer details (`added/removed assets/markets`, reason).

### 2. Probation for Newly Watched Markets

- Newly added markets enter probation (`runtime.market_probation_ms`).
- Early exit from probation is allowed when readiness threshold is met:
  - `runtime.market_probation_min_quote_updates_per_asset`
  - `runtime.market_probation_min_ready_asset_ratio`
- During probation:
  - strategy signal evaluation is skipped
  - no-signal reason uses `market_probation`
  - missing-book path uses `asset_warming_up`
  - stale/block escalation is softened

### 3. Market-Level Readiness Gate

Before strategy evaluation, each market must pass readiness:

- market not in probation
- both legs book-ready
- quote freshness acceptable
- recovering-state checks

No-signal reasons are now better separated:

- readiness-side: `market_probation`, `market_not_ready`, `market_quote_stale`, `asset_warming_up`
- strategy-side: `edge_below_threshold`, `spread_too_wide`, `depth_too_low`, etc.

### 4. Smoother Blocking

- Added asset block consecutive-cycle gate:
  `runtime.asset_block_min_consecutive_unhealthy_cycles`
- Existing market block consecutive-cycle gate remains:
  `runtime.market_block_min_consecutive_unhealthy_cycles`
- Recovering/probation assets are excluded from aggressive stale/block escalation.

### 5. Report and Dashboard Readiness Metrics

Daily report totals now include readiness-friction breakdown:

- `market_universe_changed_count`
- `book_not_ready_count`
- `quote_too_old_count`
- `book_recovering_count`
- `market_not_ready_count`
- `market_probation_count`
- `market_quote_stale_count`
- `asset_warming_up_count`

Dashboard overview adds the same readiness counters and a focused chart:

- `edge_below_threshold`
- `book_not_ready`
- `quote_too_old` / `market_quote_stale`
- `book_recovering`
- `market_not_ready`
- `market_probation`

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
- high `market_universe_changed_count`
- readiness no-signal dominance (`book_not_ready`, `market_not_ready`, `market_probation`, `book_recovering`)
- frequent market/asset blocks dominating no-signal reasons

Treat run as **close to strategy-evaluable** when:

- watched universe is stable (low churn, low cumulative drift)
- readiness-side no-signal reasons trend down materially
- block events are intermittent and clear quickly
- no-signal distribution is mostly strategy-side (`edge_below_threshold`)

## Quality Checks

```powershell
python -m pytest -q
python -m ruff check .
python -m black --check .
```
