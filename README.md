# Polymarket Structure Arb Bot (Shadow Paper Run v9)

Paper-only bot for Polymarket YES/NO complement arbitrage validation.

## Not Live-Ready

- Live order execution is not implemented.
- Auth/signing/balance/cancel-replace are not implemented.
- Use only for shadow paper observation and diagnostics.

## v9 Focus

v9 improves readiness and observability without changing alpha policy:

- reduce noisy `market_universe_changed` and unnecessary resubscribe
- reduce stale/recovery resync loops
- refine market-level blocking behavior and reporting
- make dashboard JST-first and more visual for day-to-day operations

## Core v9 Changes

### 1. Market universe change debounce

- Universe comparison is set-based (order-insensitive).
- `market_universe_change_candidate` is recorded first.
- Resubscribe happens only when:
  - change is confirmed enough times (`runtime.market_universe_change_confirmations`), or
  - asset delta is large (`runtime.market_universe_change_min_asset_delta`).
- If current and target sets are equal, no resubscribe is requested.

### 2. Stale -> resync -> recovering loop suppression

- Added cross-reason no-data cooldown: `runtime.no_data_resync_cooldown_ms`.
- Added delayed missing classification after no-data resync:
  `runtime.quote_missing_after_resync_delay_ms`.
- Recovering assets are excluded from repeated stale/missing resync selection until ready.
- `recovering_asset_count` is tracked as a separate metric.

### 3. Market block handling refinement

- Market block can require consecutive unhealthy cycles:
  `runtime.market_block_min_consecutive_unhealthy_cycles`.
- Market block lifecycle metrics are emitted:
  - `safe_mode_market_block_started`
  - `safe_mode_market_block_active`
  - `safe_mode_market_block_cleared`

### 4. Report block metrics are explicit

Daily report totals now separate:

- `global_safe_mode_count`
- `market_block_count`
- `market_block_active_count`
- `market_block_cleared_count`
- `asset_block_count`
- `total_block_events`

Warnings are also explicit:

- `global_safe_mode_triggered`
- `market_blocks_triggered`
- `asset_blocks_triggered`
- `blocking_dominates_run`

## CLI and Window Semantics

### Shadow paper run

```powershell
python -m src.main run --shadow-paper
```

### Report

```powershell
python -m src.main report --last-hours 24
python -m src.main report --date 2026-03-10
python -m src.main report --last-hours 24 --run-id <run_id>
```

`--last-hours` means "from now back N hours" (UTC-based storage window).

### Dashboard (read-only)

```powershell
python -m streamlit run src/dashboard_app.py -- --db-path data/state/state.db
```

Dashboard defaults to JST display and allows UTC switch in sidebar.
Lookback mode uses the same "now - N hours" semantics as CLI report.

## Dashboard (JST-first)

### Main improvements

- timezone selector (default: `JST (Asia/Tokyo)`)
- datetime range filtering with timezone-aware conversion to UTC query window
- visual diagnostics:
  - resync count over time
  - block events over time
  - no-signal reasons stacked chart
  - missing-book reasons stacked chart
  - projected pnl/unmatched inventory over time
- readiness badge:
  - `NOT READY`
  - `PARTIALLY READY`
  - `EVALUATION READY`

## Windows 11 Quick Setup

```powershell
cd C:\MyProjects\polymarket-structure-arb-bot
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -e .[dev,dashboard]
copy .env.example .env
```

Validate:

```powershell
python -m src.main run --validate-config --settings-path config/settings.shadow.yaml
```

## Files and Outputs

- SQLite: `data/state/state.db`
- logs: `data/logs/`
- report exports: `data/exports/`

## Evaluation Guidance

Treat run as **not ready for strategy evaluation** when one or more are dominant:

- `no_signals_but_high_resync`
- `book_state_unhealthy`
- `data_not_ready_for_evaluation`
- high `market_block_count` / `asset_block_count` dominance

Treat run as **moving toward evaluation-ready** when:

- `market_universe_changed` is infrequent and meaningful
- `stale_asset` / `book_recovering` / `quote_missing_after_resync` trend down
- block events clear quickly and no longer dominate no-signal reasons
- no-signal reasons are mostly strategy-side (`edge_below_threshold`, spread/depth guards)

## Quality checks

```powershell
python -m pytest -q
python -m ruff check .
python -m black --check .
```
