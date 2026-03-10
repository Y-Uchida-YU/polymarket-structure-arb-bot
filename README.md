# Polymarket Structure Arb Bot (Shadow Paper Run v8)

This repository provides a **paper-only** Polymarket YES/NO complement structure bot for long-running shadow evaluation.

## Important: Still Not Live-Ready

- Live order routing is not implemented.
- CLOB auth/signing (L1/L2), balances, and cancel-replace are not implemented.
- Use for `paper` / `shadow-paper` validation only.

## v8 Focus

v8 prioritizes **observability and data-readiness** over trade frequency.

- Reduce over-triggered global safe mode.
- Reduce `book_missing_after_resync`-type no-signal failures.
- Keep unhealthy scope localized (`asset` / `market`) whenever possible.
- Add a read-only local dashboard for run diagnosis.

## Core Behavior

### Safe mode scope

- `scope=asset`: only unhealthy assets are blocked.
- `scope=market`: market blocked when both legs are unhealthy.
- `scope=global`: enabled only when unhealthy conditions persist and cross configured global thresholds.

Global scope now uses persistence gates:

- `guardrails.global_unhealthy_consecutive_count`
- `guardrails.global_unhealthy_min_duration_seconds`
- `guardrails.global_unhealthy_min_asset_ratio`
- `guardrails.global_ws_unhealthy_min_asset_ratio`

### Book recovery and warm-up

- After startup/resubscribe/resync, assets can be treated as `book_recovering`.
- Recovery grace uses `runtime.resync_recovery_grace_ms`.
- During warm-up/recovery, no-signal accounting records readiness reasons instead of over-counting hard missing-book states.

### Universe control

Shadow runs should stay intentionally small and stable.

- `market_filters.max_markets_to_watch`
- `market_filters.require_orderbook_enabled`
- liquidity/activity proxies and expiry filters in `market_filters`

## Metric Definitions (Report and Dashboard)

The CLI report (`python -m src.main report ...`) and dashboard use the same definitions:

- `total_signals`: rows in `signals` within window (`detected_at`)
- `total_fills`: rows in `fills` within window (`filled_at`)
- `fill_rate`: distinct `fills.signal_id` / `total_signals`
- `matched_fill_rate`: `execution_events.matched_qty > 0` / `total_signals`
- `safe_mode_count`: metric count where `metric_name = safe_mode_entered`
- `resync_count`: rows in `resync_events` within window
- `watched_markets current/cumulative`: latest universe metrics in `metrics`
- `subscribed_assets current/cumulative`: latest universe metrics in `metrics`
- `safe_mode_scope_reasons`: parsed from `metrics.details` on safe-mode metrics
- `no_signal_reasons`: `metrics` where `metric_name like no_signal_reason:%`
- `missing_book_state_reasons`: `metrics` where `metric_name like missing_book_state_reason:%`

## Quick Start (Windows 11)

```powershell
cd C:\MyProjects\polymarket-structure-arb-bot
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -e .[dev,dashboard]
copy .env.example .env
```

### Validate config

```powershell
python -m src.main run --validate-config --settings-path config/settings.shadow.yaml
```

### Start shadow paper run

```powershell
python -m src.main run --shadow-paper
```

### Generate report

```powershell
python -m src.main report --last-hours 1
python -m src.main report --last-hours 24
python -m src.main report --last-hours 24 --run-id <run_id>
```

### Launch local dashboard (read-only)

```powershell
python -m streamlit run src/dashboard_app.py -- --db-path data/state/state.db
```

## Dashboard Views

- `Overview`: signals/fills/fill rates/safe mode/resync/pnl/universe/warm-up
- `Run Detail`: per-run summary (`run_id`, uptime, counts, warnings)
- `Diagnostics`: top resync reasons, safe-mode scope reasons, no-signal reasons, missing-book reasons
- `PnL / Inventory`: projected PnL and unmatched inventory time series
- `Market / Asset View`: market-level signal/fill/pnl diagnostics and asset-level resync trends

Dashboard is local/read-only and does not modify SQLite.

## Output Locations

- Logs: `data/logs/`
- SQLite state DB: `data/state/state.db`
- CSV/JSON exports: `data/exports/`

## Shadow Evaluation Guidance

Treat run as **not ready for strategy evaluation** when:

- `safe_mode_blocked_global` dominates no-signal reasons
- missing-book reasons dominate (`no_initial_book`, `book_not_resynced_yet`, `quote_missing_after_resync`)
- `resync_storm_detected` appears repeatedly
- report warning includes `data_not_ready_for_evaluation`

Treat run as **evaluation-ready** when:

- global safe mode is rare and short-lived
- no-signal reasons are mostly strategy-side (`edge_below_threshold`, quality guards) instead of data-readiness failures
- resync reason mix is stable and non-storm
- universe size remains within intended shadow limits

## Config Profiles

- Base: `config/settings.yaml`
- Shadow-paper profile: `config/settings.shadow.yaml`

## Quality Checks

```powershell
python -m pytest -q
python -m ruff check .
python -m black --check .
```

