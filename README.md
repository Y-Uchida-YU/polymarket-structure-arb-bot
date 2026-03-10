# Polymarket Structure Arb Bot (Shadow Paper Run v7)

This repository runs a **paper-only** complement YES/NO structure strategy for Polymarket.

## Important: Still Not Live-Ready

- Live order routing is not implemented.
- Auth/signing, balances, and cancel-replace are not implemented.
- Use `--paper` or `--shadow-paper` only.

## v7 Goal

v7 is focused on observability, stability, and accounting validity.

- Do not loosen thresholds just to increase signal count.
- Reduce false global safe mode blocks.
- Reduce missing book state noise.
- Make no-signal reasons and universe size metrics reliable enough for strategy evaluation.

## What Changed in v7

- Safe mode is now reasoned and scoped:
  - global reasons: `all_assets_stale`, `ws_unhealthy`, `resync_storm`, `exception_rate_exceeded`, `book_state_unhealthy`
  - asset-scope blocking is used for partial unhealthy assets
- Missing book state is classified:
  - `book_not_ready`
  - `no_initial_book`
  - `book_evicted`
  - `book_not_resynced_yet`
  - `quote_missing_after_resync`
- Market readiness is explicit:
  - an asset is ready only when both bid and ask exist
  - a market is ready only when both YES and NO assets are ready
- Resync suppression was tightened:
  - per-asset cooldown
  - same-reason cooldown
  - reason-specific cooldowns for stale and missing book state
  - stale resync is skipped for assets that are not ready yet
- Reporting now includes:
  - `safe_mode_reasons`
  - `missing_book_state_reasons`
  - current/cumulative universe counts
  - stronger warnings for data quality

## Universe Metrics Definitions

- `current_watched_markets`:
  Number of markets currently watched after filtering.
- `cumulative_watched_markets`:
  Unique watched markets seen during the run window.
- `current_subscribed_assets`:
  Number of assets currently subscribed (typically 2 per binary market).
- `cumulative_subscribed_assets`:
  Unique subscribed assets seen during the run window.

Startup logs and report output use the same meaning:

- `watched_markets(current/cumulative)`
- `subscribed_assets(current/cumulative)`

## How to Read Safe Mode / Missing Book / Resync

- `safe_mode_blocked_global` high:
  Usually a run-quality issue. Check `safe_mode_reasons` first.
- `safe_mode_blocked_asset` high:
  Partial universe quality issue, not necessarily full-run failure.
- `missing_book_state_reason:no_initial_book` high:
  New subscriptions are not getting initial books quickly enough.
- `missing_book_state_reason:quote_missing_after_resync` high:
  Resync is not restoring quotes reliably for some assets.
- `resyncs_by_reason:stale_asset` high:
  Freshness is unstable or resync suppression is too weak.

## Warning Meanings

- `no_signals_in_window`: no signals in the report window.
- `safe_mode_triggered`: global safe mode or safe-mode no-signal blocks occurred.
- `safe_mode_dominates_run`: safe-mode no-signal reasons dominate no-signal accounting.
- `no_signals_but_high_resync`: no signals but frequent resync activity.
- `book_state_unhealthy`: no-signal reasons indicate book readiness problems.
- `data_not_ready_for_evaluation`: run likely invalid for strategy evaluation due to data readiness/safe-mode domination.
- `universe_too_large`: current universe may be too large for stable monitoring.
- `resync_storm_detected`: excessive resync rate.

## Shadow Paper Run Validity Checklist

Treat the run as **evaluation-ready** only when most of the following hold:

- `safe_mode_blocked_global` is low and not dominant.
- `missing_book_state_reasons` are low, especially `no_initial_book` and `quote_missing_after_resync`.
- `stale_asset` resync reason is controlled and not dominating.
- Warnings do not include `data_not_ready_for_evaluation`.
- Universe size is intentional and stable (`current` counts are within expected ops limits).

Treat the run as **invalid for strategy evaluation** when one or more of these are true:

- Global safe mode dominates no-signal accounting.
- Missing book state reasons dominate no-signal accounting.
- Frequent resync storms mask normal strategy behavior.
- Universe is too large for current data pipeline capacity.

## Quick Start (Windows 11)

```powershell
cd C:\MyProjects\polymarket-structure-arb-bot
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -U pip
pip install -e .[dev]
copy .env.example .env
```

Validate config:

```powershell
python -m src.main run --validate-config --settings-path config/settings.shadow.yaml
```

Run shadow paper:

```powershell
python -m src.main run --shadow-paper
```

Generate report:

```powershell
python -m src.main report --last-hours 1
python -m src.main report --last-hours 24
python -m src.main report --last-hours 24 --run-id <run_id>
```

## Config Files

- Base: `config/settings.yaml`
- Shadow paper profile: `config/settings.shadow.yaml`

## Quality Checks

```powershell
pytest -q
ruff check .
black --check .
```