# Polymarket Structure Arb Bot (Shadow Paper Run v11)

Paper-only bot for Polymarket YES/NO complement arbitrage validation.

## Not Live-Ready

- Live order execution is not implemented.
- Auth/signing/balance/cancel-replace are not implemented.
- Use only for paper/shadow-paper diagnostics.

## v11 Goal

v11 focuses on reducing **transport/readiness friction** so shadow runs can approach strategy-evaluable quality.

Key targets:

- reduce websocket reconnect/re-subscribe storms
- stabilize reconnect-after recovery
- reduce noisy `market_quote_stale`, `book_not_ready`, `book_recovering`
- align summary counts and reason aggregates for resync/universe-change
- keep strategy threshold strict (no signal inflation by threshold loosening)

## Core v11 Changes

### 1. Transport Stabilization (WS)

- WebSocket receive-timeout no longer triggers immediate reconnect on first timeout.
- Reconnect now requires consecutive timeout threshold:
  - `runtime.websocket_receive_timeout_seconds`
  - `runtime.websocket_receive_timeout_reconnect_count`
- Reconnect reasons are classified and logged (examples):
  - `idle_watchdog_reconnect`
  - `socket_closed_remote`
  - `socket_closed_local`
  - `subscribe_failure`
  - `auth_failure_placeholder`
  - `unknown_transport_error`
- Duplicate resubscribe is suppressed when asset set is unchanged.

### 2. Reconnect Recovery Grace

- After reconnect, the app enters connection recovery grace:
  - `runtime.reconnect_recovery_grace_ms`
  - `runtime.reconnect_recovery_min_ready_asset_ratio`
- During recovery grace:
  - stale/missing/block escalation is softened
  - aggressive missing/stale/idle resync loops are suppressed
  - no-signal can be classified as `connection_recovering` / `market_recovering`

### 3. `market_quote_stale` vs `quote_too_old`

- `quote_too_old`:
  - strategy-layer quote age reject (`quote_age_exceeded`) after readiness passed.
- `market_quote_stale*`:
  - readiness-layer stale states before strategy evaluation.
  - can be split as:
    - `market_quote_stale_no_recent_quote`
    - `market_quote_stale_recovery`
    - `market_quote_stale_quote_age`

This separation is intentional: strategy-quality stale and transport/readiness stale are different signals.

### 4. Summary Count vs Reason Aggregate Alignment

- `market_universe_change_events`:
  - explicit universe-change events (metric event count).
- `resync_due_to_universe_change`:
  - resync event count whose reason is `market_universe_changed` (often per-asset).
- `ws_connected_events` / `ws_reconnect_events`:
  - explicit websocket event counts.
- This avoids confusion such as:
  - one universe-change event causing many per-asset resync rows.

### 5. Dashboard / Report Additions

Daily report totals now include transport + readiness breakdown:

- `market_universe_changed_count`
- `market_universe_change_events`
- `resync_due_to_universe_change`
- `ws_connected_events`
- `ws_reconnect_events`
- `book_not_ready_count`
- `quote_too_old_count`
- `book_recovering_count`
- `market_not_ready_count`
- `market_probation_count`
- `market_quote_stale_count`
- `asset_warming_up_count`
- `connection_recovering_count`
- `market_recovering_count`

Dashboard overview/diagnostics add:

- WS counters (`ws_connected_events`, `ws_reconnect_events`)
- WS reason tables/charts (connected/reconnect reason breakdown)
- readiness counters (`book_not_ready`, `market_quote_stale*`, `book_recovering`, `no_initial_book`, `asset_warming_up`, `connection_recovering`, `market_recovering`)
- reason-group view (strategy vs readiness vs transport/other)

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

Treat run as **close to strategy-evaluable** when:

- watched universe is stable (low churn, low cumulative drift)
- transport/readiness no-signal reasons trend down materially
- block events are intermittent and clear quickly
- no-signal distribution is mostly strategy-side (`edge_below_threshold`)

## Quality Checks

```powershell
python -m pytest -q
python -m ruff check .
python -m black --check .
```
