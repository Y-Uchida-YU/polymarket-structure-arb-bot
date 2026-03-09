# Polymarket Structure Arb Bot (Paper Trading MVP v2)

This project monitors Polymarket binary markets and emits a paper-trading signal when:

`ask_yes + ask_no <= entry_threshold_sum_ask`

The bot is intentionally **paper trading only** in this phase. Live order placement is not implemented.

## Important Scope

- This version is focused on observation quality and safer paper execution.
- Live trading is out of scope in this phase.
- Existing layered architecture is preserved: strategy / execution / risk / storage.

## What It Can Do

- Fetch active and open markets from Gamma API.
- Filter only eligible binary markets with strict conditions:
  - `active == true`
  - `closed == false`
  - `archived == false`
  - exactly 2 outcomes
  - exactly 2 `clobTokenIds`
  - `enableOrderBook == true` (missing value is excluded)
- Map YES/NO token ids robustly from normalized outcome labels.
- Subscribe to CLOB market websocket and request custom feature stream with:
  - `custom_feature_enabled: true`
- Maintain latest best bid/ask state by `asset_id`.
- Detect structure-arb signals from latest yes/no asks.
- Run paper execution with basic realism guards:
  - reject stale quotes
  - reject if one side is missing
  - reject if ask size is missing/insufficient
- Persist events:
  - CSV: signal / order / fill / pnl / error
  - SQLite: markets / quotes / signals / orders / fills / pnl / errors
- Rotate app logs daily.
- Refresh market universe periodically via `market_refresh_minutes`.
- Trigger websocket resubscribe when subscribed asset ids change.

## What It Cannot Do (Yet)

- No live order placement.
- No CLOB authenticated flow (L1/L2 auth, POLY_* headers) yet.
- No real geoblock provider integration yet.
- No real balance checks / cancel-reprice loop / inventory management yet.

## Repository Layout

```text
polymarket-structure-arb-bot/
├─ README.md
├─ .env.example
├─ pyproject.toml
├─ config/
│  ├─ settings.yaml
│  └─ markets.yaml
├─ data/
│  ├─ logs/
│  ├─ state/
│  └─ exports/
├─ src/
│  ├─ main.py
│  ├─ app/
│  │  ├─ bootstrap.py
│  │  └─ scheduler.py
│  ├─ config/
│  │  └─ loader.py
│  ├─ clients/
│  │  ├─ gamma_client.py
│  │  ├─ clob_client.py
│  │  ├─ ws_client.py
│  │  └─ geoblock_client.py
│  ├─ domain/
│  │  ├─ market.py
│  │  ├─ order.py
│  │  ├─ position.py
│  │  └─ signal.py
│  ├─ strategy/
│  │  ├─ complement_arb.py
│  │  └─ filters.py
│  ├─ execution/
│  │  ├─ order_router.py
│  │  ├─ quote_manager.py
│  │  └─ cancel_manager.py
│  ├─ risk/
│  │  ├─ limits.py
│  │  ├─ kill_switch.py
│  │  └─ exposure.py
│  ├─ storage/
│  │  ├─ sqlite_store.py
│  │  └─ csv_logger.py
│  ├─ monitoring/
│  │  ├─ healthcheck.py
│  │  └─ notifier.py
│  └─ utils/
│     ├─ clock.py
│     ├─ math_ext.py
│     └─ retry.py
└─ tests/
```

## Windows 11 Setup

1. Install Python 3.12.
2. Open PowerShell and move to project:

```powershell
cd C:\MyProjects\polymarket-structure-arb-bot
```

3. Create and activate virtual environment:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

4. Install dependencies:

```powershell
pip install -U pip
pip install -e .[dev]
```

5. Create local env file:

```powershell
copy .env.example .env
```

## Configuration

### `.env`

Current phase does not use live credentials, but keep keys for future integration:

```env
POLYMARKET_PRIVATE_KEY=
POLYMARKET_API_KEY=
POLYMARKET_API_SECRET=
POLYMARKET_PASSPHRASE=
```

### `config/settings.yaml` (key values)

- `strategy.entry_threshold_sum_ask`: signal threshold
- `strategy.expiry_block_minutes`: block new signals near expiry
- `risk.max_open_positions`: global cap
- `risk.max_positions_per_market`: per-market cap
- `risk.paper_order_size_usdc`: paper notional
- `risk.min_book_size`: minimum required visible ask size per leg
- `risk.stale_quote_ms`: max quote age to accept paper fill
- `runtime.market_refresh_minutes`: periodic market-universe refresh interval

### `config/markets.yaml`

- `include_slugs`: allow-list (if empty, no allow-list restriction)
- `exclude_slugs`: explicit deny-list
- `exclude_categories` / `exclude_keywords`: extra exclusion filters

## Run

Normal run:

```powershell
python -m src.main
```

or:

```powershell
python src/main.py
```

One-shot mode (fetch/filter only, no websocket loop):

```powershell
python -m src.main --once
```

## Quality Commands

```powershell
pytest -q
ruff check .
black --check .
```

## Placeholder Notes

- `ClobClient` is a paper-trading placeholder only.
- `GeoBlockClient` is a paper-trading placeholder only.
- Do not treat this repository as live-ready in the current phase.
