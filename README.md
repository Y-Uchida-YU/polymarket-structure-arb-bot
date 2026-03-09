# Polymarket Structure Arb Bot (MVP, Paper Trading)

Polymarket の binary market で、YES/NO の best ask 非整合 (`ask_yes + ask_no`) を監視し、
しきい値以下ならシグナルを出す MVP です。  
この版は **paper trading のみ** で、live order は送信しません。

## Features (MVP)

- Gamma API から market 一覧取得
- binary market 抽出（Yes/No の `clobTokenIds` を保持）
- market channel WebSocket に接続
- `asset_id` ごとの `best_bid_ask` 受信
- 同一市場の YES/NO ask を保持してシグナル検出
- signal/order/fill/pnl/error を CSV 保存
- SQLite に状態保存（markets / quotes / signals / orders / fills / pnl / errors）
- WebSocket 自動再接続 + 再購読
- 日次ローテーションログ

## Directory

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
   ├─ test_strategy.py
   ├─ test_risk.py
   └─ test_execution.py
```

## Win11 Setup

1. Python 3.12 をインストール（`py --version` で確認）
2. プロジェクトへ移動
   ```powershell
   cd C:\MyProjects\polymarket-structure-arb-bot
   ```
3. 仮想環境作成と有効化
   ```powershell
   py -3.12 -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```
4. 依存インストール
   ```powershell
   pip install -U pip
   pip install -e .[dev]
   ```
5. `.env` を作成
   ```powershell
   copy .env.example .env
   ```
6. `config/settings.yaml` と `config/markets.yaml` を必要に応じて編集

## `.env` 編集

`POLYMARKET_*` は将来の live execution 用です。MVP paper trading では未使用でも起動可能です。

```env
POLYMARKET_PRIVATE_KEY=
POLYMARKET_API_KEY=
POLYMARKET_API_SECRET=
POLYMARKET_PASSPHRASE=
```

## `settings.yaml` 編集ポイント

- `strategy.entry_threshold_sum_ask`: シグナル発火しきい値（例: `0.985`）
- `strategy.expiry_block_minutes`: 満期直前ブロック分数（例: `180`）
- `risk.max_open_positions`: 同時保有上限
- `risk.max_positions_per_market`: 市場ごとの上限
- `market_filters.exclude_categories`: 除外カテゴリ

## `markets.yaml` 編集ポイント

- `include_slugs`: 空なら全許可、値を入れるとその slug のみ許可
- `exclude_slugs`: 個別 market を除外
- `exclude_categories` / `exclude_keywords`: `settings.yaml` 側フィルタに追加で除外

## Run

```powershell
python -m src.main
```

または:

```powershell
python src/main.py
```

### One-shot dry run (WS接続せず市場抽出のみ)

```powershell
python -m src.main --once
```

## Test / Lint / Format

```powershell
pytest -q
ruff check .
black --check .
```

## Notes

- 外部 API 仕様は変わる可能性があるため、`src/clients/` は薄いラッパーにしています。
- WebSocket payload はフィールドゆらぎを吸収する実装にしてあります。
- 本実装は投資助言ではありません。live order は未実装です。
