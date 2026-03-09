# Polymarket Structure Arb Bot (Paper Trading MVP v3)

Polymarket の binary market を監視し、`ask_yes + ask_no` の非整合シグナルを検出する paper-trading bot です。

## Live Trading Status (Important)

- このリポジトリは **still not live-ready** です。
- live order / auth / signing / balances / cancel-replace は未実装です。
- 現在は「観測Bot + signal bot + 現実寄り paper bot」までを対象にしています。

## v3 でできること

- Gamma API から active markets を取得し、binary market を抽出
- 市場フィルタ（以下をすべて満たす市場のみ採用）
  - `active == true`
  - `closed == false`
  - `archived == false`
  - outcomes が 2 要素で Yes/No と判定可能
  - `clobTokenIds` が 2 要素
  - `enableOrderBook == true`（欠損は除外）
- `outcomes` と `clobTokenIds` の index 対応を前提に Yes/No token を安全にマッピング
- market websocket 接続と自動再接続（再接続後再購読）
- 購読 payload に `custom_feature_enabled: true` を付与
- `best_bid_ask` を asset_id ごとに保持
- tick size 管理
  - 初期取得: `/tick-size/{token_id}`
  - 変更追随: websocket `tick_size_change`
  - 価格丸め utility（tick 準拠）
- orderbook summary resync
  - 接続直後
  - stale asset 検知時
  - ws idle gap 検知時
- signal quality guard
  - max spread / min depth / max quote age
  - raw edge と adjusted edge を分離
- paper fill model（v2 より現実寄り）
  - quote age
  - available size
  - slip ticks
  - fill probability
  - partial / one-leg / reject の区別
- CSV と SQLite への永続化
  - signal / order / fill / pnl / error
  - tick size / resync / metrics
- `market_refresh_minutes` による定期市場再取得

## v3 でも未対応のこと

- live order placement
- CLOB 認証（L1/L2, POLY_* headers）
- signing
- balance / allowance
- cancel / replace
- 取引所レベルの完全 queue simulation

## ディレクトリ

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
│  ├─ clients/
│  ├─ config/
│  ├─ domain/
│  ├─ execution/
│  ├─ monitoring/
│  ├─ risk/
│  ├─ storage/
│  ├─ strategy/
│  └─ utils/
└─ tests/
```

## Windows 11 セットアップ

1. Python 3.12 をインストール
2. PowerShell でプロジェクトへ移動

```powershell
cd C:\MyProjects\polymarket-structure-arb-bot
```

3. venv 作成と有効化

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

4. 依存関係インストール

```powershell
pip install -U pip
pip install -e .[dev]
```

5. `.env` 作成

```powershell
copy .env.example .env
```

## 設定ファイル

### `.env`

現フェーズでは live key は使いませんが、将来用に保持しています。

```env
POLYMARKET_PRIVATE_KEY=
POLYMARKET_API_KEY=
POLYMARKET_API_SECRET=
POLYMARKET_PASSPHRASE=
```

### `config/settings.yaml` 主要項目

- `strategy.entry_threshold_sum_ask`: しきい値
- `strategy.enable_quality_guards`: quality guard ON/OFF
- `strategy.max_spread_per_leg`: 脚ごとの最大 spread
- `strategy.min_depth_per_leg`: 脚ごとの最小 depth
- `strategy.max_quote_age_ms_for_signal`: signal 用 freshness 条件
- `strategy.adjusted_edge_min`: adjusted edge 最低値
- `risk.paper_order_size_usdc`: paper 注文サイズ
- `risk.min_book_size`: 最低板サイズ
- `risk.stale_quote_ms`: fill 許容 quote age
- `risk.slip_ticks`: 擬似スリッページ
- `runtime.market_refresh_minutes`: 市場再取得周期
- `runtime.stale_asset_ms`: stale 判定閾値
- `runtime.book_resync_idle_ms`: ws idle gap resync 閾値

### `config/markets.yaml`

- `include_slugs`: allow-list
- `exclude_slugs`: deny-list
- `exclude_categories` / `exclude_keywords`: 追加除外

## 実行モード

通常起動:

```powershell
python -m src.main
```

1回実行のみ（市場読み込み後に終了）:

```powershell
python -m src.main --once
```

dry-run（市場読み込み経路の確認だけして終了）:

```powershell
python -m src.main --dry-run
```

設定検証のみ:

```powershell
python -m src.main --validate-config
```

明示的 paper 指定:

```powershell
python -m src.main --paper
```

起動時に設定サマリーと「live 未実装」警告を表示します。

## ログ/保存データ

### CSV (`data/exports/`)

- `signals_YYYYMMDD.csv`
  - `signal_timestamp`, `quote_age_ms`
  - `yes_bid/yes_ask`, `no_bid/no_ask`
  - `yes_size`, `no_size`
  - `tick_size_yes`, `tick_size_no`
  - `signal_edge_raw`, `signal_edge_after_slippage`
  - `fill_status`, `reject_reason`, `resync_reason`
- `orders_*.csv`, `fills_*.csv`, `pnl_*.csv`, `errors_*.csv`
- `tick_sizes_*.csv`, `resync_*.csv`, `metrics_*.csv`

### SQLite (`data/state/state.db`)

- `markets`, `quotes`, `signals`, `orders`, `fills`, `pnl_snapshots`, `errors`
- `tick_sizes`, `resync_events`, `metrics`

## 品質コマンド

```powershell
pytest -q
ruff check .
black --check .
```

## Placeholder 実装について

- `src/clients/clob_client.py` は paper placeholder です。
- `src/clients/geoblock_client.py` は paper placeholder です。
- 本番利用時は実 API 接続・認証・署名実装が必須です。
