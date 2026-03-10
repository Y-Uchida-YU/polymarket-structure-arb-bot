# Polymarket Structure Arb Bot (Shadow Paper Run v6)

Polymarket の binary YES/NO 市場を監視し、構造的な価格非整合を検出する paper-trading bot です。  
v6 は「戦略の高度化」ではなく、Shadow Paper Run の観測品質改善に集中しています。

## Important: Not Live-Ready

- 本リポジトリは **paper trading 専用** です。
- live order / auth / signing / balances / cancel-replace は未実装です。
- `--paper` / `--shadow-paper` でのみ利用してください。

## v6 の主な改善

- resync reason を分類して保存  
  `ws_connected`, `ws_reconnect`, `idle_timeout`, `missing_book_state`, `stale_asset`, `market_universe_changed`
- resync storm 抑制  
  - per-asset resync cooldown
  - full-resync cooldown
  - 1サイクルあたりの resync 対象上限
- 監視ユニバース縮小  
  - `max_markets_to_watch`
  - activity proxy (`min_recent_activity`, `min_liquidity_proxy`, `min_volume_24h_proxy`)
  - expiry window (`min_days_to_expiry`, `max_days_to_expiry`)
- no-signal reason を記録・集計  
  例: `edge_below_threshold`, `spread_too_wide`, `depth_too_low`, `quote_too_old`, `asset_not_ready`, `safe_mode_blocked`
- report を強化  
  - resyncs by reason
  - no-signal reasons
  - watched/subscribed universe 概況
  - warm-up 概況

## できること / できないこと

### できること

- Gamma API から active markets を取得し binary 市場を抽出
- market channel websocket で `best_bid_ask` を受信
- YES/NO の ask 合計によるシグナル判定
- conservative な paper fill（stale/depth/slippage/partial/one-leg を考慮）
- CSV + SQLite 保存
- run/session 単位の summary と日次 report 出力

### できないこと

- 実注文
- CLOB 認証・署名
- 資産残高の実取得
- cancel/replace 実装

## ディレクトリ

```text
polymarket-structure-arb-bot/
├─ config/
├─ data/
├─ src/
└─ tests/
```

## Windows 11 Runbook (手動運用)

1. リポジトリに移動

```powershell
cd C:\MyProjects\polymarket-structure-arb-bot
```

2. Python 3.12 仮想環境

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

3. 依存インストール

```powershell
pip install -U pip
pip install -e .[dev]
```

4. `.env` 準備

```powershell
copy .env.example .env
```

5. 設定ファイルを選択

- 通常: `config/settings.yaml`
- Shadow Paper Run 推奨: `config/settings.shadow.yaml`

6. 設定検証

```powershell
python -m src.main run --validate-config --settings-path config/settings.shadow.yaml
```

7. Shadow Paper Run 起動

```powershell
python -m src.main run --shadow-paper
```

8. 停止

- `Ctrl + C`
- 停止時に run summary を出力

## 実行モード

```powershell
python -m src.main run [options]
```

- `--paper / --no-paper` (`--no-paper` は live 未実装のため非推奨)
- `--shadow-paper` (長時間観測向け)
- `--once`
- `--dry-run`
- `--validate-config`
- `--settings-path <path>`

## Warm-up 挙動

起動直後の race を避けるため、以下の条件を満たすまでは stale guard を抑制します。

- `ws_connected_at` が未設定なら stale 判定しない
- subscribe 直後は `runtime.initial_market_data_grace_ms` の間 warming-up 扱い
- `all_assets_stale` safe mode は warm-up 解除後のみ評価

## Shadow Paper 設定の考え方 (v6)

`config/settings.shadow.yaml` は「高頻度反応」ではなく「安定観測」重視です。

- 監視市場を強く絞る (`max_markets_to_watch`)
- `stale_asset_ms` / `book_resync_idle_ms` は長め
- resync cooldown を導入して連打を抑制

## 主要設定

### market_filters

- `max_markets_to_watch`
- `require_orderbook_enabled`
- `min_days_to_expiry`, `max_days_to_expiry`
- `min_recent_activity`
- `min_liquidity_proxy`
- `min_volume_24h_proxy`
- `require_recent_trade_within_minutes` (必要時のみ利用)

### runtime

- `stale_asset_ms`
- `book_resync_idle_ms`
- `resync_cooldown_ms`
- `full_resync_cooldown_ms`
- `max_resync_assets_per_cycle`
- `market_refresh_minutes`

## 出力

### SQLite

`data/state/state.db` に保存します。主テーブル:

- `signals`, `orders`, `fills`
- `execution_events`
- `inventory_snapshots`, `pnl_snapshots`
- `resync_events`, `metrics`, `tick_sizes`, `errors`
- `run_snapshots`, `run_summaries`

### CSV

`data/exports/` に日次ファイルを保存します。主ファイル:

- `signals_YYYYMMDD.csv`
- `fills_YYYYMMDD.csv`
- `execution_events_YYYYMMDD.csv`
- `inventory_YYYYMMDD.csv`
- `pnl_YYYYMMDD.csv`
- `metrics_YYYYMMDD.csv`
- `resync_YYYYMMDD.csv`
- `run_snapshots_YYYYMMDD.csv`
- `run_summaries_YYYYMMDD.csv`

## Report

日次/時間窓レポート:

```powershell
python -m src.main report --date 2026-03-10
python -m src.main report --last-hours 24
python -m src.main report --last-hours 24 --run-id <run_id>
```

`data/exports/` に JSON/CSV を保存します。

### v6 で見るべき項目

- `resync_count` と `resyncs_by_reason`
- `no_signal_reasons`
- `watched_markets` / `subscribed_assets`
- warnings  
  例: `resync_storm_detected`, `no_signals_but_high_resync`, `universe_too_large`

## 品質チェック

```powershell
pytest -q
ruff check .
black --check .
```

## 既知の制約

- paper fill は保守的だが、取引所マッチングの完全再現ではない
- queue priority の厳密シミュレーションは未実装
- live readiness は別フェーズ
