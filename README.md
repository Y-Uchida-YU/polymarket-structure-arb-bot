# Polymarket Structure Arb Bot (Paper Trading MVP v4)

Polymarket の binary market を監視し、YES/NO の価格非整合シグナルを検出する paper-trading bot です。

## Live Trading Status (Important)

- このリポジトリは **still not live-ready** です。
- live order / auth / signing / balances / cancel-replace は未実装です。
- v4 の主目的は **paper accounting の信頼性向上** です。

## v4 の主要改善

- signal / fill / inventory / pnl の責務を分離
  - signal: エントリー条件の観測結果
  - fill: paper上で成立した約定結果
  - inventory: matched / unmatched 在庫状態
  - pnl: fill + inventory から算出した会計結果
- PnL を signal価格ではなく **actual fill price** ベースで計算
- partial / one-leg fill 時の unmatched inventory を会計へ反映
- stale / missing quote 時は unmatched valuation を保守的に評価
- PnL を分解表示
  - `estimated_edge_at_signal`
  - `projected_matched_pnl`
  - `unmatched_inventory_mtm`
  - `total_projected_pnl`

## v4 でできること

- Gamma API から active markets を取得し、binary market を抽出
- strict filter 適用（active/open/binary/enableOrderBook/カテゴリ除外）
- market websocket 接続、自動再接続、再購読
- tick size 管理（初期取得 + `tick_size_change` 追随）
- book summary resync（接続直後・stale検知時・ws idle時）
- quality guard（spread/depth/quote age/tick整合）
- fill model（stale/depth/slippage/probability/partial/one-leg）
- CSV / SQLite へ signal, fill, inventory, pnl を保存

## まだ未対応

- live order placement
- CLOB 認証（L1/L2, POLY_* headers）
- signing
- balance / allowance
- cancel / replace
- 完全な取引所マッチングエンジン再現

## ディレクトリ

```text
polymarket-structure-arb-bot/
├─ README.md
├─ .env.example
├─ pyproject.toml
├─ config/
├─ data/
├─ src/
└─ tests/
```

## Windows 11 セットアップ

1. Python 3.12 をインストール
2. PowerShell で移動

```powershell
cd C:\MyProjects\polymarket-structure-arb-bot
```

3. 仮想環境

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

4. 依存関係

```powershell
pip install -U pip
pip install -e .[dev]
```

5. `.env` 作成

```powershell
copy .env.example .env
```

## 設定の要点

### `.env`

```env
POLYMARKET_PRIVATE_KEY=
POLYMARKET_API_KEY=
POLYMARKET_API_SECRET=
POLYMARKET_PASSPHRASE=
```

### `config/settings.yaml` 主要項目

- `strategy.entry_threshold_sum_ask`
- `strategy.max_quote_age_ms_for_signal`
- `risk.paper_order_size_usdc`
- `risk.stale_quote_ms`
- `risk.slip_ticks`
- `runtime.market_refresh_minutes`
- `runtime.stale_asset_ms`
- `runtime.book_resync_idle_ms`

## 実行

通常:

```powershell
python -m src.main
```

1回実行:

```powershell
python -m src.main --once
```

dry-run:

```powershell
python -m src.main --dry-run
```

設定検証:

```powershell
python -m src.main --validate-config
```

明示的 paper:

```powershell
python -m src.main --paper
```

起動時に summary（mode / paper_trade / dry_run / once / thresholds）を表示します。

## 会計モデル（v4）

- `estimated_edge_at_signal`: signal時点の優位性見積もり
- `projected_matched_pnl`: 両脚で matched した数量の projected pnl
- `unmatched_inventory_mtm`: 片脚在庫の保守的 MTM
- `total_projected_pnl`: `projected_matched_pnl + unmatched_inventory_mtm`

補足:

- matched 部分は complete-set 的に `payout=1.0` を基準に評価
- unmatched 部分は current best bid を優先し、stale/missing なら保守評価
- したがって、見かけ上の勝率を盛らない conservative paper accounting です

## ログ/保存データ

### CSV (`data/exports/`)

- `signals_YYYYMMDD.csv`: signal観測ログ
- `orders_*.csv`, `fills_*.csv`
- `inventory_*.csv`: matched/unmatched 在庫スナップショット
- `pnl_*.csv`: 分解済みPnL
- `errors_*.csv`, `metrics_*.csv`, `tick_sizes_*.csv`, `resync_*.csv`

### SQLite (`data/state/state.db`)

- `markets`, `quotes`, `signals`, `orders`, `fills`
- `inventory_snapshots`, `pnl_snapshots`
- `errors`, `tick_sizes`, `resync_events`, `metrics`

## 品質コマンド

```powershell
pytest -q
ruff check .
black --check .
```

## Placeholder 実装

- `src/clients/clob_client.py` は paper placeholder
- `src/clients/geoblock_client.py` は paper placeholder
- 本番利用には実API接続・認証・署名実装が必要
