# Polymarket Structure Arb Bot (Paper Trading v5)

Polymarket の binary market を監視し、YES/NO の価格非整合シグナルを検出する paper bot です。  
v5 は「実市場で 24時間〜7日間の Shadow Paper Run を安全に回して評価する」ための改善版です。

## Important: Live Trading Status

- このリポジトリは **still not live-ready** です。
- live order / auth / signing / balances / cancel-replace は未実装です。
- 現在は **paper trading 専用** です。

## v5 でできること

- Gamma API から active markets を取得し、binary market を抽出
- `enableOrderBook=true` を含む厳格フィルタを通過した市場のみ監視
- WebSocket + 自動再接続 + 再購読
- tick size 管理（初期取得 + `tick_size_change` 追随）
- orderbook summary resync（接続直後 / stale検知 / ws idle）
- conservative な paper fill（stale/depth/partial/one-leg/slippage）
- signal / fill / inventory / pnl の責務分離
- run_id 付きの構造化ログ保存（CSV + SQLite）
- guardrail による異常監視（warning / safe mode / optional hard stop）
- 定期 snapshot（run 中の状態記録）
- 日次/直近時間の report 生成 CLI

## v5 の主眼

- Shadow Paper Run 中の長時間安定性
- 暴走検知（signal/fill/stale/resync/exception）
- 「なぜこの PnL になったか」を後追いできる記録
- 見かけの勝率より保守的な評価

## まだ未対応

- live order placement
- CLOB 認証（L1/L2, POLY_* headers）
- signing
- balance / allowance
- cancel / replace
- 取引所レベルの完全な queue/matching 再現

## ディレクトリ

```text
polymarket-structure-arb-bot/
├─ config/
├─ data/
├─ src/
└─ tests/
```

## Windows 11 Runbook (手動運用)

1. 作業ディレクトリへ移動

```powershell
cd C:\MyProjects\polymarket-structure-arb-bot
```

2. Python 3.12 仮想環境を作成

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

3. 依存関係をインストール

```powershell
pip install -U pip
pip install -e .[dev]
```

4. `.env` を準備

```powershell
copy .env.example .env
```

5. 設定ファイルを選ぶ

- 通常: `config/settings.yaml`
- Shadow Paper Run 推奨: `config/settings.shadow.yaml`

6. 起動前チェック

```powershell
python -m src.main run --validate-config --settings-path config/settings.shadow.yaml
```

7. Shadow Paper Run 起動（推奨）

```powershell
python -m src.main run --shadow-paper
```

8. 停止方法

- 実行中ターミナルで `Ctrl + C`
- 停止時に run summary が保存されます

9. ログ確認場所

- CSV: `data/exports/`
- SQLite: `data/state/state.db`
- 実行ログ: `data/logs/bot.log`（日次ローテーション）

Notes:
- 現時点は手動起動前提です。
- Windows タスクスケジューラ常駐化は本READMEでは扱いません。

## 実行モード

### Run

```powershell
python -m src.main run [options]
```

主要オプション:

- `--paper / --no-paper` (live は未実装)
- `--shadow-paper` (長時間観測向け)
- `--once` (市場ロードのみで終了)
- `--dry-run` (起動パス検証して終了)
- `--validate-config` (設定検証のみ)
- `--settings-path config/settings.yaml`

### Report

```powershell
python -m src.main report --date 2026-03-10
python -m src.main report --last-hours 24
python -m src.main report --last-hours 24 --run-id <run_id>
```

出力:

- コンソールにサマリー表示
- `data/exports/` へ JSON + CSV を保存

## Shadow Paper Run 専用ポイント

- `--shadow-paper` 起動時、既定で `config/settings.shadow.yaml` を優先
  - ただし `--settings-path` を明示した場合はその値を優先
- 起動サマリーに以下を表示
  - mode / runtime environment
  - paper only / live not implemented
  - strategy thresholds / risk limits
  - stale thresholds / refresh interval
  - output directories
- 定期 snapshot を保存
- guardrail 異常時は safe mode（新規 signal 停止）へ移行

## Guardrail / Safe Mode

監視対象（設定値は `settings*.yaml` の `guardrails`）:

- signal rate
- fill reject rate
- one-leg rate
- unmatched inventory rate
- stale asset rate
- resync rate
- exception rate

段階:

- warning: `metrics` + log に記録
- safe mode: 新規 signal を停止
- hard stop: 例外スパイク時のみ設定で有効化可能

safe mode 発動理由は CSV/SQLite に保存され、run summary にも集計されます。

## PnL 会計（v4/v5）

- signal は観測値、PnL は **actual fill** と inventory から計算
- matched/unmatched を分離
- 主な指標:
  - `estimated_edge_at_signal`
  - `projected_matched_pnl`
  - `unmatched_inventory_mtm`
  - `total_projected_pnl`

## 保存データ

### SQLite (`data/state/state.db`)

主要テーブル:

- `signals`, `orders`, `fills`
- `inventory_snapshots`, `pnl_snapshots`
- `execution_events`
- `resync_events`, `tick_sizes`, `metrics`, `errors`
- `run_snapshots`, `run_summaries`

### CSV (`data/exports/`)

主要ファイル:

- `signals_YYYYMMDD.csv`
- `fills_YYYYMMDD.csv`
- `execution_events_YYYYMMDD.csv`
- `inventory_YYYYMMDD.csv`
- `pnl_YYYYMMDD.csv`
- `run_snapshots_YYYYMMDD.csv`
- `run_summaries_YYYYMMDD.csv`
- `metrics_YYYYMMDD.csv`, `resync_YYYYMMDD.csv`, `errors_YYYYMMDD.csv`
- `daily_report_*.json`, `daily_report_*.csv`

## 日次レポートの読み方

`report` コマンドは最低限以下を集計します:

- total signals / fills / fill rate
- matched fill rate
- one-leg count/rate
- stale reject count/rate
- depth reject count/rate
- resync count
- safe mode count
- projected matched pnl
- unmatched inventory mtm
- total projected pnl
- top markets by signal count
- top markets by pnl
- top reject reasons

`warnings` セクションに異常兆候（例: `low_fill_rate`, `high_one_leg_rate`）が出ます。  
この warning を見て、設定や市場フィルタの見直しを行ってください。

## 品質チェック

```powershell
pytest -q
ruff check .
black --check .
```

## 既知の制約

- paper fill は簡易モデルであり、実際の queue priority を完全再現しません
- unmatched valuation は保守的 MTM だが、流動性断絶時の完全再現ではありません
- network/API 障害時は保守動作するが、収益性評価には十分な観測期間が必要です

## 今後のフェーズ（別途）

この v5 は「観測Bot + signal bot + 現実寄り paper bot」までです。  
live readiness（auth/signing/order/cancel/balance）は次フェーズで実装します。
