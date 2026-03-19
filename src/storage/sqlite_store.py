from __future__ import annotations

import sqlite3
from pathlib import Path

from src.domain.accounting import InventorySnapshot, PnLSnapshot
from src.domain.book import TickSizeUpdate
from src.domain.market import BinaryMarket
from src.domain.order import FillEvent, PaperOrder
from src.domain.run import RunSnapshot, RunSummary
from src.domain.signal import ArbSignal
from src.execution.quote_manager import BestBidAskUpdate


class SQLiteStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._create_tables()
        self._migrate_tables()
        self._create_indexes()

    def _create_tables(self) -> None:
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS markets (
                  market_id TEXT PRIMARY KEY,
                  slug TEXT NOT NULL,
                  question TEXT NOT NULL,
                  category TEXT NOT NULL,
                  end_time TEXT,
                  yes_token_id TEXT NOT NULL,
                  no_token_id TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS quotes (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT,
                  market_id TEXT NOT NULL,
                  asset_id TEXT NOT NULL,
                  side TEXT NOT NULL,
                  best_bid REAL,
                  best_ask REAL,
                  best_bid_size REAL,
                  best_ask_size REAL,
                  quote_age_ms REAL,
                  tick_size REAL,
                  source TEXT,
                  resync_reason TEXT,
                  quote_time TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                  signal_id TEXT PRIMARY KEY,
                  run_id TEXT,
                  market_id TEXT NOT NULL,
                  slug TEXT NOT NULL,
                  ask_yes REAL NOT NULL,
                  ask_no REAL NOT NULL,
                  sum_ask REAL NOT NULL,
                  threshold REAL NOT NULL,
                  reason TEXT NOT NULL,
                  quote_age_ms REAL,
                  yes_bid REAL,
                  no_bid REAL,
                  yes_size REAL,
                  no_size REAL,
                  tick_size_yes REAL,
                  tick_size_no REAL,
                  signal_edge_raw REAL,
                  signal_edge_after_slippage REAL,
                  adjusted_edge REAL,
                  fill_status TEXT,
                  reject_reason TEXT,
                  resync_reason TEXT,
                  detected_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                  order_id TEXT PRIMARY KEY,
                  run_id TEXT,
                  signal_id TEXT NOT NULL,
                  market_id TEXT NOT NULL,
                  token_id TEXT NOT NULL,
                  side TEXT NOT NULL,
                  quantity REAL NOT NULL,
                  limit_price REAL NOT NULL,
                  status TEXT NOT NULL,
                  created_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS fills (
                  fill_id TEXT PRIMARY KEY,
                  run_id TEXT,
                  order_id TEXT NOT NULL,
                  signal_id TEXT NOT NULL,
                  market_id TEXT NOT NULL,
                  token_id TEXT NOT NULL,
                  filled_qty REAL NOT NULL,
                  fill_price REAL NOT NULL,
                  fee REAL NOT NULL,
                  filled_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS pnl_snapshots (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT,
                  signal_id TEXT NOT NULL,
                  market_id TEXT NOT NULL,
                  market_slug TEXT NOT NULL DEFAULT '',
                  estimated_final_pnl REAL NOT NULL DEFAULT 0,
                  estimated_edge_at_signal REAL NOT NULL DEFAULT 0,
                  projected_matched_pnl REAL NOT NULL DEFAULT 0,
                  unmatched_inventory_mtm REAL NOT NULL DEFAULT 0,
                  total_projected_pnl REAL NOT NULL DEFAULT 0,
                  created_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS inventory_snapshots (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT,
                  signal_id TEXT NOT NULL,
                  market_id TEXT NOT NULL,
                  market_slug TEXT NOT NULL,
                  yes_filled_qty REAL NOT NULL,
                  no_filled_qty REAL NOT NULL,
                  matched_qty REAL NOT NULL,
                  unmatched_yes_qty REAL NOT NULL,
                  unmatched_no_qty REAL NOT NULL,
                  avg_fill_price_yes REAL NOT NULL,
                  avg_fill_price_no REAL NOT NULL,
                  yes_mark_price REAL NOT NULL,
                  no_mark_price REAL NOT NULL,
                  valuation_mode TEXT NOT NULL,
                  created_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS errors (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT,
                  stage TEXT NOT NULL,
                  error_message TEXT NOT NULL,
                  created_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS tick_sizes (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT,
                  asset_id TEXT NOT NULL,
                  tick_size REAL NOT NULL,
                  source TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS resync_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT,
                  asset_id TEXT NOT NULL,
                  reason TEXT NOT NULL,
                  status TEXT NOT NULL,
                  details TEXT,
                  created_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS diagnostics_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT,
                  event_name TEXT NOT NULL,
                  asset_id TEXT,
                  market_id TEXT,
                  reason TEXT,
                  latency_ms REAL,
                  details TEXT,
                  created_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT,
                  metric_name TEXT NOT NULL,
                  metric_value REAL NOT NULL,
                  details TEXT,
                  created_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS run_snapshots (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT NOT NULL,
                  active_markets INTEGER NOT NULL,
                  stale_assets INTEGER NOT NULL,
                  total_signals INTEGER NOT NULL,
                  total_fills INTEGER NOT NULL,
                  open_unmatched_inventory REAL NOT NULL,
                  cumulative_projected_pnl REAL NOT NULL,
                  safe_mode_active INTEGER NOT NULL,
                  safe_mode_reason TEXT,
                  resync_cumulative_count INTEGER NOT NULL,
                  created_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS run_summaries (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT NOT NULL,
                  mode TEXT NOT NULL,
                  started_at TEXT NOT NULL,
                  ended_at TEXT NOT NULL,
                  uptime_seconds REAL NOT NULL,
                  total_signals INTEGER NOT NULL,
                  total_fills INTEGER NOT NULL,
                  total_projected_pnl REAL NOT NULL,
                  safe_mode_count INTEGER NOT NULL,
                  exception_count INTEGER NOT NULL,
                  stale_events INTEGER NOT NULL,
                  resync_events INTEGER NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS execution_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT,
                  signal_id TEXT NOT NULL,
                  market_id TEXT NOT NULL,
                  market_slug TEXT NOT NULL,
                  fill_status TEXT NOT NULL,
                  detected_at TEXT NOT NULL,
                  completed_at TEXT NOT NULL,
                  signal_to_fill_latency_ms REAL,
                  signal_to_reject_latency_ms REAL,
                  quote_age_ms_at_signal REAL,
                  quote_age_ms_at_fill REAL,
                  raw_edge REAL,
                  adjusted_edge REAL,
                  avg_fill_price_yes REAL,
                  avg_fill_price_no REAL,
                  matched_qty REAL,
                  unmatched_yes_qty REAL,
                  unmatched_no_qty REAL,
                  total_projected_pnl REAL,
                  reject_reason TEXT,
                  safe_mode_reason TEXT,
                  resync_reason TEXT
                )
                """)

    def _migrate_tables(self) -> None:
        self._ensure_column("quotes", "run_id", "TEXT")
        self._ensure_column("quotes", "best_bid_size", "REAL")
        self._ensure_column("quotes", "best_ask_size", "REAL")
        self._ensure_column("quotes", "quote_age_ms", "REAL")
        self._ensure_column("quotes", "tick_size", "REAL")
        self._ensure_column("quotes", "source", "TEXT")
        self._ensure_column("quotes", "resync_reason", "TEXT")

        self._ensure_column("signals", "run_id", "TEXT")
        self._ensure_column("signals", "quote_age_ms", "REAL")
        self._ensure_column("signals", "yes_bid", "REAL")
        self._ensure_column("signals", "no_bid", "REAL")
        self._ensure_column("signals", "yes_size", "REAL")
        self._ensure_column("signals", "no_size", "REAL")
        self._ensure_column("signals", "tick_size_yes", "REAL")
        self._ensure_column("signals", "tick_size_no", "REAL")
        self._ensure_column("signals", "signal_edge_raw", "REAL")
        self._ensure_column("signals", "signal_edge_after_slippage", "REAL")
        self._ensure_column("signals", "adjusted_edge", "REAL")
        self._ensure_column("signals", "fill_status", "TEXT")
        self._ensure_column("signals", "reject_reason", "TEXT")
        self._ensure_column("signals", "resync_reason", "TEXT")
        self._ensure_column("orders", "run_id", "TEXT")
        self._ensure_column("fills", "run_id", "TEXT")
        self._ensure_column("pnl_snapshots", "run_id", "TEXT")
        self._ensure_column("inventory_snapshots", "run_id", "TEXT")
        self._ensure_column("errors", "run_id", "TEXT")
        self._ensure_column("tick_sizes", "run_id", "TEXT")
        self._ensure_column("resync_events", "run_id", "TEXT")
        self._ensure_column("metrics", "run_id", "TEXT")
        self._ensure_column("pnl_snapshots", "market_slug", "TEXT DEFAULT ''")
        self._ensure_column("pnl_snapshots", "estimated_final_pnl", "REAL DEFAULT 0")
        self._ensure_column("pnl_snapshots", "estimated_edge_at_signal", "REAL DEFAULT 0")
        self._ensure_column("pnl_snapshots", "projected_matched_pnl", "REAL DEFAULT 0")
        self._ensure_column("pnl_snapshots", "unmatched_inventory_mtm", "REAL DEFAULT 0")
        self._ensure_column("pnl_snapshots", "total_projected_pnl", "REAL DEFAULT 0")

    def _create_indexes(self) -> None:
        with self.conn:
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_diagnostics_events_run_name_created
                ON diagnostics_events (run_id, event_name, created_at)
                """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_diagnostics_events_name_reason_created
                ON diagnostics_events (event_name, reason, created_at)
                """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_diagnostics_events_market_created
                ON diagnostics_events (market_id, created_at)
                """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_diagnostics_events_asset_created
                ON diagnostics_events (asset_id, created_at)
                """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_metrics_run_name_created
                ON metrics (run_id, metric_name, created_at)
                """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_resync_events_run_reason_created
                ON resync_events (run_id, reason, created_at)
                """)

    def _ensure_column(self, table_name: str, column_name: str, column_type: str) -> None:
        cursor = self.conn.execute(f"PRAGMA table_info({table_name})")
        existing_columns = {str(row[1]) for row in cursor.fetchall()}
        if column_name in existing_columns:
            return
        with self.conn:
            self.conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

    def upsert_market(self, market: BinaryMarket, updated_at_iso: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO markets
                (
                  market_id, slug, question, category, end_time,
                  yes_token_id, no_token_id, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_id) DO UPDATE SET
                  slug=excluded.slug,
                  question=excluded.question,
                  category=excluded.category,
                  end_time=excluded.end_time,
                  yes_token_id=excluded.yes_token_id,
                  no_token_id=excluded.no_token_id,
                  updated_at=excluded.updated_at
                """,
                (
                    market.market_id,
                    market.slug,
                    market.question,
                    market.category,
                    market.end_time.isoformat() if market.end_time else None,
                    market.yes_token_id,
                    market.no_token_id,
                    updated_at_iso,
                ),
            )

    def save_quote(
        self,
        market_id: str,
        side: str,
        update: BestBidAskUpdate,
        run_id: str | None = None,
        quote_age_ms: float | None = None,
        tick_size: float | None = None,
        source: str | None = None,
        resync_reason: str | None = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO quotes (
                  run_id,
                  market_id,
                  asset_id,
                  side,
                  best_bid,
                  best_ask,
                  best_bid_size,
                  best_ask_size,
                  quote_age_ms,
                  tick_size,
                  source,
                  resync_reason,
                  quote_time
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    market_id,
                    update.asset_id,
                    side,
                    update.best_bid,
                    update.best_ask,
                    update.best_bid_size,
                    update.best_ask_size,
                    quote_age_ms,
                    tick_size,
                    source,
                    resync_reason,
                    update.timestamp.isoformat(),
                ),
            )

    def save_signal(self, signal: ArbSignal) -> None:
        self.save_signal_with_run(signal=signal, run_id=None)

    def save_signal_with_run(self, signal: ArbSignal, run_id: str | None) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO signals (
                  signal_id,
                  run_id,
                  market_id,
                  slug,
                  ask_yes,
                  ask_no,
                  sum_ask,
                  threshold,
                  reason,
                  quote_age_ms,
                  yes_bid,
                  no_bid,
                  yes_size,
                  no_size,
                  tick_size_yes,
                  tick_size_no,
                  signal_edge_raw,
                  signal_edge_after_slippage,
                  adjusted_edge,
                  fill_status,
                  reject_reason,
                  resync_reason,
                  detected_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.signal_id,
                    run_id,
                    signal.market_id,
                    signal.slug,
                    signal.ask_yes,
                    signal.ask_no,
                    signal.sum_ask,
                    signal.threshold,
                    signal.reason,
                    signal.quote_age_ms,
                    signal.bid_yes,
                    signal.bid_no,
                    signal.size_yes,
                    signal.size_no,
                    signal.tick_size_yes,
                    signal.tick_size_no,
                    signal.raw_edge,
                    signal.signal_edge_after_slippage,
                    signal.adjusted_edge,
                    signal.fill_status,
                    signal.reject_reason,
                    signal.resync_reason,
                    signal.detected_at.isoformat(),
                ),
            )

    def save_order(self, order: PaperOrder, run_id: str | None = None) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO orders
                (
                  order_id, run_id, signal_id, market_id, token_id, side,
                  quantity, limit_price, status, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order.order_id,
                    run_id,
                    order.signal_id,
                    order.market_id,
                    order.token_id,
                    order.side,
                    order.quantity,
                    order.limit_price,
                    order.status,
                    order.created_at.isoformat(),
                ),
            )

    def save_fill(self, fill: FillEvent, run_id: str | None = None) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO fills
                (
                  fill_id, run_id, order_id, signal_id, market_id, token_id,
                  filled_qty, fill_price, fee, filled_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fill.fill_id,
                    run_id,
                    fill.order_id,
                    fill.signal_id,
                    fill.market_id,
                    fill.token_id,
                    fill.filled_qty,
                    fill.fill_price,
                    fill.fee,
                    fill.filled_at.isoformat(),
                ),
            )

    def save_pnl_snapshot(self, snapshot: PnLSnapshot, run_id: str | None = None) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO pnl_snapshots (
                  run_id,
                  signal_id,
                  market_id,
                  market_slug,
                  estimated_final_pnl,
                  estimated_edge_at_signal,
                  projected_matched_pnl,
                  unmatched_inventory_mtm,
                  total_projected_pnl,
                  created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    snapshot.signal_id,
                    snapshot.market_id,
                    snapshot.market_slug,
                    snapshot.total_projected_pnl,
                    snapshot.estimated_edge_at_signal,
                    snapshot.projected_matched_pnl,
                    snapshot.unmatched_inventory_mtm,
                    snapshot.total_projected_pnl,
                    snapshot.timestamp.isoformat(),
                ),
            )

    def save_inventory_snapshot(
        self, snapshot: InventorySnapshot, run_id: str | None = None
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO inventory_snapshots (
                  run_id,
                  signal_id,
                  market_id,
                  market_slug,
                  yes_filled_qty,
                  no_filled_qty,
                  matched_qty,
                  unmatched_yes_qty,
                  unmatched_no_qty,
                  avg_fill_price_yes,
                  avg_fill_price_no,
                  yes_mark_price,
                  no_mark_price,
                  valuation_mode,
                  created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    snapshot.signal_id,
                    snapshot.market_id,
                    snapshot.market_slug,
                    snapshot.yes_filled_qty,
                    snapshot.no_filled_qty,
                    snapshot.matched_qty,
                    snapshot.unmatched_yes_qty,
                    snapshot.unmatched_no_qty,
                    snapshot.avg_fill_price_yes,
                    snapshot.avg_fill_price_no,
                    snapshot.yes_mark_price,
                    snapshot.no_mark_price,
                    snapshot.valuation_mode,
                    snapshot.timestamp.isoformat(),
                ),
            )

    def save_error(
        self,
        stage: str,
        error_message: str,
        created_at_iso: str,
        run_id: str | None = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO errors (run_id, stage, error_message, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    run_id,
                    stage,
                    error_message,
                    created_at_iso,
                ),
            )

    def save_tick_size_update(self, update: TickSizeUpdate, run_id: str | None = None) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO tick_sizes (run_id, asset_id, tick_size, source, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    update.asset_id,
                    update.tick_size,
                    update.source,
                    update.updated_at.isoformat(),
                ),
            )

    def save_resync_event(
        self,
        asset_id: str,
        reason: str,
        status: str,
        details: str,
        created_at_iso: str,
        run_id: str | None = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO resync_events (run_id, asset_id, reason, status, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    asset_id,
                    reason,
                    status,
                    details,
                    created_at_iso,
                ),
            )

    def save_diagnostics_event(
        self,
        *,
        event_name: str,
        created_at_iso: str,
        asset_id: str | None = None,
        market_id: str | None = None,
        reason: str | None = None,
        latency_ms: float | None = None,
        details: str = "",
        run_id: str | None = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO diagnostics_events (
                  run_id, event_name, asset_id, market_id, reason, latency_ms, details, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    event_name,
                    asset_id,
                    market_id,
                    reason,
                    latency_ms,
                    details,
                    created_at_iso,
                ),
            )

    def save_metric(
        self,
        metric_name: str,
        metric_value: float,
        details: str,
        created_at_iso: str,
        run_id: str | None = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    metric_name,
                    metric_value,
                    details,
                    created_at_iso,
                ),
            )

    def save_run_snapshot(self, snapshot: RunSnapshot) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO run_snapshots (
                  run_id,
                  active_markets,
                  stale_assets,
                  total_signals,
                  total_fills,
                  open_unmatched_inventory,
                  cumulative_projected_pnl,
                  safe_mode_active,
                  safe_mode_reason,
                  resync_cumulative_count,
                  created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.run_id,
                    snapshot.active_markets,
                    snapshot.stale_assets,
                    snapshot.total_signals,
                    snapshot.total_fills,
                    snapshot.open_unmatched_inventory,
                    snapshot.cumulative_projected_pnl,
                    int(snapshot.safe_mode_active),
                    snapshot.safe_mode_reason,
                    snapshot.resync_cumulative_count,
                    snapshot.timestamp.isoformat(),
                ),
            )

    def save_run_summary(self, summary: RunSummary) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO run_summaries (
                  run_id,
                  mode,
                  started_at,
                  ended_at,
                  uptime_seconds,
                  total_signals,
                  total_fills,
                  total_projected_pnl,
                  safe_mode_count,
                  exception_count,
                  stale_events,
                  resync_events
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    summary.run_id,
                    summary.mode,
                    summary.started_at.isoformat(),
                    summary.ended_at.isoformat(),
                    summary.uptime_seconds,
                    summary.total_signals,
                    summary.total_fills,
                    summary.total_projected_pnl,
                    summary.safe_mode_count,
                    summary.exception_count,
                    summary.stale_events,
                    summary.resync_events,
                ),
            )

    def save_execution_event(self, row: dict[str, object]) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO execution_events (
                  run_id,
                  signal_id,
                  market_id,
                  market_slug,
                  fill_status,
                  detected_at,
                  completed_at,
                  signal_to_fill_latency_ms,
                  signal_to_reject_latency_ms,
                  quote_age_ms_at_signal,
                  quote_age_ms_at_fill,
                  raw_edge,
                  adjusted_edge,
                  avg_fill_price_yes,
                  avg_fill_price_no,
                  matched_qty,
                  unmatched_yes_qty,
                  unmatched_no_qty,
                  total_projected_pnl,
                  reject_reason,
                  safe_mode_reason,
                  resync_reason
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("run_id"),
                    row.get("signal_id"),
                    row.get("market_id"),
                    row.get("market_slug"),
                    row.get("fill_status"),
                    row.get("detected_at"),
                    row.get("completed_at"),
                    row.get("signal_to_fill_latency_ms"),
                    row.get("signal_to_reject_latency_ms"),
                    row.get("quote_age_ms_at_signal"),
                    row.get("quote_age_ms_at_fill"),
                    row.get("raw_edge"),
                    row.get("adjusted_edge"),
                    row.get("avg_fill_price_yes"),
                    row.get("avg_fill_price_no"),
                    row.get("matched_qty"),
                    row.get("unmatched_yes_qty"),
                    row.get("unmatched_no_qty"),
                    row.get("total_projected_pnl"),
                    row.get("reject_reason"),
                    row.get("safe_mode_reason"),
                    row.get("resync_reason"),
                ),
            )

    def export_signals_by_date(self, date_prefix: str) -> list[dict[str, object]]:
        cursor = self.conn.execute(
            """
            SELECT
              signal_id,
              market_id,
              slug,
              ask_yes,
              ask_no,
              yes_bid,
              no_bid,
              yes_size,
              no_size,
              tick_size_yes,
              tick_size_no,
              sum_ask,
              threshold,
              quote_age_ms,
              signal_edge_raw,
              signal_edge_after_slippage,
              adjusted_edge,
              fill_status,
              reject_reason,
              resync_reason,
              reason,
              detected_at
            FROM signals
            WHERE substr(detected_at, 1, 10) = ?
            ORDER BY detected_at ASC
            """,
            (date_prefix,),
        )
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row, strict=False)) for row in rows]

    def close(self) -> None:
        self.conn.close()
