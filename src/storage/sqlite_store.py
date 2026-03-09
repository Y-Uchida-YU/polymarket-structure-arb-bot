from __future__ import annotations

import sqlite3
from pathlib import Path

from src.domain.book import TickSizeUpdate
from src.domain.market import BinaryMarket
from src.domain.order import FillEvent, PaperOrder
from src.domain.signal import ArbSignal
from src.execution.quote_manager import BestBidAskUpdate


class SQLiteStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._create_tables()
        self._migrate_tables()

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
                  signal_id TEXT NOT NULL,
                  market_id TEXT NOT NULL,
                  estimated_final_pnl REAL NOT NULL,
                  created_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS errors (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  stage TEXT NOT NULL,
                  error_message TEXT NOT NULL,
                  created_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS tick_sizes (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  asset_id TEXT NOT NULL,
                  tick_size REAL NOT NULL,
                  source TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS resync_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  asset_id TEXT NOT NULL,
                  reason TEXT NOT NULL,
                  status TEXT NOT NULL,
                  details TEXT,
                  created_at TEXT NOT NULL
                )
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  metric_name TEXT NOT NULL,
                  metric_value REAL NOT NULL,
                  details TEXT,
                  created_at TEXT NOT NULL
                )
                """)

    def _migrate_tables(self) -> None:
        self._ensure_column("quotes", "best_bid_size", "REAL")
        self._ensure_column("quotes", "best_ask_size", "REAL")
        self._ensure_column("quotes", "quote_age_ms", "REAL")
        self._ensure_column("quotes", "tick_size", "REAL")
        self._ensure_column("quotes", "source", "TEXT")
        self._ensure_column("quotes", "resync_reason", "TEXT")

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
        quote_age_ms: float | None = None,
        tick_size: float | None = None,
        source: str | None = None,
        resync_reason: str | None = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO quotes (
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
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
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO signals (
                  signal_id,
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.signal_id,
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

    def save_order(self, order: PaperOrder) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO orders
                (
                  order_id, signal_id, market_id, token_id, side,
                  quantity, limit_price, status, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order.order_id,
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

    def save_fill(self, fill: FillEvent) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO fills
                (
                  fill_id, order_id, signal_id, market_id, token_id,
                  filled_qty, fill_price, fee, filled_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fill.fill_id,
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

    def save_pnl_snapshot(
        self,
        signal_id: str,
        market_id: str,
        estimated_final_pnl: float,
        created_at_iso: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO pnl_snapshots (signal_id, market_id, estimated_final_pnl, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    signal_id,
                    market_id,
                    estimated_final_pnl,
                    created_at_iso,
                ),
            )

    def save_error(self, stage: str, error_message: str, created_at_iso: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO errors (stage, error_message, created_at)
                VALUES (?, ?, ?)
                """,
                (
                    stage,
                    error_message,
                    created_at_iso,
                ),
            )

    def save_tick_size_update(self, update: TickSizeUpdate) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO tick_sizes (asset_id, tick_size, source, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (
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
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO resync_events (asset_id, reason, status, details, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    asset_id,
                    reason,
                    status,
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
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO metrics (metric_name, metric_value, details, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    metric_name,
                    metric_value,
                    details,
                    created_at_iso,
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
