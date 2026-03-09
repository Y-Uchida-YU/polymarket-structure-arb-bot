from __future__ import annotations

import sqlite3
from pathlib import Path

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

    def _create_tables(self) -> None:
        with self.conn:
            self.conn.execute(
                """
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
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS quotes (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  market_id TEXT NOT NULL,
                  asset_id TEXT NOT NULL,
                  side TEXT NOT NULL,
                  best_bid REAL,
                  best_ask REAL,
                  quote_time TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                  signal_id TEXT PRIMARY KEY,
                  market_id TEXT NOT NULL,
                  slug TEXT NOT NULL,
                  ask_yes REAL NOT NULL,
                  ask_no REAL NOT NULL,
                  sum_ask REAL NOT NULL,
                  threshold REAL NOT NULL,
                  detected_at TEXT NOT NULL,
                  reason TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                """
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
                """
            )
            self.conn.execute(
                """
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
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pnl_snapshots (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  signal_id TEXT NOT NULL,
                  market_id TEXT NOT NULL,
                  estimated_final_pnl REAL NOT NULL,
                  created_at TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS errors (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  stage TEXT NOT NULL,
                  error_message TEXT NOT NULL,
                  created_at TEXT NOT NULL
                )
                """
            )

    def upsert_market(self, market: BinaryMarket, updated_at_iso: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO markets
                (market_id, slug, question, category, end_time, yes_token_id, no_token_id, updated_at)
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
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO quotes (market_id, asset_id, side, best_bid, best_ask, quote_time)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    market_id,
                    update.asset_id,
                    side,
                    update.best_bid,
                    update.best_ask,
                    update.timestamp.isoformat(),
                ),
            )

    def save_signal(self, signal: ArbSignal) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO signals
                (signal_id, market_id, slug, ask_yes, ask_no, sum_ask, threshold, detected_at, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.signal_id,
                    signal.market_id,
                    signal.slug,
                    signal.ask_yes,
                    signal.ask_no,
                    signal.sum_ask,
                    signal.threshold,
                    signal.detected_at.isoformat(),
                    signal.reason,
                ),
            )

    def save_order(self, order: PaperOrder) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO orders
                (order_id, signal_id, market_id, token_id, side, quantity, limit_price, status, created_at)
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
                (fill_id, order_id, signal_id, market_id, token_id, filled_qty, fill_price, fee, filled_at)
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

    def close(self) -> None:
        self.conn.close()
