from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from src.domain.accounting import InventorySnapshot, PnLSnapshot
from src.domain.signal import ArbSignal
from src.storage.csv_logger import CsvEventLogger
from src.storage.sqlite_store import SQLiteStore


def _build_signal(now: datetime) -> ArbSignal:
    signal = ArbSignal.new(
        market_id="m1",
        slug="market-1",
        yes_token_id="yes1",
        no_token_id="no1",
        ask_yes=0.45,
        ask_no=0.5,
        bid_yes=0.44,
        bid_no=0.49,
        size_yes=20.0,
        size_no=25.0,
        tick_size_yes=0.01,
        tick_size_no=0.01,
        threshold=0.98,
        raw_edge=0.03,
        adjusted_edge=0.01,
        quote_age_ms=250.0,
        signal_edge_after_slippage=0.0,
        order_size_usdc=5.0,
        fill_status="filled",
        reject_reason=None,
        resync_reason="ws_connected",
        detected_at=now,
        reason="sum_ask_le_threshold",
    )
    return signal


def test_csv_signal_log_contains_v3_columns(tmp_path: Path) -> None:
    logger = CsvEventLogger(export_dir=tmp_path)
    now = datetime(2026, 3, 1, tzinfo=UTC)
    logger.log_signal(
        {
            "signal_id": "s1",
            "market_id": "m1",
            "slug": "market-1",
            "signal_timestamp": now.isoformat(),
            "yes_bid": 0.44,
            "yes_ask": 0.45,
            "no_bid": 0.49,
            "no_ask": 0.5,
            "yes_size": 20.0,
            "no_size": 25.0,
            "tick_size_yes": 0.01,
            "tick_size_no": 0.01,
            "quote_age_ms": 250.0,
            "signal_edge_raw": 0.03,
            "signal_edge_after_slippage": 0.01,
            "fill_status": "filled",
            "reject_reason": "",
            "resync_reason": "ws_connected",
        },
        now_utc=now,
    )

    files = list(tmp_path.glob("signals_*.csv"))
    assert len(files) == 1
    header = files[0].read_text(encoding="utf-8").splitlines()[0]
    assert "signal_timestamp" in header
    assert "quote_age_ms" in header
    assert "signal_edge_after_slippage" in header
    assert "resync_reason" in header


def test_sqlite_signal_store_persists_v3_columns(tmp_path: Path) -> None:
    store = SQLiteStore(db_path=tmp_path / "state.db")
    signal = _build_signal(now=datetime(2026, 3, 1, tzinfo=UTC))
    store.save_signal(signal)

    row = store.conn.execute(
        """
        SELECT
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
          resync_reason
        FROM signals
        WHERE signal_id = ?
        """,
        (signal.signal_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == 250.0
    assert row[1] == 0.44
    assert row[7] == 0.03
    assert row[10] == "filled"
    assert row[11] == "ws_connected"

    store.close()


def test_pnl_and_inventory_snapshots_are_persisted(tmp_path: Path) -> None:
    now = datetime(2026, 3, 1, tzinfo=UTC)
    store = SQLiteStore(db_path=tmp_path / "state.db")
    pnl_snapshot = PnLSnapshot(
        signal_id="sig-1",
        market_id="m1",
        market_slug="market-1",
        timestamp=now,
        estimated_edge_at_signal=0.02,
        projected_matched_pnl=1.5,
        unmatched_inventory_mtm=-0.4,
        total_projected_pnl=1.1,
    )
    inventory_snapshot = InventorySnapshot(
        signal_id="sig-1",
        market_id="m1",
        market_slug="market-1",
        timestamp=now,
        yes_filled_qty=5.0,
        no_filled_qty=4.0,
        matched_qty=4.0,
        unmatched_yes_qty=1.0,
        unmatched_no_qty=0.0,
        avg_fill_price_yes=0.46,
        avg_fill_price_no=0.5,
        yes_mark_price=0.43,
        no_mark_price=0.49,
        valuation_mode="best_bid",
    )
    store.save_pnl_snapshot(pnl_snapshot)
    store.save_inventory_snapshot(inventory_snapshot)

    pnl_row = store.conn.execute(
        """
        SELECT
          estimated_edge_at_signal,
          projected_matched_pnl,
          unmatched_inventory_mtm,
          total_projected_pnl
        FROM pnl_snapshots
        WHERE signal_id = ?
        """,
        ("sig-1",),
    ).fetchone()
    inventory_row = store.conn.execute(
        """
        SELECT matched_qty, unmatched_yes_qty, unmatched_no_qty, avg_fill_price_yes, valuation_mode
        FROM inventory_snapshots
        WHERE signal_id = ?
        """,
        ("sig-1",),
    ).fetchone()
    assert pnl_row is not None
    assert pnl_row[0] == 0.02
    assert pnl_row[3] == 1.1
    assert inventory_row is not None
    assert inventory_row[0] == 4.0
    assert inventory_row[1] == 1.0
    assert inventory_row[4] == "best_bid"

    store.close()


def test_csv_logger_writes_inventory_and_pnl_breakdown_columns(tmp_path: Path) -> None:
    logger = CsvEventLogger(export_dir=tmp_path)
    now = datetime(2026, 3, 1, tzinfo=UTC)
    logger.log_inventory(
        {
            "signal_id": "sig-1",
            "market_id": "m1",
            "market_slug": "market-1",
            "timestamp": now.isoformat(),
            "matched_qty": 4.0,
            "unmatched_yes_qty": 1.0,
            "unmatched_no_qty": 0.0,
        },
        now_utc=now,
    )
    logger.log_pnl(
        {
            "signal_id": "sig-1",
            "market_id": "m1",
            "estimated_edge_at_signal": 0.02,
            "projected_matched_pnl": 1.5,
            "unmatched_inventory_mtm": -0.4,
            "total_projected_pnl": 1.1,
        },
        now_utc=now,
    )

    inventory_file = list(tmp_path.glob("inventory_*.csv"))[0]
    pnl_file = list(tmp_path.glob("pnl_*.csv"))[0]
    inventory_header = inventory_file.read_text(encoding="utf-8").splitlines()[0]
    pnl_header = pnl_file.read_text(encoding="utf-8").splitlines()[0]
    assert "matched_qty" in inventory_header
    assert "unmatched_yes_qty" in inventory_header
    assert "estimated_edge_at_signal" in pnl_header
    assert "projected_matched_pnl" in pnl_header
    assert "unmatched_inventory_mtm" in pnl_header
    assert "total_projected_pnl" in pnl_header


def test_sqlite_resync_event_persists_reason(tmp_path: Path) -> None:
    store = SQLiteStore(db_path=tmp_path / "state.db")
    store.save_resync_event(
        asset_id="yes1",
        reason="idle_timeout",
        status="ok",
        details="resync_applied",
        created_at_iso="2026-03-10T00:00:00+00:00",
        run_id="run-1",
    )
    row = store.conn.execute("""
        SELECT run_id, asset_id, reason, status
        FROM resync_events
        ORDER BY id DESC
        LIMIT 1
        """).fetchone()
    assert row is not None
    assert row[0] == "run-1"
    assert row[1] == "yes1"
    assert row[2] == "idle_timeout"
    assert row[3] == "ok"
    store.close()
