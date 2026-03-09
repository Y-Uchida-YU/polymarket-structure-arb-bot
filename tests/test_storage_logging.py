from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

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
