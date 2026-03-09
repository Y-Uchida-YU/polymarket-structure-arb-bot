from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from src.reporting.daily_report import DailyReportGenerator
from src.storage.sqlite_store import SQLiteStore


def _seed_report_data(db_path: Path) -> None:
    store = SQLiteStore(db_path=db_path)
    now = datetime.now(tz=UTC)
    ts = now.isoformat()

    with store.conn:
        store.conn.execute(
            """
            INSERT INTO signals (
              signal_id, run_id, market_id, slug,
              ask_yes, ask_no, sum_ask, threshold, reason, detected_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("sig-1", "run-1", "m1", "market-1", 0.45, 0.5, 0.95, 0.98, "sum_ask_le_threshold", ts),
        )
        store.conn.execute(
            """
            INSERT INTO fills (
              fill_id, run_id, order_id, signal_id, market_id,
              token_id, filled_qty, fill_price, fee, filled_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("fill-1", "run-1", "order-1", "sig-1", "m1", "yes1", 1.0, 0.46, 0.0, ts),
        )
        store.conn.execute(
            """
            INSERT INTO execution_events (
              run_id, signal_id, market_id, market_slug, fill_status, detected_at, completed_at,
              signal_to_fill_latency_ms, signal_to_reject_latency_ms,
              quote_age_ms_at_signal, quote_age_ms_at_fill,
              raw_edge, adjusted_edge, avg_fill_price_yes, avg_fill_price_no, matched_qty,
              unmatched_yes_qty, unmatched_no_qty, total_projected_pnl,
              reject_reason, safe_mode_reason, resync_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-1",
                "sig-1",
                "m1",
                "market-1",
                "filled",
                ts,
                ts,
                10.0,
                None,
                200.0,
                300.0,
                0.03,
                0.01,
                0.46,
                0.5,
                1.0,
                0.0,
                0.0,
                0.04,
                None,
                None,
                "ws_connected",
            ),
        )
        store.conn.execute(
            """
            INSERT INTO pnl_snapshots (
              run_id, signal_id, market_id, market_slug, estimated_final_pnl,
              estimated_edge_at_signal,
              projected_matched_pnl, unmatched_inventory_mtm, total_projected_pnl, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("run-1", "sig-1", "m1", "market-1", 0.04, 0.01, 0.04, 0.0, 0.04, ts),
        )
        store.conn.execute(
            """
            INSERT INTO resync_events (run_id, asset_id, reason, status, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("run-1", "yes1", "ws_connected", "ok", "resync_applied", ts),
        )
        store.conn.execute(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("run-1", "safe_mode_entered", 1.0, "high_reject_rate", ts),
        )
    store.close()


def test_daily_report_generator_outputs_core_metrics(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    _seed_report_data(db_path)
    generator = DailyReportGenerator(db_path=db_path, export_dir=tmp_path)
    report = generator.generate(date=None, last_hours=24, run_id="run-1")

    assert report["totals"]["total_signals"] == 1
    assert report["totals"]["total_fills"] == 1
    assert report["totals"]["fill_rate"] == 1.0
    assert report["totals"]["safe_mode_count"] == 1
    assert "top_markets_by_signal_count" in report
    assert "top_reject_reasons" in report


def test_daily_report_generator_writes_json_and_csv(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    _seed_report_data(db_path)
    generator = DailyReportGenerator(db_path=db_path, export_dir=tmp_path)
    report = generator.generate(
        date=(datetime.now(tz=UTC) - timedelta(hours=1)).date().isoformat(),
        last_hours=None,
        run_id="run-1",
    )
    json_path, csv_path = generator.save(report)
    assert json_path.exists()
    assert csv_path.exists()


def test_daily_report_last_hours_with_run_id_includes_boundary_timestamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path=db_path)
    fixed_now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    ts = fixed_now.isoformat()

    with store.conn:
        store.conn.execute(
            """
            INSERT INTO signals (
              signal_id, run_id, market_id, slug,
              ask_yes, ask_no, sum_ask, threshold, reason, detected_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("sig-boundary", "run-1", "m1", "market-1", 0.45, 0.5, 0.95, 0.98, "seed", ts),
        )
    store.close()

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz: object | None = None) -> datetime:
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

        @classmethod
        def fromisoformat(cls, date_string: str) -> datetime:
            return datetime.fromisoformat(date_string)

    monkeypatch.setattr("src.reporting.daily_report.datetime", FixedDateTime)
    generator = DailyReportGenerator(db_path=db_path, export_dir=tmp_path)
    report = generator.generate(date=None, last_hours=24, run_id="run-1")

    assert report["totals"]["total_signals"] == 1
