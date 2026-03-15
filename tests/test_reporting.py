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
            INSERT INTO resync_events (run_id, asset_id, reason, status, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("run-1", "no1", "idle_timeout", "ok", "resync_applied", ts),
        )
        store.conn.execute(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("run-1", "safe_mode_entered", 1.0, "high_reject_rate", ts),
        )
        store.conn.execute(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("run-1", "no_signal_reason:edge_below_threshold", 1.0, "market_id=m1", ts),
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
    assert "resyncs_by_reason" in report
    assert report["resyncs_by_reason"][0]["count"] >= 1
    assert "no_signal_reasons" in report
    assert report["no_signal_reasons"][0]["reason"] == "edge_below_threshold"


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


def test_daily_report_v7_breakdowns_and_warnings(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path=db_path)
    now = datetime.now(tz=UTC).isoformat()

    with store.conn:
        rows = [
            (
                "run-1",
                "safe_mode_entered",
                1.0,
                "scope=global;reason=all_assets_stale",
                now,
            ),
            (
                "run-1",
                "safe_mode_market_block_started",
                2.0,
                "scope=market;event=started;reason=book_state_unhealthy;active_markets=2",
                now,
            ),
            (
                "run-1",
                "safe_mode_market_block_active",
                2.0,
                "scope=market;event=active;reason=book_state_unhealthy;blocked_markets=2",
                now,
            ),
            (
                "run-1",
                "safe_mode_market_block_cleared",
                1.0,
                "scope=market;event=cleared;reason=book_state_unhealthy;active_markets=0",
                now,
            ),
            (
                "run-1",
                "safe_mode_asset_blocked",
                3.0,
                "scope=asset;reason=book_state_unhealthy;blocked_assets=3",
                now,
            ),
            ("run-1", "no_signal_reason:safe_mode_blocked_global", 1.0, "", now),
            ("run-1", "no_signal_reason:safe_mode_blocked_global", 1.0, "", now),
            ("run-1", "no_signal_reason:safe_mode_blocked_global", 1.0, "", now),
            ("run-1", "no_signal_reason:safe_mode_blocked_global", 1.0, "", now),
            ("run-1", "no_signal_reason:safe_mode_blocked_global", 1.0, "", now),
            ("run-1", "no_signal_reason:safe_mode_blocked_global", 1.0, "", now),
            ("run-1", "no_signal_reason:safe_mode_blocked_asset", 1.0, "", now),
            ("run-1", "no_signal_reason:safe_mode_blocked_asset", 1.0, "", now),
            ("run-1", "no_signal_reason:safe_mode_blocked_asset", 1.0, "", now),
            ("run-1", "no_signal_reason:safe_mode_blocked_asset", 1.0, "", now),
            ("run-1", "missing_book_state_reason:no_initial_book", 7.0, "", now),
            ("run-1", "missing_book_state_reason:quote_missing_after_resync", 4.0, "", now),
            ("run-1", "warming_up_asset_count", 5.0, "waiting_initial_market_data_grace", now),
            ("run-1", "universe_current_watched_markets", 40.0, "", now),
            ("run-1", "universe_current_subscribed_assets", 80.0, "", now),
            ("run-1", "universe_cumulative_watched_markets", 120.0, "", now),
            ("run-1", "universe_cumulative_subscribed_assets", 240.0, "", now),
        ]
        store.conn.executemany(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
    store.close()

    generator = DailyReportGenerator(db_path=db_path, export_dir=tmp_path)
    report = generator.generate(date=None, last_hours=24, run_id="run-1")

    assert report["totals"]["total_signals"] == 0
    assert report["universe"]["current_watched_markets"] == 40
    assert report["universe"]["cumulative_watched_markets"] == 120
    assert report["universe"]["current_subscribed_assets"] == 80
    assert report["universe"]["cumulative_subscribed_assets"] == 240

    safe_mode_reasons = {item["reason"]: item["count"] for item in report["safe_mode_reasons"]}
    assert safe_mode_reasons["all_assets_stale"] >= 1
    assert safe_mode_reasons["book_state_unhealthy"] >= 1
    safe_mode_scope_reasons = {
        f"{item['scope']}:{item['reason']}": item["count"]
        for item in report["safe_mode_scope_reasons"]
    }
    assert safe_mode_scope_reasons["global:all_assets_stale"] >= 1
    assert safe_mode_scope_reasons["asset:book_state_unhealthy"] >= 1

    missing_reasons = {
        item["reason"]: item["count"] for item in report["missing_book_state_reasons"]
    }
    assert missing_reasons["no_initial_book"] == 7
    assert missing_reasons["quote_missing_after_resync"] == 4

    assert report["totals"]["global_safe_mode_count"] == 1
    assert report["totals"]["market_block_count"] == 1
    assert report["totals"]["market_block_active_count"] == 1
    assert report["totals"]["market_block_cleared_count"] == 1
    assert report["totals"]["asset_block_count"] == 1
    assert report["totals"]["total_block_events"] == 3

    assert "global_safe_mode_triggered" in report["warnings"]
    assert "market_blocks_triggered" in report["warnings"]
    assert "asset_blocks_triggered" in report["warnings"]
    assert "blocking_dominates_run" in report["warnings"]
    assert "book_state_unhealthy" in report["warnings"]
    assert "data_not_ready_for_evaluation" in report["warnings"]

    console = DailyReportGenerator.format_console(report)
    assert "watched_markets(current/cumulative): 40 / 120" in console
    assert "subscribed_assets(current/cumulative): 80 / 240" in console


def test_daily_report_counts_readiness_no_signal_breakdown(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path=db_path)
    now = datetime.now(tz=UTC).isoformat()

    with store.conn:
        rows = [
            ("run-1", "market_universe_changed", 1.0, "", now),
            ("run-1", "no_signal_reason:book_not_ready", 1.0, "", now),
            ("run-1", "no_signal_reason:book_not_ready", 1.0, "", now),
            ("run-1", "no_signal_reason:quote_too_old", 1.0, "", now),
            ("run-1", "no_signal_reason:book_recovering", 1.0, "", now),
            ("run-1", "no_signal_reason:market_not_ready", 1.0, "", now),
            ("run-1", "no_signal_reason:market_probation", 1.0, "", now),
            ("run-1", "no_signal_reason:market_quote_stale", 1.0, "", now),
        ]
        store.conn.executemany(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
    store.close()

    generator = DailyReportGenerator(db_path=db_path, export_dir=tmp_path)
    report = generator.generate(date=None, last_hours=24, run_id="run-1")

    totals = report["totals"]
    assert totals["market_universe_changed_count"] == 1
    assert totals["book_not_ready_count"] == 2
    assert totals["quote_too_old_count"] == 1
    assert totals["book_recovering_count"] == 1
    assert totals["market_not_ready_count"] == 1
    assert totals["market_probation_count"] == 1
    assert totals["market_quote_stale_count"] == 1


def test_daily_report_aligns_universe_change_vs_resync_and_ws_reasons(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path=db_path)
    now = datetime.now(tz=UTC).isoformat()

    with store.conn:
        store.conn.executemany(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    "run-1",
                    "market_universe_changed",
                    1.0,
                    "added=2;removed=0;reason=hysteresis_confirmed_change",
                    now,
                ),
                ("run-1", "ws_connected_event", 1.0, "reason=ws_connected;assets=10", now),
                (
                    "run-1",
                    "ws_reconnect_event",
                    1.0,
                    "reason=socket_closed_remote;mapped=ws_reconnect",
                    now,
                ),
            ],
        )
        store.conn.executemany(
            """
            INSERT INTO resync_events (run_id, asset_id, reason, status, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                ("run-1", "a1", "market_universe_changed", "ok", "resync_applied", now),
                ("run-1", "a2", "market_universe_changed", "ok", "resync_applied", now),
                ("run-1", "a3", "market_universe_changed", "ok", "resync_applied", now),
            ],
        )
    store.close()

    generator = DailyReportGenerator(db_path=db_path, export_dir=tmp_path)
    report = generator.generate(date=None, last_hours=24, run_id="run-1")

    totals = report["totals"]
    assert totals["market_universe_change_events"] == 1
    assert totals["market_universe_changed_count"] == 1
    assert totals["resync_due_to_universe_change"] == 3
    assert totals["ws_connected_events"] == 1
    assert totals["ws_reconnect_events"] == 1
    assert report["ws_connected_reasons"][0]["reason"] == "ws_connected"
    assert report["ws_reconnect_reasons"][0]["reason"] == "socket_closed_remote"


def test_daily_report_aggregates_book_not_ready_prefix_and_market_state_metrics(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path=db_path)
    now = datetime.now(tz=UTC).isoformat()
    with store.conn:
        store.conn.executemany(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("run-1", "no_signal_reason:book_not_ready", 1.0, "", now),
                ("run-1", "no_signal_reason:book_not_ready_insufficient_updates", 1.0, "", now),
                ("run-1", "no_signal_reason:book_not_ready_missing_leg_ready", 1.0, "", now),
                ("run-1", "market_state_ready_count", 8.0, "", now),
                ("run-1", "market_state_recovering_count", 2.0, "", now),
                ("run-1", "market_state_stale_no_recent_quote_count", 3.0, "", now),
                ("run-1", "market_state_stale_quote_age_count", 1.0, "", now),
                ("run-1", "market_state_eligible_count", 5.0, "", now),
                ("run-1", "market_state_blocked_count", 1.0, "", now),
            ],
        )
    store.close()

    generator = DailyReportGenerator(db_path=db_path, export_dir=tmp_path)
    report = generator.generate(date=None, last_hours=24, run_id="run-1")

    totals = report["totals"]
    assert totals["book_not_ready_count"] == 3
    assert totals["ready_market_count"] == 8
    assert totals["recovering_market_count"] == 2
    assert totals["stale_market_count"] == 4
    assert totals["eligible_market_count"] == 5
    assert totals["blocked_market_count"] == 1
