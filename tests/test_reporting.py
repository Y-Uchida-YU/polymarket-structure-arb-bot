from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from src.domain.market import BinaryMarket
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
    assert "watched_markets(latest_snapshot/cumulative_window): 40 / 120" in console
    assert "subscribed_assets(latest_snapshot/cumulative_window): 80 / 240" in console


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
                ("run-1", "market_state_ready_ratio", 0.4, "", now),
                ("run-1", "market_state_eligible_ratio", 0.3, "", now),
                ("run-1", "market_state_blocked_count", 1.0, "", now),
                ("run-1", "universe_current_watched_markets", 2.0, "", now),
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
    assert totals["ready_market_ratio"] == 0.4
    assert totals["eligible_market_ratio"] == 0.3
    assert totals["blocked_market_count"] == 1


def test_daily_report_explains_no_eligible_markets_causes(tmp_path: Path) -> None:
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
                ("run-1", "universe_current_watched_markets", 2.0, "", now),
                ("run-1", "universe_min_watched_markets_floor", 10.0, "", now),
                ("run-1", "market_state_ready_count", 0.0, "", now),
                ("run-1", "market_state_recovering_count", 2.0, "", now),
                ("run-1", "market_state_eligible_count", 0.0, "", now),
                ("run-1", "market_state_stale_no_recent_quote_count", 0.0, "", now),
                ("run-1", "market_state_stale_quote_age_count", 0.0, "", now),
                ("run-1", "low_quality_market_count", 4.0, "", now),
                ("run-1", "low_quality_runtime_excluded_count", 3.0, "", now),
            ],
        )
    store.close()

    generator = DailyReportGenerator(db_path=db_path, export_dir=tmp_path)
    report = generator.generate(date=None, last_hours=24, run_id="run-1")

    totals = report["totals"]
    assert totals["eligible_market_count"] == 0
    assert totals["min_watched_markets_floor"] == 10
    assert totals["low_quality_runtime_excluded_count"] == 3
    assert "no_eligible_markets" in report["warnings"]
    causes = set(report["no_eligible_market_causes"])
    assert "watched_too_small" in causes
    assert "all_markets_recovering" in causes
    assert "quality_penalty_excessive" in causes


def test_daily_report_includes_low_quality_contamination_causes(tmp_path: Path) -> None:
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
                ("run-1", "universe_current_watched_markets", 2.0, "", now),
                ("run-1", "universe_min_watched_markets_floor", 2.0, "", now),
                ("run-1", "market_state_eligible_count", 0.0, "", now),
                ("run-1", "current_watched_low_quality_excluded_count", 1.0, "m1:reason", now),
                ("run-1", "low_quality_reintroduced_for_floor_count", 1.0, "m1:reason", now),
                ("run-1", "watched_floor_shortfall_due_to_low_quality_exclusion", 1.0, "", now),
            ],
        )
    store.close()

    generator = DailyReportGenerator(db_path=db_path, export_dir=tmp_path)
    report = generator.generate(date=None, last_hours=24, run_id="run-1")

    causes = set(report["no_eligible_market_causes"])
    assert "watched_universe_low_quality_contaminated" in causes
    assert "watched_floor_reintroduced_low_quality" in causes
    assert "watched_floor_shortfall_low_quality_exclusion" in causes


def test_daily_report_includes_recovery_diagnostics_section(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path=db_path)
    now = datetime.now(tz=UTC).isoformat()
    with store.conn:
        store.conn.executemany(
            """
            INSERT INTO diagnostics_events
            (run_id, event_name, asset_id, market_id, reason, latency_ms, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("run-1", "resync_started", "a1", "m1", "missing_book_state", None, "", now),
                ("run-1", "resync_started", "a2", "m1", "missing_book_state", None, "", now),
                ("run-1", "resync_started", "a3", "m2", "stale_asset", None, "", now),
                (
                    "run-1",
                    "first_quote_after_resync",
                    "a1",
                    "m1",
                    "missing_book_state",
                    100.0,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "first_quote_after_resync",
                    "a2",
                    "m1",
                    "missing_book_state",
                    300.0,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "book_ready_after_resync",
                    "a1",
                    "m1",
                    "missing_book_state",
                    500.0,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "market_recovery_started",
                    None,
                    "m1",
                    "missing_book_state",
                    None,
                    "",
                    now,
                ),
                ("run-1", "market_recovery_started", None, "m2", "stale_asset", None, "", now),
                (
                    "run-1",
                    "market_ready_after_recovery",
                    None,
                    "m1",
                    "missing_book_state",
                    1200.0,
                    "",
                    now,
                ),
                ("run-1", "stale_asset_detected", "a1", "m1", "stale_asset", None, "", now),
                ("run-1", "stale_asset_detected", "a1", "m1", "stale_asset", None, "", now),
                ("run-1", "stale_asset_detected", "a1", "m1", "stale_asset", None, "", now),
                ("run-1", "stale_asset_detected", "a2", "m1", "stale_asset", None, "", now),
                (
                    "run-1",
                    "missing_book_state_detected",
                    "a2",
                    "m1",
                    "book_not_resynced_yet",
                    None,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "missing_book_state_detected",
                    "a2",
                    "m1",
                    "book_not_resynced_yet",
                    None,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "missing_book_state_detected",
                    "a3",
                    "m2",
                    "quote_missing_after_resync",
                    None,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "market_block_entered",
                    None,
                    "m1",
                    "book_state_unhealthy",
                    None,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "market_block_entered",
                    None,
                    "m1",
                    "book_state_unhealthy",
                    None,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "market_block_entered",
                    None,
                    "m2",
                    "book_state_unhealthy",
                    None,
                    "",
                    now,
                ),
            ],
        )
    store.close()

    generator = DailyReportGenerator(db_path=db_path, export_dir=tmp_path)
    report = generator.generate(date=None, last_hours=24, run_id="run-1")

    recovery = report["recovery_diagnostics"]
    assert recovery["recovery_resync_started_count"] == 3
    assert recovery["recovery_first_quote_success_count"] == 2
    assert recovery["recovery_book_ready_success_count"] == 1
    assert recovery["recovery_market_ready_success_count"] == 1
    assert recovery["recovery_first_quote_success_rate"] == pytest.approx(2 / 3)
    assert recovery["recovery_book_ready_success_rate"] == pytest.approx(1 / 3)
    assert recovery["recovery_market_ready_success_rate"] == pytest.approx(1 / 3)
    assert recovery["avg_resync_to_first_quote_latency_ms"] == pytest.approx(200.0)
    assert recovery["max_resync_to_first_quote_latency_ms"] == pytest.approx(300.0)
    assert recovery["avg_resync_to_book_ready_latency_ms"] == pytest.approx(500.0)
    assert recovery["max_resync_to_book_ready_latency_ms"] == pytest.approx(500.0)
    assert recovery["avg_recovery_to_market_ready_latency_ms"] == pytest.approx(1200.0)
    assert recovery["max_recovery_to_market_ready_latency_ms"] == pytest.approx(1200.0)
    assert recovery["top_stale_assets"][0]["asset_id"] == "a1"
    assert recovery["top_stale_assets"][0]["count"] == 3
    assert recovery["top_missing_book_assets"][0]["asset_id"] == "a2"
    assert recovery["top_missing_book_assets"][0]["count"] == 2
    assert recovery["top_market_blocked_markets"][0]["market_id"] == "m1"
    assert recovery["top_market_blocked_markets"][0]["count"] == 2
    assert recovery["top_recovery_slow_assets"][0]["asset_id"] == "a1"
    assert recovery["top_recovery_slow_markets"][0]["market_id"] == "m1"


def test_daily_report_eligibility_gate_breakdown_uses_saved_metrics(tmp_path: Path) -> None:
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
                ("run-1", "universe_current_watched_markets", 3.0, "", now),
                ("run-1", "market_state_eligible_count", 0.0, "", now),
                ("run-1", "eligibility_gate_reason:connection_recovering", 1.0, "", now),
                ("run-1", "eligibility_gate_reason:book_recovering", 3.0, "", now),
                ("run-1", "eligibility_gate_reason:stale_quote_freshness", 2.0, "", now),
                ("run-1", "eligibility_gate_reason:blocked", 0.0, "", now),
                ("run-1", "eligibility_gate_reason:probation", 0.0, "", now),
                ("run-1", "eligibility_gate_reason:low_quality_runtime_excluded", 2.0, "", now),
                ("run-1", "eligibility_gate_reason:other_readiness_gate", 4.0, "", now),
            ],
        )
    store.close()

    generator = DailyReportGenerator(db_path=db_path, export_dir=tmp_path)
    report = generator.generate(date=None, last_hours=24, run_id="run-1")

    totals = report["totals"]
    assert totals["eligibility_gate_connection_recovering_count"] == 1
    assert totals["eligibility_gate_book_recovering_count"] == 3
    assert totals["eligibility_gate_stale_quote_freshness_count"] == 2
    assert totals["eligibility_gate_low_quality_runtime_excluded_count"] == 2
    assert totals["eligibility_gate_other_readiness_gate_count"] == 4
    reasons = {
        item["reason"]: int(round(float(item["count"])))
        for item in report["eligibility_gate_breakdown"]
    }
    assert reasons["book_recovering"] == 3
    assert reasons["other_readiness_gate"] == 4
    assert "all_markets_book_recovering" in report["no_eligible_market_causes"]


def test_daily_report_recovery_top_diagnostics_include_market_and_side(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path=db_path)
    now_dt = datetime.now(tz=UTC)
    now = now_dt.isoformat()
    store.upsert_market(
        BinaryMarket(
            market_id="m1",
            question="Will test market resolve yes?",
            slug="market-1",
            category="test",
            end_time=None,
            condition_id="cond-m1",
            yes_token_id="a1",
            no_token_id="a2",
            raw={},
        ),
        updated_at_iso=now,
    )
    with store.conn:
        store.conn.executemany(
            """
            INSERT INTO diagnostics_events
            (run_id, event_name, asset_id, market_id, reason, latency_ms, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("run-1", "stale_asset_detected", "a1", "m1", "stale_asset", None, "", now),
                (
                    "run-1",
                    "missing_book_state_detected",
                    "a2",
                    "m1",
                    "book_not_ready",
                    None,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "market_block_entered",
                    None,
                    "m1",
                    "book_state_unhealthy",
                    None,
                    "",
                    now,
                ),
            ],
        )
    store.close()

    generator = DailyReportGenerator(db_path=db_path, export_dir=tmp_path)
    report = generator.generate(date=None, last_hours=24, run_id="run-1")
    recovery = report["recovery_diagnostics"]

    assert recovery["top_stale_assets"][0]["asset_id"] == "a1"
    assert recovery["top_stale_assets"][0]["market_slug"] == "market-1"
    assert recovery["top_stale_assets"][0]["side"] == "yes"
    assert recovery["top_missing_book_assets"][0]["asset_id"] == "a2"
    assert recovery["top_missing_book_assets"][0]["side"] == "no"
    assert recovery["top_market_blocked_markets"][0]["market_slug"] == "market-1"


def test_daily_report_recovery_universe_change_funnel_counts(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path=db_path)
    now = datetime.now(tz=UTC).isoformat()
    with store.conn:
        store.conn.executemany(
            """
            INSERT INTO diagnostics_events
            (run_id, event_name, asset_id, market_id, reason, latency_ms, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("run-1", "resync_started", "a1", "m1", "market_universe_changed", None, "", now),
                ("run-1", "resync_started", "a2", "m1", "market_universe_changed", None, "", now),
                (
                    "run-1",
                    "first_quote_after_resync",
                    "a1",
                    "m1",
                    "market_universe_changed",
                    120.0,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "book_ready_after_resync",
                    "a1",
                    "m1",
                    "market_universe_changed",
                    220.0,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "market_ready_after_recovery",
                    None,
                    "m1",
                    "market_universe_changed",
                    500.0,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "first_quote_after_resync_blocked",
                    "a2",
                    "m1",
                    "connection_recovering",
                    80.0,
                    "recovery_reason=market_universe_changed;stage=first_quote_after_resync_blocked",
                    now,
                ),
            ],
        )
    store.close()

    generator = DailyReportGenerator(db_path=db_path, export_dir=tmp_path)
    report = generator.generate(date=None, last_hours=24, run_id="run-1")
    recovery = report["recovery_diagnostics"]

    assert recovery["recovery_universe_change_resync_started_count"] == 1
    assert recovery["recovery_universe_change_first_quote_success_count"] == 1
    assert recovery["recovery_universe_change_book_ready_success_count"] == 1
    assert recovery["recovery_universe_change_market_ready_success_count"] == 1
    assert recovery["recovery_universe_change_first_quote_blocked_count"] == 1
    assert recovery["recovery_universe_change_first_quote_success_rate"] == pytest.approx(1.0)


def test_daily_report_recovery_universe_change_funnel_is_monotonic(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path=db_path)
    now = datetime.now(tz=UTC).isoformat()
    with store.conn:
        store.conn.executemany(
            """
            INSERT INTO diagnostics_events
            (run_id, event_name, asset_id, market_id, reason, latency_ms, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("run-1", "resync_started", "a1", "m1", "market_universe_changed", None, "", now),
                (
                    "run-1",
                    "market_ready_after_recovery",
                    None,
                    "m1",
                    "market_universe_changed",
                    500.0,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "market_ready_after_recovery",
                    None,
                    "m2",
                    "market_universe_changed",
                    550.0,
                    "",
                    now,
                ),
            ],
        )
    store.close()

    report = DailyReportGenerator(db_path=db_path, export_dir=tmp_path).generate(
        date=None,
        last_hours=24,
        run_id="run-1",
    )
    recovery = report["recovery_diagnostics"]

    resync_count = int(recovery["recovery_universe_change_resync_started_count"])
    first_count = int(recovery["recovery_universe_change_first_quote_success_count"])
    book_count = int(recovery["recovery_universe_change_book_ready_success_count"])
    market_count = int(recovery["recovery_universe_change_market_ready_success_count"])

    assert resync_count == 1
    assert first_count == 1
    assert book_count == 1
    assert market_count == 1
    assert market_count <= book_count <= first_count <= resync_count


def test_daily_report_includes_market_stale_reason_duration_and_leg_diagnostics(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path=db_path)
    now = datetime.now(tz=UTC).isoformat()
    store.upsert_market(
        BinaryMarket(
            market_id="m1",
            question="Will stale diagnostics market 1 resolve yes?",
            slug="stale-market-1",
            category="test",
            end_time=None,
            condition_id="cond-m1",
            yes_token_id="a1",
            no_token_id="a2",
            raw={},
        ),
        updated_at_iso=now,
    )
    store.upsert_market(
        BinaryMarket(
            market_id="m2",
            question="Will stale diagnostics market 2 resolve yes?",
            slug="stale-market-2",
            category="test",
            end_time=None,
            condition_id="cond-m2",
            yes_token_id="b1",
            no_token_id="b2",
            raw={},
        ),
        updated_at_iso=now,
    )

    with store.conn:
        store.conn.executemany(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    "run-1",
                    "no_signal_reason:market_quote_stale_quote_age",
                    1.0,
                    "market_id=m1;stale_reason_key=quote_age_exceeded;stale_side=yes",
                    now,
                ),
                (
                    "run-1",
                    "no_signal_reason:market_quote_stale_no_recent_quote",
                    1.0,
                    "market_id=m1;stale_reason_key=no_recent_quote;stale_side=no",
                    now,
                ),
            ],
        )
        store.conn.executemany(
            """
            INSERT INTO diagnostics_events
            (run_id, event_name, asset_id, market_id, reason, latency_ms, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "run-1",
                    "market_stale_entered",
                    None,
                    "m1",
                    "quote_age_exceeded",
                    None,
                    (
                        "stale_reason_key=quote_age_exceeded;stale_side=yes;"
                        "stale_asset_id=a1;stale_asset_ids=a1;universe_change_related=0"
                    ),
                    now,
                ),
                (
                    "run-1",
                    "market_stale_recovered",
                    None,
                    "m1",
                    "quote_age_exceeded",
                    1200.0,
                    "stale_reason_key=quote_age_exceeded;stale_side=yes;stale_asset_ids=a1",
                    now,
                ),
                (
                    "run-1",
                    "market_stale_entered",
                    None,
                    "m1",
                    "no_recent_quote",
                    None,
                    (
                        "stale_reason_key=no_recent_quote;stale_side=no;"
                        "stale_asset_id=a2;stale_asset_ids=a2;universe_change_related=0"
                    ),
                    now,
                ),
                (
                    "run-1",
                    "market_stale_recovered",
                    None,
                    "m1",
                    "no_recent_quote",
                    800.0,
                    "stale_reason_key=no_recent_quote;stale_side=no;stale_asset_ids=a2",
                    now,
                ),
                (
                    "run-1",
                    "market_stale_entered",
                    None,
                    "m2",
                    "leg_timestamp_mismatch",
                    None,
                    (
                        "stale_reason_key=leg_timestamp_mismatch;stale_side=both;"
                        "stale_asset_ids=b1,b2;universe_change_related=1"
                    ),
                    now,
                ),
                (
                    "run-1",
                    "market_stale_episode_closed",
                    None,
                    "m2",
                    "leg_timestamp_mismatch",
                    500.0,
                    (
                        "stale_reason_key=leg_timestamp_mismatch;stale_side=both;"
                        "stale_asset_ids=b1,b2;episode_closed_reason=reason_changed;"
                        "next_state=stale_no_recent_quote"
                    ),
                    now,
                ),
                (
                    "run-1",
                    "market_ready_after_recovery_blocked",
                    None,
                    "m2",
                    "market_quote_stale_quote_age",
                    300.0,
                    (
                        "recovery_reason=market_universe_changed;stage=market_ready_after_recovery_blocked;"
                        "stale_reason_key=leg_timestamp_mismatch;stale_side=both;stale_asset_ids=b1,b2"
                    ),
                    now,
                ),
                (
                    "run-1",
                    "eligibility_gate_unmet",
                    None,
                    "m1",
                    "market_quote_stale_quote_age",
                    None,
                    (
                        "category=stale_quote_freshness;stale_reason_key=quote_age_exceeded;"
                        "stale_side=yes;stale_asset_ids=a1"
                    ),
                    now,
                ),
            ],
        )
    store.close()

    generator = DailyReportGenerator(db_path=db_path, export_dir=tmp_path)
    report = generator.generate(date=None, last_hours=24, run_id="run-1")
    recovery = report["recovery_diagnostics"]

    assert report["totals"]["market_stale_enter_count"] == 3
    assert report["totals"]["market_stale_recover_count"] == 2
    assert report["totals"]["avg_market_stale_duration_ms"] == pytest.approx(833.333, abs=0.01)
    assert report["totals"]["max_market_stale_duration_ms"] == pytest.approx(1200.0)
    assert report["totals"]["market_stale_universe_change_enter_count"] == 1

    stale_reason_map = {
        item["reason"]: int(item["count"]) for item in recovery["market_stale_reason_breakdown"]
    }
    assert stale_reason_map["quote_age_exceeded"] == 1
    assert stale_reason_map["no_recent_quote"] == 1
    assert stale_reason_map["leg_timestamp_mismatch"] == 1

    stale_side_map = {
        item["reason"]: int(item["count"]) for item in recovery["market_stale_side_breakdown"]
    }
    assert stale_side_map["yes"] == 1
    assert stale_side_map["no"] == 1
    assert stale_side_map["both"] == 1

    blocked_stale_map = {
        item["reason"]: int(item["count"])
        for item in recovery["market_ready_blocked_stale_reason_breakdown"]
    }
    assert blocked_stale_map["leg_timestamp_mismatch"] == 1

    gate_stale_map = {
        item["reason"]: int(item["count"])
        for item in recovery["eligibility_gate_stale_reason_breakdown"]
    }
    assert gate_stale_map["quote_age_exceeded"] == 1

    no_signal_stale_map = {
        item["reason"]: int(item["count"]) for item in recovery["no_signal_stale_reason_breakdown"]
    }
    assert no_signal_stale_map["quote_age_exceeded"] == 1
    assert no_signal_stale_map["no_recent_quote"] == 1

    assert recovery["top_long_stale_markets"][0]["market_slug"] == "stale-market-1"
    assert recovery["top_repeated_stale_markets"][0]["enter_count"] == 2
    assert any(
        item["side"] == "yes" and item["asset_id"] == "a1" for item in recovery["top_stale_legs"]
    )
    assert any(
        item["side"] == "no" and item["asset_id"] == "a2" for item in recovery["top_stale_legs"]
    )


def test_daily_report_includes_chronic_stale_and_missing_book_drilldown(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path=db_path)
    now = datetime.now(tz=UTC).isoformat()
    store.upsert_market(
        BinaryMarket(
            market_id="m1",
            question="Will chronic stale market 1 resolve yes?",
            slug="chronic-market-1",
            category="test",
            end_time=None,
            condition_id="cond-m1",
            yes_token_id="a1",
            no_token_id="a2",
            raw={},
        ),
        updated_at_iso=now,
    )
    store.upsert_market(
        BinaryMarket(
            market_id="m2",
            question="Will chronic stale market 2 resolve yes?",
            slug="chronic-market-2",
            category="test",
            end_time=None,
            condition_id="cond-m2",
            yes_token_id="b1",
            no_token_id="b2",
            raw={},
        ),
        updated_at_iso=now,
    )

    with store.conn:
        store.conn.executemany(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    "run-1",
                    "low_quality_runtime_exclusion_active_count",
                    1.0,
                    "m1:stage=excluded",
                    now,
                ),
                (
                    "run-1",
                    "low_quality_runtime_exclusion_enter_count",
                    1.0,
                    "m1:stage=excluded",
                    now,
                ),
                (
                    "run-1",
                    "low_quality_runtime_exclusion_cleared_count",
                    1.0,
                    "m1:stage=excluded",
                    now,
                ),
                (
                    "run-1",
                    "low_quality_reintroduced_for_floor_count",
                    1.0,
                    "m1:watched_floor_backfill_low_quality_relaxed",
                    now,
                ),
                (
                    "run-1",
                    "current_watched_low_quality_excluded_count",
                    1.0,
                    "m1:watched_floor_backfill_low_quality_relaxed",
                    now,
                ),
                (
                    "run-1",
                    "watched_floor_shortfall_due_to_low_quality_exclusion",
                    0.0,
                    "floor=2;watched=2;shortfall=0;excluded_not_watched=0;low_quality_relax_enabled=1",
                    now,
                ),
                (
                    "run-1",
                    "low_quality_reintroduced_reason:watched_floor_backfill_low_quality_relaxed",
                    1.0,
                    "reintroduced_markets=1",
                    now,
                ),
                (
                    "run-1",
                    "watched_low_quality_reason:watched_floor_backfill_low_quality_relaxed",
                    1.0,
                    "watched_low_quality_markets=1",
                    now,
                ),
                ("run-1", "chronic_stale_excluded_market_count", 1.0, "m1", now),
                ("run-1", "chronic_stale_exclusion_active_count", 1.0, "m1", now),
                (
                    "run-1",
                    "chronic_stale_reintroduced_for_floor_count",
                    1.0,
                    "m1:watched_floor_backfill_chronic_relaxed",
                    now,
                ),
                (
                    "run-1",
                    "chronic_stale_reintroduced_market_count",
                    1.0,
                    "m1:watched_floor_backfill_chronic_relaxed",
                    now,
                ),
                (
                    "run-1",
                    "watched_chronic_stale_excluded_market_count",
                    1.0,
                    "m1:watched_floor_backfill_chronic_relaxed",
                    now,
                ),
                (
                    "run-1",
                    "chronic_stale_exclusion_extended_count",
                    1.0,
                    "m1:repeated_stale_enters",
                    now,
                ),
                ("run-1", "chronic_stale_exclusion_avg_active_age_ms", 91000.0, "m1", now),
                (
                    "run-1",
                    "chronic_stale_exclusion_long_active_market_count",
                    1.0,
                    "m1:91000.0",
                    now,
                ),
            ],
        )
        store.conn.executemany(
            """
            INSERT INTO diagnostics_events
            (run_id, event_name, asset_id, market_id, reason, latency_ms, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "run-1",
                    "market_low_quality_runtime_exclusion_entered",
                    None,
                    "m1",
                    "low_quality_runtime",
                    None,
                    "runtime_exclusion_reason=stage=excluded;market_slug=chronic-market-1",
                    now,
                ),
                (
                    "run-1",
                    "market_low_quality_runtime_exclusion_cleared",
                    None,
                    "m1",
                    "low_quality_runtime",
                    None,
                    "cleared_reason=quality_recovered_or_universe_requalified;market_slug=chronic-market-1",
                    now,
                ),
                (
                    "run-1",
                    "market_low_quality_reintroduced_for_floor",
                    None,
                    "m1",
                    "watched_floor_backfill_low_quality_relaxed",
                    None,
                    "market_slug=chronic-market-1",
                    now,
                ),
                (
                    "run-1",
                    "market_chronic_stale_exclusion_entered",
                    None,
                    "m1",
                    "repeated_stale_enters",
                    None,
                    "stale_enter_count=12;max_stale_duration_ms=450000;market_slug=chronic-market-1",
                    now,
                ),
                (
                    "run-1",
                    "market_chronic_stale_exclusion_cleared",
                    None,
                    "m1",
                    "repeated_stale_enters",
                    120000.0,
                    "cleared_reason=cooldown_elapsed;market_slug=chronic-market-1",
                    now,
                ),
                (
                    "run-1",
                    "market_chronic_stale_exclusion_extended",
                    None,
                    "m1",
                    "repeated_stale_enters",
                    60000.0,
                    "extension_reason=cooldown_refreshed_by_new_chronic_signal",
                    now,
                ),
                (
                    "run-1",
                    "market_chronic_stale_reintroduced_for_floor",
                    None,
                    "m1",
                    "watched_floor_backfill_chronic_relaxed",
                    None,
                    "market_slug=chronic-market-1",
                    now,
                ),
                (
                    "run-1",
                    "missing_book_state_detected",
                    "a1",
                    "m1",
                    "quote_missing_after_resync",
                    None,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "missing_book_state_detected",
                    "a1",
                    "m1",
                    "quote_missing_after_resync",
                    None,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "missing_book_state_detected",
                    "b2",
                    "m2",
                    "book_recovering",
                    None,
                    "",
                    now,
                ),
            ],
        )
    store.close()

    report = DailyReportGenerator(db_path=db_path, export_dir=tmp_path).generate(
        date=None,
        last_hours=24,
        run_id="run-1",
    )
    recovery = report["recovery_diagnostics"]

    assert report["totals"]["chronic_stale_excluded_market_count"] == 1
    assert report["totals"]["chronic_stale_exclusion_active_count"] == 1
    assert report["totals"]["low_quality_runtime_exclusion_active_count"] == 1
    assert report["totals"]["low_quality_runtime_exclusion_enter_count"] == 1
    assert report["totals"]["low_quality_runtime_exclusion_cleared_count"] == 1
    assert report["totals"]["low_quality_reintroduced_for_floor_count"] == 1
    assert report["totals"]["current_watched_low_quality_excluded_count"] == 1
    assert report["totals"]["watched_floor_shortfall_due_to_low_quality_exclusion"] == 0
    assert report["totals"]["chronic_stale_exclusion_enter_count"] == 1
    assert report["totals"]["chronic_stale_exclusion_extended_count"] == 1
    assert report["totals"]["chronic_stale_exclusion_cleared_count"] == 1
    assert report["totals"]["chronic_stale_reintroduced_for_floor_count"] == 1
    assert report["totals"]["watched_chronic_stale_excluded_market_count"] == 1
    assert recovery["low_quality_reason_breakdown"][0]["reason"] == "low_quality_runtime"
    assert (
        recovery["low_quality_reintroduced_reason_breakdown"][0]["reason"]
        == "watched_floor_backfill_low_quality_relaxed"
    )
    assert (
        recovery["watched_low_quality_reason_breakdown"][0]["reason"]
        == "watched_floor_backfill_low_quality_relaxed"
    )
    assert recovery["chronic_stale_reason_breakdown"][0]["reason"] == "repeated_stale_enters"
    assert (
        recovery["chronic_stale_extension_reason_breakdown"][0]["reason"] == "repeated_stale_enters"
    )
    assert recovery["top_reintroduced_low_quality_markets"][0]["market_id"] == "m1"
    assert recovery["top_watched_low_quality_excluded_markets"][0]["market_id"] == "m1"
    assert recovery["top_chronic_stale_markets"][0]["market_slug"] == "chronic-market-1"
    assert recovery["top_reintroduced_chronic_stale_markets"][0]["market_id"] == "m1"
    assert recovery["top_long_active_chronic_stale_markets"][0]["market_id"] == "m1"
    assert recovery["top_quote_missing_after_resync_assets"][0]["asset_id"] == "a1"
    assert recovery["top_quote_missing_after_resync_assets"][0]["side"] == "yes"
    assert recovery["top_repeated_missing_book_markets"][0]["market_id"] == "m1"
    assert (
        recovery["top_repeated_missing_book_markets"][0]["dominant_reason"]
        == "quote_missing_after_resync"
    )
