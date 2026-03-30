from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.dashboard.data_loader import DashboardDataLoader, resolve_window
from src.storage.sqlite_store import SQLiteStore


def _seed_dashboard_data(db_path: Path) -> None:
    store = SQLiteStore(db_path=db_path)
    now = datetime.now(tz=UTC).isoformat()
    with store.conn:
        store.conn.execute(
            """
            INSERT INTO signals (
              signal_id, run_id, market_id, slug,
              ask_yes, ask_no, sum_ask, threshold, reason, detected_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("sig-1", "run-1", "m1", "market-1", 0.45, 0.50, 0.95, 0.98, "edge", now),
        )
        store.conn.execute(
            """
            INSERT INTO fills (
              fill_id, run_id, order_id, signal_id, market_id,
              token_id, filled_qty, fill_price, fee, filled_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("fill-1", "run-1", "order-1", "sig-1", "m1", "yes1", 1.0, 0.46, 0.0, now),
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
                now,
                now,
                25.0,
                None,
                100.0,
                150.0,
                0.03,
                0.02,
                0.46,
                0.50,
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
              estimated_edge_at_signal, projected_matched_pnl, unmatched_inventory_mtm,
              total_projected_pnl, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("run-1", "sig-1", "m1", "market-1", 0.04, 0.02, 0.04, 0.0, 0.04, now),
        )
        store.conn.execute(
            """
            INSERT INTO resync_events (run_id, asset_id, reason, status, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("run-1", "yes1", "ws_connected", "ok", "resync_applied", now),
        )
        store.conn.execute(
            """
            INSERT INTO quotes (
              run_id, market_id, asset_id, side, best_bid, best_ask,
              best_bid_size, best_ask_size, quote_age_ms, tick_size, source,
              resync_reason, quote_time
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("run-1", "m1", "yes1", "yes", 0.44, 0.46, 10.0, 10.0, 100.0, 0.01, "ws", "", now),
        )
        store.conn.execute(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("run-1", "safe_mode_entered", 1.0, "scope=global;reason=all_assets_stale", now),
        )
        store.conn.execute(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("run-1", "no_signal_reason:edge_below_threshold", 1.0, "", now),
        )
        store.conn.execute(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("run-1", "universe_current_watched_markets", 5.0, "", now),
        )
        store.conn.execute(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("run-1", "universe_current_subscribed_assets", 10.0, "", now),
        )
        store.conn.execute(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("run-1", "universe_cumulative_watched_markets", 8.0, "", now),
        )
        store.conn.execute(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("run-1", "universe_cumulative_subscribed_assets", 16.0, "", now),
        )
        store.conn.execute(
            """
            INSERT INTO run_snapshots (
              run_id, active_markets, stale_assets, total_signals, total_fills,
              open_unmatched_inventory, cumulative_projected_pnl, safe_mode_active,
              safe_mode_reason, resync_cumulative_count, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("run-1", 5, 0, 1, 1, 0.0, 0.04, 0, "", 1, now),
        )
    store.close()


def test_dashboard_loader_empty_state(tmp_path: Path) -> None:
    loader = DashboardDataLoader(db_path=tmp_path / "missing.db")
    window = resolve_window(last_hours=24)

    overview = loader.load_overview(window=window, run_id=None)
    recovery = loader.load_recovery_diagnostics(window=window, run_id=None)
    assert loader.has_database() is False
    assert overview["total_signals"] == 0.0
    assert overview["total_fills"] == 0.0
    assert overview["ready_market_ratio"] == 0.0
    assert overview["eligible_market_ratio"] == 0.0
    assert overview["min_watched_markets_floor"] == 0.0
    assert recovery["recovery_resync_started_count"] == 0.0
    assert recovery["recovery_first_quote_success_rate"] == 0.0
    assert recovery["top_stale_assets"].empty
    assert loader.load_run_ids() == []
    assert loader.load_pnl_timeseries(window=window, run_id=None).empty


def test_dashboard_loader_core_metrics_with_run_id_filter(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    _seed_dashboard_data(db_path)
    loader = DashboardDataLoader(db_path=db_path)
    window = resolve_window(
        start=datetime.now(tz=UTC) - timedelta(hours=1),
        end=datetime.now(tz=UTC) + timedelta(hours=1),
    )

    overview = loader.load_overview(window=window, run_id="run-1")
    assert overview["total_signals"] == 1.0
    assert overview["total_fills"] == 1.0
    assert overview["fill_rate"] == 1.0
    assert overview["safe_mode_count"] == 1.0
    assert overview["global_safe_mode_count"] == 1.0
    assert overview["market_block_count"] == 0.0
    assert overview["asset_block_count"] == 0.0
    assert overview["total_block_events"] == 1.0
    assert overview["market_universe_changed_count"] == 0.0
    assert overview["market_universe_change_events"] == 0.0
    assert overview["resync_due_to_universe_change"] == 0.0
    assert overview["ws_connected_events"] == 0.0
    assert overview["ws_reconnect_events"] == 0.0
    assert overview["book_not_ready_count"] == 0.0
    assert overview["quote_too_old_count"] == 0.0
    assert overview["book_recovering_count"] == 0.0
    assert overview["market_not_ready_count"] == 0.0
    assert overview["market_probation_count"] == 0.0
    assert overview["connection_recovering_count"] == 0.0
    assert overview["market_recovering_count"] == 0.0
    assert overview["no_initial_book_count"] == 0.0
    assert overview["asset_warming_up_count"] == 0.0
    assert overview["ready_market_ratio"] == 0.0
    assert overview["eligible_market_ratio"] == 0.0
    assert overview["min_watched_markets_floor"] == 0.0
    assert overview["low_quality_market_count"] == 0.0
    assert overview["low_quality_runtime_excluded_count"] == 0.0
    assert overview["watched_markets_current"] == 5.0
    assert overview["subscribed_assets_current"] == 10.0

    breakdowns = loader.load_reason_breakdowns(window=window, run_id="run-1")
    assert not breakdowns["resyncs_by_reason"].empty
    assert str(breakdowns["resyncs_by_reason"].iloc[0]["reason"]) == "ws_connected"
    assert not breakdowns["safe_mode_by_scope_reason"].empty
    assert str(breakdowns["safe_mode_by_scope_reason"].iloc[0]["scope"]) == "global"
    assert breakdowns["ws_connected_reasons"].empty
    assert breakdowns["ws_reconnect_reasons"].empty

    market_diag = loader.load_market_diagnostics(window=window, run_id="run-1", market_slug=None)
    assert not market_diag.empty
    assert str(market_diag.iloc[0]["market_slug"]) == "market-1"

    asset_diag = loader.load_asset_diagnostics(window=window, run_id="run-1")
    assert not asset_diag.empty
    assert str(asset_diag.iloc[0]["asset_id"]) == "yes1"

    resync_ts = loader.load_resync_timeseries(window=window, run_id="run-1", bucket_minutes=5)
    assert not resync_ts.empty
    assert "reason" in resync_ts.columns

    block_ts = loader.load_block_timeseries(window=window, run_id="run-1", bucket_minutes=5)
    assert not block_ts.empty
    assert "block_type" in block_ts.columns


def test_resolve_window_last_hours_is_relative_to_now(monkeypatch) -> None:
    fixed_now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz: object | None = None) -> datetime:
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

    monkeypatch.setattr("src.dashboard.data_loader.datetime", FixedDateTime)
    window = resolve_window(last_hours=6)
    start = datetime.fromisoformat(window.start_iso)
    end = datetime.fromisoformat(window.end_iso)
    delta = end - start
    assert delta == timedelta(hours=6)


def test_dashboard_loader_overview_includes_market_state_and_book_not_ready_prefix_counts(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    _seed_dashboard_data(db_path)
    store = SQLiteStore(db_path=db_path)
    now = datetime.now(tz=UTC).isoformat()
    with store.conn:
        store.conn.executemany(
            """
            INSERT INTO metrics (run_id, metric_name, metric_value, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("run-1", "no_signal_reason:book_not_ready_insufficient_updates", 1.0, "", now),
                ("run-1", "market_state_ready_count", 6.0, "", now),
                ("run-1", "market_state_recovering_count", 2.0, "", now),
                ("run-1", "market_state_stale_no_recent_quote_count", 1.0, "", now),
                ("run-1", "market_state_stale_quote_age_count", 1.0, "", now),
                ("run-1", "market_state_eligible_count", 4.0, "", now),
                ("run-1", "market_state_ready_ratio", 0.45, "", now),
                ("run-1", "market_state_eligible_ratio", 0.25, "", now),
                ("run-1", "market_state_blocked_count", 1.0, "", now),
                ("run-1", "eligibility_gate_reason:connection_recovering", 2.0, "", now),
                ("run-1", "eligibility_gate_reason:book_recovering", 3.0, "", now),
                ("run-1", "eligibility_gate_reason:stale_quote_freshness", 1.0, "", now),
                ("run-1", "eligibility_gate_reason:blocked", 0.0, "", now),
                ("run-1", "eligibility_gate_reason:probation", 0.0, "", now),
                ("run-1", "eligibility_gate_reason:low_quality_runtime_excluded", 4.0, "", now),
                ("run-1", "eligibility_gate_reason:other_readiness_gate", 5.0, "", now),
            ],
        )
    store.close()

    loader = DashboardDataLoader(db_path=db_path)
    window = resolve_window(last_hours=24)
    overview = loader.load_overview(window=window, run_id="run-1")

    assert overview["book_not_ready_count"] == 1.0
    assert overview["ready_market_count"] == 6.0
    assert overview["recovering_market_count"] == 2.0
    assert overview["stale_market_count"] == 2.0
    assert overview["eligible_market_count"] == 4.0
    assert overview["ready_market_ratio"] == 0.45
    assert overview["eligible_market_ratio"] == 0.25
    assert overview["blocked_market_count"] == 1.0
    assert overview["eligibility_gate_connection_recovering_count"] == 2.0
    assert overview["eligibility_gate_book_recovering_count"] == 3.0
    assert overview["eligibility_gate_stale_quote_freshness_count"] == 1.0
    assert overview["eligibility_gate_low_quality_runtime_excluded_count"] == 4.0
    assert overview["eligibility_gate_other_readiness_gate_count"] == 5.0


def test_dashboard_loader_recovery_diagnostics_summary(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    _seed_dashboard_data(db_path)
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
                    "book_ready_after_resync",
                    "a1",
                    "m1",
                    "missing_book_state",
                    250.0,
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
                (
                    "run-1",
                    "market_ready_after_recovery",
                    None,
                    "m1",
                    "missing_book_state",
                    900.0,
                    "",
                    now,
                ),
                (
                    "run-1",
                    "first_quote_after_resync_blocked",
                    "a2",
                    "m1",
                    "connection_recovering",
                    120.0,
                    "recovery_reason=missing_book_state",
                    now,
                ),
                (
                    "run-1",
                    "book_ready_after_resync_blocked",
                    "a2",
                    "m1",
                    "book_recovering",
                    80.0,
                    "recovery_reason=missing_book_state",
                    now,
                ),
                (
                    "run-1",
                    "market_ready_after_recovery_blocked",
                    None,
                    "m1",
                    "market_not_ready",
                    70.0,
                    "recovery_reason=missing_book_state",
                    now,
                ),
                (
                    "run-1",
                    "eligibility_gate_unmet",
                    None,
                    "m1",
                    "book_not_ready",
                    None,
                    "category=other_readiness_gate",
                    now,
                ),
                ("run-1", "stale_asset_detected", "a1", "m1", "stale_asset", None, "", now),
                ("run-1", "stale_asset_detected", "a1", "m1", "stale_asset", None, "", now),
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

    loader = DashboardDataLoader(db_path=db_path)
    window = resolve_window(last_hours=24)
    recovery = loader.load_recovery_diagnostics(window=window, run_id="run-1")

    assert recovery["recovery_resync_started_count"] == 2.0
    assert recovery["recovery_first_quote_success_count"] == 1.0
    assert recovery["recovery_book_ready_success_count"] == 1.0
    assert recovery["recovery_market_ready_success_count"] == 1.0
    assert recovery["recovery_first_quote_success_rate"] == 0.5
    assert recovery["recovery_market_ready_success_rate"] == 0.5
    assert recovery["recovery_first_quote_blocked_count"] == 1.0
    assert recovery["recovery_book_ready_blocked_count"] == 1.0
    assert recovery["recovery_market_ready_blocked_count"] == 1.0
    assert recovery["avg_resync_to_first_quote_latency_ms"] == 100.0
    assert recovery["avg_recovery_to_market_ready_latency_ms"] == 900.0
    assert not recovery["first_quote_blocked_reasons"].empty
    assert str(recovery["first_quote_blocked_reasons"].iloc[0]["reason"]) == "connection_recovering"
    assert not recovery["eligibility_gate_unmet_reasons"].empty
    top_stale_assets = recovery["top_stale_assets"]
    assert not top_stale_assets.empty
    assert str(top_stale_assets.iloc[0]["asset_id"]) == "a1"
    top_slow_markets = recovery["top_recovery_slow_markets"]
    assert not top_slow_markets.empty
    assert str(top_slow_markets.iloc[0]["market_id"]) == "m1"
