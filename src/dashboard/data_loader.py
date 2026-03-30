from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

from src.utils.stale_diagnostics import (
    extract_detail_value,
    normalize_stale_reason_key,
    normalize_stale_side,
    parse_kv_details,
    stale_reason_key_from_reason_and_details,
)


@dataclass(slots=True)
class DashboardWindow:
    start_iso: str
    end_iso: str


STALE_DURATION_EVENT_NAMES: tuple[str, ...] = (
    "market_stale_recovered",
    "market_stale_episode_closed",
)


def resolve_window(
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    last_hours: int = 24,
) -> DashboardWindow:
    resolved_end = end or datetime.now(tz=UTC)
    if resolved_end.tzinfo is None:
        resolved_end = resolved_end.replace(tzinfo=UTC)
    resolved_end = resolved_end.astimezone(UTC) + timedelta(microseconds=1)

    if start is None:
        start = resolved_end - timedelta(hours=max(1, int(last_hours)))
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    start = start.astimezone(UTC)
    return DashboardWindow(start_iso=start.isoformat(), end_iso=resolved_end.isoformat())


class DashboardDataLoader:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def has_database(self) -> bool:
        return self.db_path.exists()

    def load_run_ids(self) -> list[str]:
        if not self.has_database():
            return []
        query = """
        SELECT DISTINCT run_id
        FROM (
          SELECT run_id FROM run_summaries
          UNION ALL
          SELECT run_id FROM metrics
          UNION ALL
          SELECT run_id FROM signals
        )
        WHERE run_id IS NOT NULL AND run_id != ''
        ORDER BY run_id DESC
        """
        with self._connect() as conn:
            if conn is None:
                return []
            rows = conn.execute(query).fetchall()
        return [str(row[0]) for row in rows]

    def load_overview(self, *, window: DashboardWindow, run_id: str | None) -> dict[str, float]:
        if not self.has_database():
            return self._empty_overview()
        with self._connect() as conn:
            if conn is None:
                return self._empty_overview()
            total_signals = self._count(
                conn=conn,
                table="signals",
                time_column="detected_at",
                window=window,
                run_id=run_id,
            )
            total_fills = self._count(
                conn=conn,
                table="fills",
                time_column="filled_at",
                window=window,
                run_id=run_id,
            )
            signals_with_fill = self._count_distinct_signal_with_fill(
                conn=conn,
                window=window,
                run_id=run_id,
            )
            matched_fill_signals = self._count_matched_fill_signals(
                conn=conn,
                window=window,
                run_id=run_id,
            )
            global_safe_mode_count = self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="safe_mode_entered",
            )
            market_block_count = self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="safe_mode_market_block_started",
            )
            market_block_active_count = self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="safe_mode_market_block_active",
            )
            market_block_cleared_count = self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="safe_mode_market_block_cleared",
            )
            market_universe_changed_count = self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="market_universe_changed",
            )
            ws_connected_events = self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="ws_connected_event",
            )
            ws_reconnect_events = self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="ws_reconnect_event",
            )
            resync_due_to_universe_change = self._count_resync_reason(
                conn=conn,
                window=window,
                run_id=run_id,
                reason="market_universe_changed",
            )
            asset_block_count = self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="safe_mode_asset_blocked",
            )
            book_not_ready_count = self._count_metric_like(
                conn=conn,
                window=window,
                run_id=run_id,
                pattern="no_signal_reason:book_not_ready%",
            )
            quote_too_old_count = self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="no_signal_reason:quote_too_old",
            )
            market_quote_stale_count = self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="no_signal_reason:market_quote_stale",
            )
            market_quote_stale_count += self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="no_signal_reason:market_quote_stale_no_recent_quote",
            )
            market_quote_stale_count += self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="no_signal_reason:market_quote_stale_recovery",
            )
            market_quote_stale_count += self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="no_signal_reason:market_quote_stale_quote_age",
            )
            book_recovering_count = self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="no_signal_reason:book_recovering",
            )
            connection_recovering_count = self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="no_signal_reason:connection_recovering",
            )
            market_recovering_count = self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="no_signal_reason:market_recovering",
            )
            market_not_ready_count = self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="no_signal_reason:market_not_ready",
            )
            market_probation_count = self._count_metric(
                conn=conn,
                window=window,
                run_id=run_id,
                name="no_signal_reason:market_probation",
            )
            ready_market_count = self._latest_metric_value(
                conn=conn,
                window=window,
                run_id=run_id,
                metric_name="market_state_ready_count",
            )
            recovering_market_count = self._latest_metric_value(
                conn=conn,
                window=window,
                run_id=run_id,
                metric_name="market_state_recovering_count",
            )
            stale_market_count = self._latest_metric_value(
                conn=conn,
                window=window,
                run_id=run_id,
                metric_name="market_state_stale_no_recent_quote_count",
            ) + self._latest_metric_value(
                conn=conn,
                window=window,
                run_id=run_id,
                metric_name="market_state_stale_quote_age_count",
            )
            eligible_market_count = self._latest_metric_value(
                conn=conn,
                window=window,
                run_id=run_id,
                metric_name="market_state_eligible_count",
            )
            blocked_market_count = self._latest_metric_value(
                conn=conn,
                window=window,
                run_id=run_id,
                metric_name="market_state_blocked_count",
            )
            min_watched_markets_floor = self._latest_metric_value(
                conn=conn,
                window=window,
                run_id=run_id,
                metric_name="universe_min_watched_markets_floor",
            )
            low_quality_market_count = self._latest_metric_value(
                conn=conn,
                window=window,
                run_id=run_id,
                metric_name="low_quality_market_count",
            )
            low_quality_runtime_excluded_count = self._latest_metric_value(
                conn=conn,
                window=window,
                run_id=run_id,
                metric_name="low_quality_runtime_excluded_count",
            )
            ready_market_ratio = self._latest_metric_float_value(
                conn=conn,
                window=window,
                run_id=run_id,
                metric_name="market_state_ready_ratio",
            )
            eligible_market_ratio = self._latest_metric_float_value(
                conn=conn,
                window=window,
                run_id=run_id,
                metric_name="market_state_eligible_ratio",
            )
            eligibility_gate_breakdown = self._latest_metric_prefix_breakdown(
                conn=conn,
                window=window,
                run_id=run_id,
                metric_prefix="eligibility_gate_reason:",
            )
            eligibility_gate_reason_map = {
                str(item.get("reason", "")): int(round(float(item.get("count", 0.0))))
                for item in eligibility_gate_breakdown
            }
            resync_count = self._count(
                conn=conn,
                table="resync_events",
                time_column="created_at",
                window=window,
                run_id=run_id,
            )
            projected_matched_pnl, unmatched_inventory_mtm, total_projected_pnl = self._sum_pnl(
                conn=conn,
                window=window,
                run_id=run_id,
            )
            universe = self._universe_overview(conn=conn, window=window, run_id=run_id)

        fill_rate = signals_with_fill / total_signals if total_signals > 0 else 0.0
        matched_fill_rate = matched_fill_signals / total_signals if total_signals > 0 else 0.0
        watched_markets_current = float(universe["current_watched_markets"])
        return {
            "total_signals": float(total_signals),
            "total_fills": float(total_fills),
            "fill_rate": fill_rate,
            "matched_fill_rate": matched_fill_rate,
            "safe_mode_count": float(global_safe_mode_count),
            "global_safe_mode_count": float(global_safe_mode_count),
            "market_block_count": float(market_block_count),
            "market_block_active_count": float(market_block_active_count),
            "market_block_cleared_count": float(market_block_cleared_count),
            "asset_block_count": float(asset_block_count),
            "total_block_events": float(
                global_safe_mode_count + market_block_count + asset_block_count
            ),
            "market_universe_changed_count": float(market_universe_changed_count),
            "market_universe_change_events": float(market_universe_changed_count),
            "resync_due_to_universe_change": float(resync_due_to_universe_change),
            "ws_connected_events": float(ws_connected_events),
            "ws_reconnect_events": float(ws_reconnect_events),
            "book_not_ready_count": float(book_not_ready_count),
            "quote_too_old_count": float(quote_too_old_count),
            "market_quote_stale_count": float(market_quote_stale_count),
            "book_recovering_count": float(book_recovering_count),
            "market_not_ready_count": float(market_not_ready_count),
            "market_probation_count": float(market_probation_count),
            "connection_recovering_count": float(connection_recovering_count),
            "market_recovering_count": float(market_recovering_count),
            "ready_market_count": float(ready_market_count),
            "ready_market_ratio": float(ready_market_ratio),
            "recovering_market_count": float(recovering_market_count),
            "stale_market_count": float(stale_market_count),
            "eligible_market_count": float(eligible_market_count),
            "eligible_market_ratio": float(eligible_market_ratio),
            "blocked_market_count": float(blocked_market_count),
            "eligibility_gate_connection_recovering_count": float(
                eligibility_gate_reason_map.get("connection_recovering", 0)
            ),
            "eligibility_gate_book_recovering_count": float(
                eligibility_gate_reason_map.get("book_recovering", 0)
            ),
            "eligibility_gate_stale_quote_freshness_count": float(
                eligibility_gate_reason_map.get("stale_quote_freshness", 0)
            ),
            "eligibility_gate_blocked_count": float(eligibility_gate_reason_map.get("blocked", 0)),
            "eligibility_gate_probation_count": float(
                eligibility_gate_reason_map.get("probation", 0)
            ),
            "eligibility_gate_low_quality_runtime_excluded_count": float(
                eligibility_gate_reason_map.get("low_quality_runtime_excluded", 0)
            ),
            "eligibility_gate_other_readiness_gate_count": float(
                eligibility_gate_reason_map.get("other_readiness_gate", 0)
            ),
            "min_watched_markets_floor": float(min_watched_markets_floor),
            "low_quality_market_count": float(low_quality_market_count),
            "low_quality_runtime_excluded_count": float(low_quality_runtime_excluded_count),
            "no_initial_book_count": float(
                self._sum_metric(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    name="missing_book_state_reason:no_initial_book",
                )
            ),
            "asset_warming_up_count": float(
                self._sum_metric(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    name="missing_book_state_reason:asset_warming_up",
                )
            ),
            "resync_count": float(resync_count),
            "projected_matched_pnl": projected_matched_pnl,
            "unmatched_inventory_mtm": unmatched_inventory_mtm,
            "total_projected_pnl": total_projected_pnl,
            "watched_markets_current": watched_markets_current,
            "watched_markets_cumulative": float(universe["cumulative_watched_markets"]),
            "subscribed_assets_current": float(universe["current_subscribed_assets"]),
            "subscribed_assets_cumulative": float(universe["cumulative_subscribed_assets"]),
        }

    def load_run_summaries(self, *, window: DashboardWindow, run_id: str | None) -> pd.DataFrame:
        if not self.has_database():
            return pd.DataFrame()
        query = """
        SELECT
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
        FROM run_summaries
        WHERE ended_at >= ? AND started_at < ?
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        query += " ORDER BY ended_at DESC"
        return self._query_df(query=query, params=params)

    def load_reason_breakdowns(
        self,
        *,
        window: DashboardWindow,
        run_id: str | None,
    ) -> dict[str, pd.DataFrame]:
        no_signal_reasons = self._query_df(
            query=self._apply_run_filter(
                """
                SELECT metric_name, COUNT(*) AS count
                FROM metrics
                WHERE created_at >= ? AND created_at < ?
                  AND metric_name LIKE 'no_signal_reason:%'
                """,
                run_id=run_id,
            )
            + " GROUP BY metric_name ORDER BY count DESC",
            params=self._window_params(window=window, run_id=run_id),
        )
        if no_signal_reasons.empty:
            no_signal_reasons["reason"] = pd.Series(dtype="string")
        else:
            no_signal_reasons["reason"] = (
                no_signal_reasons["metric_name"].astype(str).str.split(":", n=1).str[-1]
            )

        missing_book_reasons = self._query_df(
            query=self._apply_run_filter(
                """
                SELECT metric_name, SUM(metric_value) AS count
                FROM metrics
                WHERE created_at >= ? AND created_at < ?
                  AND metric_name LIKE 'missing_book_state_reason:%'
                """,
                run_id=run_id,
            )
            + " GROUP BY metric_name ORDER BY count DESC",
            params=self._window_params(window=window, run_id=run_id),
        )
        if missing_book_reasons.empty:
            missing_book_reasons["reason"] = pd.Series(dtype="string")
        else:
            missing_book_reasons["reason"] = (
                missing_book_reasons["metric_name"].astype(str).str.split(":", n=1).str[-1]
            )
        eligibility_breakdown_rows: list[dict[str, float | str]] = []
        if self.has_database():
            with self._connect() as conn:
                if conn is not None:
                    eligibility_breakdown_rows = self._latest_metric_prefix_breakdown(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                        metric_prefix="eligibility_gate_reason:",
                    )
        eligibility_breakdown = pd.DataFrame(
            eligibility_breakdown_rows,
            columns=["reason", "count"],
        )

        return {
            "resyncs_by_reason": self._query_df(
                query=self._apply_run_filter(
                    """
                    SELECT reason, COUNT(*) AS count
                    FROM resync_events
                    WHERE created_at >= ? AND created_at < ?
                    """,
                    run_id=run_id,
                )
                + " GROUP BY reason ORDER BY count DESC",
                params=self._window_params(window=window, run_id=run_id),
            ),
            "no_signal_reasons": no_signal_reasons,
            "missing_book_state_reasons": missing_book_reasons,
            "safe_mode_by_scope_reason": self._safe_mode_by_scope_reason(
                window=window,
                run_id=run_id,
            ),
            "ws_reconnect_reasons": self._metric_reason_breakdown(
                window=window,
                run_id=run_id,
                metric_name="ws_reconnect_event",
            ),
            "ws_connected_reasons": self._metric_reason_breakdown(
                window=window,
                run_id=run_id,
                metric_name="ws_connected_event",
            ),
            "eligibility_gate_breakdown": eligibility_breakdown,
        }

    def load_pnl_timeseries(self, *, window: DashboardWindow, run_id: str | None) -> pd.DataFrame:
        run_snapshot_frame = self._query_df(
            query=self._apply_run_filter(
                """
                SELECT
                  created_at AS timestamp,
                  cumulative_projected_pnl,
                  open_unmatched_inventory,
                  total_signals,
                  total_fills,
                  resync_cumulative_count
                FROM run_snapshots
                WHERE created_at >= ? AND created_at < ?
                """,
                run_id=run_id,
            )
            + " ORDER BY created_at ASC",
            params=self._window_params(window=window, run_id=run_id),
        )
        if not run_snapshot_frame.empty:
            return run_snapshot_frame
        return self._query_df(
            query=self._apply_run_filter(
                """
                SELECT
                  created_at AS timestamp,
                  total_projected_pnl AS cumulative_projected_pnl,
                  unmatched_inventory_mtm AS open_unmatched_inventory,
                  0 AS total_signals,
                  0 AS total_fills,
                  0 AS resync_cumulative_count
                FROM pnl_snapshots
                WHERE created_at >= ? AND created_at < ?
                """,
                run_id=run_id,
            )
            + " ORDER BY created_at ASC",
            params=self._window_params(window=window, run_id=run_id),
        )

    def load_resync_timeseries(
        self,
        *,
        window: DashboardWindow,
        run_id: str | None,
        bucket_minutes: int = 10,
    ) -> pd.DataFrame:
        frame = self._query_df(
            query=self._apply_run_filter(
                """
                SELECT created_at, reason
                FROM resync_events
                WHERE created_at >= ? AND created_at < ?
                """,
                run_id=run_id,
            ),
            params=self._window_params(window=window, run_id=run_id),
        )
        if frame.empty:
            return pd.DataFrame(columns=["timestamp", "reason", "count"])
        return self._bucket_categorical_counts(
            frame=frame,
            time_column="created_at",
            category_column="reason",
            bucket_minutes=bucket_minutes,
        )

    def load_block_timeseries(
        self,
        *,
        window: DashboardWindow,
        run_id: str | None,
        bucket_minutes: int = 10,
    ) -> pd.DataFrame:
        frame = self._query_df(
            query=self._apply_run_filter(
                """
                SELECT created_at, metric_name, metric_value
                FROM metrics
                WHERE created_at >= ? AND created_at < ?
                  AND metric_name IN (
                    'safe_mode_entered',
                    'safe_mode_market_block_started',
                    'safe_mode_asset_blocked'
                  )
                """,
                run_id=run_id,
            ),
            params=self._window_params(window=window, run_id=run_id),
        )
        if frame.empty:
            return pd.DataFrame(columns=["timestamp", "block_type", "count"])
        mapped = frame.copy()
        block_type_map = {
            "safe_mode_entered": "global_safe_mode",
            "safe_mode_market_block_started": "market_block",
            "safe_mode_asset_blocked": "asset_block",
        }
        mapped["block_type"] = mapped["metric_name"].map(block_type_map).fillna("other")
        return self._bucket_categorical_counts(
            frame=mapped,
            time_column="created_at",
            category_column="block_type",
            value_column="metric_value",
            bucket_minutes=bucket_minutes,
        )

    def load_no_signal_reason_timeseries(
        self,
        *,
        window: DashboardWindow,
        run_id: str | None,
        bucket_minutes: int = 10,
    ) -> pd.DataFrame:
        frame = self._query_df(
            query=self._apply_run_filter(
                """
                SELECT created_at, metric_name, metric_value
                FROM metrics
                WHERE created_at >= ? AND created_at < ?
                  AND metric_name LIKE 'no_signal_reason:%'
                """,
                run_id=run_id,
            ),
            params=self._window_params(window=window, run_id=run_id),
        )
        if frame.empty:
            return pd.DataFrame(columns=["timestamp", "reason", "count"])
        mapped = frame.copy()
        mapped["reason"] = mapped["metric_name"].astype(str).str.split(":", n=1).str[-1]
        return self._bucket_categorical_counts(
            frame=mapped,
            time_column="created_at",
            category_column="reason",
            value_column="metric_value",
            bucket_minutes=bucket_minutes,
        )

    def load_missing_book_reason_timeseries(
        self,
        *,
        window: DashboardWindow,
        run_id: str | None,
        bucket_minutes: int = 10,
    ) -> pd.DataFrame:
        frame = self._query_df(
            query=self._apply_run_filter(
                """
                SELECT created_at, metric_name, metric_value
                FROM metrics
                WHERE created_at >= ? AND created_at < ?
                  AND metric_name LIKE 'missing_book_state_reason:%'
                """,
                run_id=run_id,
            ),
            params=self._window_params(window=window, run_id=run_id),
        )
        if frame.empty:
            return pd.DataFrame(columns=["timestamp", "reason", "count"])
        mapped = frame.copy()
        mapped["reason"] = mapped["metric_name"].astype(str).str.split(":", n=1).str[-1]
        return self._bucket_categorical_counts(
            frame=mapped,
            time_column="created_at",
            category_column="reason",
            value_column="metric_value",
            bucket_minutes=bucket_minutes,
        )

    def load_market_diagnostics(
        self,
        *,
        window: DashboardWindow,
        run_id: str | None,
        market_slug: str | None,
    ) -> pd.DataFrame:
        signal_query = self._apply_run_filter(
            """
            SELECT slug AS market_slug, COUNT(*) AS signal_count
            FROM signals
            WHERE detected_at >= ? AND detected_at < ?
            """,
            run_id=run_id,
        )
        signal_params = self._window_params(window=window, run_id=run_id)
        if market_slug:
            signal_query += " AND slug = ?"
            signal_params.append(market_slug)
        signal_query += " GROUP BY slug"
        signals = self._query_df(query=signal_query, params=signal_params)

        execution_query = self._apply_run_filter(
            """
            SELECT
              market_slug,
              COUNT(*) AS execution_count,
              SUM(
                CASE WHEN reject_reason IS NOT NULL AND reject_reason != ''
                THEN 1 ELSE 0 END
              ) AS reject_count,
              SUM(COALESCE(matched_qty, 0)) AS matched_qty,
              SUM(COALESCE(unmatched_yes_qty, 0)) AS unmatched_yes_qty,
              SUM(COALESCE(unmatched_no_qty, 0)) AS unmatched_no_qty
            FROM execution_events
            WHERE completed_at >= ? AND completed_at < ?
            """,
            run_id=run_id,
        )
        execution_params = self._window_params(window=window, run_id=run_id)
        if market_slug:
            execution_query += " AND market_slug = ?"
            execution_params.append(market_slug)
        execution_query += " GROUP BY market_slug"
        execution = self._query_df(query=execution_query, params=execution_params)

        pnl_query = self._apply_run_filter(
            """
            SELECT
              market_slug,
              SUM(COALESCE(projected_matched_pnl, 0)) AS projected_matched_pnl,
              SUM(COALESCE(unmatched_inventory_mtm, 0)) AS unmatched_inventory_mtm,
              SUM(COALESCE(total_projected_pnl, 0)) AS total_projected_pnl
            FROM pnl_snapshots
            WHERE created_at >= ? AND created_at < ?
            """,
            run_id=run_id,
        )
        pnl_params = self._window_params(window=window, run_id=run_id)
        if market_slug:
            pnl_query += " AND market_slug = ?"
            pnl_params.append(market_slug)
        pnl_query += " GROUP BY market_slug"
        pnl = self._query_df(query=pnl_query, params=pnl_params)

        merged = signals.merge(execution, on="market_slug", how="outer").merge(
            pnl,
            on="market_slug",
            how="outer",
        )
        if merged.empty:
            return merged
        merged = merged.fillna(0)
        return merged.sort_values("signal_count", ascending=False)

    def load_asset_diagnostics(
        self,
        *,
        window: DashboardWindow,
        run_id: str | None,
    ) -> pd.DataFrame:
        base_query = self._apply_run_filter(
            """
            SELECT
              asset_id,
              COUNT(*) AS resync_count,
              SUM(
                CASE WHEN reason = 'missing_book_state' THEN 1 ELSE 0 END
              ) AS missing_book_resyncs,
              SUM(CASE WHEN reason = 'stale_asset' THEN 1 ELSE 0 END) AS stale_resyncs,
              SUM(CASE WHEN reason = 'idle_timeout' THEN 1 ELSE 0 END) AS idle_resyncs
            FROM resync_events
            WHERE created_at >= ? AND created_at < ?
            """,
            run_id=run_id,
        )
        query = base_query + " GROUP BY asset_id ORDER BY resync_count DESC"
        params = self._window_params(window=window, run_id=run_id)
        frame = self._query_df(query=query, params=params)
        if frame.empty:
            return frame
        quote_query = (
            self._apply_run_filter(
                """
            SELECT asset_id, MAX(quote_time) AS last_quote_time
            FROM quotes
            WHERE quote_time >= ? AND quote_time < ?
            """,
                run_id=run_id,
            )
            + " GROUP BY asset_id"
        )
        quotes = self._query_df(
            query=quote_query,
            params=self._window_params(window=window, run_id=run_id),
        )
        if quotes.empty:
            return frame
        return frame.merge(quotes, on="asset_id", how="left")

    def load_warmup_overview(
        self,
        *,
        window: DashboardWindow,
        run_id: str | None,
    ) -> dict[str, float]:
        if not self.has_database():
            return {
                "warmup_events": 0.0,
                "latest_warming_up_assets": 0.0,
            }
        with self._connect() as conn:
            if conn is None:
                return {
                    "warmup_events": 0.0,
                    "latest_warming_up_assets": 0.0,
                }
            count_query = self._apply_run_filter(
                """
                SELECT COUNT(*)
                FROM metrics
                WHERE created_at >= ? AND created_at < ?
                  AND metric_name = 'warming_up_asset_count'
                  AND metric_value > 0
                """,
                run_id=run_id,
            )
            params = self._window_params(window=window, run_id=run_id)
            warmup_events = int(conn.execute(count_query, params).fetchone()[0])
            latest_query = (
                self._apply_run_filter(
                    """
                    SELECT metric_value
                    FROM metrics
                    WHERE created_at >= ? AND created_at < ?
                      AND metric_name = 'warming_up_asset_count'
                    """,
                    run_id=run_id,
                )
                + " ORDER BY created_at DESC LIMIT 1"
            )
            latest_row = conn.execute(
                latest_query,
                self._window_params(window=window, run_id=run_id),
            ).fetchone()
        return {
            "warmup_events": float(warmup_events),
            "latest_warming_up_assets": float(latest_row[0]) if latest_row else 0.0,
        }

    def load_recovery_diagnostics(
        self,
        *,
        window: DashboardWindow,
        run_id: str | None,
    ) -> dict[str, object]:
        empty = self._empty_recovery_diagnostics()
        if not self.has_database():
            return empty
        with self._connect() as conn:
            if conn is None or not self._table_exists(conn, "diagnostics_events"):
                return empty
            try:
                resync_started_count = self._count_diagnostics_event(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="resync_started",
                )
                first_quote_success_count = self._count_diagnostics_event(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="first_quote_after_resync",
                )
                book_ready_success_count = self._count_diagnostics_event(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="book_ready_after_resync",
                )
                market_recovery_started_count = self._count_diagnostics_event(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="market_recovery_started",
                )
                market_ready_success_count = self._count_diagnostics_event(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="market_ready_after_recovery",
                )
                first_quote_blocked_count = self._count_diagnostics_event(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="first_quote_after_resync_blocked",
                )
                book_ready_blocked_count = self._count_diagnostics_event(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="book_ready_after_resync_blocked",
                )
                market_ready_blocked_count = self._count_diagnostics_event(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="market_ready_after_recovery_blocked",
                )
                first_avg_ms, first_max_ms = self._diagnostics_latency_stats(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="first_quote_after_resync",
                )
                book_avg_ms, book_max_ms = self._diagnostics_latency_stats(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="book_ready_after_resync",
                )
                market_avg_ms, market_max_ms = self._diagnostics_latency_stats(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="market_ready_after_recovery",
                )
                universe_resync_started_count = self._count_diagnostics_event_with_reason(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="resync_started",
                    reason="market_universe_changed",
                )
                universe_first_quote_success_count = self._count_diagnostics_event_with_reason(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="first_quote_after_resync",
                    reason="market_universe_changed",
                )
                universe_book_ready_success_count = self._count_diagnostics_event_with_reason(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="book_ready_after_resync",
                    reason="market_universe_changed",
                )
                universe_market_ready_success_count = self._count_diagnostics_event_with_reason(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="market_ready_after_recovery",
                    reason="market_universe_changed",
                )
                universe_first_quote_blocked_count = (
                    self._count_diagnostics_event_with_details_like(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                        event_name="first_quote_after_resync_blocked",
                        details_like="%recovery_reason=market_universe_changed%",
                    )
                )
                universe_book_ready_blocked_count = self._count_diagnostics_event_with_details_like(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="book_ready_after_resync_blocked",
                    details_like="%recovery_reason=market_universe_changed%",
                )
                universe_market_ready_blocked_count = (
                    self._count_diagnostics_event_with_details_like(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                        event_name="market_ready_after_recovery_blocked",
                        details_like="%recovery_reason=market_universe_changed%",
                    )
                )
                market_stale_enter_count = self._count_diagnostics_event(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="market_stale_entered",
                )
                market_stale_recover_count = self._count_diagnostics_event(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="market_stale_recovered",
                )
                stale_avg_ms, stale_max_ms = self._diagnostics_latency_stats_for_events(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_names=STALE_DURATION_EVENT_NAMES,
                )
                market_stale_universe_change_enter_count = (
                    self._count_diagnostics_event_with_details_like(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                        event_name="market_stale_entered",
                        details_like="%universe_change_related=1%",
                    )
                )
                market_stale_reason_breakdown = self._diagnostics_detail_breakdown_for_event(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="market_stale_entered",
                    detail_key="stale_reason_key",
                )
                market_stale_side_breakdown = self._diagnostics_detail_breakdown_for_event(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                    event_name="market_stale_entered",
                    detail_key="stale_side",
                )
                market_ready_blocked_stale_reason_breakdown = (
                    self._diagnostics_detail_breakdown_for_event(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                        event_name="market_ready_after_recovery_blocked",
                        detail_key="stale_reason_key",
                        reason_like="market_quote_stale%",
                    )
                )
                eligibility_gate_stale_reason_breakdown = (
                    self._diagnostics_detail_breakdown_for_event(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                        event_name="eligibility_gate_unmet",
                        detail_key="stale_reason_key",
                        details_like="%category=stale_quote_freshness%",
                    )
                )
                no_signal_stale_reason_breakdown = self._metric_stale_reason_breakdown(
                    conn=conn,
                    window=window,
                    run_id=run_id,
                )
                if market_stale_reason_breakdown.empty:
                    market_stale_reason_breakdown = no_signal_stale_reason_breakdown.copy()
                universe_change_market_ready_blocked_stale_reason_breakdown = (
                    self._diagnostics_detail_breakdown_for_event(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                        event_name="market_ready_after_recovery_blocked",
                        detail_key="stale_reason_key",
                        reason_like="market_quote_stale%",
                        details_like="%recovery_reason=market_universe_changed%",
                    )
                )
                return {
                    "recovery_resync_started_count": float(resync_started_count),
                    "recovery_first_quote_success_count": float(first_quote_success_count),
                    "recovery_book_ready_success_count": float(book_ready_success_count),
                    "recovery_market_ready_success_count": float(market_ready_success_count),
                    "recovery_market_recovery_started_count": float(market_recovery_started_count),
                    "recovery_first_quote_blocked_count": float(first_quote_blocked_count),
                    "recovery_book_ready_blocked_count": float(book_ready_blocked_count),
                    "recovery_market_ready_blocked_count": float(market_ready_blocked_count),
                    "recovery_first_quote_success_rate": (
                        float(first_quote_success_count / resync_started_count)
                        if resync_started_count > 0
                        else 0.0
                    ),
                    "recovery_book_ready_success_rate": (
                        float(book_ready_success_count / resync_started_count)
                        if resync_started_count > 0
                        else 0.0
                    ),
                    "recovery_market_ready_success_rate": (
                        float(market_ready_success_count / resync_started_count)
                        if resync_started_count > 0
                        else 0.0
                    ),
                    "recovery_stage_denominator": "resync_started_count",
                    "avg_resync_to_first_quote_latency_ms": first_avg_ms,
                    "max_resync_to_first_quote_latency_ms": first_max_ms,
                    "avg_resync_to_book_ready_latency_ms": book_avg_ms,
                    "max_resync_to_book_ready_latency_ms": book_max_ms,
                    "avg_recovery_to_market_ready_latency_ms": market_avg_ms,
                    "max_recovery_to_market_ready_latency_ms": market_max_ms,
                    "first_quote_blocked_reasons": self._diagnostics_reasons_for_event(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                        event_name="first_quote_after_resync_blocked",
                    ),
                    "book_ready_blocked_reasons": self._diagnostics_reasons_for_event(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                        event_name="book_ready_after_resync_blocked",
                    ),
                    "market_ready_blocked_reasons": self._diagnostics_reasons_for_event(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                        event_name="market_ready_after_recovery_blocked",
                    ),
                    "eligibility_gate_unmet_reasons": self._diagnostics_reasons_for_event(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                        event_name="eligibility_gate_unmet",
                    ),
                    "recovery_universe_change_resync_started_count": float(
                        universe_resync_started_count
                    ),
                    "recovery_universe_change_first_quote_success_count": float(
                        universe_first_quote_success_count
                    ),
                    "recovery_universe_change_book_ready_success_count": float(
                        universe_book_ready_success_count
                    ),
                    "recovery_universe_change_market_ready_success_count": float(
                        universe_market_ready_success_count
                    ),
                    "recovery_universe_change_first_quote_blocked_count": float(
                        universe_first_quote_blocked_count
                    ),
                    "recovery_universe_change_book_ready_blocked_count": float(
                        universe_book_ready_blocked_count
                    ),
                    "recovery_universe_change_market_ready_blocked_count": float(
                        universe_market_ready_blocked_count
                    ),
                    "recovery_universe_change_first_quote_success_rate": (
                        float(universe_first_quote_success_count / universe_resync_started_count)
                        if universe_resync_started_count > 0
                        else 0.0
                    ),
                    "recovery_universe_change_book_ready_success_rate": (
                        float(universe_book_ready_success_count / universe_resync_started_count)
                        if universe_resync_started_count > 0
                        else 0.0
                    ),
                    "recovery_universe_change_market_ready_success_rate": (
                        float(universe_market_ready_success_count / universe_resync_started_count)
                        if universe_resync_started_count > 0
                        else 0.0
                    ),
                    "market_stale_enter_count": float(market_stale_enter_count),
                    "market_stale_recover_count": float(market_stale_recover_count),
                    "avg_market_stale_duration_ms": stale_avg_ms,
                    "max_market_stale_duration_ms": stale_max_ms,
                    "market_stale_universe_change_enter_count": float(
                        market_stale_universe_change_enter_count
                    ),
                    "market_stale_reason_breakdown": market_stale_reason_breakdown,
                    "market_stale_side_breakdown": market_stale_side_breakdown,
                    "market_ready_blocked_stale_reason_breakdown": (
                        market_ready_blocked_stale_reason_breakdown
                    ),
                    "eligibility_gate_stale_reason_breakdown": (
                        eligibility_gate_stale_reason_breakdown
                    ),
                    "no_signal_stale_reason_breakdown": no_signal_stale_reason_breakdown,
                    "universe_change_market_ready_blocked_stale_reason_breakdown": (
                        universe_change_market_ready_blocked_stale_reason_breakdown
                    ),
                    "top_stale_assets": self._top_diagnostics_counts(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                        event_name="stale_asset_detected",
                        entity_column="asset_id",
                    ),
                    "top_missing_book_assets": self._top_diagnostics_counts(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                        event_name="missing_book_state_detected",
                        entity_column="asset_id",
                    ),
                    "top_market_blocked_markets": self._top_diagnostics_counts(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                        event_name="market_block_entered",
                        entity_column="market_id",
                    ),
                    "top_recovery_slow_assets": self._top_slow_assets(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                    ),
                    "top_recovery_slow_markets": self._top_slow_markets(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                    ),
                    "top_long_stale_markets": self._top_long_stale_markets(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                    ),
                    "top_repeated_stale_markets": self._top_repeated_stale_markets(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                    ),
                    "top_stale_legs": self._top_stale_legs(
                        conn=conn,
                        window=window,
                        run_id=run_id,
                    ),
                }
            except sqlite3.OperationalError:
                return empty

    def _safe_mode_by_scope_reason(
        self,
        *,
        window: DashboardWindow,
        run_id: str | None,
    ) -> pd.DataFrame:
        rows = self._query_df(
            query=self._apply_run_filter(
                """
                SELECT details
                FROM metrics
                WHERE created_at >= ? AND created_at < ?
                  AND metric_name IN (
                    'safe_mode_entered',
                    'safe_mode_asset_blocked',
                    'safe_mode_market_blocked'
                  )
                """,
                run_id=run_id,
            ),
            params=self._window_params(window=window, run_id=run_id),
        )
        if rows.empty:
            return pd.DataFrame(columns=["scope", "reason", "count"])

        counts: dict[tuple[str, str], int] = {}
        for details in rows["details"].astype(str):
            scope = self._extract_kv(details, "scope") or "unknown"
            reason = self._extract_kv(details, "reason") or "unknown"
            key = (scope, reason)
            counts[key] = counts.get(key, 0) + 1
        return pd.DataFrame(
            [
                {"scope": scope, "reason": reason, "count": count}
                for (scope, reason), count in sorted(
                    counts.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )
            ]
        )

    @staticmethod
    def _bucket_categorical_counts(
        *,
        frame: pd.DataFrame,
        time_column: str,
        category_column: str,
        bucket_minutes: int,
        value_column: str | None = None,
    ) -> pd.DataFrame:
        working = frame.copy()
        working["timestamp"] = pd.to_datetime(working[time_column], utc=True, errors="coerce")
        working = working.dropna(subset=["timestamp"])
        if working.empty:
            category_name = "reason" if category_column == "metric_name" else category_column
            return pd.DataFrame(columns=["timestamp", category_name, "count"])
        bucket_size = max(1, int(bucket_minutes))
        working["timestamp"] = working["timestamp"].dt.floor(f"{bucket_size}min")
        category_name = category_column
        if value_column is None:
            grouped = (
                working.groupby(["timestamp", category_column], dropna=False)
                .size()
                .reset_index(name="count")
            )
        else:
            grouped = (
                working.groupby(["timestamp", category_column], dropna=False)[value_column]
                .sum()
                .reset_index(name="count")
            )
        grouped = grouped.rename(columns={category_column: category_name})
        grouped["count"] = grouped["count"].astype(float)
        grouped = grouped.sort_values(["timestamp", "count"], ascending=[True, False])
        return grouped.reset_index(drop=True)

    def _count(
        self,
        *,
        conn: sqlite3.Connection,
        table: str,
        time_column: str,
        window: DashboardWindow,
        run_id: str | None,
    ) -> int:
        query = f"SELECT COUNT(*) FROM {table} WHERE {time_column} >= ? AND {time_column} < ?"
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def _count_distinct_signal_with_fill(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
    ) -> int:
        query = """
        SELECT COUNT(DISTINCT signal_id)
        FROM fills
        WHERE filled_at >= ? AND filled_at < ?
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def _count_matched_fill_signals(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
    ) -> int:
        query = """
        SELECT COUNT(*)
        FROM execution_events
        WHERE completed_at >= ? AND completed_at < ?
          AND matched_qty > 0
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def _count_metric(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        name: str,
    ) -> int:
        query = """
        SELECT COUNT(*)
        FROM metrics
        WHERE created_at >= ? AND created_at < ?
          AND metric_name = ?
        """
        params: list[object] = [window.start_iso, window.end_iso, name]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def _count_diagnostics_event(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        event_name: str,
    ) -> int:
        query = """
        SELECT COUNT(*)
        FROM diagnostics_events
        WHERE created_at >= ? AND created_at < ?
          AND event_name = ?
        """
        params: list[object] = [window.start_iso, window.end_iso, event_name]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def _count_diagnostics_event_with_reason(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        event_name: str,
        reason: str,
    ) -> int:
        query = """
        SELECT COUNT(*)
        FROM diagnostics_events
        WHERE created_at >= ? AND created_at < ?
          AND event_name = ?
          AND reason = ?
        """
        params: list[object] = [window.start_iso, window.end_iso, event_name, reason]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def _count_diagnostics_event_with_details_like(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        event_name: str,
        details_like: str,
    ) -> int:
        query = """
        SELECT COUNT(*)
        FROM diagnostics_events
        WHERE created_at >= ? AND created_at < ?
          AND event_name = ?
          AND details LIKE ?
        """
        params: list[object] = [window.start_iso, window.end_iso, event_name, details_like]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def _count_metric_like(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        pattern: str,
    ) -> int:
        query = """
        SELECT COUNT(*)
        FROM metrics
        WHERE created_at >= ? AND created_at < ?
          AND metric_name LIKE ?
        """
        params: list[object] = [window.start_iso, window.end_iso, pattern]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def _diagnostics_reasons_for_event(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        event_name: str,
        limit: int = 10,
    ) -> pd.DataFrame:
        query = """
        SELECT reason, COUNT(*) AS count
        FROM diagnostics_events
        WHERE created_at >= ? AND created_at < ?
          AND event_name = ?
        """
        params: list[object] = [window.start_iso, window.end_iso, event_name]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        query += " GROUP BY reason ORDER BY count DESC LIMIT ?"
        params.append(max(1, int(limit)))
        frame = pd.read_sql_query(query, conn, params=params)
        if frame.empty:
            return pd.DataFrame(columns=["reason", "count"])
        frame["reason"] = frame["reason"].fillna("unknown")
        return frame

    def _diagnostics_detail_breakdown_for_event(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        event_name: str,
        detail_key: str,
        reason_like: str | None = None,
        details_like: str | None = None,
        limit: int = 10,
    ) -> pd.DataFrame:
        query = """
        SELECT reason, details
        FROM diagnostics_events
        WHERE created_at >= ? AND created_at < ?
          AND event_name = ?
        """
        params: list[object] = [window.start_iso, window.end_iso, event_name]
        if reason_like is not None:
            query += " AND reason LIKE ?"
            params.append(reason_like)
        if details_like is not None:
            query += " AND details LIKE ?"
            params.append(details_like)
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        rows = conn.execute(query, params).fetchall()
        counts: dict[str, int] = {}
        for reason_value, details_value in rows:
            reason_text = str(reason_value or "")
            details_text = str(details_value or "")
            detail = self._extract_kv(details_text, detail_key)
            if detail_key == "stale_reason_key":
                detail = stale_reason_key_from_reason_and_details(
                    reason=reason_text,
                    details=details_text,
                )
                detail = normalize_stale_reason_key(detail)
            elif detail_key == "stale_side":
                detail = normalize_stale_side(detail)
            key = (detail or "unknown").strip() or "unknown"
            counts[key] = counts.get(key, 0) + 1
        if not counts:
            return pd.DataFrame(columns=["reason", "count"])
        rows_out = sorted(counts.items(), key=lambda item: item[1], reverse=True)[
            : max(1, int(limit))
        ]
        return pd.DataFrame(rows_out, columns=["reason", "count"])

    def _metric_stale_reason_breakdown(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        limit: int = 10,
    ) -> pd.DataFrame:
        query = """
        SELECT metric_name, details, metric_value
        FROM metrics
        WHERE created_at >= ? AND created_at < ?
          AND metric_name LIKE 'no_signal_reason:market_quote_stale%'
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        rows = conn.execute(query, params).fetchall()
        counts: dict[str, int] = {}
        for metric_name, details, metric_value in rows:
            metric_reason = str(metric_name).split(":", 1)[-1]
            stale_reason = stale_reason_key_from_reason_and_details(
                reason=metric_reason,
                details=str(details or ""),
            )
            normalized_reason = normalize_stale_reason_key(stale_reason)
            increment = int(round(float(metric_value if metric_value is not None else 1.0)))
            counts[normalized_reason] = counts.get(normalized_reason, 0) + max(1, increment)
        if not counts:
            return pd.DataFrame(columns=["reason", "count"])
        rows_out = sorted(counts.items(), key=lambda item: item[1], reverse=True)[
            : max(1, int(limit))
        ]
        return pd.DataFrame(rows_out, columns=["reason", "count"])

    def _top_long_stale_markets(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
    ) -> pd.DataFrame:
        query = """
        SELECT
          d.market_id,
          COUNT(*) AS recover_count,
          AVG(d.latency_ms) AS avg_stale_duration_ms,
          MAX(d.latency_ms) AS max_stale_duration_ms,
          MAX(COALESCE(m.slug, '')) AS market_slug,
          MAX(COALESCE(m.question, '')) AS market_question
        FROM diagnostics_events d
        LEFT JOIN markets m
          ON m.market_id = d.market_id
        WHERE d.created_at >= ? AND d.created_at < ?
          AND d.event_name IN ('market_stale_recovered', 'market_stale_episode_closed')
          AND d.market_id IS NOT NULL
          AND d.market_id != ''
          AND d.latency_ms IS NOT NULL
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND d.run_id = ?"
            params.append(run_id)
        query += (
            " GROUP BY d.market_id ORDER BY max_stale_duration_ms DESC, "
            "avg_stale_duration_ms DESC LIMIT 5"
        )
        return pd.read_sql_query(query, conn, params=params)

    def _top_repeated_stale_markets(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
    ) -> pd.DataFrame:
        query = """
        SELECT
          d.market_id,
          SUM(CASE WHEN d.event_name = 'market_stale_entered' THEN 1 ELSE 0 END) AS enter_count,
          SUM(CASE WHEN d.event_name = 'market_stale_recovered' THEN 1 ELSE 0 END) AS recover_count,
          MAX(COALESCE(m.slug, '')) AS market_slug,
          MAX(COALESCE(m.question, '')) AS market_question
        FROM diagnostics_events d
        LEFT JOIN markets m
          ON m.market_id = d.market_id
        WHERE d.created_at >= ? AND d.created_at < ?
          AND d.event_name IN ('market_stale_entered', 'market_stale_recovered')
          AND d.market_id IS NOT NULL
          AND d.market_id != ''
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND d.run_id = ?"
            params.append(run_id)
        query += (
            " GROUP BY d.market_id"
            " HAVING SUM(CASE WHEN d.event_name = 'market_stale_entered' THEN 1 ELSE 0 END) > 0"
            " ORDER BY enter_count DESC, recover_count DESC LIMIT 5"
        )
        return pd.read_sql_query(query, conn, params=params)

    def _top_stale_legs(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
    ) -> pd.DataFrame:
        query = """
        SELECT
          d.market_id,
          d.reason,
          d.details,
          MAX(COALESCE(m.slug, '')) AS market_slug,
          MAX(COALESCE(m.question, '')) AS market_question,
          MAX(COALESCE(m.yes_token_id, '')) AS yes_asset_id,
          MAX(COALESCE(m.no_token_id, '')) AS no_asset_id
        FROM diagnostics_events d
        LEFT JOIN markets m
          ON m.market_id = d.market_id
        WHERE d.created_at >= ? AND d.created_at < ?
          AND d.event_name = 'market_stale_entered'
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND d.run_id = ?"
            params.append(run_id)
        query += " GROUP BY d.id ORDER BY d.created_at DESC"
        rows = conn.execute(query, params).fetchall()
        counts: dict[tuple[str, str, str], dict[str, object]] = {}
        for row in rows:
            market_id = str(row[0] or "")
            reason = str(row[1] or "")
            details = str(row[2] or "")
            market_slug = str(row[3] or "")
            market_question = str(row[4] or "")
            yes_asset_id = str(row[5] or "")
            no_asset_id = str(row[6] or "")
            details_map = parse_kv_details(details)
            stale_reason_key = stale_reason_key_from_reason_and_details(
                reason=reason, details=details
            )
            asset_ids_raw = details_map.get("stale_asset_ids") or details_map.get(
                "stale_asset_id", ""
            )
            candidate_assets = [item.strip() for item in asset_ids_raw.split(",") if item.strip()]
            if not candidate_assets:
                stale_side = normalize_stale_side(details_map.get("stale_side"))
                if stale_side in {"yes", "both"} and yes_asset_id:
                    candidate_assets.append(yes_asset_id)
                if stale_side in {"no", "both"} and no_asset_id:
                    candidate_assets.append(no_asset_id)
            for asset_id in candidate_assets:
                side = "unknown"
                if asset_id == yes_asset_id:
                    side = "yes"
                elif asset_id == no_asset_id:
                    side = "no"
                key = (market_id, asset_id, side)
                entry = counts.setdefault(
                    key,
                    {
                        "market_id": market_id,
                        "asset_id": asset_id,
                        "side": side,
                        "count": 0,
                        "market_slug": market_slug,
                        "market_question": market_question,
                        "stale_reason_key": normalize_stale_reason_key(stale_reason_key),
                    },
                )
                entry["count"] = int(entry["count"]) + 1
        if not counts:
            return pd.DataFrame(
                columns=[
                    "market_id",
                    "asset_id",
                    "side",
                    "count",
                    "market_slug",
                    "market_question",
                    "stale_reason_key",
                ]
            )
        frame = pd.DataFrame(counts.values())
        return frame.sort_values("count", ascending=False).head(5)

    def _sum_metric(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        name: str,
    ) -> float:
        query = """
        SELECT COALESCE(SUM(metric_value), 0)
        FROM metrics
        WHERE created_at >= ? AND created_at < ?
          AND metric_name = ?
        """
        params: list[object] = [window.start_iso, window.end_iso, name]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return float(row[0] if row else 0.0)

    def _diagnostics_latency_stats(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        event_name: str,
    ) -> tuple[float, float]:
        query = """
        SELECT AVG(latency_ms), MAX(latency_ms)
        FROM diagnostics_events
        WHERE created_at >= ? AND created_at < ?
          AND event_name = ?
          AND latency_ms IS NOT NULL
        """
        params: list[object] = [window.start_iso, window.end_iso, event_name]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        if row is None:
            return 0.0, 0.0
        avg_value = float(row[0]) if row[0] is not None else 0.0
        max_value = float(row[1]) if row[1] is not None else 0.0
        return avg_value, max_value

    def _diagnostics_latency_stats_for_events(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        event_names: tuple[str, ...],
    ) -> tuple[float, float]:
        names = [name for name in event_names if name]
        if not names:
            return 0.0, 0.0
        placeholders = ", ".join("?" for _ in names)
        query = f"""
        SELECT AVG(latency_ms), MAX(latency_ms)
        FROM diagnostics_events
        WHERE created_at >= ? AND created_at < ?
          AND event_name IN ({placeholders})
          AND latency_ms IS NOT NULL
        """
        params: list[object] = [window.start_iso, window.end_iso, *names]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        if row is None:
            return 0.0, 0.0
        avg_value = float(row[0]) if row[0] is not None else 0.0
        max_value = float(row[1]) if row[1] is not None else 0.0
        return avg_value, max_value

    def _top_diagnostics_counts(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        event_name: str,
        entity_column: str,
    ) -> pd.DataFrame:
        if entity_column not in {"asset_id", "market_id"}:
            return pd.DataFrame(columns=[entity_column, "count"])
        if entity_column == "asset_id":
            query = """
            WITH market_asset AS (
              SELECT market_id, slug, question, yes_token_id AS asset_id, 'yes' AS side
              FROM markets
              UNION ALL
              SELECT market_id, slug, question, no_token_id AS asset_id, 'no' AS side
              FROM markets
            )
            SELECT
              d.asset_id,
              COUNT(*) AS count,
              MAX(COALESCE(d.market_id, ma.market_id)) AS market_id,
              MAX(COALESCE(ma.slug, m.slug, '')) AS market_slug,
              MAX(COALESCE(ma.question, m.question, '')) AS market_question,
              MAX(COALESCE(ma.side, '')) AS side
            FROM diagnostics_events d
            LEFT JOIN market_asset ma
              ON ma.asset_id = d.asset_id
            LEFT JOIN markets m
              ON m.market_id = d.market_id
            WHERE d.created_at >= ? AND d.created_at < ?
              AND d.event_name = ?
              AND d.asset_id IS NOT NULL
              AND d.asset_id != ''
            """
            params: list[object] = [window.start_iso, window.end_iso, event_name]
            if run_id is not None:
                query += " AND d.run_id = ?"
                params.append(run_id)
            query += " GROUP BY d.asset_id ORDER BY count DESC LIMIT 5"
            return pd.read_sql_query(query, conn, params=params)
        query = """
        SELECT
          d.market_id,
          COUNT(*) AS count,
          MAX(COALESCE(m.slug, '')) AS market_slug,
          MAX(COALESCE(m.question, '')) AS market_question
        FROM diagnostics_events d
        LEFT JOIN markets m
          ON m.market_id = d.market_id
        WHERE d.created_at >= ? AND d.created_at < ?
          AND d.event_name = ?
          AND d.market_id IS NOT NULL
          AND d.market_id != ''
        """
        params = [window.start_iso, window.end_iso, event_name]
        if run_id is not None:
            query += " AND d.run_id = ?"
            params.append(run_id)
        query += " GROUP BY d.market_id ORDER BY count DESC LIMIT 5"
        return pd.read_sql_query(query, conn, params=params)

    def _top_slow_assets(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
    ) -> pd.DataFrame:
        def _query(event_name: str) -> pd.DataFrame:
            query = """
            WITH market_asset AS (
              SELECT market_id, slug, question, yes_token_id AS asset_id, 'yes' AS side
              FROM markets
              UNION ALL
              SELECT market_id, slug, question, no_token_id AS asset_id, 'no' AS side
              FROM markets
            )
            SELECT
              d.asset_id,
              AVG(d.latency_ms) AS avg_latency_ms,
              MAX(d.latency_ms) AS max_latency_ms,
              COUNT(*) AS success_count,
              MAX(COALESCE(d.market_id, ma.market_id)) AS market_id,
              MAX(COALESCE(ma.slug, m.slug, '')) AS market_slug,
              MAX(COALESCE(ma.question, m.question, '')) AS market_question,
              MAX(COALESCE(ma.side, '')) AS side
            FROM diagnostics_events d
            LEFT JOIN market_asset ma
              ON ma.asset_id = d.asset_id
            LEFT JOIN markets m
              ON m.market_id = d.market_id
            WHERE d.created_at >= ? AND d.created_at < ?
              AND d.event_name = ?
              AND d.latency_ms IS NOT NULL
              AND d.asset_id IS NOT NULL
              AND d.asset_id != ''
            """
            params: list[object] = [window.start_iso, window.end_iso, event_name]
            if run_id is not None:
                query += " AND d.run_id = ?"
                params.append(run_id)
            query += (
                " GROUP BY d.asset_id ORDER BY max_latency_ms DESC, " "avg_latency_ms DESC LIMIT 5"
            )
            return pd.read_sql_query(query, conn, params=params)

        frame = _query("book_ready_after_resync")
        if frame.empty:
            frame = _query("first_quote_after_resync")
        return frame

    def _top_slow_markets(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
    ) -> pd.DataFrame:
        query = """
        SELECT
          d.market_id,
          AVG(d.latency_ms) AS avg_latency_ms,
          MAX(d.latency_ms) AS max_latency_ms,
          COUNT(*) AS success_count,
          MAX(COALESCE(m.slug, '')) AS market_slug,
          MAX(COALESCE(m.question, '')) AS market_question
        FROM diagnostics_events d
        LEFT JOIN markets m
          ON m.market_id = d.market_id
        WHERE d.created_at >= ? AND d.created_at < ?
          AND d.event_name = 'market_ready_after_recovery'
          AND d.latency_ms IS NOT NULL
          AND d.market_id IS NOT NULL
          AND d.market_id != ''
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND d.run_id = ?"
            params.append(run_id)
        query += " GROUP BY d.market_id ORDER BY max_latency_ms DESC, avg_latency_ms DESC LIMIT 5"
        return pd.read_sql_query(query, conn, params=params)

    def _count_resync_reason(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        reason: str,
    ) -> int:
        query = """
        SELECT COUNT(*)
        FROM resync_events
        WHERE created_at >= ? AND created_at < ?
          AND reason = ?
        """
        params: list[object] = [window.start_iso, window.end_iso, reason]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def _sum_pnl(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
    ) -> tuple[float, float, float]:
        query = """
        SELECT
          COALESCE(SUM(projected_matched_pnl), 0),
          COALESCE(SUM(unmatched_inventory_mtm), 0),
          COALESCE(SUM(total_projected_pnl), 0)
        FROM pnl_snapshots
        WHERE created_at >= ? AND created_at < ?
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return (
            float(row[0] if row else 0.0),
            float(row[1] if row else 0.0),
            float(row[2] if row else 0.0),
        )

    def _universe_overview(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
    ) -> dict[str, int]:
        current_watched_markets = self._latest_metric_value(
            conn=conn,
            window=window,
            run_id=run_id,
            metric_name="universe_current_watched_markets",
        )
        current_subscribed_assets = self._latest_metric_value(
            conn=conn,
            window=window,
            run_id=run_id,
            metric_name="universe_current_subscribed_assets",
        )
        cumulative_watched_markets = self._latest_metric_value(
            conn=conn,
            window=window,
            run_id=run_id,
            metric_name="universe_cumulative_watched_markets",
        )
        cumulative_subscribed_assets = self._latest_metric_value(
            conn=conn,
            window=window,
            run_id=run_id,
            metric_name="universe_cumulative_subscribed_assets",
        )

        if cumulative_watched_markets == 0:
            row = conn.execute("SELECT COUNT(*) FROM markets").fetchone()
            cumulative_watched_markets = int(row[0] if row else 0)
        if cumulative_subscribed_assets == 0:
            query = """
            SELECT COUNT(DISTINCT asset_id)
            FROM quotes
            WHERE quote_time >= ? AND quote_time < ?
            """
            params: list[object] = [window.start_iso, window.end_iso]
            if run_id is not None:
                query += " AND run_id = ?"
                params.append(run_id)
            row = conn.execute(query, params).fetchone()
            cumulative_subscribed_assets = int(row[0] if row else 0)
        if current_watched_markets == 0:
            current_watched_markets = cumulative_watched_markets
        if current_subscribed_assets == 0:
            current_subscribed_assets = cumulative_subscribed_assets

        return {
            "current_watched_markets": current_watched_markets,
            "current_subscribed_assets": current_subscribed_assets,
            "cumulative_watched_markets": cumulative_watched_markets,
            "cumulative_subscribed_assets": cumulative_subscribed_assets,
        }

    def _latest_metric_value(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        metric_name: str,
    ) -> int:
        return int(
            self._latest_metric_float_value(
                conn=conn,
                window=window,
                run_id=run_id,
                metric_name=metric_name,
            )
        )

    def _latest_metric_float_value(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        metric_name: str,
    ) -> float:
        query = """
        SELECT metric_value
        FROM metrics
        WHERE created_at >= ? AND created_at < ?
          AND metric_name = ?
        """
        params: list[object] = [window.start_iso, window.end_iso, metric_name]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        query += " ORDER BY created_at DESC LIMIT 1"
        row = conn.execute(query, params).fetchone()
        if row is None:
            fallback_query = """
            SELECT metric_value
            FROM metrics
            WHERE metric_name = ?
            """
            fallback_params: list[object] = [metric_name]
            if run_id is not None:
                fallback_query += " AND run_id = ?"
                fallback_params.append(run_id)
            fallback_query += " ORDER BY created_at DESC LIMIT 1"
            row = conn.execute(fallback_query, fallback_params).fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0

    def _latest_metric_prefix_breakdown(
        self,
        *,
        conn: sqlite3.Connection,
        window: DashboardWindow,
        run_id: str | None,
        metric_prefix: str,
    ) -> list[dict[str, float | str]]:
        query = """
        SELECT metric_name, metric_value
        FROM metrics
        WHERE created_at >= ? AND created_at < ?
          AND metric_name LIKE ?
        """
        params: list[object] = [window.start_iso, window.end_iso, f"{metric_prefix}%"]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        query += " ORDER BY created_at DESC, id DESC"
        rows = conn.execute(query, params).fetchall()
        if not rows:
            fallback_query = """
            SELECT metric_name, metric_value
            FROM metrics
            WHERE metric_name LIKE ?
            """
            fallback_params: list[object] = [f"{metric_prefix}%"]
            if run_id is not None:
                fallback_query += " AND run_id = ?"
                fallback_params.append(run_id)
            fallback_query += " ORDER BY created_at DESC, id DESC"
            rows = conn.execute(fallback_query, fallback_params).fetchall()
        latest_by_name: dict[str, float] = {}
        for metric_name, metric_value in rows:
            key = str(metric_name)
            if key in latest_by_name:
                continue
            latest_by_name[key] = float(metric_value if metric_value is not None else 0.0)
        breakdown = [
            {
                "reason": name.split(":", 1)[1] if ":" in name else name,
                "count": value,
            }
            for name, value in latest_by_name.items()
        ]
        breakdown.sort(key=lambda item: float(item.get("count", 0.0)), reverse=True)
        return breakdown

    @staticmethod
    def _extract_kv(details: str, key: str) -> str | None:
        return extract_detail_value(details, key)

    @staticmethod
    def _window_params(*, window: DashboardWindow, run_id: str | None) -> list[object]:
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            params.append(run_id)
        return params

    @staticmethod
    def _apply_run_filter(query: str, *, run_id: str | None) -> str:
        if run_id is None:
            return query
        return query + " AND run_id = ?"

    def _metric_reason_breakdown(
        self,
        *,
        window: DashboardWindow,
        run_id: str | None,
        metric_name: str,
    ) -> pd.DataFrame:
        params: list[object] = [window.start_iso, window.end_iso, metric_name]
        if run_id is not None:
            params.append(run_id)
        frame = self._query_df(
            query=self._apply_run_filter(
                """
                SELECT details
                FROM metrics
                WHERE created_at >= ? AND created_at < ?
                  AND metric_name = ?
                """,
                run_id=run_id,
            ),
            params=params,
        )
        if frame.empty:
            return pd.DataFrame(columns=["reason", "count"])
        counts: dict[str, int] = {}
        for details in frame["details"].astype(str):
            reason = self._extract_kv(details, "reason") or "unknown"
            counts[reason] = counts.get(reason, 0) + 1
        return pd.DataFrame(
            [
                {"reason": reason, "count": count}
                for reason, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)
            ]
        )

    def _query_df(self, *, query: str, params: list[object]) -> pd.DataFrame:
        if not self.has_database():
            return pd.DataFrame()
        with self._connect() as conn:
            if conn is None:
                return pd.DataFrame()
            return pd.read_sql_query(query, conn, params=params)

    def _connect(self) -> sqlite3.Connection | None:
        if not self.has_database():
            return None
        db_path = self.db_path.resolve().as_posix()
        return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
        row = conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = ?
            LIMIT 1
            """,
            (table_name,),
        ).fetchone()
        return row is not None

    @staticmethod
    def _empty_recovery_diagnostics() -> dict[str, object]:
        return {
            "recovery_resync_started_count": 0.0,
            "recovery_first_quote_success_count": 0.0,
            "recovery_book_ready_success_count": 0.0,
            "recovery_market_ready_success_count": 0.0,
            "recovery_market_recovery_started_count": 0.0,
            "recovery_first_quote_blocked_count": 0.0,
            "recovery_book_ready_blocked_count": 0.0,
            "recovery_market_ready_blocked_count": 0.0,
            "recovery_first_quote_success_rate": 0.0,
            "recovery_book_ready_success_rate": 0.0,
            "recovery_market_ready_success_rate": 0.0,
            "recovery_stage_denominator": "resync_started_count",
            "avg_resync_to_first_quote_latency_ms": 0.0,
            "max_resync_to_first_quote_latency_ms": 0.0,
            "avg_resync_to_book_ready_latency_ms": 0.0,
            "max_resync_to_book_ready_latency_ms": 0.0,
            "avg_recovery_to_market_ready_latency_ms": 0.0,
            "max_recovery_to_market_ready_latency_ms": 0.0,
            "first_quote_blocked_reasons": pd.DataFrame(columns=["reason", "count"]),
            "book_ready_blocked_reasons": pd.DataFrame(columns=["reason", "count"]),
            "market_ready_blocked_reasons": pd.DataFrame(columns=["reason", "count"]),
            "eligibility_gate_unmet_reasons": pd.DataFrame(columns=["reason", "count"]),
            "recovery_universe_change_resync_started_count": 0.0,
            "recovery_universe_change_first_quote_success_count": 0.0,
            "recovery_universe_change_book_ready_success_count": 0.0,
            "recovery_universe_change_market_ready_success_count": 0.0,
            "recovery_universe_change_first_quote_blocked_count": 0.0,
            "recovery_universe_change_book_ready_blocked_count": 0.0,
            "recovery_universe_change_market_ready_blocked_count": 0.0,
            "recovery_universe_change_first_quote_success_rate": 0.0,
            "recovery_universe_change_book_ready_success_rate": 0.0,
            "recovery_universe_change_market_ready_success_rate": 0.0,
            "market_stale_enter_count": 0.0,
            "market_stale_recover_count": 0.0,
            "avg_market_stale_duration_ms": 0.0,
            "max_market_stale_duration_ms": 0.0,
            "market_stale_universe_change_enter_count": 0.0,
            "market_stale_reason_breakdown": pd.DataFrame(columns=["reason", "count"]),
            "market_stale_side_breakdown": pd.DataFrame(columns=["reason", "count"]),
            "market_ready_blocked_stale_reason_breakdown": pd.DataFrame(
                columns=["reason", "count"]
            ),
            "eligibility_gate_stale_reason_breakdown": pd.DataFrame(columns=["reason", "count"]),
            "no_signal_stale_reason_breakdown": pd.DataFrame(columns=["reason", "count"]),
            "universe_change_market_ready_blocked_stale_reason_breakdown": pd.DataFrame(
                columns=["reason", "count"]
            ),
            "top_stale_assets": pd.DataFrame(
                columns=[
                    "asset_id",
                    "count",
                    "market_id",
                    "market_slug",
                    "market_question",
                    "side",
                ]
            ),
            "top_missing_book_assets": pd.DataFrame(
                columns=[
                    "asset_id",
                    "count",
                    "market_id",
                    "market_slug",
                    "market_question",
                    "side",
                ]
            ),
            "top_market_blocked_markets": pd.DataFrame(
                columns=["market_id", "count", "market_slug", "market_question"]
            ),
            "top_recovery_slow_assets": pd.DataFrame(
                columns=[
                    "asset_id",
                    "avg_latency_ms",
                    "max_latency_ms",
                    "success_count",
                    "market_id",
                    "market_slug",
                    "market_question",
                    "side",
                ]
            ),
            "top_recovery_slow_markets": pd.DataFrame(
                columns=[
                    "market_id",
                    "avg_latency_ms",
                    "max_latency_ms",
                    "success_count",
                    "market_slug",
                    "market_question",
                ]
            ),
            "top_long_stale_markets": pd.DataFrame(
                columns=[
                    "market_id",
                    "recover_count",
                    "avg_stale_duration_ms",
                    "max_stale_duration_ms",
                    "market_slug",
                    "market_question",
                ]
            ),
            "top_repeated_stale_markets": pd.DataFrame(
                columns=[
                    "market_id",
                    "enter_count",
                    "recover_count",
                    "market_slug",
                    "market_question",
                ]
            ),
            "top_stale_legs": pd.DataFrame(
                columns=[
                    "market_id",
                    "asset_id",
                    "side",
                    "count",
                    "market_slug",
                    "market_question",
                    "stale_reason_key",
                ]
            ),
        }

    @staticmethod
    def _empty_overview() -> dict[str, float]:
        return {
            "total_signals": 0.0,
            "total_fills": 0.0,
            "fill_rate": 0.0,
            "matched_fill_rate": 0.0,
            "safe_mode_count": 0.0,
            "global_safe_mode_count": 0.0,
            "market_block_count": 0.0,
            "asset_block_count": 0.0,
            "total_block_events": 0.0,
            "market_universe_changed_count": 0.0,
            "market_universe_change_events": 0.0,
            "resync_due_to_universe_change": 0.0,
            "ws_connected_events": 0.0,
            "ws_reconnect_events": 0.0,
            "book_not_ready_count": 0.0,
            "quote_too_old_count": 0.0,
            "market_quote_stale_count": 0.0,
            "book_recovering_count": 0.0,
            "market_not_ready_count": 0.0,
            "market_probation_count": 0.0,
            "connection_recovering_count": 0.0,
            "market_recovering_count": 0.0,
            "ready_market_count": 0.0,
            "ready_market_ratio": 0.0,
            "recovering_market_count": 0.0,
            "stale_market_count": 0.0,
            "eligible_market_count": 0.0,
            "eligible_market_ratio": 0.0,
            "blocked_market_count": 0.0,
            "eligibility_gate_connection_recovering_count": 0.0,
            "eligibility_gate_book_recovering_count": 0.0,
            "eligibility_gate_stale_quote_freshness_count": 0.0,
            "eligibility_gate_blocked_count": 0.0,
            "eligibility_gate_probation_count": 0.0,
            "eligibility_gate_low_quality_runtime_excluded_count": 0.0,
            "eligibility_gate_other_readiness_gate_count": 0.0,
            "min_watched_markets_floor": 0.0,
            "low_quality_market_count": 0.0,
            "low_quality_runtime_excluded_count": 0.0,
            "no_initial_book_count": 0.0,
            "asset_warming_up_count": 0.0,
            "resync_count": 0.0,
            "projected_matched_pnl": 0.0,
            "unmatched_inventory_mtm": 0.0,
            "total_projected_pnl": 0.0,
            "watched_markets_current": 0.0,
            "watched_markets_cumulative": 0.0,
            "subscribed_assets_current": 0.0,
            "subscribed_assets_cumulative": 0.0,
        }
