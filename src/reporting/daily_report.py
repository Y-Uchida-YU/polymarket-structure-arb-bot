from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(slots=True)
class ReportWindow:
    start_iso: str
    end_iso: str
    label: str


class DailyReportGenerator:
    def __init__(self, db_path: Path, export_dir: Path) -> None:
        self.db_path = db_path
        self.export_dir = export_dir
        self.export_dir.mkdir(parents=True, exist_ok=True)

    def generate(
        self,
        *,
        date: str | None,
        last_hours: int | None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        window = self._resolve_window(date=date, last_hours=last_hours)
        with sqlite3.connect(self.db_path) as conn:
            total_signals = self._count(conn, "signals", "detected_at", window, run_id=run_id)
            total_fills = self._count(conn, "fills", "filled_at", window, run_id=run_id)
            signals_with_fill = self._count_distinct_signal_with_fill(conn, window, run_id=run_id)
            matched_fill_signals = self._count_matched_fill_signals(conn, window, run_id=run_id)
            one_leg_count = self._count_one_leg(conn, window, run_id=run_id)
            stale_reject_count = self._count_reject_reason(
                conn, window, run_id=run_id, reject_reason="stale_quote"
            )
            depth_reject_count = self._count_depth_reject(conn, window, run_id=run_id)
            resync_count = self._count(conn, "resync_events", "created_at", window, run_id=run_id)
            global_safe_mode_count = self._count_metric(
                conn,
                window,
                run_id=run_id,
                name="safe_mode_entered",
            )
            market_block_count = self._count_metric(
                conn,
                window,
                run_id=run_id,
                name="safe_mode_market_block_started",
            )
            market_block_active_count = self._count_metric(
                conn,
                window,
                run_id=run_id,
                name="safe_mode_market_block_active",
            )
            market_block_cleared_count = self._count_metric(
                conn,
                window,
                run_id=run_id,
                name="safe_mode_market_block_cleared",
            )
            market_universe_changed_count = self._count_metric(
                conn,
                window,
                run_id=run_id,
                name="market_universe_changed",
            )
            ws_connected_events = self._count_metric(
                conn,
                window,
                run_id=run_id,
                name="ws_connected_event",
            )
            ws_reconnect_events = self._count_metric(
                conn,
                window,
                run_id=run_id,
                name="ws_reconnect_event",
            )
            resync_due_to_universe_change = self._count_resync_reason(
                conn,
                window,
                run_id=run_id,
                reason="market_universe_changed",
            )
            asset_block_count = self._count_metric(
                conn,
                window,
                run_id=run_id,
                name="safe_mode_asset_blocked",
            )
            total_block_events = global_safe_mode_count + market_block_count + asset_block_count

            pnl_sums = self._sum_pnl(conn, window, run_id=run_id)
            top_markets_by_signal = self._top_markets_by_signal(conn, window, run_id=run_id)
            top_markets_by_pnl = self._top_markets_by_pnl(conn, window, run_id=run_id)
            top_reject_reasons = self._top_reject_reasons(conn, window, run_id=run_id)
            resync_by_reason = self._resync_by_reason(conn, window, run_id=run_id)
            no_signal_reasons = self._no_signal_reasons(conn, window, run_id=run_id)
            safe_mode_reasons = self._safe_mode_reasons(conn, window, run_id=run_id)
            safe_mode_scope_reasons = self._safe_mode_scope_reasons(conn, window, run_id=run_id)
            safe_mode_by_scope = self._safe_mode_by_scope(conn, window, run_id=run_id)
            missing_book_state_reasons = self._missing_book_state_reasons(
                conn, window, run_id=run_id
            )
            ws_reconnect_reason_breakdown = self._metric_detail_breakdown(
                conn,
                window,
                run_id=run_id,
                metric_name="ws_reconnect_event",
                detail_key="reason",
            )
            ws_connected_reason_breakdown = self._metric_detail_breakdown(
                conn,
                window,
                run_id=run_id,
                metric_name="ws_connected_event",
                detail_key="reason",
            )
            universe = self._universe_overview(conn, window, run_id=run_id)
            warmup = self._warmup_overview(conn, window, run_id=run_id)

        fill_rate = signals_with_fill / total_signals if total_signals > 0 else 0.0
        matched_fill_rate = matched_fill_signals / total_signals if total_signals > 0 else 0.0
        one_leg_rate = one_leg_count / total_signals if total_signals > 0 else 0.0
        stale_reject_rate = stale_reject_count / total_signals if total_signals > 0 else 0.0
        depth_reject_rate = depth_reject_count / total_signals if total_signals > 0 else 0.0
        no_signal_total = sum(int(item.get("count", 0)) for item in no_signal_reasons)
        safe_mode_blocked_total = sum(
            int(item.get("count", 0))
            for item in no_signal_reasons
            if str(item.get("reason", "")).startswith("safe_mode_blocked_")
        )
        no_signal_reason_map = {
            str(item.get("reason", "")): int(item.get("count", 0)) for item in no_signal_reasons
        }
        book_not_ready_count = sum(
            count
            for reason, count in no_signal_reason_map.items()
            if reason == "book_not_ready" or reason.startswith("book_not_ready_")
        )
        quote_too_old_count = no_signal_reason_map.get("quote_too_old", 0)
        book_recovering_count = no_signal_reason_map.get("book_recovering", 0)
        market_not_ready_count = no_signal_reason_map.get("market_not_ready", 0)
        market_probation_count = no_signal_reason_map.get("market_probation", 0)
        market_quote_stale_count = sum(
            count
            for reason, count in no_signal_reason_map.items()
            if reason.startswith("market_quote_stale")
        )
        asset_warming_up_count = no_signal_reason_map.get("asset_warming_up", 0)
        connection_recovering_count = no_signal_reason_map.get("connection_recovering", 0)
        market_recovering_count = no_signal_reason_map.get("market_recovering", 0)
        ready_market_count = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="market_state_ready_count",
        )
        recovering_market_count = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="market_state_recovering_count",
        )
        stale_market_count = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="market_state_stale_no_recent_quote_count",
        ) + self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="market_state_stale_quote_age_count",
        )
        eligible_market_count = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="market_state_eligible_count",
        )
        blocked_market_count = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="market_state_blocked_count",
        )
        watched_market_count_current = int(universe.get("current_watched_markets", 0))
        min_watched_markets_floor = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="universe_min_watched_markets_floor",
        )
        low_quality_market_count = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="low_quality_market_count",
        )
        low_quality_runtime_excluded_count = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="low_quality_runtime_excluded_count",
        )
        ready_market_ratio = (
            ready_market_count / max(1, watched_market_count_current)
            if watched_market_count_current > 0
            else 0.0
        )
        eligible_market_ratio = (
            eligible_market_count / max(1, watched_market_count_current)
            if watched_market_count_current > 0
            else 0.0
        )
        readiness_no_signal_total = (
            book_not_ready_count
            + quote_too_old_count
            + book_recovering_count
            + market_not_ready_count
            + market_probation_count
            + market_quote_stale_count
            + asset_warming_up_count
            + connection_recovering_count
            + market_recovering_count
        )
        no_eligible_markets_causes: list[str] = []
        if watched_market_count_current <= 0:
            no_eligible_markets_causes.append("watched_too_small")
        if watched_market_count_current < max(1, min_watched_markets_floor):
            no_eligible_markets_causes.append("watched_too_small")
        if watched_market_count_current > 0 and recovering_market_count >= watched_market_count_current:
            no_eligible_markets_causes.append("all_markets_recovering")
        if watched_market_count_current > 0 and ready_market_count == 0 and market_not_ready_count > 0:
            no_eligible_markets_causes.append("all_markets_not_ready")
        if watched_market_count_current > 0 and stale_market_count >= watched_market_count_current:
            no_eligible_markets_causes.append("all_markets_stale")
        if low_quality_runtime_excluded_count >= max(1, watched_market_count_current):
            no_eligible_markets_causes.append("quality_penalty_excessive")
        if eligible_market_count == 0 and not no_eligible_markets_causes:
            no_eligible_markets_causes.append("eligibility_gate_unmet")
        book_no_signal_total = sum(
            int(item.get("count", 0))
            for item in no_signal_reasons
            if (
                str(item.get("reason", "")).startswith("book_not_ready")
                or str(item.get("reason", ""))
                in {
                    "book_recovering",
                    "book_missing_after_resync",
                    "book_not_resynced_yet",
                    "book_evicted",
                    "no_initial_book",
                    "asset_warming_up",
                    "connection_recovering",
                    "market_recovering",
                    "market_not_ready",
                    "market_probation",
                    "market_quote_stale",
                    "market_quote_stale_no_recent_quote",
                    "market_quote_stale_recovery",
                    "market_quote_stale_quote_age",
                }
            )
        )
        missing_book_total = sum(int(item.get("count", 0)) for item in missing_book_state_reasons)

        warnings: list[str] = []
        if total_signals == 0:
            warnings.append("no_signals_in_window")
        if fill_rate < 0.1 and total_signals > 0:
            warnings.append("low_fill_rate")
        if one_leg_rate > 0.3:
            warnings.append("high_one_leg_rate")
        if stale_reject_rate > 0.3:
            warnings.append("high_stale_reject_rate")
        if global_safe_mode_count > 0:
            warnings.append("global_safe_mode_triggered")
        if market_block_count > 0:
            warnings.append("market_blocks_triggered")
        if asset_block_count > 0:
            warnings.append("asset_blocks_triggered")
        if safe_mode_blocked_total >= max(10, int(no_signal_total * 0.5)):
            warnings.append("blocking_dominates_run")
        if readiness_no_signal_total >= max(100, int(no_signal_total * 0.25)):
            warnings.append("readiness_friction_high")
        if resync_count >= 1_000:
            warnings.append("resync_storm_detected")
        if ws_reconnect_events >= max(10, int(ws_connected_events * 0.5)):
            warnings.append("websocket_instability")
        if total_signals == 0 and resync_count >= 100:
            warnings.append("no_signals_but_high_resync")
        if book_no_signal_total > 0 or missing_book_total > 0:
            warnings.append("book_state_unhealthy")
        if total_signals == 0 and (
            safe_mode_blocked_total > 0
            or missing_book_total > 0
            or warmup.get("warmup_events", 0) > 0
        ):
            warnings.append("data_not_ready_for_evaluation")
        if universe["current_watched_markets"] > 0 and eligible_market_count == 0:
            warnings.append("no_eligible_markets")
        if stale_market_count > max(5, ready_market_count):
            warnings.append("freshness_friction_high")
        if universe["current_watched_markets"] > 150 or universe["current_subscribed_assets"] > 300:
            warnings.append("universe_too_large")

        report = {
            "window": {
                "label": window.label,
                "start": window.start_iso,
                "end": window.end_iso,
            },
            "run_id_filter": run_id,
            "totals": {
                "total_signals": total_signals,
                "total_fills": total_fills,
                "fill_rate": fill_rate,
                "matched_fill_rate": matched_fill_rate,
                "one_leg_occurrence_count": one_leg_count,
                "one_leg_occurrence_rate": one_leg_rate,
                "stale_reject_count": stale_reject_count,
                "stale_reject_rate": stale_reject_rate,
                "depth_reject_count": depth_reject_count,
                "depth_reject_rate": depth_reject_rate,
                "resync_count": resync_count,
                "safe_mode_count": global_safe_mode_count,
                "global_safe_mode_count": global_safe_mode_count,
                "market_block_count": market_block_count,
                "market_block_active_count": market_block_active_count,
                "market_block_cleared_count": market_block_cleared_count,
                "asset_block_count": asset_block_count,
                "total_block_events": total_block_events,
                "market_universe_changed_count": market_universe_changed_count,
                "market_universe_change_events": market_universe_changed_count,
                "resync_due_to_universe_change": resync_due_to_universe_change,
                "ws_connected_events": ws_connected_events,
                "ws_reconnect_events": ws_reconnect_events,
                "book_not_ready_count": book_not_ready_count,
                "quote_too_old_count": quote_too_old_count,
                "book_recovering_count": book_recovering_count,
                "market_not_ready_count": market_not_ready_count,
                "market_probation_count": market_probation_count,
                "market_quote_stale_count": market_quote_stale_count,
                "asset_warming_up_count": asset_warming_up_count,
                "connection_recovering_count": connection_recovering_count,
                "market_recovering_count": market_recovering_count,
                "ready_market_count": ready_market_count,
                "ready_market_ratio": ready_market_ratio,
                "recovering_market_count": recovering_market_count,
                "stale_market_count": stale_market_count,
                "eligible_market_count": eligible_market_count,
                "eligible_market_ratio": eligible_market_ratio,
                "blocked_market_count": blocked_market_count,
                "min_watched_markets_floor": min_watched_markets_floor,
                "low_quality_market_count": low_quality_market_count,
                "low_quality_runtime_excluded_count": low_quality_runtime_excluded_count,
                "projected_matched_pnl": pnl_sums["projected_matched_pnl"],
                "unmatched_inventory_mtm": pnl_sums["unmatched_inventory_mtm"],
                "total_projected_pnl": pnl_sums["total_projected_pnl"],
            },
            "universe": universe,
            "warmup": warmup,
            "no_eligible_market_causes": sorted(set(no_eligible_markets_causes)),
            "resyncs_by_reason": resync_by_reason,
            "safe_mode_reasons": safe_mode_reasons,
            "safe_mode_scope_reasons": safe_mode_scope_reasons,
            "safe_mode_by_scope": safe_mode_by_scope,
            "ws_reconnect_reasons": ws_reconnect_reason_breakdown,
            "ws_connected_reasons": ws_connected_reason_breakdown,
            "no_signal_reasons": no_signal_reasons,
            "missing_book_state_reasons": missing_book_state_reasons,
            "top_markets_by_signal_count": top_markets_by_signal,
            "top_markets_by_pnl": top_markets_by_pnl,
            "top_reject_reasons": top_reject_reasons,
            "warnings": warnings,
        }
        return report

    def save(self, report: dict[str, Any]) -> tuple[Path, Path]:
        now = datetime.now(tz=UTC)
        label = str(report.get("window", {}).get("label", "window"))
        run_filter = str(report.get("run_id_filter") or "all")
        safe_label = label.replace(":", "").replace(" ", "_")
        report_base = f"daily_report_{safe_label}_{run_filter}_{now:%Y%m%d_%H%M%S}"
        json_path = self.export_dir / f"{report_base}.json"
        csv_path = self.export_dir / f"{report_base}.csv"

        with json_path.open("w", encoding="utf-8") as file:
            json.dump(report, file, ensure_ascii=False, indent=2)

        summary_rows = [
            {"metric": key, "value": value} for key, value in report.get("totals", {}).items()
        ]
        summary_rows.append({"metric": "warnings", "value": "|".join(report.get("warnings", []))})
        summary_rows.append({"metric": "window_start", "value": report["window"]["start"]})
        summary_rows.append({"metric": "window_end", "value": report["window"]["end"]})
        pd.DataFrame(summary_rows).to_csv(csv_path, index=False)
        return json_path, csv_path

    @staticmethod
    def format_console(report: dict[str, Any]) -> str:
        totals = report["totals"]
        universe = report.get("universe", {})
        warmup = report.get("warmup", {})
        one_leg_text = (
            f"{totals['one_leg_occurrence_count']} / {totals['one_leg_occurrence_rate']:.3f}"
        )
        stale_reject_text = f"{totals['stale_reject_count']} / {totals['stale_reject_rate']:.3f}"
        depth_reject_text = f"{totals['depth_reject_count']} / {totals['depth_reject_rate']:.3f}"
        lines = [
            "=== Shadow Paper Daily Report ===",
            f"window: {report['window']['start']} -> {report['window']['end']}",
            f"run_id_filter: {report.get('run_id_filter')}",
            f"total_signals: {totals['total_signals']}",
            f"total_fills: {totals['total_fills']}",
            f"fill_rate: {totals['fill_rate']:.3f}",
            f"matched_fill_rate: {totals['matched_fill_rate']:.3f}",
            f"one_leg_count/rate: {one_leg_text}",
            f"stale_reject_count/rate: {stale_reject_text}",
            f"depth_reject_count/rate: {depth_reject_text}",
            f"resync_count: {totals['resync_count']}",
            f"global_safe_mode_count: {totals['global_safe_mode_count']}",
            f"market_block_count: {totals['market_block_count']}",
            f"market_block_active_count: {totals['market_block_active_count']}",
            f"market_block_cleared_count: {totals['market_block_cleared_count']}",
            f"asset_block_count: {totals['asset_block_count']}",
            f"total_block_events: {totals['total_block_events']}",
            f"market_universe_changed_count: {totals['market_universe_changed_count']}",
            f"market_universe_change_events: {totals['market_universe_change_events']}",
            f"resync_due_to_universe_change: {totals['resync_due_to_universe_change']}",
            f"ws_connected_events: {totals['ws_connected_events']}",
            f"ws_reconnect_events: {totals['ws_reconnect_events']}",
            f"book_not_ready_count: {totals['book_not_ready_count']}",
            f"quote_too_old_count: {totals['quote_too_old_count']}",
            f"book_recovering_count: {totals['book_recovering_count']}",
            f"market_not_ready_count: {totals['market_not_ready_count']}",
            f"market_probation_count: {totals['market_probation_count']}",
            f"market_quote_stale_count: {totals['market_quote_stale_count']}",
            f"asset_warming_up_count: {totals['asset_warming_up_count']}",
            f"connection_recovering_count: {totals['connection_recovering_count']}",
            f"market_recovering_count: {totals['market_recovering_count']}",
            f"ready_market_count: {totals['ready_market_count']}",
            f"ready_market_ratio: {totals['ready_market_ratio']:.3f}",
            f"recovering_market_count: {totals['recovering_market_count']}",
            f"stale_market_count: {totals['stale_market_count']}",
            f"eligible_market_count: {totals['eligible_market_count']}",
            f"eligible_market_ratio: {totals['eligible_market_ratio']:.3f}",
            f"blocked_market_count: {totals['blocked_market_count']}",
            f"min_watched_markets_floor: {totals['min_watched_markets_floor']}",
            f"low_quality_market_count: {totals['low_quality_market_count']}",
            (
                "low_quality_runtime_excluded_count: "
                f"{totals['low_quality_runtime_excluded_count']}"
            ),
            (
                "watched_markets(current/cumulative): "
                f"{universe.get('current_watched_markets', 0)} / "
                f"{universe.get('cumulative_watched_markets', 0)}"
            ),
            (
                "subscribed_assets(current/cumulative): "
                f"{universe.get('current_subscribed_assets', 0)} / "
                f"{universe.get('cumulative_subscribed_assets', 0)}"
            ),
            (
                "warmup_events/latest_warming_up_assets: "
                f"{warmup.get('warmup_events', 0)} / {warmup.get('latest_warming_up_assets', 0)}"
            ),
            (
                "no_eligible_market_causes: "
                + ", ".join(report.get("no_eligible_market_causes", []))
            ),
            f"projected_matched_pnl: {totals['projected_matched_pnl']:.6f}",
            f"unmatched_inventory_mtm: {totals['unmatched_inventory_mtm']:.6f}",
            f"total_projected_pnl: {totals['total_projected_pnl']:.6f}",
        ]
        if report.get("resyncs_by_reason"):
            reason_summary = ", ".join(
                f"{item['reason']}={item['count']}" for item in report["resyncs_by_reason"][:5]
            )
            lines.append(f"top_resync_reasons: {reason_summary}")
        if report.get("safe_mode_reasons"):
            safe_mode_summary = ", ".join(
                f"{item['reason']}={item['count']}" for item in report["safe_mode_reasons"][:5]
            )
            lines.append(f"top_safe_mode_reasons: {safe_mode_summary}")
        if report.get("safe_mode_scope_reasons"):
            scope_reason_summary = ", ".join(
                f"{item['scope']}:{item['reason']}={item['count']}"
                for item in report["safe_mode_scope_reasons"][:5]
            )
            lines.append(f"top_safe_mode_scope_reasons: {scope_reason_summary}")
        if report.get("ws_reconnect_reasons"):
            ws_reconnect_summary = ", ".join(
                f"{item['reason']}={item['count']}" for item in report["ws_reconnect_reasons"][:5]
            )
            lines.append(f"top_ws_reconnect_reasons: {ws_reconnect_summary}")
        if report.get("ws_connected_reasons"):
            ws_connected_summary = ", ".join(
                f"{item['reason']}={item['count']}" for item in report["ws_connected_reasons"][:5]
            )
            lines.append(f"top_ws_connected_reasons: {ws_connected_summary}")
        if report.get("no_signal_reasons"):
            no_signal_summary = ", ".join(
                f"{item['reason']}={item['count']}" for item in report["no_signal_reasons"][:5]
            )
            lines.append(f"top_no_signal_reasons: {no_signal_summary}")
        if report.get("missing_book_state_reasons"):
            missing_summary = ", ".join(
                f"{item['reason']}={item['count']}"
                for item in report["missing_book_state_reasons"][:5]
            )
            lines.append(f"top_missing_book_state_reasons: {missing_summary}")
        if report.get("warnings"):
            lines.append("warnings: " + ", ".join(report["warnings"]))
        return "\n".join(lines)

    @staticmethod
    def _resolve_window(date: str | None, last_hours: int | None) -> ReportWindow:
        if date:
            start = datetime.fromisoformat(f"{date}T00:00:00+00:00")
            end = start + timedelta(days=1)
            return ReportWindow(
                start_iso=start.isoformat(),
                end_iso=end.isoformat(),
                label=f"date_{date}",
            )
        hours = max(1, int(last_hours or 24))
        # Keep the query end bound exclusive while avoiding boundary misses when
        # a row timestamp equals "now" at microsecond precision.
        end = datetime.now(tz=UTC) + timedelta(microseconds=1)
        start = end - timedelta(hours=hours)
        return ReportWindow(
            start_iso=start.isoformat(),
            end_iso=end.isoformat(),
            label=f"last_{hours}h",
        )

    def _count(
        self,
        conn: sqlite3.Connection,
        table: str,
        time_column: str,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> int:
        query = f"""
        SELECT COUNT(*)
        FROM {table}
        WHERE {time_column} >= ? AND {time_column} < ?
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def _count_distinct_signal_with_fill(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
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
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
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

    def _count_one_leg(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> int:
        query = """
        SELECT COUNT(*)
        FROM execution_events
        WHERE completed_at >= ? AND completed_at < ?
          AND fill_status IN ('one_leg_yes_only', 'one_leg_no_only')
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def _count_reject_reason(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        reject_reason: str,
    ) -> int:
        query = """
        SELECT COUNT(*)
        FROM execution_events
        WHERE completed_at >= ? AND completed_at < ?
          AND reject_reason = ?
        """
        params: list[object] = [window.start_iso, window.end_iso, reject_reason]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def _count_depth_reject(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> int:
        query = """
        SELECT COUNT(*)
        FROM execution_events
        WHERE completed_at >= ? AND completed_at < ?
          AND reject_reason IN ('insufficient_depth', 'min_book_size_not_met')
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def _count_metric(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
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

    def _count_resync_reason(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
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

    def _metric_detail_breakdown(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        metric_name: str,
        detail_key: str,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT details
        FROM metrics
        WHERE created_at >= ? AND created_at < ?
          AND metric_name = ?
        """
        params: list[object] = [window.start_iso, window.end_iso, metric_name]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        rows = conn.execute(query, params).fetchall()
        counts: dict[str, int] = {}
        for row in rows:
            details = str(row[0] or "")
            key = self._extract_kv_from_details(details, detail_key) or "unknown"
            counts[key] = counts.get(key, 0) + 1
        return [
            {"reason": reason, "count": count}
            for reason, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)
        ]

    def _count_metrics(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        names: list[str],
    ) -> int:
        if not names:
            return 0
        placeholders = ",".join("?" for _ in names)
        query = f"""
        SELECT COUNT(*)
        FROM metrics
        WHERE created_at >= ? AND created_at < ?
          AND metric_name IN ({placeholders})
        """
        params: list[object] = [window.start_iso, window.end_iso, *names]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def _sum_pnl(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> dict[str, float]:
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
        return {
            "projected_matched_pnl": float(row[0] if row else 0.0),
            "unmatched_inventory_mtm": float(row[1] if row else 0.0),
            "total_projected_pnl": float(row[2] if row else 0.0),
        }

    def _top_markets_by_signal(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT slug, COUNT(*) AS signal_count
        FROM signals
        WHERE detected_at >= ? AND detected_at < ?
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        query += " GROUP BY slug ORDER BY signal_count DESC LIMIT 5"
        rows = conn.execute(query, params).fetchall()
        return [{"market_slug": row[0], "signal_count": int(row[1])} for row in rows]

    def _top_markets_by_pnl(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT market_slug, COALESCE(SUM(total_projected_pnl), 0) AS pnl
        FROM pnl_snapshots
        WHERE created_at >= ? AND created_at < ?
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        query += " GROUP BY market_slug ORDER BY pnl DESC LIMIT 5"
        rows = conn.execute(query, params).fetchall()
        return [{"market_slug": row[0], "total_projected_pnl": float(row[1])} for row in rows]

    def _top_reject_reasons(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT reject_reason, COUNT(*)
        FROM execution_events
        WHERE completed_at >= ? AND completed_at < ?
          AND reject_reason IS NOT NULL
          AND reject_reason != ''
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        query += " GROUP BY reject_reason ORDER BY COUNT(*) DESC LIMIT 5"
        rows = conn.execute(query, params).fetchall()
        return [{"reject_reason": row[0], "count": int(row[1])} for row in rows]

    def _resync_by_reason(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT reason, COUNT(*)
        FROM resync_events
        WHERE created_at >= ? AND created_at < ?
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        query += " GROUP BY reason ORDER BY COUNT(*) DESC"
        rows = conn.execute(query, params).fetchall()
        return [{"reason": str(row[0]), "count": int(row[1])} for row in rows]

    def _no_signal_reasons(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT metric_name, COUNT(*)
        FROM metrics
        WHERE created_at >= ? AND created_at < ?
          AND metric_name LIKE 'no_signal_reason:%'
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        query += " GROUP BY metric_name ORDER BY COUNT(*) DESC"
        rows = conn.execute(query, params).fetchall()
        results: list[dict[str, Any]] = []
        for name, count in rows:
            metric_name = str(name)
            _, _, reason = metric_name.partition(":")
            results.append({"reason": reason or metric_name, "count": int(count)})
        return results

    def _universe_overview(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> dict[str, int]:
        current_watched_markets = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="universe_current_watched_markets",
        )
        current_subscribed_assets = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="universe_current_subscribed_assets",
        )
        cumulative_watched_markets = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="universe_cumulative_watched_markets",
        )
        cumulative_subscribed_assets = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="universe_cumulative_subscribed_assets",
        )
        if cumulative_watched_markets == 0:
            watched_row = conn.execute("SELECT COUNT(*) FROM markets").fetchone()
            cumulative_watched_markets = int(watched_row[0] if watched_row else 0)
        if cumulative_subscribed_assets == 0:
            cumulative_query = """
            SELECT COUNT(DISTINCT asset_id)
            FROM quotes
            WHERE quote_time >= ? AND quote_time < ?
            """
            cumulative_params: list[object] = [window.start_iso, window.end_iso]
            if run_id is not None:
                cumulative_query += " AND run_id = ?"
                cumulative_params.append(run_id)
            cumulative_row = conn.execute(cumulative_query, cumulative_params).fetchone()
            cumulative_subscribed_assets = int(cumulative_row[0] if cumulative_row else 0)
        if current_watched_markets == 0:
            current_watched_markets = cumulative_watched_markets
        if current_subscribed_assets == 0:
            current_subscribed_assets = cumulative_subscribed_assets

        return {
            "watched_markets": current_watched_markets,
            "subscribed_assets": current_subscribed_assets,
            "current_watched_markets": current_watched_markets,
            "current_subscribed_assets": current_subscribed_assets,
            "cumulative_watched_markets": cumulative_watched_markets,
            "cumulative_subscribed_assets": cumulative_subscribed_assets,
        }

    def _safe_mode_reasons(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT details
        FROM metrics
        WHERE created_at >= ? AND created_at < ?
          AND metric_name IN (
            'safe_mode_entered',
            'safe_mode_asset_blocked',
            'safe_mode_market_blocked'
          )
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        rows = conn.execute(query, params).fetchall()
        counts: dict[str, int] = {}
        for row in rows:
            details = str(row[0] or "")
            reason = self._extract_kv_from_details(details, "reason") or "unknown"
            counts[reason] = counts.get(reason, 0) + 1
        return [
            {"reason": reason, "count": count}
            for reason, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)
        ]

    def _safe_mode_scope_reasons(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT details
        FROM metrics
        WHERE created_at >= ? AND created_at < ?
          AND metric_name IN (
            'safe_mode_entered',
            'safe_mode_asset_blocked',
            'safe_mode_market_blocked'
          )
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        rows = conn.execute(query, params).fetchall()
        counts: dict[tuple[str, str], int] = {}
        for row in rows:
            details = str(row[0] or "")
            scope = self._extract_kv_from_details(details, "scope") or "unknown"
            reason = self._extract_kv_from_details(details, "reason") or "unknown"
            key = (scope, reason)
            counts[key] = counts.get(key, 0) + 1
        return [
            {"scope": scope, "reason": reason, "count": count}
            for (scope, reason), count in sorted(
                counts.items(), key=lambda item: item[1], reverse=True
            )
        ]

    def _safe_mode_by_scope(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> list[dict[str, Any]]:
        scope_reason_rows = self._safe_mode_scope_reasons(conn, window, run_id=run_id)
        counts: dict[str, int] = {}
        for row in scope_reason_rows:
            scope = str(row.get("scope", "unknown"))
            count = int(row.get("count", 0))
            counts[scope] = counts.get(scope, 0) + count
        return [
            {"scope": scope, "count": count}
            for scope, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)
        ]

    def _missing_book_state_reasons(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT metric_name, SUM(metric_value)
        FROM metrics
        WHERE created_at >= ? AND created_at < ?
          AND metric_name LIKE 'missing_book_state_reason:%'
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        query += " GROUP BY metric_name ORDER BY SUM(metric_value) DESC"
        rows = conn.execute(query, params).fetchall()
        results: list[dict[str, Any]] = []
        for name, total in rows:
            metric_name = str(name or "")
            _, _, reason = metric_name.partition(":")
            results.append({"reason": reason or metric_name, "count": int(float(total or 0.0))})
        return results

    def _latest_metric_value(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        metric_name: str,
    ) -> int:
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
        return int(float(row[0])) if row and row[0] is not None else 0

    @staticmethod
    def _extract_kv_from_details(details: str, key: str) -> str | None:
        compact = details.strip()
        if key == "reason" and compact and "=" not in compact:
            return compact
        prefix = f"{key}="
        for token in details.split(";"):
            token = token.strip()
            if token.startswith(prefix):
                value = token[len(prefix) :].strip()
                return value or None
        return None

    def _warmup_overview(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> dict[str, Any]:
        count_query = """
        SELECT COUNT(*)
        FROM metrics
        WHERE created_at >= ? AND created_at < ?
          AND metric_name = 'warming_up_asset_count'
          AND metric_value > 0
        """
        count_params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            count_query += " AND run_id = ?"
            count_params.append(run_id)
        row = conn.execute(count_query, count_params).fetchone()
        warmup_events = int(row[0] if row else 0)

        latest_query = """
        SELECT metric_value, details, created_at
        FROM metrics
        WHERE created_at >= ? AND created_at < ?
          AND metric_name = 'warming_up_asset_count'
        """
        latest_params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            latest_query += " AND run_id = ?"
            latest_params.append(run_id)
        latest_query += " ORDER BY created_at DESC LIMIT 1"
        latest_row = conn.execute(latest_query, latest_params).fetchone()
        return {
            "warmup_events": warmup_events,
            "latest_warming_up_assets": float(latest_row[0]) if latest_row else 0.0,
            "latest_warmup_reason": str(latest_row[1]) if latest_row and latest_row[1] else "",
            "latest_warmup_at": str(latest_row[2]) if latest_row and latest_row[2] else "",
        }
