from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.stale_diagnostics import (
    extract_detail_value,
    normalize_stale_reason_key,
    normalize_stale_side,
    parse_kv_details,
    stale_reason_key_from_reason_and_details,
)


@dataclass(slots=True)
class ReportWindow:
    start_iso: str
    end_iso: str
    label: str


STALE_DURATION_EVENT_NAMES: tuple[str, ...] = (
    "market_stale_recovered",
    "market_stale_episode_closed",
)


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
            recovery_diagnostics = self._recovery_diagnostics(conn, window, run_id=run_id)

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
        chronic_stale_excluded_market_count = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="chronic_stale_excluded_market_count",
        )
        chronic_stale_exclusion_active_count = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="chronic_stale_exclusion_active_count",
        )
        chronic_stale_reintroduced_for_floor_count = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="chronic_stale_reintroduced_for_floor_count",
        )
        chronic_stale_reintroduced_market_count = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="chronic_stale_reintroduced_market_count",
        )
        watched_chronic_stale_excluded_market_count = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="watched_chronic_stale_excluded_market_count",
        )
        chronic_stale_exclusion_extended_count = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="chronic_stale_exclusion_extended_count",
        )
        chronic_stale_exclusion_avg_active_age_ms = self._latest_metric_float_value(
            conn,
            window,
            run_id=run_id,
            metric_name="chronic_stale_exclusion_avg_active_age_ms",
        )
        chronic_stale_exclusion_long_active_market_count = self._latest_metric_value(
            conn,
            window,
            run_id=run_id,
            metric_name="chronic_stale_exclusion_long_active_market_count",
        )
        ready_market_ratio = self._latest_metric_float_value(
            conn,
            window,
            run_id=run_id,
            metric_name="market_state_ready_ratio",
        )
        eligible_market_ratio = self._latest_metric_float_value(
            conn,
            window,
            run_id=run_id,
            metric_name="market_state_eligible_ratio",
        )
        eligibility_gate_breakdown = self._latest_metric_prefix_breakdown(
            conn,
            window,
            run_id=run_id,
            metric_prefix="eligibility_gate_reason:",
        )
        eligibility_gate_reason_map = {
            str(item.get("reason", "")): int(round(float(item.get("count", 0.0))))
            for item in eligibility_gate_breakdown
        }
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
        if (
            watched_market_count_current > 0
            and recovering_market_count >= watched_market_count_current
        ):
            no_eligible_markets_causes.append("all_markets_recovering")
        if (
            watched_market_count_current > 0
            and ready_market_count == 0
            and market_not_ready_count > 0
        ):
            no_eligible_markets_causes.append("all_markets_not_ready")
        if watched_market_count_current > 0 and stale_market_count >= watched_market_count_current:
            no_eligible_markets_causes.append("all_markets_stale")
        if low_quality_runtime_excluded_count >= max(1, watched_market_count_current):
            no_eligible_markets_causes.append("quality_penalty_excessive")
        if (
            watched_market_count_current > 0
            and eligibility_gate_reason_map.get("connection_recovering", 0)
            >= watched_market_count_current
        ):
            no_eligible_markets_causes.append("all_markets_connection_recovering")
        if (
            watched_market_count_current > 0
            and eligibility_gate_reason_map.get("book_recovering", 0)
            >= watched_market_count_current
        ):
            no_eligible_markets_causes.append("all_markets_book_recovering")
        if (
            watched_market_count_current > 0
            and eligibility_gate_reason_map.get("stale_quote_freshness", 0)
            >= watched_market_count_current
        ):
            no_eligible_markets_causes.append("all_markets_stale_freshness")
        if (
            watched_market_count_current > 0
            and eligibility_gate_reason_map.get("chronic_stale_excluded", 0)
            >= watched_market_count_current
        ):
            no_eligible_markets_causes.append("all_markets_chronic_stale_excluded")
        if watched_chronic_stale_excluded_market_count > 0 and eligible_market_count == 0:
            no_eligible_markets_causes.append("watched_universe_chronic_contaminated")
        if chronic_stale_reintroduced_market_count > 0 and eligible_market_count == 0:
            no_eligible_markets_causes.append("watched_floor_reintroduced_chronic_stale")
        if (
            watched_market_count_current > 0
            and eligibility_gate_reason_map.get("blocked", 0) >= watched_market_count_current
        ):
            no_eligible_markets_causes.append("all_markets_blocked")
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
                "market_stale_enter_count": int(
                    recovery_diagnostics.get("market_stale_enter_count", 0)
                ),
                "market_stale_recover_count": int(
                    recovery_diagnostics.get("market_stale_recover_count", 0)
                ),
                "avg_market_stale_duration_ms": float(
                    recovery_diagnostics.get("avg_market_stale_duration_ms", 0.0)
                ),
                "max_market_stale_duration_ms": float(
                    recovery_diagnostics.get("max_market_stale_duration_ms", 0.0)
                ),
                "market_stale_universe_change_enter_count": int(
                    recovery_diagnostics.get("market_stale_universe_change_enter_count", 0)
                ),
                "eligibility_gate_connection_recovering_count": eligibility_gate_reason_map.get(
                    "connection_recovering",
                    0,
                ),
                "eligibility_gate_book_recovering_count": eligibility_gate_reason_map.get(
                    "book_recovering",
                    0,
                ),
                "eligibility_gate_stale_quote_freshness_count": eligibility_gate_reason_map.get(
                    "stale_quote_freshness",
                    0,
                ),
                "eligibility_gate_blocked_count": eligibility_gate_reason_map.get("blocked", 0),
                "eligibility_gate_probation_count": eligibility_gate_reason_map.get(
                    "probation",
                    0,
                ),
                "eligibility_gate_low_quality_runtime_excluded_count": (
                    eligibility_gate_reason_map.get(
                        "low_quality_runtime_excluded",
                        0,
                    )
                ),
                "eligibility_gate_chronic_stale_excluded_count": eligibility_gate_reason_map.get(
                    "chronic_stale_excluded",
                    0,
                ),
                "eligibility_gate_other_readiness_gate_count": eligibility_gate_reason_map.get(
                    "other_readiness_gate",
                    0,
                ),
                "min_watched_markets_floor": min_watched_markets_floor,
                "low_quality_market_count": low_quality_market_count,
                "low_quality_runtime_excluded_count": low_quality_runtime_excluded_count,
                "chronic_stale_excluded_market_count": chronic_stale_excluded_market_count,
                "chronic_stale_exclusion_active_count": chronic_stale_exclusion_active_count,
                "chronic_stale_reintroduced_for_floor_count": (
                    chronic_stale_reintroduced_for_floor_count
                ),
                "chronic_stale_reintroduced_market_count": (
                    chronic_stale_reintroduced_market_count
                ),
                "watched_chronic_stale_excluded_market_count": (
                    watched_chronic_stale_excluded_market_count
                ),
                "chronic_stale_exclusion_enter_count": int(
                    recovery_diagnostics.get("chronic_stale_exclusion_enter_count", 0)
                ),
                "chronic_stale_exclusion_extended_count": chronic_stale_exclusion_extended_count,
                "chronic_stale_exclusion_cleared_count": int(
                    recovery_diagnostics.get("chronic_stale_exclusion_cleared_count", 0)
                ),
                "chronic_stale_exclusion_avg_active_age_ms": (
                    chronic_stale_exclusion_avg_active_age_ms
                ),
                "chronic_stale_exclusion_long_active_market_count": (
                    chronic_stale_exclusion_long_active_market_count
                ),
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
            "eligibility_gate_breakdown": eligibility_gate_breakdown,
            "recovery_diagnostics": recovery_diagnostics,
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
        recovery = report.get("recovery_diagnostics", {})
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
            (
                "market_stale_transitions(enter/recover): "
                f"{totals.get('market_stale_enter_count', 0)}/"
                f"{totals.get('market_stale_recover_count', 0)}"
            ),
            (
                "market_stale_duration_avg_max_ms: "
                f"{float(totals.get('avg_market_stale_duration_ms', 0.0)):.1f}/"
                f"{float(totals.get('max_market_stale_duration_ms', 0.0)):.1f}"
            ),
            (
                "market_stale_universe_change_enter_count: "
                f"{totals.get('market_stale_universe_change_enter_count', 0)}"
            ),
            f"eligible_market_count: {totals['eligible_market_count']}",
            f"eligible_market_ratio: {totals['eligible_market_ratio']:.3f}",
            f"blocked_market_count: {totals['blocked_market_count']}",
            (
                "eligibility_gate_breakdown("
                "connection/book/stale/blocked/"
                "probation/low_quality/chronic/other): "
                f"{totals.get('eligibility_gate_connection_recovering_count', 0)}/"
                f"{totals.get('eligibility_gate_book_recovering_count', 0)}/"
                f"{totals.get('eligibility_gate_stale_quote_freshness_count', 0)}/"
                f"{totals.get('eligibility_gate_blocked_count', 0)}/"
                f"{totals.get('eligibility_gate_probation_count', 0)}/"
                f"{totals.get('eligibility_gate_low_quality_runtime_excluded_count', 0)}/"
                f"{totals.get('eligibility_gate_chronic_stale_excluded_count', 0)}/"
                f"{totals.get('eligibility_gate_other_readiness_gate_count', 0)}"
            ),
            f"min_watched_markets_floor: {totals['min_watched_markets_floor']}",
            f"low_quality_market_count: {totals['low_quality_market_count']}",
            (
                "low_quality_runtime_excluded_count: "
                f"{totals['low_quality_runtime_excluded_count']}"
            ),
            (
                "chronic_stale_exclusion(active/enter/cleared): "
                f"{totals.get('chronic_stale_exclusion_active_count', 0)}/"
                f"{totals.get('chronic_stale_exclusion_enter_count', 0)}/"
                f"{totals.get('chronic_stale_exclusion_cleared_count', 0)}"
            ),
            (
                "chronic_stale_exclusion(extended/avg_age_ms/long_active): "
                f"{totals.get('chronic_stale_exclusion_extended_count', 0)}/"
                f"{float(totals.get('chronic_stale_exclusion_avg_active_age_ms', 0.0)):.1f}/"
                f"{totals.get('chronic_stale_exclusion_long_active_market_count', 0)}"
            ),
            (
                "chronic_stale_reintroduced(for_floor/current_watched_chronic): "
                f"{totals.get('chronic_stale_reintroduced_for_floor_count', 0)}/"
                f"{totals.get('watched_chronic_stale_excluded_market_count', 0)}"
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
            (
                "recovery_funnel(resync/first_quote/book_ready/market_ready): "
                f"{int(recovery.get('recovery_resync_started_count', 0))}/"
                f"{int(recovery.get('recovery_first_quote_success_count', 0))}/"
                f"{int(recovery.get('recovery_book_ready_success_count', 0))}/"
                f"{int(recovery.get('recovery_market_ready_success_count', 0))}"
            ),
            (
                "recovery_blocked(first_quote/book_ready/market_ready): "
                f"{int(recovery.get('recovery_first_quote_blocked_count', 0))}/"
                f"{int(recovery.get('recovery_book_ready_blocked_count', 0))}/"
                f"{int(recovery.get('recovery_market_ready_blocked_count', 0))}"
            ),
            (
                "recovery_success_rate(first/book/market): "
                f"{float(recovery.get('recovery_first_quote_success_rate', 0.0)):.3f}/"
                f"{float(recovery.get('recovery_book_ready_success_rate', 0.0)):.3f}/"
                f"{float(recovery.get('recovery_market_ready_success_rate', 0.0)):.3f}"
            ),
            (
                "recovery_universe_change_funnel(resync/first/book/market): "
                f"{int(recovery.get('recovery_universe_change_resync_started_count', 0))}/"
                f"{int(recovery.get('recovery_universe_change_first_quote_success_count', 0))}/"
                f"{int(recovery.get('recovery_universe_change_book_ready_success_count', 0))}/"
                f"{int(recovery.get('recovery_universe_change_market_ready_success_count', 0))}"
            ),
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
        if report.get("eligibility_gate_breakdown"):
            eligibility_summary = ", ".join(
                f"{item['reason']}={int(float(item['count']))}"
                for item in report["eligibility_gate_breakdown"][:7]
            )
            lines.append(f"eligibility_gate_breakdown: {eligibility_summary}")
        if recovery.get("first_quote_blocked_reasons"):
            first_quote_blocked = ", ".join(
                f"{item['reason']}={item['count']}"
                for item in recovery["first_quote_blocked_reasons"][:5]
            )
            lines.append(f"top_recovery_first_quote_blocked_reasons: {first_quote_blocked}")
        if recovery.get("book_ready_blocked_reasons"):
            book_ready_blocked = ", ".join(
                f"{item['reason']}={item['count']}"
                for item in recovery["book_ready_blocked_reasons"][:5]
            )
            lines.append(f"top_recovery_book_ready_blocked_reasons: {book_ready_blocked}")
        if recovery.get("market_ready_blocked_reasons"):
            market_ready_blocked = ", ".join(
                f"{item['reason']}={item['count']}"
                for item in recovery["market_ready_blocked_reasons"][:5]
            )
            lines.append(f"top_recovery_market_ready_blocked_reasons: {market_ready_blocked}")
        if recovery.get("market_stale_reason_breakdown"):
            stale_reason_summary = ", ".join(
                f"{item['reason']}={item['count']}"
                for item in recovery["market_stale_reason_breakdown"][:5]
            )
            lines.append(f"market_stale_reason_breakdown: {stale_reason_summary}")
        if recovery.get("market_stale_side_breakdown"):
            stale_side_summary = ", ".join(
                f"{item['reason']}={item['count']}"
                for item in recovery["market_stale_side_breakdown"][:5]
            )
            lines.append(f"market_stale_side_breakdown: {stale_side_summary}")
        if recovery.get("no_signal_stale_reason_breakdown"):
            stale_no_signal_summary = ", ".join(
                f"{item['reason']}={item['count']}"
                for item in recovery["no_signal_stale_reason_breakdown"][:5]
            )
            lines.append(f"no_signal_stale_reason_breakdown: {stale_no_signal_summary}")
        if recovery.get("chronic_stale_reason_breakdown"):
            chronic_reason_summary = ", ".join(
                f"{item['reason']}={item['count']}"
                for item in recovery["chronic_stale_reason_breakdown"][:5]
            )
            lines.append(f"chronic_stale_reason_breakdown: {chronic_reason_summary}")
        if recovery.get("chronic_stale_extension_reason_breakdown"):
            chronic_extension_summary = ", ".join(
                f"{item['reason']}={item['count']}"
                for item in recovery["chronic_stale_extension_reason_breakdown"][:5]
            )
            lines.append(
                "chronic_stale_extension_reason_breakdown: " f"{chronic_extension_summary}"
            )
        if recovery.get("watched_chronic_stale_reason_breakdown"):
            watched_chronic_summary = ", ".join(
                f"{item['reason']}={item['count']}"
                for item in recovery["watched_chronic_stale_reason_breakdown"][:5]
            )
            lines.append(f"watched_chronic_stale_reason_breakdown: {watched_chronic_summary}")
        if recovery.get("market_ready_blocked_stale_reason_breakdown"):
            blocked_stale_summary = ", ".join(
                f"{item['reason']}={item['count']}"
                for item in recovery["market_ready_blocked_stale_reason_breakdown"][:5]
            )
            lines.append(f"market_ready_blocked_stale_reason_breakdown: {blocked_stale_summary}")
        if recovery.get("eligibility_gate_stale_reason_breakdown"):
            gate_stale_summary = ", ".join(
                f"{item['reason']}={item['count']}"
                for item in recovery["eligibility_gate_stale_reason_breakdown"][:5]
            )
            lines.append(f"eligibility_gate_stale_reason_breakdown: {gate_stale_summary}")
        if recovery.get("universe_change_market_ready_blocked_stale_reason_breakdown"):
            universe_blocked_stale_summary = ", ".join(
                f"{item['reason']}={item['count']}"
                for item in recovery["universe_change_market_ready_blocked_stale_reason_breakdown"][
                    :5
                ]
            )
            lines.append(
                "universe_change_market_ready_blocked_stale_reason_breakdown: "
                f"{universe_blocked_stale_summary}"
            )
        if recovery.get("top_stale_assets"):
            stale_summary = ", ".join(
                (
                    f"{item.get('market_slug') or item.get('market_id')}:"
                    f"{item.get('side') or 'unknown'}({item.get('asset_id')})="
                    f"{int(item.get('count', 0))}"
                )
                for item in recovery["top_stale_assets"][:5]
            )
            lines.append(f"top_stale_assets: {stale_summary}")
        if recovery.get("top_missing_book_assets"):
            missing_asset_summary = ", ".join(
                (
                    f"{item.get('market_slug') or item.get('market_id')}:"
                    f"{item.get('side') or 'unknown'}({item.get('asset_id')})="
                    f"{int(item.get('count', 0))}"
                )
                for item in recovery["top_missing_book_assets"][:5]
            )
            lines.append(f"top_missing_book_assets: {missing_asset_summary}")
        if recovery.get("top_quote_missing_after_resync_assets"):
            quote_missing_summary = ", ".join(
                (
                    f"{item.get('market_slug') or item.get('market_id')}:"
                    f"{item.get('side') or 'unknown'}({item.get('asset_id')})="
                    f"{int(item.get('count', 0))}"
                )
                for item in recovery["top_quote_missing_after_resync_assets"][:5]
            )
            lines.append(f"top_quote_missing_after_resync_assets: {quote_missing_summary}")
        if recovery.get("top_market_blocked_markets"):
            blocked_summary = ", ".join(
                f"{item['market_id']}={item['count']}"
                for item in recovery["top_market_blocked_markets"][:5]
            )
            lines.append(f"top_market_blocked_markets: {blocked_summary}")
        if recovery.get("top_long_stale_markets"):
            long_stale_summary = ", ".join(
                (
                    f"{item.get('market_slug') or item.get('market_id')}="
                    f"{float(item.get('max_stale_duration_ms', 0.0)):.1f}ms"
                )
                for item in recovery["top_long_stale_markets"][:5]
            )
            lines.append(f"top_long_stale_markets: {long_stale_summary}")
        if recovery.get("top_repeated_stale_markets"):
            repeated_stale_summary = ", ".join(
                (
                    f"{item.get('market_slug') or item.get('market_id')}"
                    f"({item.get('market_id')})="
                    f"{int(item.get('enter_count', 0))}"
                )
                for item in recovery["top_repeated_stale_markets"][:5]
            )
            lines.append(f"top_repeated_stale_markets: {repeated_stale_summary}")
        if recovery.get("top_chronic_stale_markets"):
            chronic_summary = ", ".join(
                (
                    f"{item.get('market_slug') or item.get('market_id')}"
                    f"({item.get('market_id')}):"
                    f"{item.get('chronic_reason')}="
                    f"{int(item.get('enter_count', 0))}"
                )
                for item in recovery["top_chronic_stale_markets"][:5]
            )
            lines.append(f"top_chronic_stale_markets: {chronic_summary}")
        if recovery.get("top_reintroduced_chronic_stale_markets"):
            reintroduced_summary = ", ".join(
                (
                    f"{item.get('market_slug') or item.get('market_id')}"
                    f"({item.get('market_id')}):"
                    f"{item.get('reintroduced_reason')}="
                    f"{int(item.get('reintroduced_count', 0))}"
                )
                for item in recovery["top_reintroduced_chronic_stale_markets"][:5]
            )
            lines.append(f"top_reintroduced_chronic_stale_markets: {reintroduced_summary}")
        if recovery.get("top_long_active_chronic_stale_markets"):
            long_active_summary = ", ".join(
                (
                    f"{item.get('market_slug') or item.get('market_id')}"
                    f"({item.get('market_id')})="
                    f"{float(item.get('active_age_ms', 0.0)):.1f}ms"
                )
                for item in recovery["top_long_active_chronic_stale_markets"][:5]
            )
            lines.append(f"top_long_active_chronic_stale_markets: {long_active_summary}")
        if recovery.get("top_repeated_missing_book_markets"):
            missing_market_summary = ", ".join(
                (
                    f"{item.get('market_slug') or item.get('market_id')}"
                    f"({item.get('market_id')}):"
                    f"{item.get('dominant_reason')}="
                    f"{int(item.get('count', 0))}"
                )
                for item in recovery["top_repeated_missing_book_markets"][:5]
            )
            lines.append(f"top_repeated_missing_book_markets: {missing_market_summary}")
        if recovery.get("top_stale_legs"):
            stale_legs_summary = ", ".join(
                (
                    f"{item.get('market_slug') or item.get('market_id')}:"
                    f"{item.get('side')}({item.get('asset_id')})="
                    f"{int(item.get('count', 0))}"
                )
                for item in recovery["top_stale_legs"][:5]
            )
            lines.append(f"top_stale_legs: {stale_legs_summary}")
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
        return int(
            self._latest_metric_float_value(
                conn,
                window,
                run_id=run_id,
                metric_name=metric_name,
            )
        )

    def _latest_metric_float_value(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
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

    def _latest_metric_details(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        metric_name: str,
    ) -> str:
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
        query += " ORDER BY created_at DESC LIMIT 1"
        row = conn.execute(query, params).fetchone()
        if row is None:
            fallback_query = """
            SELECT details
            FROM metrics
            WHERE metric_name = ?
            """
            fallback_params: list[object] = [metric_name]
            if run_id is not None:
                fallback_query += " AND run_id = ?"
                fallback_params.append(run_id)
            fallback_query += " ORDER BY created_at DESC LIMIT 1"
            row = conn.execute(fallback_query, fallback_params).fetchone()
        return str(row[0]) if row and row[0] is not None else ""

    def _latest_metric_prefix_breakdown(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        metric_prefix: str,
    ) -> list[dict[str, Any]]:
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
    def _parse_market_numeric_details(details: str) -> list[tuple[str, float]]:
        results: list[tuple[str, float]] = []
        compact = str(details or "").strip()
        if not compact:
            return results
        for token in compact.split(","):
            if ":" not in token:
                continue
            market_id, value_text = token.split(":", 1)
            market_id = market_id.strip()
            value_text = value_text.strip()
            if not market_id:
                continue
            try:
                numeric_value = float(value_text)
            except ValueError:
                continue
            results.append((market_id, numeric_value))
        return results

    def _market_context_map(
        self,
        conn: sqlite3.Connection,
        *,
        market_ids: list[str],
    ) -> dict[str, dict[str, str]]:
        unique_ids = [market_id for market_id in dict.fromkeys(market_ids) if market_id]
        if not unique_ids:
            return {}
        placeholders = ", ".join("?" for _ in unique_ids)
        query = f"""
        SELECT market_id, MAX(COALESCE(slug, '')) AS slug, MAX(COALESCE(question, '')) AS question
        FROM markets
        WHERE market_id IN ({placeholders})
        GROUP BY market_id
        """
        rows = conn.execute(query, unique_ids).fetchall()
        return {
            str(row[0]): {
                "market_slug": str(row[1] or ""),
                "market_question": str(row[2] or ""),
            }
            for row in rows
            if str(row[0] or "")
        }

    def _top_markets_from_latest_metric_details(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        metric_name: str,
        value_key: str,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        parsed = self._parse_market_numeric_details(
            self._latest_metric_details(
                conn,
                window,
                run_id=run_id,
                metric_name=metric_name,
            )
        )
        if not parsed:
            return []
        contexts = self._market_context_map(
            conn,
            market_ids=[market_id for market_id, _ in parsed],
        )
        rows: list[dict[str, Any]] = []
        for market_id, value in parsed:
            context = contexts.get(market_id, {"market_slug": "", "market_question": ""})
            rows.append(
                {
                    "market_id": market_id,
                    "market_slug": context["market_slug"],
                    "market_question": context["market_question"],
                    value_key: float(value),
                }
            )
        rows.sort(key=lambda item: float(item.get(value_key, 0.0)), reverse=True)
        return rows[: max(1, int(limit))]

    @staticmethod
    def _extract_kv_from_details(details: str, key: str) -> str | None:
        compact = details.strip()
        if key == "reason" and compact and "=" not in compact:
            return compact
        return extract_detail_value(details, key)

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
    def _empty_recovery_diagnostics() -> dict[str, Any]:
        return {
            "recovery_resync_started_count": 0,
            "recovery_first_quote_success_count": 0,
            "recovery_book_ready_success_count": 0,
            "recovery_market_ready_success_count": 0,
            "recovery_market_recovery_started_count": 0,
            "recovery_first_quote_blocked_count": 0,
            "recovery_book_ready_blocked_count": 0,
            "recovery_market_ready_blocked_count": 0,
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
            "first_quote_blocked_reasons": [],
            "book_ready_blocked_reasons": [],
            "market_ready_blocked_reasons": [],
            "eligibility_gate_unmet_reasons": [],
            "recovery_universe_change_resync_started_count": 0,
            "recovery_universe_change_first_quote_success_count": 0,
            "recovery_universe_change_book_ready_success_count": 0,
            "recovery_universe_change_market_ready_success_count": 0,
            "recovery_universe_change_first_quote_blocked_count": 0,
            "recovery_universe_change_book_ready_blocked_count": 0,
            "recovery_universe_change_market_ready_blocked_count": 0,
            "recovery_universe_change_first_quote_success_rate": 0.0,
            "recovery_universe_change_book_ready_success_rate": 0.0,
            "recovery_universe_change_market_ready_success_rate": 0.0,
            "market_stale_enter_count": 0,
            "market_stale_recover_count": 0,
            "avg_market_stale_duration_ms": 0.0,
            "max_market_stale_duration_ms": 0.0,
            "market_stale_universe_change_enter_count": 0,
            "chronic_stale_exclusion_enter_count": 0,
            "chronic_stale_exclusion_active_count": 0,
            "chronic_stale_exclusion_extended_count": 0,
            "chronic_stale_exclusion_cleared_count": 0,
            "chronic_stale_exclusion_avg_active_age_ms": 0.0,
            "chronic_stale_exclusion_long_active_market_count": 0,
            "chronic_stale_reason_breakdown": [],
            "chronic_stale_extension_reason_breakdown": [],
            "watched_chronic_stale_reason_breakdown": [],
            "market_stale_reason_breakdown": [],
            "market_stale_side_breakdown": [],
            "market_ready_blocked_stale_reason_breakdown": [],
            "eligibility_gate_stale_reason_breakdown": [],
            "no_signal_stale_reason_breakdown": [],
            "universe_change_market_ready_blocked_stale_reason_breakdown": [],
            "top_long_stale_markets": [],
            "top_repeated_stale_markets": [],
            "top_chronic_stale_markets": [],
            "top_reintroduced_chronic_stale_markets": [],
            "top_long_active_chronic_stale_markets": [],
            "top_stale_legs": [],
            "top_stale_assets": [],
            "top_missing_book_assets": [],
            "top_quote_missing_after_resync_assets": [],
            "top_repeated_missing_book_markets": [],
            "top_market_blocked_markets": [],
            "top_recovery_slow_assets": [],
            "top_recovery_slow_markets": [],
        }

    def _count_diagnostics_event(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
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

    def _diagnostics_latency_stats(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
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
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
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

    def _top_asset_counts_for_event(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        event_name: str,
        reason: str | None = None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
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
        if reason is not None:
            query += " AND d.reason = ?"
            params.append(reason)
        if run_id is not None:
            query += " AND d.run_id = ?"
            params.append(run_id)
        query += " GROUP BY d.asset_id ORDER BY count DESC LIMIT ?"
        params.append(int(limit))
        rows = conn.execute(query, params).fetchall()
        return [
            {
                "asset_id": str(row[0]),
                "count": int(row[1]),
                "market_id": str(row[2]) if row[2] is not None else "",
                "market_slug": str(row[3]) if row[3] is not None else "",
                "market_question": str(row[4]) if row[4] is not None else "",
                "side": str(row[5]) if row[5] is not None else "",
            }
            for row in rows
        ]

    def _top_market_counts_for_event(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        event_name: str,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
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
        params: list[object] = [window.start_iso, window.end_iso, event_name]
        if run_id is not None:
            query += " AND d.run_id = ?"
            params.append(run_id)
        query += " GROUP BY d.market_id ORDER BY count DESC LIMIT ?"
        params.append(int(limit))
        rows = conn.execute(query, params).fetchall()
        return [
            {
                "market_id": str(row[0]),
                "count": int(row[1]),
                "market_slug": str(row[2]) if row[2] is not None else "",
                "market_question": str(row[3]) if row[3] is not None else "",
            }
            for row in rows
        ]

    def _top_slow_assets(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> list[dict[str, Any]]:
        def _query(event_name: str) -> list[tuple[object, object, object, object]]:
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
              AVG(d.latency_ms),
              MAX(d.latency_ms),
              COUNT(*),
              MAX(COALESCE(d.market_id, ma.market_id)),
              MAX(COALESCE(ma.slug, m.slug, '')),
              MAX(COALESCE(ma.question, m.question, '')),
              MAX(COALESCE(ma.side, ''))
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
                " GROUP BY d.asset_id ORDER BY MAX(d.latency_ms) DESC, "
                "AVG(d.latency_ms) DESC LIMIT 5"
            )
            return conn.execute(query, params).fetchall()

        rows = _query("book_ready_after_resync")
        if not rows:
            rows = _query("first_quote_after_resync")
        return [
            {
                "asset_id": str(row[0]),
                "avg_latency_ms": float(row[1] if row[1] is not None else 0.0),
                "max_latency_ms": float(row[2] if row[2] is not None else 0.0),
                "success_count": int(row[3] if row[3] is not None else 0),
                "market_id": str(row[4]) if row[4] is not None else "",
                "market_slug": str(row[5]) if row[5] is not None else "",
                "market_question": str(row[6]) if row[6] is not None else "",
                "side": str(row[7]) if row[7] is not None else "",
            }
            for row in rows
        ]

    def _top_slow_markets(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT
          d.market_id,
          AVG(d.latency_ms),
          MAX(d.latency_ms),
          COUNT(*),
          MAX(COALESCE(m.slug, '')),
          MAX(COALESCE(m.question, ''))
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
        query += (
            " GROUP BY d.market_id ORDER BY MAX(d.latency_ms) DESC, "
            "AVG(d.latency_ms) DESC LIMIT 5"
        )
        rows = conn.execute(query, params).fetchall()
        return [
            {
                "market_id": str(row[0]),
                "avg_latency_ms": float(row[1] if row[1] is not None else 0.0),
                "max_latency_ms": float(row[2] if row[2] is not None else 0.0),
                "success_count": int(row[3] if row[3] is not None else 0),
                "market_slug": str(row[4]) if row[4] is not None else "",
                "market_question": str(row[5]) if row[5] is not None else "",
            }
            for row in rows
        ]

    def _count_diagnostics_event_with_reason(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
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

    def _count_distinct_market_diagnostics_events_with_reason(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        event_names: tuple[str, ...],
        reason: str,
    ) -> int:
        names = [name for name in event_names if name]
        if not names:
            return 0
        placeholders = ", ".join("?" for _ in names)
        query = f"""
        SELECT COUNT(DISTINCT market_id)
        FROM diagnostics_events
        WHERE created_at >= ? AND created_at < ?
          AND event_name IN ({placeholders})
          AND reason = ?
          AND market_id IS NOT NULL
          AND market_id != ''
        """
        params: list[object] = [window.start_iso, window.end_iso, *names, reason]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        row = conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def _count_diagnostics_event_with_details_like(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
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

    def _diagnostics_reasons_for_event(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        event_name: str,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
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
        rows = conn.execute(query, params).fetchall()
        return [
            {
                "reason": str(row[0]) if row[0] is not None else "unknown",
                "count": int(row[1] if row[1] is not None else 0),
            }
            for row in rows
        ]

    def _diagnostics_detail_breakdown_for_event(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        event_name: str,
        detail_key: str,
        reason_like: str | None = None,
        details_like: str | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
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
            detail = self._extract_kv_from_details(details_text, detail_key)
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
        items = sorted(counts.items(), key=lambda item: item[1], reverse=True)
        return [{"reason": reason, "count": count} for reason, count in items[: max(1, int(limit))]]

    def _metric_stale_reason_breakdown(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
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
            reason = str(metric_name).split(":", 1)[-1]
            stale_reason_key = stale_reason_key_from_reason_and_details(
                reason=reason,
                details=str(details or ""),
            )
            normalized = normalize_stale_reason_key(stale_reason_key)
            increment = int(round(float(metric_value if metric_value is not None else 1.0)))
            counts[normalized] = counts.get(normalized, 0) + max(1, increment)
        items = sorted(counts.items(), key=lambda item: item[1], reverse=True)
        return [{"reason": reason, "count": count} for reason, count in items[: max(1, int(limit))]]

    def _top_long_stale_markets(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
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
            " GROUP BY d.market_id ORDER BY MAX(d.latency_ms) DESC, AVG(d.latency_ms) DESC LIMIT ?"
        )
        params.append(max(1, int(limit)))
        rows = conn.execute(query, params).fetchall()
        return [
            {
                "market_id": str(row[0]),
                "recover_count": int(row[1] if row[1] is not None else 0),
                "avg_stale_duration_ms": float(row[2] if row[2] is not None else 0.0),
                "max_stale_duration_ms": float(row[3] if row[3] is not None else 0.0),
                "market_slug": str(row[4] if row[4] is not None else ""),
                "market_question": str(row[5] if row[5] is not None else ""),
            }
            for row in rows
        ]

    def _top_repeated_stale_markets(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
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
            " ORDER BY enter_count DESC, recover_count DESC LIMIT ?"
        )
        params.append(max(1, int(limit)))
        rows = conn.execute(query, params).fetchall()
        return [
            {
                "market_id": str(row[0]),
                "enter_count": int(row[1] if row[1] is not None else 0),
                "recover_count": int(row[2] if row[2] is not None else 0),
                "market_slug": str(row[3] if row[3] is not None else ""),
                "market_question": str(row[4] if row[4] is not None else ""),
            }
            for row in rows
        ]

    def _top_chronic_stale_markets(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT
          d.market_id,
          d.reason,
          d.details,
          MAX(COALESCE(m.slug, '')) AS market_slug,
          MAX(COALESCE(m.question, '')) AS market_question
        FROM diagnostics_events d
        LEFT JOIN markets m
          ON m.market_id = d.market_id
        WHERE d.created_at >= ? AND d.created_at < ?
          AND d.event_name = 'market_chronic_stale_exclusion_entered'
          AND d.market_id IS NOT NULL
          AND d.market_id != ''
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND d.run_id = ?"
            params.append(run_id)
        query += " GROUP BY d.id ORDER BY d.created_at DESC"
        rows = conn.execute(query, params).fetchall()
        grouped: dict[str, dict[str, Any]] = {}
        for market_id_value, reason_value, details_value, slug_value, question_value in rows:
            market_id = str(market_id_value or "")
            if not market_id:
                continue
            reason = str(reason_value or "")
            details_map = parse_kv_details(str(details_value or ""))
            stale_enter_count = int(float(details_map.get("stale_enter_count", "0") or 0.0))
            max_stale_duration_ms = float(details_map.get("max_stale_duration_ms", "0") or 0.0)
            entry = grouped.setdefault(
                market_id,
                {
                    "market_id": market_id,
                    "market_slug": str(slug_value or ""),
                    "market_question": str(question_value or ""),
                    "enter_count": 0,
                    "chronic_reason": reason or "repeated_stale_enters",
                    "latest_stale_enter_count": 0,
                    "max_observed_stale_duration_ms": 0.0,
                },
            )
            entry["enter_count"] = int(entry["enter_count"]) + 1
            entry["latest_stale_enter_count"] = max(
                int(entry["latest_stale_enter_count"]),
                stale_enter_count,
            )
            entry["max_observed_stale_duration_ms"] = max(
                float(entry["max_observed_stale_duration_ms"]),
                max_stale_duration_ms,
            )
            if not str(entry["chronic_reason"]) and reason:
                entry["chronic_reason"] = reason
        items = sorted(
            grouped.values(),
            key=lambda item: (
                int(item.get("enter_count", 0)),
                float(item.get("max_observed_stale_duration_ms", 0.0)),
            ),
            reverse=True,
        )
        return items[: max(1, int(limit))]

    def _top_reintroduced_chronic_stale_markets(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT
          d.market_id,
          d.reason,
          COUNT(*) AS reintroduced_count,
          MAX(COALESCE(m.slug, '')) AS market_slug,
          MAX(COALESCE(m.question, '')) AS market_question
        FROM diagnostics_events d
        LEFT JOIN markets m
          ON m.market_id = d.market_id
        WHERE d.created_at >= ? AND d.created_at < ?
          AND d.event_name = 'market_chronic_stale_reintroduced_for_floor'
          AND d.market_id IS NOT NULL
          AND d.market_id != ''
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND d.run_id = ?"
            params.append(run_id)
        query += (
            " GROUP BY d.market_id, d.reason"
            " ORDER BY reintroduced_count DESC, d.market_id ASC LIMIT ?"
        )
        params.append(max(1, int(limit)))
        rows = conn.execute(query, params).fetchall()
        return [
            {
                "market_id": str(row[0] or ""),
                "reintroduced_reason": str(row[1] or "watched_floor_backfill_chronic_relaxed"),
                "reintroduced_count": int(row[2] if row[2] is not None else 0),
                "market_slug": str(row[3] or ""),
                "market_question": str(row[4] or ""),
            }
            for row in rows
            if str(row[0] or "")
        ]

    def _top_repeated_missing_book_markets(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT
          d.market_id,
          d.reason,
          COUNT(*) AS count,
          MAX(COALESCE(m.slug, '')) AS market_slug,
          MAX(COALESCE(m.question, '')) AS market_question
        FROM diagnostics_events d
        LEFT JOIN markets m
          ON m.market_id = d.market_id
        WHERE d.created_at >= ? AND d.created_at < ?
          AND d.event_name = 'missing_book_state_detected'
          AND d.market_id IS NOT NULL
          AND d.market_id != ''
        """
        params: list[object] = [window.start_iso, window.end_iso]
        if run_id is not None:
            query += " AND d.run_id = ?"
            params.append(run_id)
        query += " GROUP BY d.market_id, d.reason ORDER BY count DESC"
        rows = conn.execute(query, params).fetchall()
        grouped: dict[str, dict[str, Any]] = {}
        for market_id_value, reason_value, count_value, slug_value, question_value in rows:
            market_id = str(market_id_value or "")
            if not market_id:
                continue
            count = int(count_value if count_value is not None else 0)
            reason = str(reason_value or "unknown")
            entry = grouped.setdefault(
                market_id,
                {
                    "market_id": market_id,
                    "market_slug": str(slug_value or ""),
                    "market_question": str(question_value or ""),
                    "count": 0,
                    "dominant_reason": reason,
                },
            )
            entry["count"] = int(entry["count"]) + count
            dominant_reason = str(entry["dominant_reason"])
            if count > int(entry.get("_dominant_count", 0)):
                entry["dominant_reason"] = reason
                entry["_dominant_count"] = count
            elif not dominant_reason:
                entry["dominant_reason"] = reason
        items = sorted(grouped.values(), key=lambda item: int(item.get("count", 0)), reverse=True)
        for item in items:
            item.pop("_dominant_count", None)
        return items[: max(1, int(limit))]

    def _top_stale_legs(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
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
        counts: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in rows:
            market_id = str(row[0] or "")
            reason = str(row[1] or "")
            details = str(row[2] or "")
            market_slug = str(row[3] or "")
            market_question = str(row[4] or "")
            yes_asset_id = str(row[5] or "")
            no_asset_id = str(row[6] or "")
            detail_map = parse_kv_details(details)
            stale_reason_key = stale_reason_key_from_reason_and_details(
                reason=reason, details=details
            )
            asset_ids_raw = detail_map.get("stale_asset_ids") or detail_map.get(
                "stale_asset_id", ""
            )
            candidate_assets = [item.strip() for item in asset_ids_raw.split(",") if item.strip()]
            if not candidate_assets:
                stale_side = normalize_stale_side(detail_map.get("stale_side"))
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
                entry["count"] += 1
        top_items = sorted(
            counts.values(),
            key=lambda item: int(item.get("count", 0)),
            reverse=True,
        )
        return top_items[: max(1, int(limit))]

    def _recovery_diagnostics(
        self,
        conn: sqlite3.Connection,
        window: ReportWindow,
        *,
        run_id: str | None,
    ) -> dict[str, Any]:
        if not self._table_exists(conn, "diagnostics_events"):
            return self._empty_recovery_diagnostics()
        try:
            resync_started_count = self._count_diagnostics_event(
                conn,
                window,
                run_id=run_id,
                event_name="resync_started",
            )
            first_quote_success_count = self._count_diagnostics_event(
                conn,
                window,
                run_id=run_id,
                event_name="first_quote_after_resync",
            )
            book_ready_success_count = self._count_diagnostics_event(
                conn,
                window,
                run_id=run_id,
                event_name="book_ready_after_resync",
            )
            market_recovery_started_count = self._count_diagnostics_event(
                conn,
                window,
                run_id=run_id,
                event_name="market_recovery_started",
            )
            market_ready_success_count = self._count_diagnostics_event(
                conn,
                window,
                run_id=run_id,
                event_name="market_ready_after_recovery",
            )
            first_quote_blocked_count = self._count_diagnostics_event(
                conn,
                window,
                run_id=run_id,
                event_name="first_quote_after_resync_blocked",
            )
            book_ready_blocked_count = self._count_diagnostics_event(
                conn,
                window,
                run_id=run_id,
                event_name="book_ready_after_resync_blocked",
            )
            market_ready_blocked_count = self._count_diagnostics_event(
                conn,
                window,
                run_id=run_id,
                event_name="market_ready_after_recovery_blocked",
            )
            first_avg_ms, first_max_ms = self._diagnostics_latency_stats(
                conn,
                window,
                run_id=run_id,
                event_name="first_quote_after_resync",
            )
            book_avg_ms, book_max_ms = self._diagnostics_latency_stats(
                conn,
                window,
                run_id=run_id,
                event_name="book_ready_after_resync",
            )
            market_avg_ms, market_max_ms = self._diagnostics_latency_stats(
                conn,
                window,
                run_id=run_id,
                event_name="market_ready_after_recovery",
            )
            # Universe-change funnel is defined in consistent market-level stages:
            # resync-started -> first-quote-progressed -> book-ready-progressed -> market-ready.
            # "progressed" is cumulative by stage so counts remain monotonic.
            universe_resync_started_count = (
                self._count_distinct_market_diagnostics_events_with_reason(
                    conn,
                    window,
                    run_id=run_id,
                    event_names=("resync_started",),
                    reason="market_universe_changed",
                )
            )
            universe_first_quote_success_count = (
                self._count_distinct_market_diagnostics_events_with_reason(
                    conn,
                    window,
                    run_id=run_id,
                    event_names=(
                        "first_quote_after_resync",
                        "book_ready_after_resync",
                        "market_ready_after_recovery",
                    ),
                    reason="market_universe_changed",
                )
            )
            universe_book_ready_success_count = (
                self._count_distinct_market_diagnostics_events_with_reason(
                    conn,
                    window,
                    run_id=run_id,
                    event_names=("book_ready_after_resync", "market_ready_after_recovery"),
                    reason="market_universe_changed",
                )
            )
            universe_market_ready_success_count = (
                self._count_distinct_market_diagnostics_events_with_reason(
                    conn,
                    window,
                    run_id=run_id,
                    event_names=("market_ready_after_recovery",),
                    reason="market_universe_changed",
                )
            )
            universe_first_quote_success_count = min(
                universe_first_quote_success_count,
                universe_resync_started_count,
            )
            universe_book_ready_success_count = min(
                universe_book_ready_success_count,
                universe_first_quote_success_count,
            )
            universe_market_ready_success_count = min(
                universe_market_ready_success_count,
                universe_book_ready_success_count,
            )
            universe_first_quote_blocked_count = self._count_diagnostics_event_with_details_like(
                conn,
                window,
                run_id=run_id,
                event_name="first_quote_after_resync_blocked",
                details_like="%recovery_reason=market_universe_changed%",
            )
            universe_book_ready_blocked_count = self._count_diagnostics_event_with_details_like(
                conn,
                window,
                run_id=run_id,
                event_name="book_ready_after_resync_blocked",
                details_like="%recovery_reason=market_universe_changed%",
            )
            universe_market_ready_blocked_count = self._count_diagnostics_event_with_details_like(
                conn,
                window,
                run_id=run_id,
                event_name="market_ready_after_recovery_blocked",
                details_like="%recovery_reason=market_universe_changed%",
            )
            market_stale_enter_count = self._count_diagnostics_event(
                conn,
                window,
                run_id=run_id,
                event_name="market_stale_entered",
            )
            market_stale_recover_count = self._count_diagnostics_event(
                conn,
                window,
                run_id=run_id,
                event_name="market_stale_recovered",
            )
            stale_avg_ms, stale_max_ms = self._diagnostics_latency_stats_for_events(
                conn,
                window,
                run_id=run_id,
                event_names=STALE_DURATION_EVENT_NAMES,
            )
            market_stale_universe_change_enter_count = (
                self._count_diagnostics_event_with_details_like(
                    conn,
                    window,
                    run_id=run_id,
                    event_name="market_stale_entered",
                    details_like="%universe_change_related=1%",
                )
            )
            chronic_stale_exclusion_enter_count = self._count_diagnostics_event(
                conn,
                window,
                run_id=run_id,
                event_name="market_chronic_stale_exclusion_entered",
            )
            chronic_stale_exclusion_extended_count = self._count_diagnostics_event(
                conn,
                window,
                run_id=run_id,
                event_name="market_chronic_stale_exclusion_extended",
            )
            chronic_stale_exclusion_cleared_count = self._count_diagnostics_event(
                conn,
                window,
                run_id=run_id,
                event_name="market_chronic_stale_exclusion_cleared",
            )
            chronic_stale_exclusion_active_count = self._latest_metric_value(
                conn,
                window,
                run_id=run_id,
                metric_name="chronic_stale_exclusion_active_count",
            )
            chronic_stale_reason_breakdown = self._diagnostics_reasons_for_event(
                conn,
                window,
                run_id=run_id,
                event_name="market_chronic_stale_exclusion_entered",
            )
            chronic_stale_extension_reason_breakdown = self._diagnostics_reasons_for_event(
                conn,
                window,
                run_id=run_id,
                event_name="market_chronic_stale_exclusion_extended",
            )
            chronic_stale_exclusion_avg_active_age_ms = self._latest_metric_float_value(
                conn,
                window,
                run_id=run_id,
                metric_name="chronic_stale_exclusion_avg_active_age_ms",
            )
            chronic_stale_exclusion_long_active_market_count = self._latest_metric_value(
                conn,
                window,
                run_id=run_id,
                metric_name="chronic_stale_exclusion_long_active_market_count",
            )
            chronic_stale_reintroduced_for_floor_count = self._latest_metric_value(
                conn,
                window,
                run_id=run_id,
                metric_name="chronic_stale_reintroduced_for_floor_count",
            )
            chronic_stale_reintroduced_market_count = self._latest_metric_value(
                conn,
                window,
                run_id=run_id,
                metric_name="chronic_stale_reintroduced_market_count",
            )
            watched_chronic_stale_excluded_market_count = self._latest_metric_value(
                conn,
                window,
                run_id=run_id,
                metric_name="watched_chronic_stale_excluded_market_count",
            )
            watched_chronic_stale_reason_breakdown = self._latest_metric_prefix_breakdown(
                conn,
                window,
                run_id=run_id,
                metric_prefix="watched_chronic_stale_reason:",
            )
            market_stale_reason_breakdown = self._diagnostics_detail_breakdown_for_event(
                conn,
                window,
                run_id=run_id,
                event_name="market_stale_entered",
                detail_key="stale_reason_key",
            )
            market_stale_side_breakdown = self._diagnostics_detail_breakdown_for_event(
                conn,
                window,
                run_id=run_id,
                event_name="market_stale_entered",
                detail_key="stale_side",
            )
            market_ready_blocked_stale_reason_breakdown = (
                self._diagnostics_detail_breakdown_for_event(
                    conn,
                    window,
                    run_id=run_id,
                    event_name="market_ready_after_recovery_blocked",
                    detail_key="stale_reason_key",
                    reason_like="market_quote_stale%",
                )
            )
            eligibility_gate_stale_reason_breakdown = self._diagnostics_detail_breakdown_for_event(
                conn,
                window,
                run_id=run_id,
                event_name="eligibility_gate_unmet",
                detail_key="stale_reason_key",
                details_like="%category=stale_quote_freshness%",
            )
            no_signal_stale_reason_breakdown = self._metric_stale_reason_breakdown(
                conn,
                window,
                run_id=run_id,
            )
            if not market_stale_reason_breakdown:
                market_stale_reason_breakdown = no_signal_stale_reason_breakdown
            universe_change_market_ready_blocked_stale_reason_breakdown = (
                self._diagnostics_detail_breakdown_for_event(
                    conn,
                    window,
                    run_id=run_id,
                    event_name="market_ready_after_recovery_blocked",
                    detail_key="stale_reason_key",
                    reason_like="market_quote_stale%",
                    details_like="%recovery_reason=market_universe_changed%",
                )
            )
            return {
                "recovery_resync_started_count": resync_started_count,
                "recovery_first_quote_success_count": first_quote_success_count,
                "recovery_book_ready_success_count": book_ready_success_count,
                "recovery_market_ready_success_count": market_ready_success_count,
                "recovery_market_recovery_started_count": market_recovery_started_count,
                "recovery_first_quote_blocked_count": first_quote_blocked_count,
                "recovery_book_ready_blocked_count": book_ready_blocked_count,
                "recovery_market_ready_blocked_count": market_ready_blocked_count,
                "recovery_first_quote_success_rate": (
                    first_quote_success_count / resync_started_count
                    if resync_started_count > 0
                    else 0.0
                ),
                "recovery_book_ready_success_rate": (
                    book_ready_success_count / resync_started_count
                    if resync_started_count > 0
                    else 0.0
                ),
                "recovery_market_ready_success_rate": (
                    market_ready_success_count / resync_started_count
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
                    conn,
                    window,
                    run_id=run_id,
                    event_name="first_quote_after_resync_blocked",
                ),
                "book_ready_blocked_reasons": self._diagnostics_reasons_for_event(
                    conn,
                    window,
                    run_id=run_id,
                    event_name="book_ready_after_resync_blocked",
                ),
                "market_ready_blocked_reasons": self._diagnostics_reasons_for_event(
                    conn,
                    window,
                    run_id=run_id,
                    event_name="market_ready_after_recovery_blocked",
                ),
                "eligibility_gate_unmet_reasons": self._diagnostics_reasons_for_event(
                    conn,
                    window,
                    run_id=run_id,
                    event_name="eligibility_gate_unmet",
                ),
                "recovery_universe_change_resync_started_count": (universe_resync_started_count),
                "recovery_universe_change_first_quote_success_count": (
                    universe_first_quote_success_count
                ),
                "recovery_universe_change_book_ready_success_count": (
                    universe_book_ready_success_count
                ),
                "recovery_universe_change_market_ready_success_count": (
                    universe_market_ready_success_count
                ),
                "recovery_universe_change_first_quote_blocked_count": (
                    universe_first_quote_blocked_count
                ),
                "recovery_universe_change_book_ready_blocked_count": (
                    universe_book_ready_blocked_count
                ),
                "recovery_universe_change_market_ready_blocked_count": (
                    universe_market_ready_blocked_count
                ),
                "recovery_universe_change_first_quote_success_rate": (
                    universe_first_quote_success_count / universe_resync_started_count
                    if universe_resync_started_count > 0
                    else 0.0
                ),
                "recovery_universe_change_book_ready_success_rate": (
                    universe_book_ready_success_count / universe_resync_started_count
                    if universe_resync_started_count > 0
                    else 0.0
                ),
                "recovery_universe_change_market_ready_success_rate": (
                    universe_market_ready_success_count / universe_resync_started_count
                    if universe_resync_started_count > 0
                    else 0.0
                ),
                "market_stale_enter_count": market_stale_enter_count,
                "market_stale_recover_count": market_stale_recover_count,
                "avg_market_stale_duration_ms": stale_avg_ms,
                "max_market_stale_duration_ms": stale_max_ms,
                "market_stale_universe_change_enter_count": (
                    market_stale_universe_change_enter_count
                ),
                "chronic_stale_exclusion_enter_count": chronic_stale_exclusion_enter_count,
                "chronic_stale_exclusion_extended_count": chronic_stale_exclusion_extended_count,
                "chronic_stale_exclusion_active_count": chronic_stale_exclusion_active_count,
                "chronic_stale_exclusion_cleared_count": chronic_stale_exclusion_cleared_count,
                "chronic_stale_exclusion_avg_active_age_ms": (
                    chronic_stale_exclusion_avg_active_age_ms
                ),
                "chronic_stale_exclusion_long_active_market_count": (
                    chronic_stale_exclusion_long_active_market_count
                ),
                "chronic_stale_reintroduced_for_floor_count": (
                    chronic_stale_reintroduced_for_floor_count
                ),
                "chronic_stale_reintroduced_market_count": (
                    chronic_stale_reintroduced_market_count
                ),
                "watched_chronic_stale_excluded_market_count": (
                    watched_chronic_stale_excluded_market_count
                ),
                "chronic_stale_reason_breakdown": chronic_stale_reason_breakdown,
                "chronic_stale_extension_reason_breakdown": (
                    chronic_stale_extension_reason_breakdown
                ),
                "watched_chronic_stale_reason_breakdown": (watched_chronic_stale_reason_breakdown),
                "market_stale_reason_breakdown": market_stale_reason_breakdown,
                "market_stale_side_breakdown": market_stale_side_breakdown,
                "market_ready_blocked_stale_reason_breakdown": (
                    market_ready_blocked_stale_reason_breakdown
                ),
                "eligibility_gate_stale_reason_breakdown": eligibility_gate_stale_reason_breakdown,
                "no_signal_stale_reason_breakdown": no_signal_stale_reason_breakdown,
                "universe_change_market_ready_blocked_stale_reason_breakdown": (
                    universe_change_market_ready_blocked_stale_reason_breakdown
                ),
                "top_stale_assets": self._top_asset_counts_for_event(
                    conn,
                    window,
                    run_id=run_id,
                    event_name="stale_asset_detected",
                ),
                "top_missing_book_assets": self._top_asset_counts_for_event(
                    conn,
                    window,
                    run_id=run_id,
                    event_name="missing_book_state_detected",
                ),
                "top_quote_missing_after_resync_assets": self._top_asset_counts_for_event(
                    conn,
                    window,
                    run_id=run_id,
                    event_name="missing_book_state_detected",
                    reason="quote_missing_after_resync",
                ),
                "top_market_blocked_markets": self._top_market_counts_for_event(
                    conn,
                    window,
                    run_id=run_id,
                    event_name="market_block_entered",
                ),
                "top_recovery_slow_assets": self._top_slow_assets(
                    conn,
                    window,
                    run_id=run_id,
                ),
                "top_recovery_slow_markets": self._top_slow_markets(
                    conn,
                    window,
                    run_id=run_id,
                ),
                "top_long_stale_markets": self._top_long_stale_markets(
                    conn,
                    window,
                    run_id=run_id,
                ),
                "top_repeated_stale_markets": self._top_repeated_stale_markets(
                    conn,
                    window,
                    run_id=run_id,
                ),
                "top_chronic_stale_markets": self._top_chronic_stale_markets(
                    conn,
                    window,
                    run_id=run_id,
                ),
                "top_reintroduced_chronic_stale_markets": (
                    self._top_reintroduced_chronic_stale_markets(
                        conn,
                        window,
                        run_id=run_id,
                    )
                ),
                "top_long_active_chronic_stale_markets": (
                    self._top_markets_from_latest_metric_details(
                        conn,
                        window,
                        run_id=run_id,
                        metric_name="chronic_stale_exclusion_long_active_market_count",
                        value_key="active_age_ms",
                    )
                ),
                "top_repeated_missing_book_markets": self._top_repeated_missing_book_markets(
                    conn,
                    window,
                    run_id=run_id,
                ),
                "top_stale_legs": self._top_stale_legs(
                    conn,
                    window,
                    run_id=run_id,
                ),
            }
        except sqlite3.OperationalError:
            return self._empty_recovery_diagnostics()

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
