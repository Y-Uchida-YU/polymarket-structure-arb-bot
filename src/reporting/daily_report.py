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
            safe_mode_count = self._count_metric(
                conn,
                window,
                run_id=run_id,
                name="safe_mode_entered",
            )

            pnl_sums = self._sum_pnl(conn, window, run_id=run_id)
            top_markets_by_signal = self._top_markets_by_signal(conn, window, run_id=run_id)
            top_markets_by_pnl = self._top_markets_by_pnl(conn, window, run_id=run_id)
            top_reject_reasons = self._top_reject_reasons(conn, window, run_id=run_id)

        fill_rate = signals_with_fill / total_signals if total_signals > 0 else 0.0
        matched_fill_rate = matched_fill_signals / total_signals if total_signals > 0 else 0.0
        one_leg_rate = one_leg_count / total_signals if total_signals > 0 else 0.0
        stale_reject_rate = stale_reject_count / total_signals if total_signals > 0 else 0.0
        depth_reject_rate = depth_reject_count / total_signals if total_signals > 0 else 0.0

        warnings: list[str] = []
        if total_signals == 0:
            warnings.append("no_signals_in_window")
        if fill_rate < 0.1 and total_signals > 0:
            warnings.append("low_fill_rate")
        if one_leg_rate > 0.3:
            warnings.append("high_one_leg_rate")
        if stale_reject_rate > 0.3:
            warnings.append("high_stale_reject_rate")
        if safe_mode_count > 0:
            warnings.append("safe_mode_triggered")

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
                "safe_mode_count": safe_mode_count,
                "projected_matched_pnl": pnl_sums["projected_matched_pnl"],
                "unmatched_inventory_mtm": pnl_sums["unmatched_inventory_mtm"],
                "total_projected_pnl": pnl_sums["total_projected_pnl"],
            },
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
            f"safe_mode_count: {totals['safe_mode_count']}",
            f"projected_matched_pnl: {totals['projected_matched_pnl']:.6f}",
            f"unmatched_inventory_mtm: {totals['unmatched_inventory_mtm']:.6f}",
            f"total_projected_pnl: {totals['total_projected_pnl']:.6f}",
        ]
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
