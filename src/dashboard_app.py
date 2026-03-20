from __future__ import annotations

import argparse
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from src.dashboard.data_loader import DashboardDataLoader, DashboardWindow, resolve_window

TIMEZONE_LABELS = {
    "JST (Asia/Tokyo)": "Asia/Tokyo",
    "UTC": "UTC",
}
DEFAULT_TIMEZONE_LABEL = "JST (Asia/Tokyo)"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--db-path",
        default="data/state/state.db",
        help="SQLite file path (default: data/state/state.db)",
    )
    return parser.parse_args()


def _show_empty_state(db_path: Path) -> None:
    st.info("No SQLite data found yet. Start a shadow paper run, then reload this page.")
    st.code(f"Expected DB path: {db_path}")


def _to_local(frame: pd.DataFrame, *, columns: list[str], timezone: ZoneInfo) -> pd.DataFrame:
    if frame.empty:
        return frame
    converted = frame.copy()
    for column in columns:
        if column not in converted.columns:
            continue
        converted[column] = (
            pd.to_datetime(converted[column], utc=True, errors="coerce")
            .dt.tz_convert(timezone)
            .dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        )
    return converted


def _to_local_window(window: DashboardWindow, timezone: ZoneInfo) -> tuple[str, str]:
    start = datetime.fromisoformat(window.start_iso).astimezone(timezone)
    end = datetime.fromisoformat(window.end_iso).astimezone(timezone)
    return (
        start.strftime("%Y-%m-%d %H:%M:%S %Z"),
        end.strftime("%Y-%m-%d %H:%M:%S %Z"),
    )


def _to_utc_from_local_input(local_dt: datetime, timezone: ZoneInfo) -> datetime:
    if local_dt.tzinfo is None:
        local_dt = local_dt.replace(tzinfo=timezone)
    return local_dt.astimezone(UTC)


def _reason_chart_frame(frame: pd.DataFrame, *, key_column: str, top_n: int = 6) -> pd.DataFrame:
    if frame.empty or key_column not in frame.columns:
        return pd.DataFrame()
    ranked = frame.groupby(key_column)["count"].sum().sort_values(ascending=False)
    keep = set(ranked.head(top_n).index)
    filtered = frame.copy()
    filtered[key_column] = filtered[key_column].where(filtered[key_column].isin(keep), "other")
    return (
        filtered.pivot_table(
            index="timestamp",
            columns=key_column,
            values="count",
            aggfunc="sum",
            fill_value=0.0,
        )
        .sort_index()
        .astype(float)
    )


def _timeseries_to_local(frame: pd.DataFrame, timezone: ZoneInfo) -> pd.DataFrame:
    if frame.empty:
        return frame
    converted = frame.copy()
    converted["timestamp"] = pd.to_datetime(converted["timestamp"], utc=True, errors="coerce")
    converted = converted.dropna(subset=["timestamp"])
    if converted.empty:
        return converted
    converted["timestamp"] = converted["timestamp"].dt.tz_convert(timezone)
    return converted


def _as_frame(value: object, *, columns: list[str]) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value
    return pd.DataFrame(columns=columns)


def _evaluation_status(
    *,
    overview: dict[str, float],
    no_signal_reasons: pd.DataFrame,
    missing_book_reasons: pd.DataFrame,
) -> tuple[str, str]:
    total_signals = int(overview.get("total_signals", 0.0))
    resync_count = float(overview.get("resync_count", 0.0))
    ws_reconnect_events = float(overview.get("ws_reconnect_events", 0.0))
    total_blocks = float(overview.get("total_block_events", 0.0))
    watched_markets_current = float(overview.get("watched_markets_current", 0.0))
    min_watched_floor = float(overview.get("min_watched_markets_floor", 0.0))
    ready_market_count = float(overview.get("ready_market_count", 0.0))
    ready_market_ratio = float(overview.get("ready_market_ratio", 0.0))
    stale_market_count = float(overview.get("stale_market_count", 0.0))
    eligible_market_count = float(overview.get("eligible_market_count", 0.0))
    eligible_market_ratio = float(overview.get("eligible_market_ratio", 0.0))
    low_quality_runtime_excluded_count = float(
        overview.get("low_quality_runtime_excluded_count", 0.0)
    )
    readiness_friction_count = (
        float(overview.get("book_not_ready_count", 0.0))
        + float(overview.get("quote_too_old_count", 0.0))
        + float(overview.get("market_quote_stale_count", 0.0))
        + float(overview.get("book_recovering_count", 0.0))
        + float(overview.get("market_not_ready_count", 0.0))
        + float(overview.get("market_probation_count", 0.0))
        + float(overview.get("connection_recovering_count", 0.0))
        + float(overview.get("market_recovering_count", 0.0))
    )

    no_signal_total = (
        float(no_signal_reasons["count"].sum()) if not no_signal_reasons.empty else 0.0
    )
    blocking_no_signal = 0.0
    if not no_signal_reasons.empty:
        blocking_no_signal = float(
            no_signal_reasons[
                no_signal_reasons["reason"].astype(str).str.startswith("safe_mode_blocked_")
            ]["count"].sum()
        )
    missing_book_total = (
        float(missing_book_reasons["count"].sum()) if not missing_book_reasons.empty else 0.0
    )
    blocking_ratio = blocking_no_signal / no_signal_total if no_signal_total > 0 else 0.0

    not_ready = total_signals == 0 and (
        resync_count >= 100
        or ws_reconnect_events >= 15
        or total_blocks >= 30
        or readiness_friction_count >= 200
        or missing_book_total >= 50
        or blocking_ratio >= 0.5
        or (watched_markets_current > 0 and eligible_market_count <= 0)
        or (watched_markets_current > 0 and watched_markets_current < max(1.0, min_watched_floor))
        or (
            watched_markets_current > 0
            and low_quality_runtime_excluded_count >= watched_markets_current
        )
        or stale_market_count > max(5.0, ready_market_count)
    )
    if not_ready:
        return "NOT READY", "red"
    partial = total_signals == 0 and (
        resync_count >= 40
        or ws_reconnect_events >= 5
        or total_blocks >= 10
        or missing_book_total > 0
        or readiness_friction_count > 0
        or ready_market_ratio < 0.5
        or eligible_market_ratio < 0.2
    )
    if partial:
        return "PARTIALLY READY", "orange"
    return "EVALUATION READY", "green"


def _no_eligible_causes(overview: dict[str, float]) -> list[str]:
    watched_markets = int(float(overview.get("watched_markets_current", 0.0)))
    min_floor = int(float(overview.get("min_watched_markets_floor", 0.0)))
    ready_markets = int(float(overview.get("ready_market_count", 0.0)))
    recovering_markets = int(float(overview.get("recovering_market_count", 0.0)))
    stale_markets = int(float(overview.get("stale_market_count", 0.0)))
    eligible_markets = int(float(overview.get("eligible_market_count", 0.0)))
    market_not_ready = int(float(overview.get("market_not_ready_count", 0.0)))
    low_quality_excluded = int(float(overview.get("low_quality_runtime_excluded_count", 0.0)))
    gate_connection_recovering = int(
        float(overview.get("eligibility_gate_connection_recovering_count", 0.0))
    )
    gate_book_recovering = int(float(overview.get("eligibility_gate_book_recovering_count", 0.0)))
    gate_stale = int(float(overview.get("eligibility_gate_stale_quote_freshness_count", 0.0)))
    gate_blocked = int(float(overview.get("eligibility_gate_blocked_count", 0.0)))

    causes: list[str] = []
    if watched_markets < max(1, min_floor):
        causes.append("watched_too_small")
    if watched_markets > 0 and recovering_markets >= watched_markets:
        causes.append("all_markets_recovering")
    if watched_markets > 0 and ready_markets == 0 and market_not_ready > 0:
        causes.append("all_markets_not_ready")
    if watched_markets > 0 and stale_markets >= watched_markets:
        causes.append("all_markets_stale")
    if watched_markets > 0 and low_quality_excluded >= watched_markets:
        causes.append("quality_penalty_excessive")
    if watched_markets > 0 and gate_connection_recovering >= watched_markets:
        causes.append("all_markets_connection_recovering")
    if watched_markets > 0 and gate_book_recovering >= watched_markets:
        causes.append("all_markets_book_recovering")
    if watched_markets > 0 and gate_stale >= watched_markets:
        causes.append("all_markets_stale_freshness")
    if watched_markets > 0 and gate_blocked >= watched_markets:
        causes.append("all_markets_blocked")
    if eligible_markets <= 0 and not causes:
        causes.append("eligibility_gate_unmet")
    return sorted(set(causes))


def _overview_section(
    *,
    overview: dict[str, float],
    warmup: dict[str, float],
    no_signal_reasons: pd.DataFrame,
    missing_book_reasons: pd.DataFrame,
) -> None:
    status_label, status_color = _evaluation_status(
        overview=overview,
        no_signal_reasons=no_signal_reasons,
        missing_book_reasons=missing_book_reasons,
    )
    st.subheader("Overview")
    status_html = (
        "<div style='padding:8px 12px;border-radius:8px;"
        "background-color:#111827;color:white;display:inline-block;'>"
        f"Run Readiness: <span style='color:{status_color};font-weight:700'>{status_label}</span>"
        "</div>"
    )
    st.markdown(status_html, unsafe_allow_html=True)
    no_eligible_causes = _no_eligible_causes(overview)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Signals", int(overview["total_signals"]))
    col2.metric("Total Fills", int(overview["total_fills"]))
    col3.metric("Fill Rate", f"{overview['fill_rate']:.3f}")
    col4.metric("Matched Fill Rate", f"{overview['matched_fill_rate']:.3f}")

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Resync Count", int(overview["resync_count"]))
    col6.metric("Global Safe Mode", int(overview["global_safe_mode_count"]))
    col7.metric("Market Blocks", int(overview["market_block_count"]))
    col8.metric("Asset Blocks", int(overview["asset_block_count"]))

    col9, col10, col11, col12 = st.columns(4)
    col9.metric("Total Block Events", int(overview["total_block_events"]))
    col10.metric("Projected Matched PnL", f"{overview['projected_matched_pnl']:.4f}")
    col11.metric("Unmatched Inventory MTM", f"{overview['unmatched_inventory_mtm']:.4f}")
    col12.metric("Total Projected PnL", f"{overview['total_projected_pnl']:.4f}")

    col13, col14, col15, col16 = st.columns(4)
    col13.metric("Universe Changed", int(overview["market_universe_changed_count"]))
    col14.metric("Book Not Ready", int(overview["book_not_ready_count"]))
    col15.metric(
        "Quote Stale",
        int(overview["quote_too_old_count"] + overview["market_quote_stale_count"]),
    )
    col16.metric("Book Recovering", int(overview["book_recovering_count"]))

    col17, col18, col19, col20 = st.columns(4)
    col17.metric("Market Not Ready", int(overview["market_not_ready_count"]))
    col18.metric("Market Probation", int(overview["market_probation_count"]))
    col19.metric("WS Reconnect", int(overview["ws_reconnect_events"]))
    col20.metric("WS Connected", int(overview["ws_connected_events"]))

    col21, col22, col23, col24 = st.columns(4)
    col21.metric("No Initial Book", int(overview["no_initial_book_count"]))
    col22.metric("Asset Warming Up", int(overview["asset_warming_up_count"]))
    col23.metric("Connection Recovering", int(overview["connection_recovering_count"]))
    col24.metric("Market Recovering", int(overview["market_recovering_count"]))

    col25, col26, col27, col28, col29 = st.columns(5)
    col25.metric("Ready Markets", int(overview["ready_market_count"]))
    col26.metric("Recovering Markets", int(overview["recovering_market_count"]))
    col27.metric("Stale Markets", int(overview["stale_market_count"]))
    col28.metric("Eligible Markets", int(overview["eligible_market_count"]))
    col29.metric("Blocked Markets", int(overview["blocked_market_count"]))

    col30, col31, col32, col33, col34 = st.columns(5)
    col30.metric("Ready Ratio", f"{overview['ready_market_ratio']:.2f}")
    col31.metric("Eligible Ratio", f"{overview['eligible_market_ratio']:.2f}")
    col32.metric("Watched Floor", int(overview["min_watched_markets_floor"]))
    col33.metric("Low-Quality Markets", int(overview["low_quality_market_count"]))
    col34.metric(
        "Runtime Excluded",
        int(overview["low_quality_runtime_excluded_count"]),
    )

    col35, col36, col37, col38 = st.columns(4)
    col35.metric(
        "Gate: Conn Recovering",
        int(overview["eligibility_gate_connection_recovering_count"]),
    )
    col36.metric(
        "Gate: Book Recovering",
        int(overview["eligibility_gate_book_recovering_count"]),
    )
    col37.metric(
        "Gate: Stale/Freshness",
        int(overview["eligibility_gate_stale_quote_freshness_count"]),
    )
    col38.metric(
        "Gate: Other Readiness",
        int(overview["eligibility_gate_other_readiness_gate_count"]),
    )

    col39, col40, col41 = st.columns(3)
    col39.metric("Gate: Blocked", int(overview["eligibility_gate_blocked_count"]))
    col40.metric("Gate: Probation", int(overview["eligibility_gate_probation_count"]))
    col41.metric(
        "Gate: Low-Quality Excluded",
        int(overview["eligibility_gate_low_quality_runtime_excluded_count"]),
    )

    st.caption(
        "Universe (current/cumulative): "
        f"markets {int(overview['watched_markets_current'])}/"
        f"{int(overview['watched_markets_cumulative'])}, "
        f"assets {int(overview['subscribed_assets_current'])}/"
        f"{int(overview['subscribed_assets_cumulative'])}"
    )
    st.caption(
        f"Warm-up events: {int(warmup['warmup_events'])}, "
        f"latest warming-up assets: {int(warmup['latest_warming_up_assets'])}"
    )
    if no_eligible_causes:
        st.caption(f"No-eligible causes: {', '.join(no_eligible_causes)}")


def _run_detail_section(run_summaries: pd.DataFrame, timezone: ZoneInfo) -> None:
    st.subheader("Run Detail")
    if run_summaries.empty:
        st.write("No run summaries in selected window.")
        return
    frame = _to_local(
        run_summaries,
        columns=["started_at", "ended_at"],
        timezone=timezone,
    )
    frame["warnings"] = ""
    frame.loc[frame["safe_mode_count"] > 0, "warnings"] += "global_safe_mode;"
    frame.loc[frame["exception_count"] > 0, "warnings"] += "exceptions;"
    frame.loc[frame["resync_events"] > 1000, "warnings"] += "resync_storm;"
    frame["warnings"] = frame["warnings"].str.rstrip(";")
    st.dataframe(frame, use_container_width=True, hide_index=True)


def _diagnostics_section(
    *,
    breakdowns: dict[str, pd.DataFrame],
    resync_ts: pd.DataFrame,
    block_ts: pd.DataFrame,
    no_signal_ts: pd.DataFrame,
    missing_book_ts: pd.DataFrame,
    timezone: ZoneInfo,
) -> None:
    st.subheader("Diagnostics")

    resync_local = _timeseries_to_local(resync_ts, timezone)
    if not resync_local.empty:
        resync_pivot = _reason_chart_frame(resync_local, key_column="reason", top_n=6)
        if not resync_pivot.empty:
            st.markdown("**Resync Count Over Time**")
            st.area_chart(resync_pivot, use_container_width=True)

    block_local = _timeseries_to_local(block_ts, timezone)
    if not block_local.empty:
        block_pivot = _reason_chart_frame(block_local, key_column="block_type", top_n=4)
        if not block_pivot.empty:
            st.markdown("**Block Events Over Time**")
            st.area_chart(block_pivot, use_container_width=True)

    no_signal_local = _timeseries_to_local(no_signal_ts, timezone)
    if not no_signal_local.empty:
        focus_reasons = {
            "edge_below_threshold",
            "book_not_ready",
            "book_not_ready_insufficient_updates",
            "book_not_ready_missing_leg_ready",
            "quote_too_old",
            "market_quote_stale",
            "market_quote_stale_no_recent_quote",
            "market_quote_stale_quote_age",
            "book_recovering",
            "market_not_ready",
            "market_probation",
        }
        focus = no_signal_local[no_signal_local["reason"].astype(str).isin(focus_reasons)]
        if not focus.empty:
            focus_pivot = _reason_chart_frame(focus, key_column="reason", top_n=10)
            if not focus_pivot.empty:
                st.markdown("**Readiness vs Strategy Reasons Over Time**")
                st.area_chart(focus_pivot, use_container_width=True)
        no_signal_pivot = _reason_chart_frame(no_signal_local, key_column="reason", top_n=8)
        if not no_signal_pivot.empty:
            st.markdown("**No-Signal Reasons (Stacked)**")
            st.area_chart(no_signal_pivot, use_container_width=True)

    missing_book_local = _timeseries_to_local(missing_book_ts, timezone)
    if not missing_book_local.empty:
        missing_pivot = _reason_chart_frame(missing_book_local, key_column="reason", top_n=8)
        if not missing_pivot.empty:
            st.markdown("**Missing Book Reasons (Stacked)**")
            st.area_chart(missing_pivot, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Top Resync Reasons**")
        resyncs = breakdowns["resyncs_by_reason"]
        if resyncs.empty:
            st.write("No data.")
        else:
            st.bar_chart(resyncs.set_index("reason")["count"], use_container_width=True)
            st.dataframe(resyncs, use_container_width=True, hide_index=True)
    with col2:
        st.markdown("**Top Safe Mode Scope/Reason**")
        safe_modes = breakdowns["safe_mode_by_scope_reason"]
        if safe_modes.empty:
            st.write("No data.")
        else:
            label_series = safe_modes.apply(
                lambda row: f"{row['scope']}:{row['reason']}",
                axis=1,
            )
            bar_frame = pd.DataFrame({"scope_reason": label_series, "count": safe_modes["count"]})
            st.bar_chart(bar_frame.set_index("scope_reason")["count"], use_container_width=True)
            st.dataframe(safe_modes, use_container_width=True, hide_index=True)

    col3, col4 = st.columns(2)
    with col3:
        st.markdown("**Top No-Signal Reasons**")
        no_signals = breakdowns["no_signal_reasons"]
        if no_signals.empty:
            st.write("No data.")
        else:
            view = no_signals[["reason", "count"]]
            st.bar_chart(view.set_index("reason")["count"], use_container_width=True)
            st.dataframe(view, use_container_width=True, hide_index=True)
    with col4:
        st.markdown("**Eligibility Gate Breakdown (Latest Snapshot)**")
        eligibility = breakdowns["eligibility_gate_breakdown"]
        if eligibility.empty:
            st.write("No data.")
        else:
            view = eligibility[["reason", "count"]]
            st.bar_chart(view.set_index("reason")["count"], use_container_width=True)
            st.dataframe(view, use_container_width=True, hide_index=True)

    col5, col6 = st.columns(2)
    with col5:
        st.markdown("**Top WS Reconnect Reasons**")
        ws_reconnect = breakdowns["ws_reconnect_reasons"]
        if ws_reconnect.empty:
            st.write("No data.")
        else:
            st.bar_chart(ws_reconnect.set_index("reason")["count"], use_container_width=True)
            st.dataframe(ws_reconnect, use_container_width=True, hide_index=True)
    with col6:
        st.markdown("**Top WS Connected Reasons**")
        ws_connected = breakdowns["ws_connected_reasons"]
        if ws_connected.empty:
            st.write("No data.")
        else:
            st.bar_chart(ws_connected.set_index("reason")["count"], use_container_width=True)
            st.dataframe(ws_connected, use_container_width=True, hide_index=True)

    if not no_signal_local.empty:
        grouped = no_signal_local.copy()
        strategy_reasons = {"edge_below_threshold", "spread_too_wide", "depth_too_low"}
        readiness_reasons = {
            "book_not_ready",
            "book_recovering",
            "market_not_ready",
            "market_probation",
            "market_quote_stale",
            "market_quote_stale_no_recent_quote",
            "market_quote_stale_recovery",
            "market_quote_stale_quote_age",
            "asset_warming_up",
            "no_initial_book",
            "connection_recovering",
            "market_recovering",
        }

        def _classify_reason(reason: object) -> str:
            key = str(reason)
            if key in strategy_reasons:
                return "strategy"
            if key.startswith("book_not_ready") or key in readiness_reasons:
                return "readiness"
            return "transport_or_other"

        grouped["reason_group"] = grouped["reason"].map(_classify_reason)
        grouped_pivot = _reason_chart_frame(grouped, key_column="reason_group", top_n=3)
        if not grouped_pivot.empty:
            st.markdown("**Reason Group Mix (Strategy vs Readiness vs Transport/Other)**")
            st.area_chart(grouped_pivot, use_container_width=True)

    st.markdown("**Missing Book State Reasons**")
    missing = breakdowns["missing_book_state_reasons"]
    if missing.empty:
        st.write("No data.")
    else:
        view = missing[["reason", "count"]]
        st.bar_chart(view.set_index("reason")["count"], use_container_width=True)
        st.dataframe(view, use_container_width=True, hide_index=True)


def _pnl_section(pnl_series: pd.DataFrame, timezone: ZoneInfo) -> None:
    st.subheader("PnL / Inventory")
    if pnl_series.empty:
        st.write("No snapshot/PnL time-series data in selected window.")
        return
    frame = _timeseries_to_local(pnl_series, timezone)
    if frame.empty:
        st.write("No valid timestamps in selected records.")
        return
    chart_data = frame.set_index("timestamp")[
        ["cumulative_projected_pnl", "open_unmatched_inventory"]
    ].sort_index()
    st.line_chart(chart_data, use_container_width=True)
    display_frame = frame.copy()
    display_frame["timestamp"] = display_frame["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S %Z")
    st.dataframe(display_frame, use_container_width=True, hide_index=True)


def _recovery_diagnostics_section(recovery: dict[str, object]) -> None:
    st.subheader("Recovery Diagnostics")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Resync Started", int(float(recovery.get("recovery_resync_started_count", 0.0))))
    col2.metric(
        "First Quote Success",
        int(float(recovery.get("recovery_first_quote_success_count", 0.0))),
    )
    col3.metric(
        "Book Ready Success",
        int(float(recovery.get("recovery_book_ready_success_count", 0.0))),
    )
    col4.metric(
        "Market Ready Success",
        int(float(recovery.get("recovery_market_ready_success_count", 0.0))),
    )

    col5, col6, col7, col8 = st.columns(4)
    col5.metric(
        "First Quote Blocked",
        int(float(recovery.get("recovery_first_quote_blocked_count", 0.0))),
    )
    col6.metric(
        "Book Ready Blocked",
        int(float(recovery.get("recovery_book_ready_blocked_count", 0.0))),
    )
    col7.metric(
        "Market Ready Blocked",
        int(float(recovery.get("recovery_market_ready_blocked_count", 0.0))),
    )
    col8.metric(
        "Market Recovery Started",
        int(float(recovery.get("recovery_market_recovery_started_count", 0.0))),
    )

    col9, col10, col11 = st.columns(3)
    col9.metric(
        "First Quote Success Rate",
        f"{float(recovery.get('recovery_first_quote_success_rate', 0.0)):.2%}",
    )
    col10.metric(
        "Book Ready Success Rate",
        f"{float(recovery.get('recovery_book_ready_success_rate', 0.0)):.2%}",
    )
    col11.metric(
        "Market Ready Success Rate",
        f"{float(recovery.get('recovery_market_ready_success_rate', 0.0)):.2%}",
    )

    col12, col13, col14, col15 = st.columns(4)
    col12.metric(
        "UC Resync Started",
        int(float(recovery.get("recovery_universe_change_resync_started_count", 0.0))),
    )
    col13.metric(
        "UC First Quote Success",
        int(float(recovery.get("recovery_universe_change_first_quote_success_count", 0.0))),
    )
    col14.metric(
        "UC Book Ready Success",
        int(float(recovery.get("recovery_universe_change_book_ready_success_count", 0.0))),
    )
    col15.metric(
        "UC Market Ready Success",
        int(float(recovery.get("recovery_universe_change_market_ready_success_count", 0.0))),
    )

    col16, col17, col18 = st.columns(3)
    col16.metric(
        "UC First Quote Rate",
        f"{float(recovery.get('recovery_universe_change_first_quote_success_rate', 0.0)):.2%}",
    )
    col17.metric(
        "UC Book Ready Rate",
        f"{float(recovery.get('recovery_universe_change_book_ready_success_rate', 0.0)):.2%}",
    )
    col18.metric(
        "UC Market Ready Rate",
        f"{float(recovery.get('recovery_universe_change_market_ready_success_rate', 0.0)):.2%}",
    )

    col19, col20, col21 = st.columns(3)
    col19.metric(
        "Avg/Max Resync->FirstQuote (ms)",
        (
            f"{float(recovery.get('avg_resync_to_first_quote_latency_ms', 0.0)):.1f}"
            f" / {float(recovery.get('max_resync_to_first_quote_latency_ms', 0.0)):.1f}"
        ),
    )
    col20.metric(
        "Avg/Max Resync->BookReady (ms)",
        (
            f"{float(recovery.get('avg_resync_to_book_ready_latency_ms', 0.0)):.1f}"
            f" / {float(recovery.get('max_resync_to_book_ready_latency_ms', 0.0)):.1f}"
        ),
    )
    col21.metric(
        "Avg/Max Recovery->MarketReady (ms)",
        (
            f"{float(recovery.get('avg_recovery_to_market_ready_latency_ms', 0.0)):.1f}"
            f" / {float(recovery.get('max_recovery_to_market_ready_latency_ms', 0.0)):.1f}"
        ),
    )

    col22, col23 = st.columns(2)
    with col22:
        st.markdown("**First Quote Blocked Reasons**")
        blocked_first = _as_frame(
            recovery.get("first_quote_blocked_reasons"),
            columns=["reason", "count"],
        )
        if blocked_first.empty:
            st.write("No data.")
        else:
            st.bar_chart(blocked_first.set_index("reason")["count"], use_container_width=True)
            st.dataframe(blocked_first, use_container_width=True, hide_index=True)
    with col23:
        st.markdown("**Book/Market Ready Blocked Reasons**")
        blocked_book = _as_frame(
            recovery.get("book_ready_blocked_reasons"),
            columns=["reason", "count"],
        )
        blocked_market = _as_frame(
            recovery.get("market_ready_blocked_reasons"),
            columns=["reason", "count"],
        )
        merged = pd.concat([blocked_book, blocked_market], ignore_index=True)
        if merged.empty:
            st.write("No data.")
        else:
            grouped = (
                merged.groupby("reason", as_index=False)["count"]
                .sum()
                .sort_values("count", ascending=False)
            )
            st.bar_chart(grouped.set_index("reason")["count"], use_container_width=True)
            st.dataframe(grouped, use_container_width=True, hide_index=True)

    st.markdown("**Eligibility Gate Unmet Reasons (Diagnostics Events)**")
    gate_unmet = _as_frame(
        recovery.get("eligibility_gate_unmet_reasons"),
        columns=["reason", "count"],
    )
    if gate_unmet.empty:
        st.write("No data.")
    else:
        st.bar_chart(gate_unmet.set_index("reason")["count"], use_container_width=True)
        st.dataframe(gate_unmet, use_container_width=True, hide_index=True)

    col24, col25 = st.columns(2)
    with col24:
        st.markdown("**Top Stale Assets**")
        stale_assets = _as_frame(
            recovery.get("top_stale_assets"),
            columns=["asset_id", "count", "market_slug", "side", "market_id"],
        )
        if stale_assets.empty:
            st.write("No data.")
        else:
            st.dataframe(stale_assets, use_container_width=True, hide_index=True)
    with col25:
        st.markdown("**Top Missing Book Assets**")
        missing_assets = _as_frame(
            recovery.get("top_missing_book_assets"),
            columns=["asset_id", "count", "market_slug", "side", "market_id"],
        )
        if missing_assets.empty:
            st.write("No data.")
        else:
            st.dataframe(missing_assets, use_container_width=True, hide_index=True)

    col26, col27 = st.columns(2)
    with col26:
        st.markdown("**Top Market Blocked Markets**")
        blocked_markets = _as_frame(
            recovery.get("top_market_blocked_markets"),
            columns=["market_id", "market_slug", "count"],
        )
        if blocked_markets.empty:
            st.write("No data.")
        else:
            st.dataframe(blocked_markets, use_container_width=True, hide_index=True)
    with col27:
        st.markdown("**Top Recovery Slow Assets**")
        slow_assets = _as_frame(
            recovery.get("top_recovery_slow_assets"),
            columns=[
                "asset_id",
                "side",
                "market_slug",
                "avg_latency_ms",
                "max_latency_ms",
                "success_count",
            ],
        )
        if slow_assets.empty:
            st.write("No data.")
        else:
            st.dataframe(slow_assets, use_container_width=True, hide_index=True)

    st.markdown("**Top Recovery Slow Markets**")
    slow_markets = _as_frame(
        recovery.get("top_recovery_slow_markets"),
        columns=["market_id", "market_slug", "avg_latency_ms", "max_latency_ms", "success_count"],
    )
    if slow_markets.empty:
        st.write("No data.")
    else:
        st.dataframe(slow_markets, use_container_width=True, hide_index=True)


def _market_asset_section(markets: pd.DataFrame, assets: pd.DataFrame, timezone: ZoneInfo) -> None:
    st.subheader("Market / Asset View")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Market Diagnostics**")
        if markets.empty:
            st.write("No market-level records.")
        else:
            st.dataframe(markets, use_container_width=True, hide_index=True)
    with col2:
        st.markdown("**Asset Diagnostics**")
        if assets.empty:
            st.write("No asset-level records.")
        else:
            display_assets = _to_local(assets, columns=["last_quote_time"], timezone=timezone)
            st.dataframe(display_assets, use_container_width=True, hide_index=True)


def _build_window(
    *,
    mode: str,
    lookback_hours: int,
    start_local: datetime,
    end_local: datetime,
    timezone: ZoneInfo,
) -> DashboardWindow:
    if mode == "Lookback Hours":
        return resolve_window(last_hours=lookback_hours)
    start_utc = _to_utc_from_local_input(start_local, timezone)
    end_utc = _to_utc_from_local_input(end_local, timezone)
    if end_utc <= start_utc:
        end_utc = start_utc + timedelta(seconds=1)
    return resolve_window(start=start_utc, end=end_utc)


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path).resolve()
    st.set_page_config(
        page_title="Polymarket Shadow Paper Dashboard",
        layout="wide",
    )
    st.title("Polymarket Shadow Paper Dashboard")
    st.caption("Read-only local viewer for SQLite diagnostics. Default timezone is JST.")

    loader = DashboardDataLoader(db_path=db_path)
    if not loader.has_database():
        _show_empty_state(db_path=db_path)
        return

    with st.sidebar:
        st.header("Filters")
        timezone_label = st.selectbox(
            "Display Timezone",
            options=list(TIMEZONE_LABELS.keys()),
            index=list(TIMEZONE_LABELS.keys()).index(DEFAULT_TIMEZONE_LABEL),
        )
        timezone = ZoneInfo(TIMEZONE_LABELS[timezone_label])
        run_ids = loader.load_run_ids()
        selected_run = st.selectbox("run_id", options=["(all)", *run_ids], index=0)

        filter_mode = st.radio("Window Mode", options=["Lookback Hours", "DateTime Range"], index=0)
        lookback_hours = st.number_input(
            "Lookback Hours",
            min_value=1,
            max_value=24 * 30,
            value=24,
            step=1,
            disabled=(filter_mode != "Lookback Hours"),
        )
        now_local = datetime.now(tz=timezone)
        default_start_local = now_local - timedelta(hours=int(lookback_hours))
        start_date = st.date_input(
            f"Start Date ({timezone.key})",
            value=default_start_local.date(),
            disabled=(filter_mode != "DateTime Range"),
        )
        start_time = st.time_input(
            f"Start Time ({timezone.key})",
            value=default_start_local.time().replace(microsecond=0),
            disabled=(filter_mode != "DateTime Range"),
        )
        end_date = st.date_input(
            f"End Date ({timezone.key})",
            value=now_local.date(),
            disabled=(filter_mode != "DateTime Range"),
        )
        end_time = st.time_input(
            f"End Time ({timezone.key})",
            value=now_local.time().replace(microsecond=0),
            disabled=(filter_mode != "DateTime Range"),
        )
        market_slug = st.text_input("market_slug contains", value="").strip()
        start_time_value = start_time if isinstance(start_time, time) else time.min
        end_time_value = end_time if isinstance(end_time, time) else time.max
        start_local = datetime.combine(start_date, start_time_value)
        end_local = datetime.combine(end_date, end_time_value)

    window = _build_window(
        mode=filter_mode,
        lookback_hours=int(lookback_hours),
        start_local=start_local,
        end_local=end_local,
        timezone=timezone,
    )
    run_id = None if selected_run == "(all)" else selected_run
    market_slug_filter = market_slug or None

    overview = loader.load_overview(window=window, run_id=run_id)
    warmup = loader.load_warmup_overview(window=window, run_id=run_id)
    run_summaries = loader.load_run_summaries(window=window, run_id=run_id)
    breakdowns = loader.load_reason_breakdowns(window=window, run_id=run_id)
    pnl_series = loader.load_pnl_timeseries(window=window, run_id=run_id)
    resync_ts = loader.load_resync_timeseries(window=window, run_id=run_id)
    block_ts = loader.load_block_timeseries(window=window, run_id=run_id)
    no_signal_ts = loader.load_no_signal_reason_timeseries(window=window, run_id=run_id)
    missing_book_ts = loader.load_missing_book_reason_timeseries(window=window, run_id=run_id)
    recovery_diag = loader.load_recovery_diagnostics(window=window, run_id=run_id)
    market_diag = loader.load_market_diagnostics(
        window=window,
        run_id=run_id,
        market_slug=market_slug_filter,
    )
    asset_diag = loader.load_asset_diagnostics(window=window, run_id=run_id)

    start_local_str, end_local_str = _to_local_window(window, timezone)
    st.caption(f"DB: {db_path}")
    st.caption(f"Window ({timezone.key}): {start_local_str} -> {end_local_str}")
    st.caption(f"Window (UTC): {window.start_iso} -> {window.end_iso}")

    _overview_section(
        overview=overview,
        warmup=warmup,
        no_signal_reasons=breakdowns["no_signal_reasons"],
        missing_book_reasons=breakdowns["missing_book_state_reasons"],
    )
    _run_detail_section(run_summaries=run_summaries, timezone=timezone)
    _diagnostics_section(
        breakdowns=breakdowns,
        resync_ts=resync_ts,
        block_ts=block_ts,
        no_signal_ts=no_signal_ts,
        missing_book_ts=missing_book_ts,
        timezone=timezone,
    )
    _recovery_diagnostics_section(recovery_diag)
    _pnl_section(pnl_series=pnl_series, timezone=timezone)
    _market_asset_section(markets=market_diag, assets=asset_diag, timezone=timezone)


if __name__ == "__main__":
    main()
