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


def _evaluation_status(
    *,
    overview: dict[str, float],
    no_signal_reasons: pd.DataFrame,
    missing_book_reasons: pd.DataFrame,
) -> tuple[str, str]:
    total_signals = int(overview.get("total_signals", 0.0))
    resync_count = float(overview.get("resync_count", 0.0))
    total_blocks = float(overview.get("total_block_events", 0.0))

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
        or total_blocks >= 30
        or missing_book_total >= 50
        or blocking_ratio >= 0.5
    )
    if not_ready:
        return "NOT READY", "red"
    partial = total_signals == 0 and (
        resync_count >= 40 or total_blocks >= 10 or missing_book_total > 0
    )
    if partial:
        return "PARTIALLY READY", "orange"
    return "EVALUATION READY", "green"


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
    _pnl_section(pnl_series=pnl_series, timezone=timezone)
    _market_asset_section(markets=market_diag, assets=asset_diag, timezone=timezone)


if __name__ == "__main__":
    main()
