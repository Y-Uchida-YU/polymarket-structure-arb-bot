from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from src.dashboard.data_loader import DashboardDataLoader, resolve_window


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--db-path",
        default="data/state/state.db",
        help="SQLite file path (default: data/state/state.db)",
    )
    return parser.parse_args()


def _to_utc_datetime(day: date, end_of_day: bool) -> datetime:
    if end_of_day:
        return datetime.combine(day, time.max, tzinfo=UTC)
    return datetime.combine(day, time.min, tzinfo=UTC)


def _show_empty_state(db_path: Path) -> None:
    st.info(
        "No SQLite data found yet. Start a shadow paper run first, then reload this page.",
        icon="ℹ️",
    )
    st.code(f"Expected DB path: {db_path}")


def _overview_section(overview: dict[str, float], warmup: dict[str, float]) -> None:
    st.subheader("Overview")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Signals", int(overview["total_signals"]))
    col2.metric("Total Fills", int(overview["total_fills"]))
    col3.metric("Fill Rate", f"{overview['fill_rate']:.3f}")
    col4.metric("Matched Fill Rate", f"{overview['matched_fill_rate']:.3f}")

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Safe Mode Count", int(overview["safe_mode_count"]))
    col6.metric("Resync Count", int(overview["resync_count"]))
    col7.metric("Projected Matched PnL", f"{overview['projected_matched_pnl']:.4f}")
    col8.metric("Total Projected PnL", f"{overview['total_projected_pnl']:.4f}")

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


def _run_detail_section(run_summaries: pd.DataFrame) -> None:
    st.subheader("Run Detail")
    if run_summaries.empty:
        st.write("No run summaries in selected window.")
        return
    frame = run_summaries.copy()
    frame["warnings"] = ""
    frame.loc[frame["safe_mode_count"] > 0, "warnings"] += "safe_mode;"
    frame.loc[frame["exception_count"] > 0, "warnings"] += "exceptions;"
    frame.loc[frame["resync_events"] > 1000, "warnings"] += "resync_storm;"
    frame["warnings"] = frame["warnings"].str.rstrip(";")
    st.dataframe(frame, use_container_width=True, hide_index=True)


def _diagnostics_section(breakdowns: dict[str, pd.DataFrame]) -> None:
    st.subheader("Diagnostics")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Top Resync Reasons**")
        resyncs = breakdowns["resyncs_by_reason"]
        if resyncs.empty:
            st.write("No data.")
        else:
            st.dataframe(resyncs, use_container_width=True, hide_index=True)
    with col2:
        st.markdown("**Top Safe Mode Scope/Reason**")
        safe_modes = breakdowns["safe_mode_by_scope_reason"]
        if safe_modes.empty:
            st.write("No data.")
        else:
            st.dataframe(safe_modes, use_container_width=True, hide_index=True)

    col3, col4 = st.columns(2)
    with col3:
        st.markdown("**Top No-Signal Reasons**")
        no_signals = breakdowns["no_signal_reasons"]
        if no_signals.empty:
            st.write("No data.")
        else:
            view = no_signals[["reason", "count"]] if "reason" in no_signals.columns else no_signals
            st.dataframe(view, use_container_width=True, hide_index=True)
    with col4:
        st.markdown("**Missing Book State Reasons**")
        missing = breakdowns["missing_book_state_reasons"]
        if missing.empty:
            st.write("No data.")
        else:
            view = missing[["reason", "count"]] if "reason" in missing.columns else missing
            st.dataframe(view, use_container_width=True, hide_index=True)


def _pnl_section(pnl_series: pd.DataFrame) -> None:
    st.subheader("PnL / Inventory")
    if pnl_series.empty:
        st.write("No snapshot/PnL time-series data in selected window.")
        return
    frame = pnl_series.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["timestamp"])
    if frame.empty:
        st.write("No valid timestamps in selected records.")
        return
    chart_data = frame.set_index("timestamp")[
        ["cumulative_projected_pnl", "open_unmatched_inventory"]
    ]
    st.line_chart(chart_data, use_container_width=True)
    st.dataframe(frame, use_container_width=True, hide_index=True)


def _market_asset_section(markets: pd.DataFrame, assets: pd.DataFrame) -> None:
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
            st.dataframe(assets, use_container_width=True, hide_index=True)


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path).resolve()
    st.set_page_config(
        page_title="Polymarket Shadow Paper Dashboard",
        page_icon="📊",
        layout="wide",
    )
    st.title("Polymarket Shadow Paper Dashboard")
    st.caption("Read-only local viewer for SQLite/CSV-derived shadow paper diagnostics.")

    loader = DashboardDataLoader(db_path=db_path)
    if not loader.has_database():
        _show_empty_state(db_path=db_path)
        return

    with st.sidebar:
        st.header("Filters")
        run_ids = loader.load_run_ids()
        selected_run = st.selectbox(
            "run_id",
            options=["(all)", *run_ids],
            index=0,
        )
        last_hours = st.number_input("Last Hours", min_value=1, max_value=24 * 30, value=24, step=1)
        end_day = st.date_input("End Date (UTC)", value=datetime.now(tz=UTC).date())
        use_custom_start = st.checkbox("Custom Start Date")
        start_day: date | None = None
        if use_custom_start:
            start_day = st.date_input("Start Date (UTC)", value=end_day - timedelta(days=1))
        market_slug = st.text_input("market_slug contains", value="").strip()

    end_dt = _to_utc_datetime(end_day, end_of_day=True)
    start_dt = _to_utc_datetime(start_day, end_of_day=False) if start_day else None
    window = resolve_window(start=start_dt, end=end_dt, last_hours=int(last_hours))
    run_id = None if selected_run == "(all)" else selected_run
    market_slug_filter = market_slug or None

    overview = loader.load_overview(window=window, run_id=run_id)
    warmup = loader.load_warmup_overview(window=window, run_id=run_id)
    run_summaries = loader.load_run_summaries(window=window, run_id=run_id)
    breakdowns = loader.load_reason_breakdowns(window=window, run_id=run_id)
    pnl_series = loader.load_pnl_timeseries(window=window, run_id=run_id)
    market_diag = loader.load_market_diagnostics(
        window=window,
        run_id=run_id,
        market_slug=market_slug_filter,
    )
    asset_diag = loader.load_asset_diagnostics(window=window, run_id=run_id)

    st.caption(f"DB: {db_path}")
    st.caption(f"Window: {window.start_iso} -> {window.end_iso}")

    _overview_section(overview=overview, warmup=warmup)
    _run_detail_section(run_summaries=run_summaries)
    _diagnostics_section(breakdowns=breakdowns)
    _pnl_section(pnl_series=pnl_series)
    _market_asset_section(markets=market_diag, assets=asset_diag)


if __name__ == "__main__":
    main()
