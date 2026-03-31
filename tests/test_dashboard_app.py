from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pandas as pd

from src.dashboard.data_loader import DashboardWindow
from src.dashboard_app import _as_frame, _build_window, _no_eligible_causes, _to_local_window


def test_to_local_window_converts_utc_to_jst() -> None:
    window = DashboardWindow(
        start_iso="2026-03-10T00:00:00+00:00",
        end_iso="2026-03-10T01:00:00+00:00",
    )
    start_text, end_text = _to_local_window(window, ZoneInfo("Asia/Tokyo"))

    assert start_text.startswith("2026-03-10 09:00:00")
    assert end_text.startswith("2026-03-10 10:00:00")


def test_build_window_datetime_range_uses_local_timezone() -> None:
    timezone = ZoneInfo("Asia/Tokyo")
    start_local = datetime(2026, 3, 10, 9, 0, 0)
    end_local = datetime(2026, 3, 10, 10, 0, 0)

    window = _build_window(
        mode="DateTime Range",
        lookback_hours=24,
        start_local=start_local,
        end_local=end_local,
        timezone=timezone,
    )

    assert datetime.fromisoformat(window.start_iso) == datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    assert datetime.fromisoformat(window.end_iso) >= datetime(2026, 3, 10, 1, 0, tzinfo=UTC)


def test_no_eligible_causes_derived_from_overview() -> None:
    causes = _no_eligible_causes(
        {
            "watched_markets_current": 2.0,
            "min_watched_markets_floor": 10.0,
            "ready_market_count": 0.0,
            "recovering_market_count": 2.0,
            "stale_market_count": 0.0,
            "eligible_market_count": 0.0,
            "market_not_ready_count": 1.0,
            "low_quality_runtime_excluded_count": 3.0,
        }
    )
    assert "watched_too_small" in causes
    assert "all_markets_recovering" in causes
    assert "quality_penalty_excessive" in causes


def test_no_eligible_causes_includes_chronic_stale_excluded() -> None:
    causes = _no_eligible_causes(
        {
            "watched_markets_current": 2.0,
            "min_watched_markets_floor": 0.0,
            "ready_market_count": 2.0,
            "recovering_market_count": 0.0,
            "stale_market_count": 0.0,
            "eligible_market_count": 0.0,
            "market_not_ready_count": 0.0,
            "low_quality_runtime_excluded_count": 0.0,
            "chronic_stale_excluded_market_count": 2.0,
            "eligibility_gate_chronic_stale_excluded_count": 2.0,
        }
    )
    assert "all_markets_chronic_stale_excluded" in causes


def test_no_eligible_causes_includes_chronic_universe_contamination() -> None:
    causes = _no_eligible_causes(
        {
            "watched_markets_current": 2.0,
            "min_watched_markets_floor": 0.0,
            "ready_market_count": 1.0,
            "recovering_market_count": 0.0,
            "stale_market_count": 0.0,
            "eligible_market_count": 0.0,
            "market_not_ready_count": 0.0,
            "low_quality_runtime_excluded_count": 0.0,
            "chronic_stale_excluded_market_count": 0.0,
            "watched_chronic_stale_excluded_market_count": 1.0,
            "chronic_stale_reintroduced_for_floor_count": 1.0,
        }
    )
    assert "watched_universe_chronic_contaminated" in causes
    assert "watched_floor_reintroduced_chronic_stale" in causes


def test_as_frame_returns_empty_frame_when_value_is_missing() -> None:
    frame = _as_frame(None, columns=["asset_id", "count"])
    assert isinstance(frame, pd.DataFrame)
    assert list(frame.columns) == ["asset_id", "count"]
    assert frame.empty


def test_as_frame_returns_input_dataframe() -> None:
    source = pd.DataFrame([{"asset_id": "a1", "count": 2}])
    frame = _as_frame(source, columns=["asset_id", "count"])
    assert frame.equals(source)
