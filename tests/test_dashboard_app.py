from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from src.dashboard.data_loader import DashboardWindow
from src.dashboard_app import _build_window, _to_local_window


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
