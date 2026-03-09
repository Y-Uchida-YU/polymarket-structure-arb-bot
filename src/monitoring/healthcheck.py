from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(slots=True)
class HealthState:
    started_at: datetime
    last_ws_message_at: datetime | None = None
    last_signal_at: datetime | None = None
    last_error_at: datetime | None = None
    asset_last_update_at: dict[str, datetime] = field(default_factory=dict)
    stale_asset_count: int = 0
    all_assets_stale: bool = False


class HealthCheck:
    def __init__(self, started_at: datetime) -> None:
        self.state = HealthState(started_at=started_at)

    def on_ws_message(self, ts: datetime) -> None:
        self.state.last_ws_message_at = ts

    def on_signal(self, ts: datetime) -> None:
        self.state.last_signal_at = ts

    def on_error(self, ts: datetime) -> None:
        self.state.last_error_at = ts

    def on_asset_quote_update(self, asset_id: str, ts: datetime) -> None:
        self.state.asset_last_update_at[asset_id] = ts

    def stale_assets(
        self, tracked_assets: list[str], now_utc: datetime, max_age_ms: int
    ) -> list[str]:
        stale: list[str] = []
        for asset_id in tracked_assets:
            last = self.state.asset_last_update_at.get(asset_id)
            if last is None:
                stale.append(asset_id)
                continue
            age_ms = (now_utc - last).total_seconds() * 1000.0
            if age_ms > max_age_ms:
                stale.append(asset_id)
        self.state.stale_asset_count = len(stale)
        self.state.all_assets_stale = len(stale) == len(tracked_assets) and len(tracked_assets) > 0
        return stale
