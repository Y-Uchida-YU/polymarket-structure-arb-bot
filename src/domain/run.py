from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class RunSnapshot:
    run_id: str
    timestamp: datetime
    active_markets: int
    stale_assets: int
    total_signals: int
    total_fills: int
    open_unmatched_inventory: float
    cumulative_projected_pnl: float
    safe_mode_active: bool
    safe_mode_reason: str | None
    resync_cumulative_count: int


@dataclass(slots=True)
class RunSummary:
    run_id: str
    mode: str
    started_at: datetime
    ended_at: datetime
    uptime_seconds: float
    total_signals: int
    total_fills: int
    total_projected_pnl: float
    safe_mode_count: int
    exception_count: int
    stale_events: int
    resync_events: int
