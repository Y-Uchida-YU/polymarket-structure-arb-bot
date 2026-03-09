from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class HealthState:
    started_at: datetime
    last_ws_message_at: datetime | None = None
    last_signal_at: datetime | None = None
    last_error_at: datetime | None = None


class HealthCheck:
    def __init__(self, started_at: datetime) -> None:
        self.state = HealthState(started_at=started_at)

    def on_ws_message(self, ts: datetime) -> None:
        self.state.last_ws_message_at = ts

    def on_signal(self, ts: datetime) -> None:
        self.state.last_signal_at = ts

    def on_error(self, ts: datetime) -> None:
        self.state.last_error_at = ts
