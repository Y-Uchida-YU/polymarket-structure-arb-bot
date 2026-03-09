from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta

from src.config.loader import GuardrailSettings


@dataclass(slots=True)
class GuardrailMetrics:
    signal_rate_per_min: float
    reject_rate: float
    one_leg_rate: float
    unmatched_rate: float
    stale_asset_rate: float
    resync_rate_per_min: float
    exception_rate_per_min: float
    window_signals: int
    window_rejects: int
    window_resyncs: int
    window_exceptions: int


@dataclass(slots=True)
class GuardrailDecision:
    warnings: list[str]
    enter_safe_mode_reason: str | None
    exit_safe_mode: bool
    hard_stop_reason: str | None
    metrics: GuardrailMetrics


class GuardrailMonitor:
    def __init__(self, settings: GuardrailSettings) -> None:
        self.settings = settings
        self.signal_events: deque[datetime] = deque()
        self.reject_events: deque[datetime] = deque()
        self.one_leg_events: deque[datetime] = deque()
        self.unmatched_events: deque[datetime] = deque()
        self.resync_events: deque[datetime] = deque()
        self.exception_events: deque[datetime] = deque()

    def record_signal(self, ts: datetime) -> None:
        self.signal_events.append(ts)

    def record_reject(self, ts: datetime) -> None:
        self.reject_events.append(ts)

    def record_one_leg(self, ts: datetime) -> None:
        self.one_leg_events.append(ts)

    def record_unmatched(self, ts: datetime) -> None:
        self.unmatched_events.append(ts)

    def record_resync(self, ts: datetime) -> None:
        self.resync_events.append(ts)

    def record_exception(self, ts: datetime) -> None:
        self.exception_events.append(ts)

    def evaluate(
        self,
        *,
        now_utc: datetime,
        stale_asset_rate: float,
        safe_mode_active: bool,
        safe_mode_entered_at: datetime | None,
    ) -> GuardrailDecision:
        self._trim(now_utc)
        window_minutes = max(1, self.settings.window_minutes)
        signal_count = len(self.signal_events)
        reject_count = len(self.reject_events)
        one_leg_count = len(self.one_leg_events)
        unmatched_count = len(self.unmatched_events)
        resync_count = len(self.resync_events)
        exception_count = len(self.exception_events)

        signal_rate = signal_count / window_minutes
        resync_rate = resync_count / window_minutes
        exception_rate = exception_count / window_minutes
        reject_rate = reject_count / signal_count if signal_count > 0 else 0.0
        one_leg_rate = one_leg_count / signal_count if signal_count > 0 else 0.0
        unmatched_rate = unmatched_count / signal_count if signal_count > 0 else 0.0

        warnings: list[str] = []
        enter_safe_mode_reason: str | None = None
        hard_stop_reason: str | None = None

        if signal_rate > self.settings.max_signal_rate_per_min:
            warnings.append("high_signal_rate")
            enter_safe_mode_reason = enter_safe_mode_reason or "high_signal_rate"
        if reject_rate > self.settings.max_reject_rate:
            warnings.append("high_reject_rate")
            enter_safe_mode_reason = enter_safe_mode_reason or "high_reject_rate"
        if one_leg_rate > self.settings.max_one_leg_rate:
            warnings.append("high_one_leg_rate")
            enter_safe_mode_reason = enter_safe_mode_reason or "high_one_leg_rate"
        if unmatched_rate > self.settings.max_unmatched_rate:
            warnings.append("high_unmatched_rate")
            enter_safe_mode_reason = enter_safe_mode_reason or "high_unmatched_rate"
        if stale_asset_rate > self.settings.max_stale_asset_rate:
            warnings.append("high_stale_asset_rate")
            enter_safe_mode_reason = enter_safe_mode_reason or "high_stale_asset_rate"
        if resync_rate > self.settings.max_resync_rate_per_min:
            warnings.append("high_resync_rate")
        if exception_rate > self.settings.max_exception_rate_per_min:
            warnings.append("high_exception_rate")
            enter_safe_mode_reason = enter_safe_mode_reason or "high_exception_rate"
        if (
            self.settings.hard_stop_on_exception_spike
            and exception_rate > self.settings.hard_stop_exception_rate_per_min
        ):
            hard_stop_reason = "hard_stop_exception_spike"

        should_exit_safe_mode = False
        if safe_mode_active and enter_safe_mode_reason is None:
            if safe_mode_entered_at is not None:
                cooldown = timedelta(minutes=max(1, self.settings.safe_mode_cooldown_minutes))
                should_exit_safe_mode = now_utc - safe_mode_entered_at >= cooldown

        return GuardrailDecision(
            warnings=warnings,
            enter_safe_mode_reason=enter_safe_mode_reason if not safe_mode_active else None,
            exit_safe_mode=should_exit_safe_mode,
            hard_stop_reason=hard_stop_reason,
            metrics=GuardrailMetrics(
                signal_rate_per_min=signal_rate,
                reject_rate=reject_rate,
                one_leg_rate=one_leg_rate,
                unmatched_rate=unmatched_rate,
                stale_asset_rate=stale_asset_rate,
                resync_rate_per_min=resync_rate,
                exception_rate_per_min=exception_rate,
                window_signals=signal_count,
                window_rejects=reject_count,
                window_resyncs=resync_count,
                window_exceptions=exception_count,
            ),
        )

    def _trim(self, now_utc: datetime) -> None:
        cutoff = now_utc - timedelta(minutes=max(1, self.settings.window_minutes))
        for queue in (
            self.signal_events,
            self.reject_events,
            self.one_leg_events,
            self.unmatched_events,
            self.resync_events,
            self.exception_events,
        ):
            while queue and queue[0] < cutoff:
                queue.popleft()
