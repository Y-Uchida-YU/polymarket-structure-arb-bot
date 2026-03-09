from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.config.loader import GuardrailSettings
from src.risk.guardrails import GuardrailMonitor


def test_high_signal_rate_triggers_safe_mode_decision() -> None:
    settings = GuardrailSettings(
        window_minutes=1,
        max_signal_rate_per_min=2.0,
        max_reject_rate=1.0,
        max_one_leg_rate=1.0,
        max_unmatched_rate=1.0,
        max_stale_asset_rate=1.0,
        max_exception_rate_per_min=100.0,
    )
    monitor = GuardrailMonitor(settings=settings)
    now = datetime(2026, 3, 10, tzinfo=UTC)
    for _ in range(5):
        monitor.record_signal(now)

    decision = monitor.evaluate(
        now_utc=now,
        stale_asset_rate=0.0,
        safe_mode_active=False,
        safe_mode_entered_at=None,
    )
    assert decision.enter_safe_mode_reason == "high_signal_rate"


def test_high_stale_asset_rate_triggers_safe_mode_decision() -> None:
    settings = GuardrailSettings(
        window_minutes=5,
        max_stale_asset_rate=0.3,
        max_signal_rate_per_min=100.0,
        max_reject_rate=1.0,
        max_one_leg_rate=1.0,
        max_unmatched_rate=1.0,
        max_exception_rate_per_min=100.0,
    )
    monitor = GuardrailMonitor(settings=settings)
    now = datetime(2026, 3, 10, tzinfo=UTC)

    decision = monitor.evaluate(
        now_utc=now,
        stale_asset_rate=0.8,
        safe_mode_active=False,
        safe_mode_entered_at=None,
    )
    assert decision.enter_safe_mode_reason == "high_stale_asset_rate"


def test_high_resync_rate_triggers_warning_only() -> None:
    settings = GuardrailSettings(
        window_minutes=1,
        max_resync_rate_per_min=1.0,
        max_signal_rate_per_min=100.0,
        max_reject_rate=1.0,
        max_one_leg_rate=1.0,
        max_unmatched_rate=1.0,
        max_stale_asset_rate=1.0,
        max_exception_rate_per_min=100.0,
    )
    monitor = GuardrailMonitor(settings=settings)
    now = datetime(2026, 3, 10, tzinfo=UTC)
    for _ in range(3):
        monitor.record_resync(now)

    decision = monitor.evaluate(
        now_utc=now,
        stale_asset_rate=0.0,
        safe_mode_active=False,
        safe_mode_entered_at=None,
    )
    assert "high_resync_rate" in decision.warnings
    assert decision.enter_safe_mode_reason is None


def test_safe_mode_can_exit_after_cooldown() -> None:
    settings = GuardrailSettings(
        window_minutes=1,
        safe_mode_cooldown_minutes=5,
        max_signal_rate_per_min=100.0,
        max_reject_rate=1.0,
        max_one_leg_rate=1.0,
        max_unmatched_rate=1.0,
        max_stale_asset_rate=1.0,
        max_resync_rate_per_min=100.0,
        max_exception_rate_per_min=100.0,
    )
    monitor = GuardrailMonitor(settings=settings)
    now = datetime(2026, 3, 10, tzinfo=UTC)
    entered = now - timedelta(minutes=6)

    decision = monitor.evaluate(
        now_utc=now,
        stale_asset_rate=0.0,
        safe_mode_active=True,
        safe_mode_entered_at=entered,
    )
    assert decision.exit_safe_mode
