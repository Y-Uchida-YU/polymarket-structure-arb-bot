from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.domain.market import BinaryMarket
from src.domain.position import Position
from src.risk.exposure import ExposureManager
from src.risk.limits import RiskLimiter


def make_market(end_time: datetime | None) -> BinaryMarket:
    return BinaryMarket(
        market_id="market-1",
        question="Sample question",
        slug="sample-question",
        category="crypto",
        end_time=end_time,
        condition_id="cond-1",
        yes_token_id="yes-token",
        no_token_id="no-token",
        raw={},
    )


def test_blocks_when_within_expiry_window() -> None:
    limiter = RiskLimiter(
        max_open_positions=10,
        max_positions_per_market=2,
        max_daily_signals=100,
        expiry_block_minutes=180,
    )
    exposure = ExposureManager()
    now = datetime(2026, 3, 1, tzinfo=UTC)
    market = make_market(now + timedelta(minutes=120))
    decision = limiter.evaluate_new_signal(market=market, exposure=exposure, now_utc=now)
    assert not decision.allowed
    assert decision.reason == "within_expiry_block"


def test_blocks_when_max_open_positions_reached() -> None:
    limiter = RiskLimiter(
        max_open_positions=1,
        max_positions_per_market=1,
        max_daily_signals=100,
        expiry_block_minutes=180,
    )
    exposure = ExposureManager()
    market = make_market(datetime(2027, 1, 1, tzinfo=UTC))
    exposure.add_position(
        Position(
            market_id="existing",
            signal_id="sig-existing",
            yes_qty=1.0,
            no_qty=1.0,
            yes_entry_price=0.4,
            no_entry_price=0.5,
            opened_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    )
    decision = limiter.evaluate_new_signal(
        market=market,
        exposure=exposure,
        now_utc=datetime(2026, 3, 1, tzinfo=UTC),
    )
    assert not decision.allowed
    assert decision.reason == "max_open_positions_reached"
