from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from src.domain.market import BinaryMarket
from src.risk.exposure import ExposureManager
from src.strategy.filters import is_within_expiry_block


@dataclass(slots=True)
class LimitDecision:
    allowed: bool
    reason: str


class RiskLimiter:
    def __init__(
        self,
        max_open_positions: int,
        max_positions_per_market: int,
        max_daily_signals: int,
        expiry_block_minutes: int,
    ) -> None:
        self.max_open_positions = max_open_positions
        self.max_positions_per_market = max_positions_per_market
        self.max_daily_signals = max_daily_signals
        self.expiry_block_minutes = expiry_block_minutes

    def evaluate_new_signal(
        self,
        market: BinaryMarket,
        exposure: ExposureManager,
        now_utc: datetime,
    ) -> LimitDecision:
        if exposure.open_positions_count() >= self.max_open_positions:
            return LimitDecision(False, "max_open_positions_reached")
        if exposure.positions_in_market(market.market_id) >= self.max_positions_per_market:
            return LimitDecision(False, "max_positions_per_market_reached")
        if exposure.daily_signal_count(now_utc.date()) >= self.max_daily_signals:
            return LimitDecision(False, "max_daily_signals_reached")
        if is_within_expiry_block(
            end_time=market.end_time,
            now_utc=now_utc,
            expiry_block_minutes=self.expiry_block_minutes,
        ):
            return LimitDecision(False, "within_expiry_block")
        return LimitDecision(True, "ok")
