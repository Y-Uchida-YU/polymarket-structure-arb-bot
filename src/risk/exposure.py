from __future__ import annotations

from collections import defaultdict
from datetime import date

from src.domain.position import Position


class ExposureManager:
    def __init__(self) -> None:
        self.positions_by_signal: dict[str, Position] = {}
        self.signals_by_market: dict[str, set[str]] = defaultdict(set)
        self._daily_date: date | None = None
        self._daily_signal_count: int = 0

    def add_position(self, position: Position) -> None:
        self.positions_by_signal[position.signal_id] = position
        self.signals_by_market[position.market_id].add(position.signal_id)

    def open_positions_count(self) -> int:
        return len(self.positions_by_signal)

    def positions_in_market(self, market_id: str) -> int:
        return len(self.signals_by_market.get(market_id, set()))

    def daily_signal_count(self, today: date) -> int:
        if self._daily_date != today:
            self._daily_date = today
            self._daily_signal_count = 0
        return self._daily_signal_count

    def increment_daily_signal_count(self, today: date) -> None:
        if self._daily_date != today:
            self._daily_date = today
            self._daily_signal_count = 0
        self._daily_signal_count += 1
