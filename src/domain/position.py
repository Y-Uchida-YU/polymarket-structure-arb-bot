from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class Position:
    market_id: str
    signal_id: str
    yes_qty: float
    no_qty: float
    yes_entry_price: float
    no_entry_price: float
    opened_at: datetime

    @property
    def total_cost(self) -> float:
        return self.yes_qty * self.yes_entry_price + self.no_qty * self.no_entry_price

    def estimated_pnl(self, yes_mark: float, no_mark: float) -> float:
        value = self.yes_qty * yes_mark + self.no_qty * no_mark
        return value - self.total_cost
