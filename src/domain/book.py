from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class TickSizeUpdate:
    asset_id: str
    tick_size: float
    updated_at: datetime
    source: str


@dataclass(slots=True)
class BookSummary:
    asset_id: str
    best_bid: float | None
    best_ask: float | None
    best_bid_size: float | None
    best_ask_size: float | None
    timestamp: datetime
    source: str
