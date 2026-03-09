from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class BinaryMarket:
    market_id: str
    question: str
    slug: str
    category: str
    end_time: datetime | None
    condition_id: str | None
    yes_token_id: str
    no_token_id: str
    raw: dict[str, object]


@dataclass(slots=True)
class MarketPairQuote:
    market_id: str
    yes_ask: float | None
    no_ask: float | None
    updated_at: datetime
