from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4


@dataclass(slots=True)
class ArbSignal:
    signal_id: str
    market_id: str
    slug: str
    yes_token_id: str
    no_token_id: str
    ask_yes: float
    ask_no: float
    sum_ask: float
    threshold: float
    detected_at: datetime
    reason: str

    @classmethod
    def new(
        cls,
        market_id: str,
        slug: str,
        yes_token_id: str,
        no_token_id: str,
        ask_yes: float,
        ask_no: float,
        threshold: float,
        detected_at: datetime,
        reason: str,
    ) -> ArbSignal:
        return cls(
            signal_id=str(uuid4()),
            market_id=market_id,
            slug=slug,
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            ask_yes=ask_yes,
            ask_no=ask_no,
            sum_ask=ask_yes + ask_no,
            threshold=threshold,
            detected_at=detected_at,
            reason=reason,
        )
