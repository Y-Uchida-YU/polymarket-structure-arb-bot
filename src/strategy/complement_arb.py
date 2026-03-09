from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from src.domain.market import BinaryMarket
from src.domain.signal import ArbSignal


@dataclass(slots=True)
class ComplementArbConfig:
    entry_threshold_sum_ask: float
    min_ask: float
    max_ask: float


def should_emit_signal(
    ask_yes: float | None,
    ask_no: float | None,
    threshold: float,
    min_ask: float,
    max_ask: float,
) -> bool:
    if ask_yes is None or ask_no is None:
        return False
    if ask_yes < min_ask or ask_yes > max_ask:
        return False
    if ask_no < min_ask or ask_no > max_ask:
        return False
    return ask_yes + ask_no <= threshold


class ComplementArbStrategy:
    def __init__(self, config: ComplementArbConfig) -> None:
        self.config = config

    def evaluate(
        self,
        market: BinaryMarket,
        ask_yes: float | None,
        ask_no: float | None,
        now_utc: datetime,
    ) -> ArbSignal | None:
        if not should_emit_signal(
            ask_yes=ask_yes,
            ask_no=ask_no,
            threshold=self.config.entry_threshold_sum_ask,
            min_ask=self.config.min_ask,
            max_ask=self.config.max_ask,
        ):
            return None

        assert ask_yes is not None
        assert ask_no is not None
        return ArbSignal.new(
            market_id=market.market_id,
            slug=market.slug,
            yes_token_id=market.yes_token_id,
            no_token_id=market.no_token_id,
            ask_yes=ask_yes,
            ask_no=ask_no,
            threshold=self.config.entry_threshold_sum_ask,
            detected_at=now_utc,
            reason="sum_ask_le_threshold",
        )
