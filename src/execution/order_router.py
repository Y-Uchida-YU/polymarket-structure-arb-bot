from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from src.domain.order import FillEvent, PaperOrder
from src.domain.signal import ArbSignal


@dataclass(slots=True)
class PaperExecutionResult:
    orders: list[PaperOrder]
    fills: list[FillEvent]
    estimated_final_pnl: float


class PaperOrderRouter:
    def __init__(self, order_size_usdc: float) -> None:
        self.order_size_usdc = order_size_usdc

    def execute_signal(self, signal: ArbSignal, now_utc: datetime) -> PaperExecutionResult:
        if signal.sum_ask <= 0:
            raise ValueError("sum_ask must be positive")

        qty = self.order_size_usdc / signal.sum_ask

        yes_order = PaperOrder.new(
            signal_id=signal.signal_id,
            market_id=signal.market_id,
            token_id=signal.yes_token_id,
            side="BUY",
            quantity=qty,
            limit_price=signal.ask_yes,
            created_at=now_utc,
        )
        no_order = PaperOrder.new(
            signal_id=signal.signal_id,
            market_id=signal.market_id,
            token_id=signal.no_token_id,
            side="BUY",
            quantity=qty,
            limit_price=signal.ask_no,
            created_at=now_utc,
        )

        yes_fill = FillEvent.new(
            order_id=yes_order.order_id,
            signal_id=signal.signal_id,
            market_id=signal.market_id,
            token_id=signal.yes_token_id,
            filled_qty=qty,
            fill_price=signal.ask_yes,
            filled_at=now_utc,
        )
        no_fill = FillEvent.new(
            order_id=no_order.order_id,
            signal_id=signal.signal_id,
            market_id=signal.market_id,
            token_id=signal.no_token_id,
            filled_qty=qty,
            fill_price=signal.ask_no,
            filled_at=now_utc,
        )

        estimated_final_pnl = qty * (1.0 - signal.sum_ask)
        return PaperExecutionResult(
            orders=[yes_order, no_order],
            fills=[yes_fill, no_fill],
            estimated_final_pnl=estimated_final_pnl,
        )
