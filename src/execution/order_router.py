from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from src.domain.order import FillEvent, PaperOrder
from src.domain.signal import ArbSignal
from src.execution.quote_manager import BestBidAskUpdate


@dataclass(slots=True)
class PaperExecutionResult:
    orders: list[PaperOrder]
    fills: list[FillEvent]
    estimated_final_pnl: float
    accepted: bool
    rejection_reason: str | None


class PaperOrderRouter:
    def __init__(
        self,
        order_size_usdc: float,
        min_book_size: float = 0.0,
        stale_quote_ms: int = 3_000,
    ) -> None:
        self.order_size_usdc = order_size_usdc
        self.min_book_size = max(0.0, min_book_size)
        self.stale_quote_ms = max(0, stale_quote_ms)

    def execute_signal(
        self,
        signal: ArbSignal,
        now_utc: datetime,
        yes_quote: BestBidAskUpdate | None = None,
        no_quote: BestBidAskUpdate | None = None,
    ) -> PaperExecutionResult:
        if signal.sum_ask <= 0:
            raise ValueError("sum_ask must be positive")

        if yes_quote is not None or no_quote is not None:
            quote_validation = self._validate_quotes(
                yes_quote=yes_quote,
                no_quote=no_quote,
                now_utc=now_utc,
            )
            if quote_validation is not None:
                return PaperExecutionResult(
                    orders=[],
                    fills=[],
                    estimated_final_pnl=0.0,
                    accepted=False,
                    rejection_reason=quote_validation,
                )

        qty = self.order_size_usdc / signal.sum_ask
        if yes_quote is not None and no_quote is not None:
            if yes_quote.best_ask_size is None or no_quote.best_ask_size is None:
                return PaperExecutionResult(
                    orders=[],
                    fills=[],
                    estimated_final_pnl=0.0,
                    accepted=False,
                    rejection_reason="missing_book_size",
                )
            if yes_quote.best_ask_size < qty or no_quote.best_ask_size < qty:
                return PaperExecutionResult(
                    orders=[],
                    fills=[],
                    estimated_final_pnl=0.0,
                    accepted=False,
                    rejection_reason="insufficient_book_size_for_qty",
                )

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
            accepted=True,
            rejection_reason=None,
        )

    def _validate_quotes(
        self,
        yes_quote: BestBidAskUpdate | None,
        no_quote: BestBidAskUpdate | None,
        now_utc: datetime,
    ) -> str | None:
        if yes_quote is None or no_quote is None:
            return "missing_one_or_both_quotes"
        if yes_quote.best_ask is None or no_quote.best_ask is None:
            return "missing_best_ask"
        if self._is_stale(yes_quote, now_utc) or self._is_stale(no_quote, now_utc):
            return "stale_quote"
        if self.min_book_size > 0:
            if yes_quote.best_ask_size is None or no_quote.best_ask_size is None:
                return "missing_book_size"
            if (
                yes_quote.best_ask_size < self.min_book_size
                or no_quote.best_ask_size < self.min_book_size
            ):
                return "min_book_size_not_met"
        return None

    def _is_stale(self, quote: BestBidAskUpdate, now_utc: datetime) -> bool:
        quote_age_ms = (now_utc - quote.timestamp).total_seconds() * 1000.0
        return quote_age_ms > self.stale_quote_ms
