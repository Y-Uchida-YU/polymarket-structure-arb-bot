from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from src.domain.accounting import InventorySnapshot, PnLSnapshot
from src.domain.order import FillEvent, PaperOrder
from src.domain.signal import ArbSignal
from src.execution.fill_model import FillModelConfig, PaperFillModel
from src.execution.pnl_engine import PaperPnLEngine, PnLEngineConfig
from src.execution.quote_manager import BestBidAskUpdate


@dataclass(slots=True)
class PaperExecutionResult:
    orders: list[PaperOrder]
    fills: list[FillEvent]
    accepted: bool
    rejection_reason: str | None
    fill_status: str
    quote_age_ms: float
    fill_probability_score: float
    signal_edge_after_slippage: float
    partial_fill: bool
    inventory_snapshot: InventorySnapshot
    pnl_snapshot: PnLSnapshot
    estimated_edge_at_signal: float
    projected_matched_pnl: float
    unmatched_inventory_mtm: float
    total_projected_pnl: float
    estimated_final_pnl: float


class PaperOrderRouter:
    def __init__(
        self,
        order_size_usdc: float,
        min_book_size: float = 0.0,
        stale_quote_ms: int = 3_000,
        fill_latency_ms: int = 300,
        slip_ticks: int = 1,
        base_fill_probability: float = 0.95,
        allow_partial_fills: bool = True,
    ) -> None:
        self.order_size_usdc = order_size_usdc
        self.fill_model = PaperFillModel(
            FillModelConfig(
                stale_quote_ms=max(0, stale_quote_ms),
                min_book_size=max(0.0, min_book_size),
                fill_latency_ms=max(0, fill_latency_ms),
                slip_ticks=max(0, slip_ticks),
                base_fill_probability=max(0.0, min(1.0, base_fill_probability)),
                allow_partial_fills=allow_partial_fills,
            )
        )
        self.pnl_engine = PaperPnLEngine(
            PnLEngineConfig(
                stale_quote_ms=max(0, stale_quote_ms),
                conservative_unmatched_mark=0.0,
            )
        )

    def execute_signal(
        self,
        signal: ArbSignal,
        now_utc: datetime,
        yes_quote: BestBidAskUpdate | None = None,
        no_quote: BestBidAskUpdate | None = None,
        yes_tick_size: float | None = None,
        no_tick_size: float | None = None,
    ) -> PaperExecutionResult:
        if signal.sum_ask <= 0:
            raise ValueError("sum_ask must be positive")

        if yes_quote is None and no_quote is None:
            yes_quote, no_quote = self._fallback_quotes_from_signal(signal=signal, now_utc=now_utc)

        fill_decision = self.fill_model.evaluate(
            signal=signal,
            now_utc=now_utc,
            yes_quote=yes_quote,
            no_quote=no_quote,
            yes_tick_size=yes_tick_size,
            no_tick_size=no_tick_size,
            default_order_size_usdc=self.order_size_usdc,
        )

        orders: list[PaperOrder] = []
        fills: list[FillEvent] = []

        if fill_decision.yes_leg.filled_qty > 0 and fill_decision.yes_leg.fill_price is not None:
            yes_order = PaperOrder.new(
                signal_id=signal.signal_id,
                market_id=signal.market_id,
                token_id=signal.yes_token_id,
                side="BUY",
                quantity=fill_decision.yes_leg.filled_qty,
                limit_price=fill_decision.yes_leg.fill_price,
                created_at=now_utc,
                status=fill_decision.yes_leg.status,
            )
            orders.append(yes_order)
            fills.append(
                FillEvent.new(
                    order_id=yes_order.order_id,
                    signal_id=signal.signal_id,
                    market_id=signal.market_id,
                    token_id=signal.yes_token_id,
                    filled_qty=fill_decision.yes_leg.filled_qty,
                    fill_price=fill_decision.yes_leg.fill_price,
                    filled_at=now_utc,
                )
            )

        if fill_decision.no_leg.filled_qty > 0 and fill_decision.no_leg.fill_price is not None:
            no_order = PaperOrder.new(
                signal_id=signal.signal_id,
                market_id=signal.market_id,
                token_id=signal.no_token_id,
                side="BUY",
                quantity=fill_decision.no_leg.filled_qty,
                limit_price=fill_decision.no_leg.fill_price,
                created_at=now_utc,
                status=fill_decision.no_leg.status,
            )
            orders.append(no_order)
            fills.append(
                FillEvent.new(
                    order_id=no_order.order_id,
                    signal_id=signal.signal_id,
                    market_id=signal.market_id,
                    token_id=signal.no_token_id,
                    filled_qty=fill_decision.no_leg.filled_qty,
                    fill_price=fill_decision.no_leg.fill_price,
                    filled_at=now_utc,
                )
            )

        inventory_snapshot, pnl_snapshot = self.pnl_engine.build_snapshots(
            signal=signal,
            fills=fills,
            now_utc=now_utc,
            yes_quote=yes_quote,
            no_quote=no_quote,
        )

        quote_age_ms = max(fill_decision.yes_leg.quote_age_ms, fill_decision.no_leg.quote_age_ms)
        partial_fill = fill_decision.fill_status.startswith("partial")
        return PaperExecutionResult(
            orders=orders,
            fills=fills,
            accepted=fill_decision.accepted,
            rejection_reason=fill_decision.reject_reason,
            fill_status=fill_decision.fill_status,
            quote_age_ms=quote_age_ms,
            fill_probability_score=fill_decision.fill_probability_score,
            signal_edge_after_slippage=fill_decision.signal_edge_after_slippage,
            partial_fill=partial_fill,
            inventory_snapshot=inventory_snapshot,
            pnl_snapshot=pnl_snapshot,
            estimated_edge_at_signal=pnl_snapshot.estimated_edge_at_signal,
            projected_matched_pnl=pnl_snapshot.projected_matched_pnl,
            unmatched_inventory_mtm=pnl_snapshot.unmatched_inventory_mtm,
            total_projected_pnl=pnl_snapshot.total_projected_pnl,
            estimated_final_pnl=pnl_snapshot.total_projected_pnl,
        )

    def _fallback_quotes_from_signal(
        self,
        signal: ArbSignal,
        now_utc: datetime,
    ) -> tuple[BestBidAskUpdate, BestBidAskUpdate]:
        # Keep compatibility for unit-tests that execute ArbSignal without quote snapshots.
        fallback_qty = max(1.0, self.order_size_usdc / max(signal.sum_ask, 1e-9))
        yes_quote = BestBidAskUpdate(
            asset_id=signal.yes_token_id,
            best_bid=signal.bid_yes,
            best_ask=signal.ask_yes,
            best_bid_size=signal.size_yes or fallback_qty * 2.0,
            best_ask_size=signal.size_yes or fallback_qty * 2.0,
            event_type="fallback_signal_quote",
            timestamp=now_utc,
        )
        no_quote = BestBidAskUpdate(
            asset_id=signal.no_token_id,
            best_bid=signal.bid_no,
            best_ask=signal.ask_no,
            best_bid_size=signal.size_no or fallback_qty * 2.0,
            best_ask_size=signal.size_no or fallback_qty * 2.0,
            event_type="fallback_signal_quote",
            timestamp=now_utc,
        )
        return yes_quote, no_quote
