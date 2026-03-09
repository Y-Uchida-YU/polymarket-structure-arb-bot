from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from src.domain.signal import ArbSignal
from src.execution.quote_manager import BestBidAskUpdate
from src.utils.price import align_price_to_tick, normalize_tick_size


@dataclass(slots=True)
class FillModelConfig:
    stale_quote_ms: int
    min_book_size: float
    fill_latency_ms: int
    slip_ticks: int
    base_fill_probability: float
    allow_partial_fills: bool


@dataclass(slots=True)
class LegFillDecision:
    token_id: str
    requested_qty: float
    available_size: float
    filled_qty: float
    quote_age_ms: float
    fill_price: float | None
    status: str
    reject_reason: str | None


@dataclass(slots=True)
class FillDecision:
    accepted: bool
    fill_status: str
    reject_reason: str | None
    yes_leg: LegFillDecision
    no_leg: LegFillDecision
    signal_edge_after_slippage: float
    fill_probability_score: float


class PaperFillModel:
    def __init__(self, config: FillModelConfig) -> None:
        self.config = config

    def evaluate(
        self,
        signal: ArbSignal,
        now_utc: datetime,
        yes_quote: BestBidAskUpdate | None,
        no_quote: BestBidAskUpdate | None,
        yes_tick_size: float | None,
        no_tick_size: float | None,
        default_order_size_usdc: float,
    ) -> FillDecision:
        order_size_usdc = (
            signal.order_size_usdc if signal.order_size_usdc > 0 else default_order_size_usdc
        )
        requested_qty = order_size_usdc / signal.sum_ask

        if yes_quote is None or no_quote is None:
            return self._rejected_decision(
                signal=signal,
                requested_qty=requested_qty,
                reason="missing_one_or_both_quotes",
            )
        if yes_quote.best_ask is None or no_quote.best_ask is None:
            return self._rejected_decision(
                signal=signal,
                requested_qty=requested_qty,
                reason="missing_best_ask",
            )

        yes_age_ms = self._quote_age_ms(now_utc=now_utc, quote=yes_quote)
        no_age_ms = self._quote_age_ms(now_utc=now_utc, quote=no_quote)
        if yes_age_ms > self.config.stale_quote_ms or no_age_ms > self.config.stale_quote_ms:
            return self._rejected_decision(
                signal=signal,
                requested_qty=requested_qty,
                reason="stale_quote",
                yes_age_ms=yes_age_ms,
                no_age_ms=no_age_ms,
            )

        yes_available = max(0.0, yes_quote.best_ask_size or 0.0)
        no_available = max(0.0, no_quote.best_ask_size or 0.0)

        if self.config.min_book_size > 0:
            if (
                yes_available < self.config.min_book_size
                or no_available < self.config.min_book_size
            ):
                return self._rejected_decision(
                    signal=signal,
                    requested_qty=requested_qty,
                    reason="min_book_size_not_met",
                    yes_available=yes_available,
                    no_available=no_available,
                    yes_age_ms=yes_age_ms,
                    no_age_ms=no_age_ms,
                )

        fill_probability_score = self._fill_probability_score(
            requested_qty=requested_qty,
            yes_available=yes_available,
            no_available=no_available,
            yes_age_ms=yes_age_ms,
            no_age_ms=no_age_ms,
        )
        if self.config.base_fill_probability <= 0:
            return self._rejected_decision(
                signal=signal,
                requested_qty=requested_qty,
                reason="low_fill_probability",
                yes_available=yes_available,
                no_available=no_available,
                yes_age_ms=yes_age_ms,
                no_age_ms=no_age_ms,
                fill_probability_score=fill_probability_score,
            )

        if self.config.allow_partial_fills:
            yes_fill_qty = min(requested_qty, yes_available)
            no_fill_qty = min(requested_qty, no_available)
        else:
            if yes_available < requested_qty or no_available < requested_qty:
                return self._rejected_decision(
                    signal=signal,
                    requested_qty=requested_qty,
                    reason="insufficient_depth",
                    yes_available=yes_available,
                    no_available=no_available,
                    yes_age_ms=yes_age_ms,
                    no_age_ms=no_age_ms,
                    fill_probability_score=fill_probability_score,
                )
            yes_fill_qty = requested_qty
            no_fill_qty = requested_qty

        yes_tick = normalize_tick_size(yes_tick_size)
        no_tick = normalize_tick_size(no_tick_size)
        yes_fill_price = align_price_to_tick(
            price=yes_quote.best_ask + yes_tick * self.config.slip_ticks,
            tick_size=yes_tick,
            side="BUY",
        )
        no_fill_price = align_price_to_tick(
            price=no_quote.best_ask + no_tick * self.config.slip_ticks,
            tick_size=no_tick,
            side="BUY",
        )
        signal_edge_after_slippage = signal.threshold - (yes_fill_price + no_fill_price)

        yes_status = (
            "filled"
            if yes_fill_qty >= requested_qty
            else ("partial" if yes_fill_qty > 0 else "rejected")
        )
        no_status = (
            "filled"
            if no_fill_qty >= requested_qty
            else ("partial" if no_fill_qty > 0 else "rejected")
        )
        fill_status = self._derive_fill_status(
            yes_fill_qty=yes_fill_qty, no_fill_qty=no_fill_qty, requested_qty=requested_qty
        )

        return FillDecision(
            accepted=yes_fill_qty > 0 and no_fill_qty > 0,
            fill_status=fill_status,
            reject_reason=None if yes_fill_qty > 0 and no_fill_qty > 0 else "one_leg_missing_fill",
            yes_leg=LegFillDecision(
                token_id=signal.yes_token_id,
                requested_qty=requested_qty,
                available_size=yes_available,
                filled_qty=yes_fill_qty,
                quote_age_ms=yes_age_ms,
                fill_price=yes_fill_price if yes_fill_qty > 0 else None,
                status=yes_status,
                reject_reason=None if yes_fill_qty > 0 else "insufficient_depth",
            ),
            no_leg=LegFillDecision(
                token_id=signal.no_token_id,
                requested_qty=requested_qty,
                available_size=no_available,
                filled_qty=no_fill_qty,
                quote_age_ms=no_age_ms,
                fill_price=no_fill_price if no_fill_qty > 0 else None,
                status=no_status,
                reject_reason=None if no_fill_qty > 0 else "insufficient_depth",
            ),
            signal_edge_after_slippage=signal_edge_after_slippage,
            fill_probability_score=fill_probability_score,
        )

    def _rejected_decision(
        self,
        signal: ArbSignal,
        requested_qty: float,
        reason: str,
        yes_available: float = 0.0,
        no_available: float = 0.0,
        yes_age_ms: float = 0.0,
        no_age_ms: float = 0.0,
        fill_probability_score: float = 0.0,
    ) -> FillDecision:
        return FillDecision(
            accepted=False,
            fill_status="rejected",
            reject_reason=reason,
            yes_leg=LegFillDecision(
                token_id=signal.yes_token_id,
                requested_qty=requested_qty,
                available_size=yes_available,
                filled_qty=0.0,
                quote_age_ms=yes_age_ms,
                fill_price=None,
                status="rejected",
                reject_reason=reason,
            ),
            no_leg=LegFillDecision(
                token_id=signal.no_token_id,
                requested_qty=requested_qty,
                available_size=no_available,
                filled_qty=0.0,
                quote_age_ms=no_age_ms,
                fill_price=None,
                status="rejected",
                reject_reason=reason,
            ),
            signal_edge_after_slippage=0.0,
            fill_probability_score=fill_probability_score,
        )

    def _quote_age_ms(self, now_utc: datetime, quote: BestBidAskUpdate) -> float:
        network_age_ms = max(0.0, (now_utc - quote.timestamp).total_seconds() * 1000.0)
        return network_age_ms + max(0, self.config.fill_latency_ms)

    def _fill_probability_score(
        self,
        requested_qty: float,
        yes_available: float,
        no_available: float,
        yes_age_ms: float,
        no_age_ms: float,
    ) -> float:
        if requested_qty <= 0:
            return 0.0
        depth_ratio = min(yes_available / requested_qty, no_available / requested_qty, 1.0)
        freshness_ratio = min(
            max(0.0, 1.0 - yes_age_ms / max(1, self.config.stale_quote_ms)),
            max(0.0, 1.0 - no_age_ms / max(1, self.config.stale_quote_ms)),
        )
        return (
            max(0.0, self.config.base_fill_probability)
            * max(0.0, depth_ratio)
            * max(0.0, freshness_ratio)
        )

    @staticmethod
    def _derive_fill_status(yes_fill_qty: float, no_fill_qty: float, requested_qty: float) -> str:
        if yes_fill_qty == 0 and no_fill_qty == 0:
            return "rejected"
        if yes_fill_qty > 0 and no_fill_qty == 0:
            return "one_leg_yes_only"
        if yes_fill_qty == 0 and no_fill_qty > 0:
            return "one_leg_no_only"
        if yes_fill_qty >= requested_qty and no_fill_qty >= requested_qty:
            return "filled"
        return "partial_both"
