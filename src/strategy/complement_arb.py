from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from src.domain.market import BinaryMarket
from src.domain.signal import ArbSignal
from src.execution.quote_manager import BestBidAskUpdate
from src.utils.price import is_price_aligned_to_tick, normalize_tick_size


@dataclass(slots=True)
class ComplementArbConfig:
    entry_threshold_sum_ask: float
    min_ask: float
    max_ask: float
    enable_quality_guards: bool = True
    max_spread_per_leg: float = 0.05
    min_depth_per_leg: float = 5.0
    max_quote_age_ms_for_signal: int = 3000
    adjusted_edge_min: float = 0.0
    slippage_penalty_ticks: float = 1.0


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


@dataclass(slots=True)
class SignalDiagnostics:
    signal_ok: bool
    reason: str
    raw_edge: float
    adjusted_edge: float
    quote_age_ms: float
    spread_yes: float
    spread_no: float
    depth_yes: float
    depth_no: float


class ComplementArbStrategy:
    def __init__(self, config: ComplementArbConfig) -> None:
        self.config = config

    def evaluate_with_quotes(
        self,
        market: BinaryMarket,
        yes_quote: BestBidAskUpdate | None,
        no_quote: BestBidAskUpdate | None,
        now_utc: datetime,
        tick_size_yes: float | None,
        tick_size_no: float | None,
        order_size_usdc: float,
    ) -> ArbSignal | None:
        diagnostics = self.diagnose_with_quotes(
            yes_quote=yes_quote,
            no_quote=no_quote,
            now_utc=now_utc,
            tick_size_yes=tick_size_yes,
            tick_size_no=tick_size_no,
        )
        if not diagnostics.signal_ok:
            return None
        return self.build_signal_from_diagnostics(
            market=market,
            yes_quote=yes_quote,
            no_quote=no_quote,
            now_utc=now_utc,
            tick_size_yes=tick_size_yes,
            tick_size_no=tick_size_no,
            order_size_usdc=order_size_usdc,
            diagnostics=diagnostics,
        )

    def build_signal_from_diagnostics(
        self,
        market: BinaryMarket,
        yes_quote: BestBidAskUpdate | None,
        no_quote: BestBidAskUpdate | None,
        now_utc: datetime,
        tick_size_yes: float | None,
        tick_size_no: float | None,
        order_size_usdc: float,
        diagnostics: SignalDiagnostics,
    ) -> ArbSignal:
        assert yes_quote is not None and no_quote is not None
        assert yes_quote.best_ask is not None and no_quote.best_ask is not None
        return ArbSignal.new(
            market_id=market.market_id,
            slug=market.slug,
            yes_token_id=market.yes_token_id,
            no_token_id=market.no_token_id,
            ask_yes=yes_quote.best_ask,
            ask_no=no_quote.best_ask,
            bid_yes=yes_quote.best_bid,
            bid_no=no_quote.best_bid,
            size_yes=yes_quote.best_ask_size,
            size_no=no_quote.best_ask_size,
            tick_size_yes=tick_size_yes,
            tick_size_no=tick_size_no,
            threshold=self.config.entry_threshold_sum_ask,
            raw_edge=diagnostics.raw_edge,
            adjusted_edge=diagnostics.adjusted_edge,
            quote_age_ms=diagnostics.quote_age_ms,
            order_size_usdc=order_size_usdc,
            detected_at=now_utc,
            reason=diagnostics.reason,
        )

    def diagnose_with_quotes(
        self,
        yes_quote: BestBidAskUpdate | None,
        no_quote: BestBidAskUpdate | None,
        now_utc: datetime,
        tick_size_yes: float | None,
        tick_size_no: float | None,
    ) -> SignalDiagnostics:
        return self._build_diagnostics(
            yes_quote=yes_quote,
            no_quote=no_quote,
            now_utc=now_utc,
            tick_size_yes=tick_size_yes,
            tick_size_no=tick_size_no,
        )

    def _build_diagnostics(
        self,
        yes_quote: BestBidAskUpdate | None,
        no_quote: BestBidAskUpdate | None,
        now_utc: datetime,
        tick_size_yes: float | None,
        tick_size_no: float | None,
    ) -> SignalDiagnostics:
        if yes_quote is None or no_quote is None:
            return SignalDiagnostics(False, "missing_quote", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        if yes_quote.best_ask is None or no_quote.best_ask is None:
            return SignalDiagnostics(
                False,
                "missing_best_ask",
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                yes_quote.best_ask_size or 0.0,
                no_quote.best_ask_size or 0.0,
            )

        ask_yes = yes_quote.best_ask
        ask_no = no_quote.best_ask
        sum_ask = ask_yes + ask_no
        raw_edge = self.config.entry_threshold_sum_ask - sum_ask
        tick_yes = normalize_tick_size(tick_size_yes)
        tick_no = normalize_tick_size(tick_size_no)
        if not is_price_aligned_to_tick(ask_yes, tick_yes) or not is_price_aligned_to_tick(
            ask_no, tick_no
        ):
            return SignalDiagnostics(
                False,
                "tick_alignment_guard",
                raw_edge,
                raw_edge,
                0.0,
                0.0,
                0.0,
                yes_quote.best_ask_size or 0.0,
                no_quote.best_ask_size or 0.0,
            )
        quote_age_ms = max(
            (now_utc - yes_quote.timestamp).total_seconds() * 1000.0,
            (now_utc - no_quote.timestamp).total_seconds() * 1000.0,
        )
        spread_yes = (
            (yes_quote.best_ask - yes_quote.best_bid) if yes_quote.best_bid is not None else 0.0
        )
        spread_no = (
            (no_quote.best_ask - no_quote.best_bid) if no_quote.best_bid is not None else 0.0
        )
        depth_yes = yes_quote.best_ask_size or 0.0
        depth_no = no_quote.best_ask_size or 0.0

        if not should_emit_signal(
            ask_yes=ask_yes,
            ask_no=ask_no,
            threshold=self.config.entry_threshold_sum_ask,
            min_ask=self.config.min_ask,
            max_ask=self.config.max_ask,
        ):
            return SignalDiagnostics(
                False,
                "sum_ask_or_ask_bounds",
                raw_edge,
                raw_edge,
                quote_age_ms,
                spread_yes,
                spread_no,
                depth_yes,
                depth_no,
            )

        if self.config.enable_quality_guards:
            if quote_age_ms > self.config.max_quote_age_ms_for_signal:
                return SignalDiagnostics(
                    False,
                    "quote_age_exceeded",
                    raw_edge,
                    raw_edge,
                    quote_age_ms,
                    spread_yes,
                    spread_no,
                    depth_yes,
                    depth_no,
                )
            if (
                depth_yes < self.config.min_depth_per_leg
                or depth_no < self.config.min_depth_per_leg
            ):
                return SignalDiagnostics(
                    False,
                    "min_depth_guard",
                    raw_edge,
                    raw_edge,
                    quote_age_ms,
                    spread_yes,
                    spread_no,
                    depth_yes,
                    depth_no,
                )
            if (
                spread_yes > self.config.max_spread_per_leg
                or spread_no > self.config.max_spread_per_leg
            ):
                return SignalDiagnostics(
                    False,
                    "spread_guard",
                    raw_edge,
                    raw_edge,
                    quote_age_ms,
                    spread_yes,
                    spread_no,
                    depth_yes,
                    depth_no,
                )

        spread_penalty = max(0.0, spread_yes) + max(0.0, spread_no)
        slippage_penalty = self.config.slippage_penalty_ticks * (tick_yes + tick_no)
        stale_penalty = (
            quote_age_ms / max(1, self.config.max_quote_age_ms_for_signal) * (tick_yes + tick_no)
        )
        adjusted_edge = raw_edge - spread_penalty - slippage_penalty - stale_penalty

        if adjusted_edge < self.config.adjusted_edge_min:
            return SignalDiagnostics(
                False,
                "adjusted_edge_below_min",
                raw_edge,
                adjusted_edge,
                quote_age_ms,
                spread_yes,
                spread_no,
                depth_yes,
                depth_no,
            )
        return SignalDiagnostics(
            True,
            "sum_ask_le_threshold",
            raw_edge,
            adjusted_edge,
            quote_age_ms,
            spread_yes,
            spread_no,
            depth_yes,
            depth_no,
        )

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
