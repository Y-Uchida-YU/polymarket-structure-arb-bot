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
    bid_yes: float | None
    bid_no: float | None
    size_yes: float | None
    size_no: float | None
    tick_size_yes: float | None
    tick_size_no: float | None
    sum_ask: float
    threshold: float
    raw_edge: float
    adjusted_edge: float
    quote_age_ms: float
    signal_edge_after_slippage: float
    order_size_usdc: float
    fill_status: str
    reject_reason: str | None
    resync_reason: str | None
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
        bid_yes: float | None = None,
        bid_no: float | None = None,
        size_yes: float | None = None,
        size_no: float | None = None,
        tick_size_yes: float | None = None,
        tick_size_no: float | None = None,
        raw_edge: float | None = None,
        adjusted_edge: float | None = None,
        quote_age_ms: float = 0.0,
        signal_edge_after_slippage: float = 0.0,
        order_size_usdc: float = 0.0,
        fill_status: str = "pending",
        reject_reason: str | None = None,
        resync_reason: str | None = None,
    ) -> ArbSignal:
        sum_ask = ask_yes + ask_no
        return cls(
            signal_id=str(uuid4()),
            market_id=market_id,
            slug=slug,
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            ask_yes=ask_yes,
            ask_no=ask_no,
            bid_yes=bid_yes,
            bid_no=bid_no,
            size_yes=size_yes,
            size_no=size_no,
            tick_size_yes=tick_size_yes,
            tick_size_no=tick_size_no,
            sum_ask=sum_ask,
            threshold=threshold,
            raw_edge=raw_edge if raw_edge is not None else threshold - sum_ask,
            adjusted_edge=adjusted_edge if adjusted_edge is not None else threshold - sum_ask,
            quote_age_ms=quote_age_ms,
            signal_edge_after_slippage=signal_edge_after_slippage,
            order_size_usdc=order_size_usdc,
            fill_status=fill_status,
            reject_reason=reject_reason,
            resync_reason=resync_reason,
            detected_at=detected_at,
            reason=reason,
        )
