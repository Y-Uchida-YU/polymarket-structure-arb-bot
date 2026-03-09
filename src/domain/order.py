from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4


@dataclass(slots=True)
class PaperOrder:
    order_id: str
    signal_id: str
    market_id: str
    token_id: str
    side: str
    quantity: float
    limit_price: float
    status: str
    created_at: datetime

    @classmethod
    def new(
        cls,
        signal_id: str,
        market_id: str,
        token_id: str,
        side: str,
        quantity: float,
        limit_price: float,
        created_at: datetime,
    ) -> "PaperOrder":
        return cls(
            order_id=str(uuid4()),
            signal_id=signal_id,
            market_id=market_id,
            token_id=token_id,
            side=side,
            quantity=quantity,
            limit_price=limit_price,
            status="filled",
            created_at=created_at,
        )


@dataclass(slots=True)
class FillEvent:
    fill_id: str
    order_id: str
    signal_id: str
    market_id: str
    token_id: str
    filled_qty: float
    fill_price: float
    fee: float
    filled_at: datetime

    @classmethod
    def new(
        cls,
        order_id: str,
        signal_id: str,
        market_id: str,
        token_id: str,
        filled_qty: float,
        fill_price: float,
        filled_at: datetime,
        fee: float = 0.0,
    ) -> "FillEvent":
        return cls(
            fill_id=str(uuid4()),
            order_id=order_id,
            signal_id=signal_id,
            market_id=market_id,
            token_id=token_id,
            filled_qty=filled_qty,
            fill_price=fill_price,
            fee=fee,
            filled_at=filled_at,
        )
