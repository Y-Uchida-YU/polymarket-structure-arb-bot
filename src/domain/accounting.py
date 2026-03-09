from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class InventorySnapshot:
    signal_id: str
    market_id: str
    market_slug: str
    timestamp: datetime
    yes_filled_qty: float
    no_filled_qty: float
    matched_qty: float
    unmatched_yes_qty: float
    unmatched_no_qty: float
    avg_fill_price_yes: float
    avg_fill_price_no: float
    yes_mark_price: float
    no_mark_price: float
    valuation_mode: str


@dataclass(slots=True)
class PnLSnapshot:
    signal_id: str
    market_id: str
    market_slug: str
    timestamp: datetime
    estimated_edge_at_signal: float
    projected_matched_pnl: float
    unmatched_inventory_mtm: float
    total_projected_pnl: float
