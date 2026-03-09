from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from src.domain.accounting import InventorySnapshot, PnLSnapshot
from src.domain.order import FillEvent
from src.domain.signal import ArbSignal
from src.execution.quote_manager import BestBidAskUpdate


@dataclass(slots=True)
class PnLEngineConfig:
    stale_quote_ms: int
    conservative_unmatched_mark: float = 0.0


class PaperPnLEngine:
    def __init__(self, config: PnLEngineConfig) -> None:
        self.config = config

    def build_snapshots(
        self,
        signal: ArbSignal,
        fills: list[FillEvent],
        now_utc: datetime,
        yes_quote: BestBidAskUpdate | None,
        no_quote: BestBidAskUpdate | None,
    ) -> tuple[InventorySnapshot, PnLSnapshot]:
        yes_fills = [fill for fill in fills if fill.token_id == signal.yes_token_id]
        no_fills = [fill for fill in fills if fill.token_id == signal.no_token_id]

        yes_filled_qty = sum(fill.filled_qty for fill in yes_fills)
        no_filled_qty = sum(fill.filled_qty for fill in no_fills)
        matched_qty = min(yes_filled_qty, no_filled_qty)
        unmatched_yes_qty = max(0.0, yes_filled_qty - no_filled_qty)
        unmatched_no_qty = max(0.0, no_filled_qty - yes_filled_qty)

        avg_fill_price_yes = self._weighted_average_fill_price(yes_fills)
        avg_fill_price_no = self._weighted_average_fill_price(no_fills)

        yes_mark_price, yes_mark_mode = self._mark_price(
            quote=yes_quote,
            now_utc=now_utc,
            fallback=self.config.conservative_unmatched_mark,
        )
        no_mark_price, no_mark_mode = self._mark_price(
            quote=no_quote,
            now_utc=now_utc,
            fallback=self.config.conservative_unmatched_mark,
        )
        valuation_mode = yes_mark_mode if unmatched_yes_qty > 0 else no_mark_mode
        if unmatched_yes_qty > 0 and unmatched_no_qty > 0:
            valuation_mode = "mixed"

        matched_cost = matched_qty * (avg_fill_price_yes + avg_fill_price_no)
        projected_matched_pnl = matched_qty * 1.0 - matched_cost

        unmatched_yes_cost = unmatched_yes_qty * avg_fill_price_yes
        unmatched_no_cost = unmatched_no_qty * avg_fill_price_no
        unmatched_yes_value = unmatched_yes_qty * yes_mark_price
        unmatched_no_value = unmatched_no_qty * no_mark_price
        unmatched_inventory_mtm = (unmatched_yes_value - unmatched_yes_cost) + (
            unmatched_no_value - unmatched_no_cost
        )
        total_projected_pnl = projected_matched_pnl + unmatched_inventory_mtm

        inventory_snapshot = InventorySnapshot(
            signal_id=signal.signal_id,
            market_id=signal.market_id,
            market_slug=signal.slug,
            timestamp=now_utc,
            yes_filled_qty=yes_filled_qty,
            no_filled_qty=no_filled_qty,
            matched_qty=matched_qty,
            unmatched_yes_qty=unmatched_yes_qty,
            unmatched_no_qty=unmatched_no_qty,
            avg_fill_price_yes=avg_fill_price_yes,
            avg_fill_price_no=avg_fill_price_no,
            yes_mark_price=yes_mark_price,
            no_mark_price=no_mark_price,
            valuation_mode=valuation_mode,
        )
        pnl_snapshot = PnLSnapshot(
            signal_id=signal.signal_id,
            market_id=signal.market_id,
            market_slug=signal.slug,
            timestamp=now_utc,
            estimated_edge_at_signal=signal.adjusted_edge,
            projected_matched_pnl=projected_matched_pnl,
            unmatched_inventory_mtm=unmatched_inventory_mtm,
            total_projected_pnl=total_projected_pnl,
        )
        return inventory_snapshot, pnl_snapshot

    @staticmethod
    def _weighted_average_fill_price(fills: list[FillEvent]) -> float:
        total_qty = sum(fill.filled_qty for fill in fills)
        if total_qty <= 0:
            return 0.0
        total_cost = sum(fill.filled_qty * fill.fill_price for fill in fills)
        return total_cost / total_qty

    def _mark_price(
        self,
        quote: BestBidAskUpdate | None,
        now_utc: datetime,
        fallback: float,
    ) -> tuple[float, str]:
        if quote is None:
            return fallback, "missing_quote_fallback"
        age_ms = max(0.0, (now_utc - quote.timestamp).total_seconds() * 1000.0)
        if age_ms > self.config.stale_quote_ms:
            return fallback, "stale_quote_fallback"
        if quote.best_bid is None:
            return fallback, "missing_bid_fallback"
        return quote.best_bid, "best_bid"
