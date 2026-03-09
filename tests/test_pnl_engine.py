from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.domain.order import FillEvent
from src.domain.signal import ArbSignal
from src.execution.order_router import PaperOrderRouter
from src.execution.pnl_engine import PaperPnLEngine, PnLEngineConfig
from src.execution.quote_manager import BestBidAskUpdate


def _signal(now: datetime) -> ArbSignal:
    return ArbSignal.new(
        market_id="market-1",
        slug="sample",
        yes_token_id="yes-token",
        no_token_id="no-token",
        ask_yes=0.4,
        ask_no=0.5,
        threshold=0.98,
        detected_at=now,
        reason="sum_ask_le_threshold",
        adjusted_edge=0.01,
        order_size_usdc=10.0,
    )


def _quote(
    asset_id: str, bid: float, ask: float, ask_size: float, ts: datetime
) -> BestBidAskUpdate:
    return BestBidAskUpdate(
        asset_id=asset_id,
        best_bid=bid,
        best_ask=ask,
        best_bid_size=100.0,
        best_ask_size=ask_size,
        event_type="best_bid_ask",
        timestamp=ts,
    )


def test_slippage_worsens_total_projected_pnl() -> None:
    now = datetime(2026, 3, 1, tzinfo=UTC)
    signal = _signal(now)
    yes_quote = _quote("yes-token", bid=0.39, ask=0.4, ask_size=100.0, ts=now)
    no_quote = _quote("no-token", bid=0.49, ask=0.5, ask_size=100.0, ts=now)

    base_router = PaperOrderRouter(order_size_usdc=10.0, slip_ticks=0, stale_quote_ms=5000)
    slipped_router = PaperOrderRouter(order_size_usdc=10.0, slip_ticks=2, stale_quote_ms=5000)

    base_result = base_router.execute_signal(
        signal=signal,
        now_utc=now,
        yes_quote=yes_quote,
        no_quote=no_quote,
        yes_tick_size=0.01,
        no_tick_size=0.01,
    )
    slipped_result = slipped_router.execute_signal(
        signal=signal,
        now_utc=now,
        yes_quote=yes_quote,
        no_quote=no_quote,
        yes_tick_size=0.01,
        no_tick_size=0.01,
    )
    assert slipped_result.total_projected_pnl < base_result.total_projected_pnl


def test_total_projected_pnl_uses_actual_fill_price_not_signal_price() -> None:
    now = datetime(2026, 3, 1, tzinfo=UTC)
    signal = ArbSignal.new(
        market_id="market-1",
        slug="sample",
        yes_token_id="yes-token",
        no_token_id="no-token",
        ask_yes=0.1,
        ask_no=0.1,
        threshold=0.98,
        detected_at=now,
        reason="sum_ask_le_threshold",
        order_size_usdc=10.0,
    )
    yes_quote = _quote("yes-token", bid=0.44, ask=0.45, ask_size=100.0, ts=now)
    no_quote = _quote("no-token", bid=0.49, ask=0.5, ask_size=100.0, ts=now)
    router = PaperOrderRouter(order_size_usdc=10.0, slip_ticks=0, stale_quote_ms=5000)

    result = router.execute_signal(
        signal=signal,
        now_utc=now,
        yes_quote=yes_quote,
        no_quote=no_quote,
    )
    matched_qty = result.inventory_snapshot.matched_qty
    expected = matched_qty * (1.0 - (0.45 + 0.5))
    assert result.projected_matched_pnl == pytest.approx(expected)
    assert result.total_projected_pnl != pytest.approx(matched_qty * (1.0 - (0.1 + 0.1)))


def test_unmatched_inventory_remains_when_one_leg_fills() -> None:
    now = datetime(2026, 3, 1, tzinfo=UTC)
    signal = _signal(now)
    yes_quote = _quote("yes-token", bid=0.39, ask=0.4, ask_size=5.0, ts=now)
    no_quote = _quote("no-token", bid=0.49, ask=0.5, ask_size=0.0, ts=now)
    router = PaperOrderRouter(
        order_size_usdc=10.0,
        stale_quote_ms=5000,
        allow_partial_fills=True,
        base_fill_probability=1.0,
    )

    result = router.execute_signal(
        signal=signal,
        now_utc=now,
        yes_quote=yes_quote,
        no_quote=no_quote,
    )
    assert result.inventory_snapshot.matched_qty == 0.0
    assert result.inventory_snapshot.unmatched_yes_qty > 0.0
    assert result.inventory_snapshot.unmatched_no_qty == 0.0


def test_stale_quote_uses_conservative_unmatched_valuation() -> None:
    now = datetime(2026, 3, 1, tzinfo=UTC)
    stale_time = now - timedelta(seconds=5)
    signal = _signal(now)
    engine = PaperPnLEngine(PnLEngineConfig(stale_quote_ms=1000, conservative_unmatched_mark=0.0))
    yes_fill = FillEvent.new(
        order_id="o1",
        signal_id=signal.signal_id,
        market_id=signal.market_id,
        token_id=signal.yes_token_id,
        filled_qty=3.0,
        fill_price=0.45,
        filled_at=now,
    )
    yes_quote = _quote("yes-token", bid=0.44, ask=0.46, ask_size=10.0, ts=stale_time)
    no_quote = _quote("no-token", bid=0.5, ask=0.52, ask_size=10.0, ts=stale_time)

    inventory_snapshot, pnl_snapshot = engine.build_snapshots(
        signal=signal,
        fills=[yes_fill],
        now_utc=now,
        yes_quote=yes_quote,
        no_quote=no_quote,
    )
    assert inventory_snapshot.unmatched_yes_qty == pytest.approx(3.0)
    assert inventory_snapshot.yes_mark_price == 0.0
    assert pnl_snapshot.unmatched_inventory_mtm == pytest.approx(-3.0 * 0.45)
