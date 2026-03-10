from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from src.domain.signal import ArbSignal
from src.execution.order_router import PaperOrderRouter
from src.execution.quote_manager import BestBidAskUpdate, QuoteManager


def test_quote_manager_ingests_best_bid_ask() -> None:
    manager = QuoteManager(
        token_to_market_side={
            "yes-token": ("market-1", "yes"),
            "no-token": ("market-1", "no"),
        }
    )
    payload = json.dumps(
        [
            {"event_type": "best_bid_ask", "asset_id": "yes-token", "ask": "0.41", "bid": "0.39"},
            {"event_type": "best_bid_ask", "asset_id": "no-token", "ask": "0.55", "bid": "0.53"},
        ]
    )
    updates = manager.ingest_ws_message(payload)
    assert len(updates) == 2
    ask_yes, ask_no = manager.get_market_asks("market-1")
    assert ask_yes == 0.41
    assert ask_no == 0.55


def test_quote_manager_ready_checks_require_ask_on_both_legs() -> None:
    manager = QuoteManager(
        token_to_market_side={
            "yes-token": ("market-1", "yes"),
            "no-token": ("market-1", "no"),
        }
    )
    payload = json.dumps(
        [
            {"event_type": "best_bid_ask", "asset_id": "yes-token", "ask": "0.41", "bid": "0.39"},
            {"event_type": "best_bid_ask", "asset_id": "no-token", "ask": "0.55"},
        ]
    )
    manager.ingest_ws_message(payload)

    assert manager.is_asset_ready("yes-token") is True
    assert manager.is_asset_ready("no-token") is True
    assert manager.is_market_ready("market-1") is True


def test_paper_order_router_executes_two_leg_fill() -> None:
    router = PaperOrderRouter(order_size_usdc=10.0)
    signal = ArbSignal.new(
        market_id="market-1",
        slug="sample",
        yes_token_id="yes-token",
        no_token_id="no-token",
        ask_yes=0.45,
        ask_no=0.50,
        threshold=0.98,
        detected_at=datetime(2026, 3, 1, tzinfo=UTC),
        reason="sum_ask_le_threshold",
    )
    result = router.execute_signal(signal=signal, now_utc=datetime(2026, 3, 1, tzinfo=UTC))
    assert result.accepted
    assert len(result.orders) == 2
    assert len(result.fills) == 2
    assert result.estimated_final_pnl > 0


def test_paper_order_router_rejects_for_insufficient_book_size() -> None:
    now = datetime(2026, 3, 1, tzinfo=UTC)
    router = PaperOrderRouter(order_size_usdc=10.0, min_book_size=1.0, stale_quote_ms=5000)
    signal = ArbSignal.new(
        market_id="market-1",
        slug="sample",
        yes_token_id="yes-token",
        no_token_id="no-token",
        ask_yes=0.4,
        ask_no=0.5,
        threshold=0.98,
        detected_at=now,
        reason="sum_ask_le_threshold",
    )
    yes_quote = BestBidAskUpdate(
        asset_id="yes-token",
        best_bid=0.39,
        best_ask=0.4,
        best_bid_size=10.0,
        best_ask_size=0.5,
        event_type="best_bid_ask",
        timestamp=now,
    )
    no_quote = BestBidAskUpdate(
        asset_id="no-token",
        best_bid=0.49,
        best_ask=0.5,
        best_bid_size=10.0,
        best_ask_size=2.0,
        event_type="best_bid_ask",
        timestamp=now,
    )
    result = router.execute_signal(
        signal=signal,
        now_utc=now,
        yes_quote=yes_quote,
        no_quote=no_quote,
    )
    assert not result.accepted
    assert result.rejection_reason in {"min_book_size_not_met", "insufficient_book_size_for_qty"}
    assert not result.orders
    assert not result.fills


def test_paper_order_router_rejects_for_stale_quote() -> None:
    now = datetime(2026, 3, 1, tzinfo=UTC)
    router = PaperOrderRouter(order_size_usdc=10.0, stale_quote_ms=1000)
    signal = ArbSignal.new(
        market_id="market-1",
        slug="sample",
        yes_token_id="yes-token",
        no_token_id="no-token",
        ask_yes=0.4,
        ask_no=0.5,
        threshold=0.98,
        detected_at=now,
        reason="sum_ask_le_threshold",
    )
    stale_timestamp = now - timedelta(seconds=2)
    yes_quote = BestBidAskUpdate(
        asset_id="yes-token",
        best_bid=0.39,
        best_ask=0.4,
        best_bid_size=10.0,
        best_ask_size=30.0,
        event_type="best_bid_ask",
        timestamp=stale_timestamp,
    )
    no_quote = BestBidAskUpdate(
        asset_id="no-token",
        best_bid=0.49,
        best_ask=0.5,
        best_bid_size=10.0,
        best_ask_size=30.0,
        event_type="best_bid_ask",
        timestamp=now,
    )
    result = router.execute_signal(
        signal=signal,
        now_utc=now,
        yes_quote=yes_quote,
        no_quote=no_quote,
    )
    assert not result.accepted
    assert result.rejection_reason == "stale_quote"


def test_paper_order_router_rejects_when_one_leg_missing() -> None:
    now = datetime(2026, 3, 1, tzinfo=UTC)
    router = PaperOrderRouter(order_size_usdc=10.0, stale_quote_ms=1000)
    signal = ArbSignal.new(
        market_id="market-1",
        slug="sample",
        yes_token_id="yes-token",
        no_token_id="no-token",
        ask_yes=0.4,
        ask_no=0.5,
        threshold=0.98,
        detected_at=now,
        reason="sum_ask_le_threshold",
    )
    yes_quote = BestBidAskUpdate(
        asset_id="yes-token",
        best_bid=0.39,
        best_ask=0.4,
        best_bid_size=10.0,
        best_ask_size=30.0,
        event_type="best_bid_ask",
        timestamp=now,
    )
    result = router.execute_signal(signal=signal, now_utc=now, yes_quote=yes_quote, no_quote=None)
    assert not result.accepted
    assert result.rejection_reason == "missing_one_or_both_quotes"


def test_paper_order_router_applies_slippage_ticks_to_fill_price() -> None:
    now = datetime(2026, 3, 1, tzinfo=UTC)
    router = PaperOrderRouter(order_size_usdc=10.0, stale_quote_ms=5000, slip_ticks=2)
    signal = ArbSignal.new(
        market_id="market-1",
        slug="sample",
        yes_token_id="yes-token",
        no_token_id="no-token",
        ask_yes=0.4,
        ask_no=0.5,
        threshold=0.98,
        detected_at=now,
        reason="sum_ask_le_threshold",
    )
    yes_quote = BestBidAskUpdate(
        asset_id="yes-token",
        best_bid=0.39,
        best_ask=0.4,
        best_bid_size=10.0,
        best_ask_size=30.0,
        event_type="best_bid_ask",
        timestamp=now,
    )
    no_quote = BestBidAskUpdate(
        asset_id="no-token",
        best_bid=0.49,
        best_ask=0.5,
        best_bid_size=10.0,
        best_ask_size=30.0,
        event_type="best_bid_ask",
        timestamp=now,
    )
    result = router.execute_signal(
        signal=signal,
        now_utc=now,
        yes_quote=yes_quote,
        no_quote=no_quote,
        yes_tick_size=0.01,
        no_tick_size=0.01,
    )
    fill_prices = {fill.token_id: fill.fill_price for fill in result.fills}
    assert fill_prices["yes-token"] == 0.42
    assert fill_prices["no-token"] == 0.52


def test_paper_order_router_partial_fill_when_partial_enabled() -> None:
    now = datetime(2026, 3, 1, tzinfo=UTC)
    router = PaperOrderRouter(
        order_size_usdc=10.0,
        stale_quote_ms=5000,
        allow_partial_fills=True,
        base_fill_probability=1.0,
    )
    signal = ArbSignal.new(
        market_id="market-1",
        slug="sample",
        yes_token_id="yes-token",
        no_token_id="no-token",
        ask_yes=0.4,
        ask_no=0.5,
        threshold=0.98,
        detected_at=now,
        reason="sum_ask_le_threshold",
    )
    yes_quote = BestBidAskUpdate(
        asset_id="yes-token",
        best_bid=0.39,
        best_ask=0.4,
        best_bid_size=10.0,
        best_ask_size=4.0,
        event_type="best_bid_ask",
        timestamp=now,
    )
    no_quote = BestBidAskUpdate(
        asset_id="no-token",
        best_bid=0.49,
        best_ask=0.5,
        best_bid_size=10.0,
        best_ask_size=5.0,
        event_type="best_bid_ask",
        timestamp=now,
    )
    result = router.execute_signal(
        signal=signal,
        now_utc=now,
        yes_quote=yes_quote,
        no_quote=no_quote,
    )
    assert result.accepted
    assert result.fill_status == "partial_both"
    assert len(result.fills) == 2


def test_paper_order_router_one_leg_only_when_other_leg_depth_zero() -> None:
    now = datetime(2026, 3, 1, tzinfo=UTC)
    router = PaperOrderRouter(
        order_size_usdc=10.0,
        stale_quote_ms=5000,
        allow_partial_fills=True,
        base_fill_probability=1.0,
    )
    signal = ArbSignal.new(
        market_id="market-1",
        slug="sample",
        yes_token_id="yes-token",
        no_token_id="no-token",
        ask_yes=0.4,
        ask_no=0.5,
        threshold=0.98,
        detected_at=now,
        reason="sum_ask_le_threshold",
    )
    yes_quote = BestBidAskUpdate(
        asset_id="yes-token",
        best_bid=0.39,
        best_ask=0.4,
        best_bid_size=10.0,
        best_ask_size=5.0,
        event_type="best_bid_ask",
        timestamp=now,
    )
    no_quote = BestBidAskUpdate(
        asset_id="no-token",
        best_bid=0.49,
        best_ask=0.5,
        best_bid_size=10.0,
        best_ask_size=0.0,
        event_type="best_bid_ask",
        timestamp=now,
    )
    result = router.execute_signal(
        signal=signal,
        now_utc=now,
        yes_quote=yes_quote,
        no_quote=no_quote,
    )
    assert not result.accepted
    assert result.fill_status == "one_leg_yes_only"
    assert len(result.fills) == 1
