from __future__ import annotations

from datetime import UTC, datetime

import pytest

from src.domain.market import BinaryMarket
from src.execution.quote_manager import BestBidAskUpdate
from src.strategy.complement_arb import (
    ComplementArbConfig,
    ComplementArbStrategy,
    should_emit_signal,
)


def make_market() -> BinaryMarket:
    return BinaryMarket(
        market_id="m1",
        question="Will X happen?",
        slug="will-x-happen",
        category="crypto",
        end_time=datetime(2030, 1, 1, tzinfo=UTC),
        condition_id="c1",
        yes_token_id="yes1",
        no_token_id="no1",
        raw={},
    )


def test_should_emit_signal_true_when_sum_below_threshold() -> None:
    assert should_emit_signal(ask_yes=0.47, ask_no=0.50, threshold=0.98, min_ask=0.01, max_ask=0.99)


def test_strategy_returns_signal_object() -> None:
    strategy = ComplementArbStrategy(
        ComplementArbConfig(entry_threshold_sum_ask=0.98, min_ask=0.01, max_ask=0.99)
    )
    signal = strategy.evaluate(
        market=make_market(),
        ask_yes=0.48,
        ask_no=0.49,
        now_utc=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert signal is not None
    assert signal.sum_ask == pytest.approx(0.97)


def test_signal_rejected_when_spread_exceeds_guard() -> None:
    strategy = ComplementArbStrategy(
        ComplementArbConfig(
            entry_threshold_sum_ask=0.99,
            min_ask=0.01,
            max_ask=0.99,
            max_spread_per_leg=0.01,
            min_depth_per_leg=1.0,
            max_quote_age_ms_for_signal=5000,
            adjusted_edge_min=0.0,
        )
    )
    now = datetime(2026, 1, 1, tzinfo=UTC)
    yes_quote = BestBidAskUpdate(
        asset_id="yes1",
        best_bid=0.35,
        best_ask=0.4,
        best_bid_size=20.0,
        best_ask_size=20.0,
        event_type="best_bid_ask",
        timestamp=now,
    )
    no_quote = BestBidAskUpdate(
        asset_id="no1",
        best_bid=0.54,
        best_ask=0.58,
        best_bid_size=20.0,
        best_ask_size=20.0,
        event_type="best_bid_ask",
        timestamp=now,
    )
    signal = strategy.evaluate_with_quotes(
        market=make_market(),
        yes_quote=yes_quote,
        no_quote=no_quote,
        now_utc=now,
        tick_size_yes=0.01,
        tick_size_no=0.01,
        order_size_usdc=5.0,
    )
    assert signal is None


def test_signal_rejected_when_quote_age_exceeds_guard() -> None:
    strategy = ComplementArbStrategy(
        ComplementArbConfig(
            entry_threshold_sum_ask=0.99,
            min_ask=0.01,
            max_ask=0.99,
            max_quote_age_ms_for_signal=1000,
            min_depth_per_leg=1.0,
            max_spread_per_leg=0.05,
        )
    )
    now = datetime(2026, 1, 1, tzinfo=UTC)
    old = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    yes_quote = BestBidAskUpdate(
        asset_id="yes1",
        best_bid=0.39,
        best_ask=0.4,
        best_bid_size=20.0,
        best_ask_size=20.0,
        event_type="best_bid_ask",
        timestamp=old,
    )
    no_quote = BestBidAskUpdate(
        asset_id="no1",
        best_bid=0.57,
        best_ask=0.58,
        best_bid_size=20.0,
        best_ask_size=20.0,
        event_type="best_bid_ask",
        timestamp=old,
    )
    signal = strategy.evaluate_with_quotes(
        market=make_market(),
        yes_quote=yes_quote,
        no_quote=no_quote,
        now_utc=now.replace(second=2),
        tick_size_yes=0.01,
        tick_size_no=0.01,
        order_size_usdc=5.0,
    )
    assert signal is None


def test_signal_rejected_when_adjusted_edge_below_min() -> None:
    strategy = ComplementArbStrategy(
        ComplementArbConfig(
            entry_threshold_sum_ask=0.99,
            min_ask=0.01,
            max_ask=0.99,
            adjusted_edge_min=0.02,
            slippage_penalty_ticks=2.0,
            min_depth_per_leg=1.0,
            max_spread_per_leg=0.05,
        )
    )
    now = datetime(2026, 1, 1, tzinfo=UTC)
    yes_quote = BestBidAskUpdate(
        asset_id="yes1",
        best_bid=0.4,
        best_ask=0.46,
        best_bid_size=20.0,
        best_ask_size=20.0,
        event_type="best_bid_ask",
        timestamp=now,
    )
    no_quote = BestBidAskUpdate(
        asset_id="no1",
        best_bid=0.47,
        best_ask=0.52,
        best_bid_size=20.0,
        best_ask_size=20.0,
        event_type="best_bid_ask",
        timestamp=now,
    )
    signal = strategy.evaluate_with_quotes(
        market=make_market(),
        yes_quote=yes_quote,
        no_quote=no_quote,
        now_utc=now,
        tick_size_yes=0.01,
        tick_size_no=0.01,
        order_size_usdc=5.0,
    )
    assert signal is None
