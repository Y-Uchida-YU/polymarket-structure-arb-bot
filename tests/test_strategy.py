from __future__ import annotations

from datetime import UTC, datetime

import pytest

from src.domain.market import BinaryMarket
from src.strategy.complement_arb import ComplementArbConfig, ComplementArbStrategy, should_emit_signal


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
