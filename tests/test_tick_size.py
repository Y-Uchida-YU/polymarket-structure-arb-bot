from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import httpx
import pytest

from src.clients.tick_size_client import TickSizeClient
from src.domain.market import BinaryMarket
from src.execution.quote_manager import BestBidAskUpdate, QuoteManager
from src.strategy.complement_arb import ComplementArbConfig, ComplementArbStrategy
from src.utils.price import align_price_to_tick, is_price_aligned_to_tick


def _market() -> BinaryMarket:
    return BinaryMarket(
        market_id="m1",
        question="Will test happen?",
        slug="will-test-happen",
        category="crypto",
        end_time=datetime(2030, 1, 1, tzinfo=UTC),
        condition_id="c1",
        yes_token_id="yes1",
        no_token_id="no1",
        raw={},
    )


def test_tick_size_snapshot_and_ws_update() -> None:
    manager = QuoteManager(token_to_market_side={"yes1": ("m1", "yes")})
    manager.apply_tick_size_snapshot(
        asset_id="yes1", tick_size=0.01, timestamp=datetime(2026, 1, 1, tzinfo=UTC)
    )
    assert manager.get_tick_size("yes1") == 0.01

    payload = json.dumps(
        {
            "event_type": "tick_size_change",
            "asset_id": "yes1",
            "tick_size": "0.005",
            "timestamp": "2026-01-01T00:00:01Z",
        }
    )
    manager.ingest_ws_message(payload)
    assert manager.get_tick_size("yes1") == 0.005


def test_tick_size_client_fetches_initial_tick_size(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeAsyncClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        async def __aenter__(self) -> FakeAsyncClient:
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

        async def get(self, endpoint: str) -> httpx.Response:
            request = httpx.Request("GET", f"https://clob.polymarket.com{endpoint}")
            return httpx.Response(
                status_code=200,
                json={"minimum_tick_size": "0.01"},
                request=request,
            )

    from src.clients import tick_size_client as module

    monkeypatch.setattr(module.httpx, "AsyncClient", FakeAsyncClient)
    client = TickSizeClient(base_url="https://clob.polymarket.com")

    update = asyncio.run(client.fetch_tick_size("yes1"))
    assert update is not None
    assert update.asset_id == "yes1"
    assert update.tick_size == 0.01


def test_signal_rejected_when_quote_not_tick_aligned() -> None:
    strategy = ComplementArbStrategy(
        ComplementArbConfig(
            entry_threshold_sum_ask=0.99,
            min_ask=0.01,
            max_ask=0.99,
            enable_quality_guards=True,
        )
    )
    yes_quote = BestBidAskUpdate(
        asset_id="yes1",
        best_bid=0.39,
        best_ask=0.405,
        best_bid_size=100.0,
        best_ask_size=100.0,
        event_type="best_bid_ask",
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
    )
    no_quote = BestBidAskUpdate(
        asset_id="no1",
        best_bid=0.59,
        best_ask=0.58,
        best_bid_size=100.0,
        best_ask_size=100.0,
        event_type="best_bid_ask",
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
    )
    signal = strategy.evaluate_with_quotes(
        market=_market(),
        yes_quote=yes_quote,
        no_quote=no_quote,
        now_utc=datetime(2026, 1, 1, tzinfo=UTC),
        tick_size_yes=0.01,
        tick_size_no=0.01,
        order_size_usdc=5.0,
    )
    assert signal is None


def test_price_alignment_utility_rounds_conservatively() -> None:
    assert not is_price_aligned_to_tick(price=0.401, tick_size=0.01)
    assert align_price_to_tick(price=0.401, tick_size=0.01, side="BUY") == 0.41
    assert align_price_to_tick(price=0.401, tick_size=0.01, side="SELL") == 0.4
