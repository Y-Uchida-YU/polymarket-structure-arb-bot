from __future__ import annotations

import json
from datetime import UTC, datetime

from src.domain.signal import ArbSignal
from src.execution.order_router import PaperOrderRouter
from src.execution.quote_manager import QuoteManager


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
    assert len(result.orders) == 2
    assert len(result.fills) == 2
    assert result.estimated_final_pnl > 0
