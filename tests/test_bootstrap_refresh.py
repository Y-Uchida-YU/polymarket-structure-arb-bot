from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.app.bootstrap import PolymarketStructureArbApp
from src.config.loader import AppConfig, MarketsConfig, Settings
from src.domain.book import BookSummary, TickSizeUpdate
from src.utils.clock import utc_now


class FakeGammaClient:
    def __init__(self, responses: list[list[dict[str, object]]]) -> None:
        self.responses = responses
        self.index = 0

    async def fetch_active_markets(
        self,
        page_size: int = 200,  # noqa: ARG002
        max_pages: int = 10,  # noqa: ARG002
    ) -> list[dict[str, object]]:
        response = self.responses[min(self.index, len(self.responses) - 1)]
        self.index += 1
        return response


class FakeTickSizeClient:
    async def fetch_tick_size(self, token_id: str) -> TickSizeUpdate:
        return TickSizeUpdate(
            asset_id=token_id,
            tick_size=0.01,
            updated_at=utc_now(),
            source="test",
        )


class FakeBookClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def fetch_book_summary(self, asset_id: str) -> BookSummary:
        self.calls.append(asset_id)
        return BookSummary(
            asset_id=asset_id,
            best_bid=0.48,
            best_ask=0.5,
            best_bid_size=100.0,
            best_ask_size=100.0,
            timestamp=utc_now(),
            source="test_book",
        )


def make_raw_market(
    market_id: str,
    yes_token: str,
    no_token: str,
) -> dict[str, object]:
    return {
        "id": market_id,
        "question": f"Will event {market_id} happen?",
        "slug": f"event-{market_id}",
        "category": "crypto",
        "active": True,
        "closed": False,
        "archived": False,
        "outcomes": '["Yes", "No"]',
        "clobTokenIds": f'["{yes_token}", "{no_token}"]',
        "endDate": "2030-01-01T00:00:00Z",
        "conditionId": f"cond-{market_id}",
        "enableOrderBook": True,
    }


def make_config(tmp_path: Path) -> AppConfig:
    settings = Settings(
        storage={
            "sqlite_path": "state.db",
            "export_dir": "exports",
            "log_dir": "logs",
        },
        runtime={
            "market_refresh_minutes": 1,
        },
    )
    return AppConfig(
        root_dir=tmp_path,
        settings=settings,
        markets=MarketsConfig(),
    )


def test_market_refresh_updates_internal_market_map(tmp_path: Path) -> None:
    logger = logging.getLogger("test_market_refresh")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    config = make_config(tmp_path)
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(
        responses=[
            [
                make_raw_market("m1", "yes1", "no1"),
            ],
            [
                make_raw_market("m1", "yes1", "no1"),
                make_raw_market("m2", "yes2", "no2"),
            ],
        ]
    )

    first = asyncio.run(app.load_markets())
    second = asyncio.run(app.load_markets())

    assert first.market_count == 1
    assert not first.asset_ids_changed
    assert second.market_count == 2
    assert second.asset_ids_changed
    assert set(app.markets_by_id.keys()) == {"m1", "m2"}
    assert app.token_to_market_side["yes1"] == ("m1", "yes")
    assert app.token_to_market_side["no2"] == ("m2", "no")

    asyncio.run(app.shutdown())


def test_ws_connected_triggers_resync(tmp_path: Path) -> None:
    logger = logging.getLogger("test_resync_on_connect")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    config = make_config(tmp_path)
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClient()
    app.gamma_client = FakeGammaClient(
        responses=[
            [
                make_raw_market("m1", "yes1", "no1"),
            ],
        ]
    )

    asyncio.run(app.load_markets())
    asyncio.run(app.on_ws_connected(["yes1", "no1"]))

    yes_quote, no_quote = app.quote_manager.get_market_quotes("m1")
    assert yes_quote is not None
    assert no_quote is not None
    assert app.state.ws_connected_at is not None
    assert app.state.subscription_started_at is not None
    assert isinstance(app.book_client, FakeBookClient)
    assert set(app.book_client.calls) == {"yes1", "no1"}

    asyncio.run(app.shutdown())


def test_missing_state_triggers_resync_in_freshness_check(tmp_path: Path) -> None:
    logger = logging.getLogger("test_freshness_resync")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "stale_asset_ms": 1,
            "initial_market_data_grace_ms": 1,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClient()
    app.gamma_client = FakeGammaClient(
        responses=[
            [
                make_raw_market("m1", "yes1", "no1"),
            ],
        ]
    )

    asyncio.run(app.load_markets())
    app.state.ws_connected_at = datetime.now(tz=UTC)
    app.state.subscription_started_at = datetime.now(tz=UTC) - timedelta(milliseconds=5)
    app.state.first_quote_received_at = None
    asyncio.run(app.check_data_freshness_and_resync())

    assert isinstance(app.book_client, FakeBookClient)
    assert "yes1" in app.book_client.calls
    assert "no1" in app.book_client.calls
    assert app.state.safe_mode_active

    asyncio.run(app.shutdown())


def test_startup_without_ws_connection_does_not_enter_all_assets_stale_safe_mode(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("test_warmup_without_ws")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "stale_asset_ms": 1,
            "initial_market_data_grace_ms": 60_000,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClient()
    app.gamma_client = FakeGammaClient(
        responses=[
            [
                make_raw_market("m1", "yes1", "no1"),
            ],
        ]
    )

    asyncio.run(app.load_markets())
    asyncio.run(app.check_data_freshness_and_resync())

    assert isinstance(app.book_client, FakeBookClient)
    assert app.book_client.calls == []
    assert app.state.safe_mode_active is False
    assert app.state.safe_mode_reason is None
    assert app.state.stale_assets == set()

    asyncio.run(app.shutdown())


def test_ws_connected_within_grace_does_not_enter_safe_mode(tmp_path: Path) -> None:
    logger = logging.getLogger("test_warmup_with_grace")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "stale_asset_ms": 1,
            "initial_market_data_grace_ms": 60_000,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClient()
    app.gamma_client = FakeGammaClient(
        responses=[
            [
                make_raw_market("m1", "yes1", "no1"),
            ],
        ]
    )

    asyncio.run(app.load_markets())
    app.state.ws_connected_at = datetime.now(tz=UTC)
    app.state.subscription_started_at = datetime.now(tz=UTC)
    app.state.first_quote_received_at = None
    asyncio.run(app.check_data_freshness_and_resync())

    assert isinstance(app.book_client, FakeBookClient)
    assert app.book_client.calls == []
    assert app.state.safe_mode_active is False
    assert app.state.safe_mode_reason is None
    assert app.state.stale_assets == set()

    asyncio.run(app.shutdown())
