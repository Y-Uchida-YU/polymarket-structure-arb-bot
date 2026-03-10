from __future__ import annotations

import asyncio
import json
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


class FakeBookClientNoData:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def fetch_book_summary(self, asset_id: str) -> BookSummary | None:
        self.calls.append(asset_id)
        return None


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
            "per_asset_book_grace_ms": 1,
        },
        guardrails={
            "global_unhealthy_consecutive_count": 1,
            "global_unhealthy_min_duration_seconds": 0,
            "global_unhealthy_min_asset_ratio": 1.0,
            "global_ws_unhealthy_min_asset_ratio": 1.0,
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


def test_resync_cooldown_suppresses_storm_on_repeated_stale_checks(tmp_path: Path) -> None:
    logger = logging.getLogger("test_resync_cooldown")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "stale_asset_ms": 1,
            "initial_market_data_grace_ms": 1,
            "per_asset_book_grace_ms": 1,
            "resync_cooldown_ms": 60_000,
            "max_resync_assets_per_cycle": 10,
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
    app.state.subscription_started_at = datetime.now(tz=UTC) - timedelta(seconds=2)
    app.state.first_quote_received_at = None

    asyncio.run(app.check_data_freshness_and_resync())
    first_call_count = len(app.book_client.calls)
    asyncio.run(app.check_data_freshness_and_resync())
    second_call_count = len(app.book_client.calls)

    assert first_call_count >= 2
    assert second_call_count == first_call_count

    asyncio.run(app.shutdown())


def test_partial_asset_staleness_blocks_asset_scope_without_global_safe_mode(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("test_asset_scope_safe_mode")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "stale_asset_ms": 60_000,
            "initial_market_data_grace_ms": 1,
            "per_asset_book_grace_ms": 1,
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
                make_raw_market("m2", "yes2", "no2"),
            ],
        ]
    )

    asyncio.run(app.load_markets())
    asyncio.run(app.on_ws_connected(["yes1", "no1", "yes2", "no2"]))
    app.state.first_quote_received_at = datetime.now(tz=UTC)
    app.healthcheck.state.asset_last_update_at["yes1"] = datetime.now(tz=UTC) - timedelta(minutes=5)

    asyncio.run(app.check_data_freshness_and_resync())

    assert app.state.safe_mode_active is False
    assert app.state.safe_mode_reason is None
    assert app.state.safe_mode_scope == "asset"
    assert app.state.asset_safe_mode_reason == "book_state_unhealthy"
    assert "yes1" in app.state.safe_mode_blocked_assets
    assert "no1" not in app.state.safe_mode_blocked_assets

    asyncio.run(app.shutdown())


def test_all_assets_stale_enters_global_safe_mode(tmp_path: Path) -> None:
    logger = logging.getLogger("test_global_safe_mode")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "stale_asset_ms": 1,
            "initial_market_data_grace_ms": 1,
            "per_asset_book_grace_ms": 1,
        },
        guardrails={
            "global_unhealthy_consecutive_count": 1,
            "global_unhealthy_min_duration_seconds": 0,
            "global_unhealthy_min_asset_ratio": 1.0,
            "global_ws_unhealthy_min_asset_ratio": 1.0,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    app.state.ws_connected_at = datetime.now(tz=UTC)
    app.state.subscription_started_at = datetime.now(tz=UTC) - timedelta(seconds=2)
    app.state.first_quote_received_at = None
    asyncio.run(app.check_data_freshness_and_resync())

    assert app.state.safe_mode_active is True
    assert app.state.safe_mode_reason == "all_assets_stale"
    assert app.state.safe_mode_scope == "global"

    asyncio.run(app.shutdown())


def test_global_unhealthy_requires_persistence_before_entering_safe_mode(tmp_path: Path) -> None:
    logger = logging.getLogger("test_global_unhealthy_persistence")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "stale_asset_ms": 1,
            "initial_market_data_grace_ms": 1,
            "per_asset_book_grace_ms": 1,
        },
        guardrails={
            "global_unhealthy_consecutive_count": 2,
            "global_unhealthy_min_duration_seconds": 0,
            "global_unhealthy_min_asset_ratio": 1.0,
            "global_ws_unhealthy_min_asset_ratio": 1.0,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    app.state.ws_connected_at = datetime.now(tz=UTC)
    app.state.subscription_started_at = datetime.now(tz=UTC) - timedelta(seconds=2)
    app.state.first_quote_received_at = datetime.now(tz=UTC)

    asyncio.run(app.check_data_freshness_and_resync())
    assert app.state.safe_mode_active is False
    assert app.state.global_unhealthy_consecutive_count == 1

    asyncio.run(app.check_data_freshness_and_resync())
    assert app.state.safe_mode_active is True
    assert app.state.safe_mode_scope == "global"

    asyncio.run(app.shutdown())


def test_market_scope_block_applies_without_global_stop(tmp_path: Path) -> None:
    logger = logging.getLogger("test_market_scope_block")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "stale_asset_ms": 1000,
            "initial_market_data_grace_ms": 1,
            "per_asset_book_grace_ms": 1,
        },
        guardrails={
            "global_unhealthy_consecutive_count": 3,
            "global_unhealthy_min_duration_seconds": 9999,
            "global_unhealthy_min_asset_ratio": 1.0,
            "global_ws_unhealthy_min_asset_ratio": 1.0,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClient()
    app.gamma_client = FakeGammaClient(
        responses=[[make_raw_market("m1", "yes1", "no1"), make_raw_market("m2", "yes2", "no2")]]
    )

    asyncio.run(app.load_markets())
    asyncio.run(app.on_ws_connected(["yes1", "no1", "yes2", "no2"]))
    app.state.first_quote_received_at = datetime.now(tz=UTC)
    old = datetime.now(tz=UTC) - timedelta(seconds=5)
    now = datetime.now(tz=UTC)
    app.healthcheck.on_asset_quote_update("yes1", old)
    app.healthcheck.on_asset_quote_update("no1", old)
    app.healthcheck.on_asset_quote_update("yes2", now)
    app.healthcheck.on_asset_quote_update("no2", now)

    asyncio.run(app.check_data_freshness_and_resync())

    assert app.state.safe_mode_active is False
    assert app.state.safe_mode_scope == "market"
    assert "m1" in app.state.safe_mode_blocked_markets
    assert "m2" not in app.state.safe_mode_blocked_markets
    metric_row = app.sqlite_store.conn.execute("""
        SELECT COUNT(*)
        FROM metrics
        WHERE metric_name = 'safe_mode_market_blocked'
        """).fetchone()
    assert int(metric_row[0] if metric_row else 0) >= 1

    asyncio.run(app.shutdown())


def test_per_asset_grace_keeps_missing_book_as_not_ready_without_resync(tmp_path: Path) -> None:
    logger = logging.getLogger("test_book_not_ready_grace")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "stale_asset_ms": 60_000,
            "initial_market_data_grace_ms": 1,
            "per_asset_book_grace_ms": 60_000,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    app.state.ws_connected_at = datetime.now(tz=UTC)
    app.state.subscription_started_at = datetime.now(tz=UTC) - timedelta(seconds=2)
    app.state.first_quote_received_at = datetime.now(tz=UTC)
    now = datetime.now(tz=UTC)
    app.healthcheck.on_asset_quote_update("yes1", now)
    app.healthcheck.on_asset_quote_update("no1", now)

    asyncio.run(app.check_data_freshness_and_resync())

    assert isinstance(app.book_client, FakeBookClient)
    assert app.book_client.calls == []
    assert app.state.last_book_missing_reason_by_asset.get("yes1") == "book_not_ready"
    assert app.state.last_book_missing_reason_by_asset.get("no1") == "book_not_ready"
    row = app.sqlite_store.conn.execute("""
        SELECT COALESCE(SUM(metric_value), 0)
        FROM metrics
        WHERE metric_name = 'missing_book_state_reason:book_not_ready'
        """).fetchone()
    assert int(float(row[0] if row else 0)) >= 2

    asyncio.run(app.shutdown())


def test_ready_condition_unmet_records_book_not_ready_no_signal_reason(tmp_path: Path) -> None:
    logger = logging.getLogger("test_book_not_ready_no_signal")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    config = make_config(tmp_path)
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    payload = json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": "yes1",
            "ask": "0.48",
            "bid": "0.47",
            "ask_size": "100",
            "bid_size": "100",
        }
    )
    asyncio.run(app.handle_ws_message(payload))

    row = app.sqlite_store.conn.execute("""
        SELECT COUNT(*)
        FROM metrics
        WHERE metric_name = 'no_signal_reason:book_not_ready'
        """).fetchone()
    assert int(row[0] if row else 0) >= 1

    asyncio.run(app.shutdown())


def test_same_reason_resync_cooldown_blocks_repeated_missing_book_resync(tmp_path: Path) -> None:
    logger = logging.getLogger("test_same_reason_resync_cooldown")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "stale_asset_ms": 1,
            "initial_market_data_grace_ms": 1,
            "per_asset_book_grace_ms": 1,
            "resync_cooldown_ms": 0,
            "same_reason_resync_cooldown_ms": 60_000,
            "missing_book_resync_cooldown_ms": 60_000,
            "max_resync_assets_per_cycle": 10,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClientNoData()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    app.state.ws_connected_at = datetime.now(tz=UTC)
    app.state.subscription_started_at = datetime.now(tz=UTC) - timedelta(seconds=2)
    app.state.first_quote_received_at = datetime.now(tz=UTC)

    asyncio.run(app.check_data_freshness_and_resync())
    first_call_count = len(app.book_client.calls)
    asyncio.run(app.check_data_freshness_and_resync())
    second_call_count = len(app.book_client.calls)

    assert first_call_count >= 2
    assert second_call_count == first_call_count

    asyncio.run(app.shutdown())


def test_recovering_assets_do_not_loop_quote_missing_after_resync(tmp_path: Path) -> None:
    logger = logging.getLogger("test_recovering_assets")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "stale_asset_ms": 1,
            "initial_market_data_grace_ms": 1,
            "per_asset_book_grace_ms": 1,
            "resync_recovery_grace_ms": 120_000,
            "resync_cooldown_ms": 0,
            "same_reason_resync_cooldown_ms": 0,
            "missing_book_resync_cooldown_ms": 0,
            "max_resync_assets_per_cycle": 10,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClientNoData()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    app.state.ws_connected_at = datetime.now(tz=UTC)
    app.state.subscription_started_at = datetime.now(tz=UTC) - timedelta(seconds=2)
    app.state.first_quote_received_at = datetime.now(tz=UTC)

    asyncio.run(app.check_data_freshness_and_resync())
    first_call_count = len(app.book_client.calls)
    asyncio.run(app.check_data_freshness_and_resync())
    second_call_count = len(app.book_client.calls)

    assert first_call_count >= 2
    assert second_call_count == first_call_count
    assert app.state.last_book_missing_reason_by_asset.get("yes1") == "book_recovering"
    assert app.state.last_book_missing_reason_by_asset.get("no1") == "book_recovering"
    recovering_metric = app.sqlite_store.conn.execute("""
        SELECT COALESCE(SUM(metric_value), 0)
        FROM metrics
        WHERE metric_name = 'missing_book_state_reason:book_recovering'
        """).fetchone()
    quote_missing_metric = app.sqlite_store.conn.execute("""
        SELECT COALESCE(SUM(metric_value), 0)
        FROM metrics
        WHERE metric_name = 'missing_book_state_reason:quote_missing_after_resync'
        """).fetchone()
    assert int(float(recovering_metric[0] if recovering_metric else 0)) >= 2
    assert int(float(quote_missing_metric[0] if quote_missing_metric else 0)) == 0

    asyncio.run(app.shutdown())


def test_recovering_asset_transitions_to_ready_when_quote_arrives(tmp_path: Path) -> None:
    logger = logging.getLogger("test_recovering_to_ready")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "stale_asset_ms": 1,
            "initial_market_data_grace_ms": 1,
            "per_asset_book_grace_ms": 1,
            "resync_recovery_grace_ms": 120_000,
            "resync_cooldown_ms": 0,
            "same_reason_resync_cooldown_ms": 0,
            "missing_book_resync_cooldown_ms": 0,
            "max_resync_assets_per_cycle": 10,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClientNoData()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    app.state.ws_connected_at = datetime.now(tz=UTC)
    app.state.subscription_started_at = datetime.now(tz=UTC) - timedelta(seconds=2)
    app.state.first_quote_received_at = datetime.now(tz=UTC)

    asyncio.run(app.check_data_freshness_and_resync())
    assert "yes1" in app.state.asset_recovering_until
    assert "no1" in app.state.asset_recovering_until

    yes_payload = json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": "yes1",
            "ask": "0.48",
            "bid": "0.47",
            "ask_size": "100",
            "bid_size": "100",
        }
    )
    no_payload = json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": "no1",
            "ask": "0.52",
            "bid": "0.51",
            "ask_size": "100",
            "bid_size": "100",
        }
    )
    asyncio.run(app.handle_ws_message(yes_payload))
    asyncio.run(app.handle_ws_message(no_payload))

    assert "yes1" in app.state.ready_assets
    assert "no1" in app.state.ready_assets
    assert "yes1" not in app.state.asset_recovering_until
    assert "no1" not in app.state.asset_recovering_until

    asyncio.run(app.shutdown())


def test_universe_metrics_track_current_and_cumulative_counts(tmp_path: Path) -> None:
    logger = logging.getLogger("test_universe_metrics")
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
                make_raw_market("m2", "yes2", "no2"),
            ],
            [
                make_raw_market("m1", "yes1", "no1"),
            ],
        ]
    )

    asyncio.run(app.load_markets())
    asyncio.run(app.load_markets())

    assert app.state.watched_markets == 1
    assert app.state.subscribed_assets == 2
    assert app.state.cumulative_watched_market_ids == {"m1", "m2"}
    assert app.state.cumulative_subscribed_asset_ids == {"yes1", "no1", "yes2", "no2"}
    latest = app.sqlite_store.conn.execute("""
        SELECT metric_name, metric_value
        FROM metrics
        WHERE metric_name LIKE 'universe_%'
        ORDER BY created_at ASC
        """).fetchall()
    metric_map = {str(name): int(float(value)) for name, value in latest}
    assert metric_map["universe_current_watched_markets"] == 1
    assert metric_map["universe_current_subscribed_assets"] == 2
    assert metric_map["universe_cumulative_watched_markets"] == 2
    assert metric_map["universe_cumulative_subscribed_assets"] == 4

    asyncio.run(app.shutdown())
