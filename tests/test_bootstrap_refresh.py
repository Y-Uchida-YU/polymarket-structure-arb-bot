from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

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


def test_recovery_diagnostics_records_funnel_events_once_per_cycle(tmp_path: Path) -> None:
    logger = logging.getLogger("test_recovery_diag_funnel")
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
            "resync_recovery_grace_ms": 60_000,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClientNoData()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    now = datetime.now(tz=UTC)
    app.state.ws_connected_at = now
    app.state.subscription_started_at = now - timedelta(seconds=2)
    app.state.first_quote_received_at = now

    asyncio.run(
        app.resync_assets(
            asset_ids=["yes1", "no1"],
            reason="missing_book_state",
            force=True,
        )
    )

    assert "yes1" in app.state.asset_recovery_started_at
    assert "no1" in app.state.asset_recovery_started_at
    assert app.state.asset_recovery_reason_by_asset.get("yes1") == "missing_book_state"
    assert "m1" in app.state.market_recovery_started_at_for_diagnostics

    yes_payload = json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": "yes1",
            "ask": "0.47",
            "bid": "0.46",
            "ask_size": "100",
            "bid_size": "100",
        }
    )
    no_payload = json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": "no1",
            "ask": "0.53",
            "bid": "0.52",
            "ask_size": "100",
            "bid_size": "100",
        }
    )
    asyncio.run(app.handle_ws_message(yes_payload))
    asyncio.run(app.handle_ws_message(no_payload))
    asyncio.run(app.handle_ws_message(yes_payload))
    asyncio.run(app.handle_ws_message(no_payload))

    resync_started = app.sqlite_store.conn.execute("""
        SELECT COUNT(*)
        FROM diagnostics_events
        WHERE event_name = 'resync_started'
        """).fetchone()
    first_quote = app.sqlite_store.conn.execute("""
        SELECT COUNT(*)
        FROM diagnostics_events
        WHERE event_name = 'first_quote_after_resync'
        """).fetchone()
    book_ready = app.sqlite_store.conn.execute("""
        SELECT COUNT(*)
        FROM diagnostics_events
        WHERE event_name = 'book_ready_after_resync'
        """).fetchone()
    market_ready = app.sqlite_store.conn.execute("""
        SELECT COUNT(*)
        FROM diagnostics_events
        WHERE event_name = 'market_ready_after_recovery'
        """).fetchone()
    assert int(resync_started[0] if resync_started else 0) == 2
    assert int(first_quote[0] if first_quote else 0) == 2
    assert int(book_ready[0] if book_ready else 0) == 2
    assert int(market_ready[0] if market_ready else 0) == 1
    assert "yes1" not in app.state.asset_recovery_started_at
    assert "no1" not in app.state.asset_recovery_started_at
    assert "m1" not in app.state.market_recovery_started_at_for_diagnostics

    asyncio.run(app.shutdown())


def test_recovery_diagnostics_first_quote_and_book_ready_increment_on_new_cycle(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("test_recovery_diag_cycle_increment")
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
            "resync_recovery_grace_ms": 60_000,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClientNoData()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    now = datetime.now(tz=UTC)
    app.state.ws_connected_at = now
    app.state.subscription_started_at = now - timedelta(seconds=2)
    app.state.first_quote_received_at = now

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

    asyncio.run(
        app.resync_assets(
            asset_ids=["yes1"],
            reason="missing_book_state",
            force=True,
        )
    )
    asyncio.run(app.handle_ws_message(yes_payload))
    asyncio.run(app.handle_ws_message(yes_payload))

    asyncio.run(
        app.resync_assets(
            asset_ids=["yes1"],
            reason="missing_book_state",
            force=True,
        )
    )
    asyncio.run(app.handle_ws_message(yes_payload))
    asyncio.run(app.handle_ws_message(yes_payload))

    first_quote = app.sqlite_store.conn.execute("""
        SELECT COUNT(*)
        FROM diagnostics_events
        WHERE event_name = 'first_quote_after_resync'
        """).fetchone()
    book_ready = app.sqlite_store.conn.execute("""
        SELECT COUNT(*)
        FROM diagnostics_events
        WHERE event_name = 'book_ready_after_resync'
        """).fetchone()
    assert int(first_quote[0] if first_quote else 0) == 2
    assert int(book_ready[0] if book_ready else 0) == 2

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
            "asset_block_min_consecutive_unhealthy_cycles": 1,
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
            "asset_block_min_consecutive_unhealthy_cycles": 1,
            "market_block_min_consecutive_unhealthy_cycles": 1,
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


def test_refresh_market_universe_order_only_change_does_not_resubscribe(tmp_path: Path) -> None:
    logger = logging.getLogger("test_universe_order_only_change")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_universe_change_confirmations": 2,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(
        responses=[
            [make_raw_market("m1", "yes1", "no1"), make_raw_market("m2", "yes2", "no2")],
            [make_raw_market("m2", "yes2", "no2"), make_raw_market("m1", "yes1", "no1")],
        ]
    )

    asyncio.run(app.load_markets())
    asyncio.run(app.refresh_market_universe())

    assert app.resubscribe_event.is_set() is False
    assert app.state.pending_universe_signature is None
    assert app.state.pending_universe_confirmation_count == 0
    changed_row = app.sqlite_store.conn.execute("""
        SELECT COUNT(*)
        FROM metrics
        WHERE metric_name = 'market_universe_changed'
        """).fetchone()
    assert int(changed_row[0] if changed_row else 0) == 0

    asyncio.run(app.shutdown())


def test_refresh_market_universe_requires_confirmation_before_apply(tmp_path: Path) -> None:
    logger = logging.getLogger("test_universe_change_confirmation")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_universe_change_confirmations": 2,
            "market_universe_change_min_asset_delta": 99,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(
        responses=[
            [make_raw_market("m1", "yes1", "no1")],
            [make_raw_market("m1", "yes1", "no1"), make_raw_market("m2", "yes2", "no2")],
            [make_raw_market("m1", "yes1", "no1"), make_raw_market("m2", "yes2", "no2")],
        ]
    )

    asyncio.run(app.load_markets())
    asyncio.run(app.refresh_market_universe())

    assert app.resubscribe_event.is_set() is False
    assert set(app.markets_by_id.keys()) == {"m1"}
    assert app.state.pending_universe_confirmation_count == 1

    asyncio.run(app.refresh_market_universe())

    assert app.resubscribe_event.is_set() is True
    assert set(app.markets_by_id.keys()) == {"m1", "m2"}
    assert app.state.pending_universe_signature is None
    assert app.state.pending_universe_confirmation_count == 0
    changed_row = app.sqlite_store.conn.execute("""
        SELECT COUNT(*)
        FROM metrics
        WHERE metric_name = 'market_universe_changed'
        """).fetchone()
    assert int(changed_row[0] if changed_row else 0) == 1

    asyncio.run(app.shutdown())


def test_market_block_clears_after_market_recovery(tmp_path: Path) -> None:
    logger = logging.getLogger("test_market_block_clear")
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
            "asset_block_min_consecutive_unhealthy_cycles": 1,
            "market_block_min_consecutive_unhealthy_cycles": 1,
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
    assert "m1" in app.state.safe_mode_blocked_markets
    assert app.state.safe_mode_scope == "market"

    recovered = datetime.now(tz=UTC)
    app.healthcheck.on_asset_quote_update("yes1", recovered)
    app.healthcheck.on_asset_quote_update("no1", recovered)
    app.healthcheck.on_asset_quote_update("yes2", recovered)
    app.healthcheck.on_asset_quote_update("no2", recovered)

    asyncio.run(app.check_data_freshness_and_resync())
    assert "m1" not in app.state.safe_mode_blocked_markets
    assert app.state.safe_mode_scope is None
    cleared_row = app.sqlite_store.conn.execute("""
        SELECT COUNT(*)
        FROM metrics
        WHERE metric_name = 'safe_mode_market_block_cleared'
        """).fetchone()
    assert int(cleared_row[0] if cleared_row else 0) >= 1

    asyncio.run(app.shutdown())


def test_no_data_resync_cooldown_blocks_repeat_loop_after_recovery(tmp_path: Path) -> None:
    logger = logging.getLogger("test_no_data_resync_cooldown")
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
            "resync_recovery_grace_ms": 1,
            "resync_cooldown_ms": 0,
            "same_reason_resync_cooldown_ms": 0,
            "missing_book_resync_cooldown_ms": 0,
            "quote_missing_after_resync_delay_ms": 0,
            "no_data_resync_cooldown_ms": 120_000,
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
    assert first_call_count >= 2

    app.state.asset_recovering_until["yes1"] = datetime.now(tz=UTC) - timedelta(seconds=1)
    app.state.asset_recovering_until["no1"] = datetime.now(tz=UTC) - timedelta(seconds=1)
    asyncio.run(app.check_data_freshness_and_resync())
    second_call_count = len(app.book_client.calls)

    assert second_call_count == first_call_count

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
        WHERE metric_name IN (
            'no_signal_reason:book_not_ready',
            'no_signal_reason:market_not_ready'
        )
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


def test_new_market_probation_blocks_signal_until_ready(tmp_path: Path) -> None:
    logger = logging.getLogger("test_market_probation_blocks_signal")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_probation_ms": 300_000,
            "market_probation_min_quote_updates_per_asset": 3,
            "market_probation_min_ready_asset_ratio": 1.0,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
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

    row = app.sqlite_store.conn.execute("""
        SELECT COUNT(*)
        FROM metrics
        WHERE metric_name = 'no_signal_reason:market_probation'
        """).fetchone()
    assert int(row[0] if row else 0) >= 1

    asyncio.run(app.shutdown())


def test_market_probation_clears_after_readiness_threshold(tmp_path: Path) -> None:
    logger = logging.getLogger("test_market_probation_clears")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_probation_ms": 300_000,
            "market_probation_min_quote_updates_per_asset": 1,
            "market_probation_min_ready_asset_ratio": 1.0,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
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
            "ask": "0.53",
            "bid": "0.52",
            "ask_size": "100",
            "bid_size": "100",
        }
    )

    asyncio.run(app.handle_ws_message(yes_payload))
    asyncio.run(app.handle_ws_message(no_payload))

    assert "m1" not in app.state.market_probation_until
    edge_row = app.sqlite_store.conn.execute("""
        SELECT COUNT(*)
        FROM metrics
        WHERE metric_name = 'no_signal_reason:edge_below_threshold'
        """).fetchone()
    assert int(edge_row[0] if edge_row else 0) >= 1

    asyncio.run(app.shutdown())


def test_probation_assets_do_not_trigger_asset_or_market_block(tmp_path: Path) -> None:
    logger = logging.getLogger("test_probation_assets_not_blocked")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_probation_ms": 300_000,
            "market_probation_min_quote_updates_per_asset": 3,
            "stale_asset_ms": 1,
            "initial_market_data_grace_ms": 1,
            "per_asset_book_grace_ms": 1,
            "asset_block_min_consecutive_unhealthy_cycles": 1,
            "market_block_min_consecutive_unhealthy_cycles": 1,
        },
        guardrails={
            "global_unhealthy_consecutive_count": 10,
            "global_unhealthy_min_duration_seconds": 9999,
            "global_unhealthy_min_asset_ratio": 1.0,
            "global_ws_unhealthy_min_asset_ratio": 1.0,
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

    assert app.state.safe_mode_blocked_assets == set()
    assert app.state.safe_mode_blocked_markets == set()
    warmup_row = app.sqlite_store.conn.execute("""
        SELECT COALESCE(SUM(metric_value), 0)
        FROM metrics
        WHERE metric_name = 'missing_book_state_reason:asset_warming_up'
        """).fetchone()
    assert int(float(warmup_row[0] if warmup_row else 0.0)) >= 2

    asyncio.run(app.shutdown())


def test_healthy_universe_skips_large_market_rotation(tmp_path: Path) -> None:
    logger = logging.getLogger("test_universe_hysteresis_skip")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_universe_change_confirmations": 1,
            "market_universe_change_min_asset_delta": 99,
        },
        market_filters={
            "max_markets_to_watch": 2,
            "min_recent_activity": 0.0,
            "min_liquidity_proxy": 0.0,
            "min_volume_24h_proxy": 0.0,
            "prefer_existing_watched_markets": True,
            "existing_market_hysteresis_score_ratio": 0.9,
            "max_market_replacements_per_refresh": 1,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClient()
    app.gamma_client = FakeGammaClient(
        responses=[
            [make_raw_market("m1", "yes1", "no1"), make_raw_market("m2", "yes2", "no2")],
            [make_raw_market("m3", "yes3", "no3"), make_raw_market("m4", "yes4", "no4")],
        ]
    )

    asyncio.run(app.load_markets())
    asyncio.run(app.on_ws_connected(["yes1", "no1", "yes2", "no2"]))
    app.state.first_quote_received_at = datetime.now(tz=UTC)
    app.state.book_state_unhealthy = False
    app.state.ws_unhealthy = False

    asyncio.run(app.refresh_market_universe())

    assert set(app.markets_by_id.keys()) == {"m1", "m2"}
    assert app.resubscribe_event.is_set() is False
    row = app.sqlite_store.conn.execute("""
        SELECT COUNT(*)
        FROM metrics
        WHERE metric_name = 'market_universe_changed'
        """).fetchone()
    assert int(row[0] if row else 0) == 0

    asyncio.run(app.shutdown())


def test_ws_reconnect_event_metric_records_raw_reason(tmp_path: Path) -> None:
    logger = logging.getLogger("test_ws_reconnect_metric")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    config = make_config(tmp_path)
    app = PolymarketStructureArbApp(config=config, logger=logger)

    app.on_ws_reconnect_required("socket_closed_remote")

    assert app.state.pending_connect_resync_reason == "ws_reconnect"
    assert app.state.last_ws_reconnect_reason == "socket_closed_remote"
    row = app.sqlite_store.conn.execute("""
        SELECT details
        FROM metrics
        WHERE metric_name = 'ws_reconnect_event'
        ORDER BY created_at DESC
        LIMIT 1
        """).fetchone()
    assert row is not None
    assert "reason=socket_closed_remote" in str(row[0])

    asyncio.run(app.shutdown())


def test_connection_recovering_grace_suppresses_missing_book_resync(tmp_path: Path) -> None:
    logger = logging.getLogger("test_connection_recovering_grace")
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
            "reconnect_recovery_grace_ms": 120_000,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClientNoData()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    now = datetime.now(tz=UTC)
    app.state.ws_connected_at = now
    app.state.subscription_started_at = now - timedelta(seconds=2)
    app.state.first_quote_received_at = now
    app.state.connection_recovering_until = now + timedelta(seconds=60)

    asyncio.run(app.check_data_freshness_and_resync())

    assert isinstance(app.book_client, FakeBookClientNoData)
    assert app.book_client.calls == []
    assert app.state.last_book_missing_reason_by_asset.get("yes1") == "connection_recovering"
    assert app.state.last_book_missing_reason_by_asset.get("no1") == "connection_recovering"
    row = app.sqlite_store.conn.execute("""
        SELECT COALESCE(SUM(metric_value), 0)
        FROM metrics
        WHERE metric_name = 'missing_book_state_reason:connection_recovering'
        """).fetchone()
    assert int(float(row[0] if row else 0.0)) >= 2

    asyncio.run(app.shutdown())


def test_reconnect_connected_uses_selective_resync_when_quotes_are_fresh(tmp_path: Path) -> None:
    logger = logging.getLogger("test_reconnect_selective_resync")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "stale_asset_ms": 60_000,
            "initial_market_data_grace_ms": 1,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    yes_payload = json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": "yes1",
            "ask": "0.45",
            "bid": "0.44",
            "ask_size": "50",
            "bid_size": "50",
        }
    )
    no_payload = json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": "no1",
            "ask": "0.55",
            "bid": "0.54",
            "ask_size": "50",
            "bid_size": "50",
        }
    )
    asyncio.run(app.handle_ws_message(yes_payload))
    asyncio.run(app.handle_ws_message(no_payload))
    app.state.pending_connect_resync_reason = "ws_reconnect"

    asyncio.run(app.on_ws_connected(["yes1", "no1"]))

    assert isinstance(app.book_client, FakeBookClient)
    assert app.book_client.calls == []

    asyncio.run(app.shutdown())


def test_market_no_signal_reason_cooldown_suppresses_repeated_market_not_ready_logs(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("test_market_no_signal_reason_cooldown")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_probation_ms": 0,
            "market_no_signal_reason_cooldown_ms": 60_000,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
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
    asyncio.run(app.handle_ws_message(yes_payload))
    asyncio.run(app.handle_ws_message(yes_payload))
    asyncio.run(app.handle_ws_message(yes_payload))

    row = app.sqlite_store.conn.execute("""
        SELECT COUNT(*)
        FROM metrics
        WHERE metric_name = 'no_signal_reason:market_not_ready'
        """).fetchone()
    assert int(row[0] if row else 0) == 1

    asyncio.run(app.shutdown())


def test_market_eligibility_requires_min_quote_updates_before_strategy(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("test_market_eligibility_updates")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_probation_ms": 0,
            "market_eligibility_min_quote_updates_per_asset": 2,
            "market_no_signal_reason_cooldown_ms": 0,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    yes_payload = json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": "yes1",
            "ask": "0.47",
            "bid": "0.46",
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

    market = app.markets_by_id["m1"]
    now = datetime.now(tz=UTC)
    readiness_ok, _, _ = app._evaluate_market_readiness(market=market, now_utc=now)
    assert readiness_ok is True
    eligible_ok, reason, _ = app._evaluate_market_signal_eligibility(market=market, now_utc=now)
    assert eligible_ok is False
    assert reason == "book_not_ready_insufficient_updates"

    asyncio.run(app.handle_ws_message(yes_payload))
    asyncio.run(app.handle_ws_message(no_payload))
    eligible_ok, reason, _ = app._evaluate_market_signal_eligibility(
        market=market,
        now_utc=datetime.now(tz=UTC),
    )
    assert eligible_ok is True
    assert reason == "market_eligible"

    asyncio.run(app.shutdown())


def test_probation_assets_are_excluded_from_stale_resync_candidates(tmp_path: Path) -> None:
    logger = logging.getLogger("test_probation_stale_resync_exclusion")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_probation_ms": 0,
            "stale_asset_ms": 1,
            "initial_market_data_grace_ms": 1,
            "resync_cooldown_ms": 0,
            "same_reason_resync_cooldown_ms": 0,
            "stale_asset_resync_cooldown_ms": 0,
            "stale_asset_resync_additional_cooldown_ms": 0,
            "max_resync_assets_per_cycle": 10,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    now = datetime.now(tz=UTC)
    app.state.ws_connected_at = now
    app.state.subscription_started_at = now - timedelta(seconds=2)
    app.state.first_quote_received_at = now
    app.state.market_probation_until["m1"] = now + timedelta(minutes=5)

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
    stale_ts = datetime.now(tz=UTC) - timedelta(seconds=10)
    app.healthcheck.on_asset_quote_update("yes1", stale_ts)
    app.healthcheck.on_asset_quote_update("no1", stale_ts)

    asyncio.run(app.check_data_freshness_and_resync())

    assert isinstance(app.book_client, FakeBookClient)
    assert app.book_client.calls == []

    asyncio.run(app.shutdown())


def test_runtime_low_quality_market_is_excluded_on_refresh(tmp_path: Path) -> None:
    logger = logging.getLogger("test_runtime_low_quality_exclusion")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_universe_change_confirmations": 1,
            "low_quality_market_penalty_threshold": 3,
            "low_quality_market_min_observations": 1,
            "low_quality_market_exclusion_consecutive_cycles": 1,
            "min_watched_markets_floor": 0,
            "watched_floor_relax_runtime_exclusion": False,
        },
        market_filters={
            "max_markets_to_watch": 2,
            "min_recent_activity": 0.0,
            "min_liquidity_proxy": 0.0,
            "min_volume_24h_proxy": 0.0,
            "min_days_to_expiry": 0.0,
            "max_days_to_expiry": 3650.0,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(
        responses=[
            [make_raw_market("m1", "yes1", "no1")],
            [make_raw_market("m1", "yes1", "no1"), make_raw_market("m2", "yes2", "no2")],
        ]
    )

    asyncio.run(app.load_markets())
    app.state.market_quality_penalty_by_market["m2"] = 4
    app.state.market_refresh_observed_count["m2"] = 2
    app.state.market_low_quality_consecutive_cycles["m2"] = 2

    asyncio.run(app.refresh_market_universe())

    assert set(app.markets_by_id.keys()) == {"m1"}
    assert app.resubscribe_event.is_set() is False
    assert "m2" not in app.state.market_exclusion_reason_by_market
    assert app.state.last_refresh_runtime_excluded_market_ids == {"m2"}
    assert app.state.last_refresh_runtime_excluded_count == 1
    assert "m2" in app.state.last_refresh_runtime_excluded_reason_by_market
    assert app.state.last_refresh_runtime_excluded_at is not None
    row = app.sqlite_store.conn.execute("""
        SELECT details
        FROM metrics
        WHERE metric_name = 'market_filter_exclusion_summary'
        ORDER BY created_at DESC
        LIMIT 1
        """).fetchone()
    assert row is not None
    assert "low_quality_runtime" in str(row[0])

    asyncio.run(app.shutdown())


def test_freshness_metric_uses_last_refresh_runtime_exclusion_state(tmp_path: Path) -> None:
    logger = logging.getLogger("test_refresh_exclusion_metric_source")
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
    app.book_client = FakeBookClientNoData()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    now = datetime.now(tz=UTC)
    app.state.ws_connected_at = now
    app.state.subscription_started_at = now - timedelta(seconds=2)
    app.state.first_quote_received_at = now
    app.state.market_exclusion_reason_by_market = {}
    app.state.last_refresh_runtime_excluded_market_ids = {"mx"}
    app.state.last_refresh_runtime_excluded_reason_by_market = {"mx": "stage=excluded;penalty=8"}
    app.state.last_refresh_runtime_excluded_count = 1
    app.state.last_refresh_runtime_excluded_at = now

    asyncio.run(app.check_data_freshness_and_resync())

    row = app.sqlite_store.conn.execute("""
        SELECT metric_value, details
        FROM metrics
        WHERE metric_name = 'low_quality_runtime_excluded_count'
        ORDER BY created_at DESC
        LIMIT 1
        """).fetchone()
    assert row is not None
    assert int(float(row[0])) == 1
    assert "mx:stage=excluded;penalty=8" in str(row[1])

    asyncio.run(app.shutdown())


def test_runtime_low_quality_market_not_excluded_on_temporary_deterioration(tmp_path: Path) -> None:
    logger = logging.getLogger("test_runtime_low_quality_temporary")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "low_quality_market_penalty_threshold": 3,
            "low_quality_market_min_observations": 1,
            "low_quality_market_exclusion_consecutive_cycles": 3,
            "min_watched_markets_floor": 0,
            "watched_floor_relax_runtime_exclusion": False,
        },
        market_filters={
            "max_markets_to_watch": 2,
            "min_recent_activity": 0.0,
            "min_liquidity_proxy": 0.0,
            "min_volume_24h_proxy": 0.0,
            "min_days_to_expiry": 0.0,
            "max_days_to_expiry": 3650.0,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(
        responses=[[make_raw_market("m1", "yes1", "no1"), make_raw_market("m2", "yes2", "no2")]]
    )

    asyncio.run(app.load_markets())
    app.state.market_quality_penalty_by_market["m2"] = 6
    app.state.market_refresh_observed_count["m2"] = 2
    app.state.market_low_quality_consecutive_cycles["m2"] = 2

    excluded = app._runtime_low_quality_excluded_market_ids()
    assert "m2" not in excluded

    app.state.market_low_quality_consecutive_cycles["m2"] = 3
    excluded = app._runtime_low_quality_excluded_market_ids()
    assert "m2" in excluded
    assert "m2" in app.state.market_exclusion_reason_by_market

    asyncio.run(app.shutdown())


def test_chronic_stale_exclusion_triggered_by_repeated_enters(tmp_path: Path) -> None:
    logger = logging.getLogger("test_chronic_stale_exclusion_repeated")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_stale_exclusion_window_minutes": 60,
            "market_stale_exclusion_min_enter_count": 2,
            "market_stale_exclusion_max_single_duration_ms": 999_999,
            "market_stale_exclusion_cooldown_ms": 60_000,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])
    asyncio.run(app.load_markets())

    now = datetime.now(tz=UTC)
    with app.sqlite_store.conn:
        app.sqlite_store.conn.executemany(
            """
            INSERT INTO diagnostics_events
            (run_id, event_name, asset_id, market_id, reason, latency_ms, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    app.state.run_id,
                    "market_stale_entered",
                    None,
                    "m1",
                    "quote_age_exceeded",
                    None,
                    "",
                    now.isoformat(),
                ),
                (
                    app.state.run_id,
                    "market_stale_entered",
                    None,
                    "m1",
                    "no_recent_quote",
                    None,
                    "",
                    now.isoformat(),
                ),
            ],
        )

    summary = app._refresh_chronic_stale_runtime_exclusions(now_utc=now + timedelta(milliseconds=1))
    assert "m1" in summary.active_market_ids
    assert "m1" in summary.entered_market_ids
    assert app.state.market_chronic_stale_reason_by_market["m1"] == "repeated_stale_enters"

    market = app.markets_by_id["m1"]
    eligibility_ok, eligibility_reason, _ = app._evaluate_market_signal_eligibility(
        market=market,
        now_utc=now,
    )
    assert eligibility_ok is False
    assert eligibility_reason == "chronic_stale_excluded"

    row = app.sqlite_store.conn.execute("""
        SELECT reason
        FROM diagnostics_events
        WHERE event_name = 'market_chronic_stale_exclusion_entered'
        ORDER BY id DESC
        LIMIT 1
        """).fetchone()
    assert row is not None
    assert str(row[0]) == "repeated_stale_enters"

    asyncio.run(app.shutdown())


def test_chronic_stale_exclusion_triggered_by_long_duration(tmp_path: Path) -> None:
    logger = logging.getLogger("test_chronic_stale_exclusion_long_duration")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_stale_exclusion_window_minutes": 60,
            "market_stale_exclusion_min_enter_count": 99,
            "market_stale_exclusion_max_single_duration_ms": 300_000,
            "market_stale_exclusion_cooldown_ms": 60_000,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])
    asyncio.run(app.load_markets())

    now = datetime.now(tz=UTC)
    with app.sqlite_store.conn:
        app.sqlite_store.conn.execute(
            """
            INSERT INTO diagnostics_events
            (run_id, event_name, asset_id, market_id, reason, latency_ms, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                app.state.run_id,
                "market_stale_recovered",
                None,
                "m1",
                "leg_timestamp_mismatch",
                450_000.0,
                "",
                now.isoformat(),
            ),
        )

    summary = app._refresh_chronic_stale_runtime_exclusions(now_utc=now + timedelta(milliseconds=1))
    assert "m1" in summary.active_market_ids
    assert app.state.market_chronic_stale_reason_by_market["m1"] == "long_single_stale_duration"

    asyncio.run(app.shutdown())


def test_chronic_stale_exclusion_clears_after_cooldown(tmp_path: Path) -> None:
    logger = logging.getLogger("test_chronic_stale_exclusion_clears_after_cooldown")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_stale_exclusion_window_minutes": 60,
            "market_stale_exclusion_min_enter_count": 99,
            "market_stale_exclusion_max_single_duration_ms": 999_999,
            "market_stale_exclusion_cooldown_ms": 1_000,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])
    asyncio.run(app.load_markets())

    now = datetime.now(tz=UTC)
    app.state.market_chronic_stale_excluded_until["m1"] = now - timedelta(seconds=1)
    app.state.market_chronic_stale_exclusion_started_at["m1"] = now - timedelta(minutes=1)
    app.state.market_chronic_stale_reason_by_market["m1"] = "repeated_stale_enters"
    app.state.market_chronic_stale_details_by_market["m1"] = {
        "market_id": "m1",
        "market_slug": "event-m1",
    }

    summary = app._refresh_chronic_stale_runtime_exclusions(now_utc=now + timedelta(milliseconds=1))
    assert "m1" not in summary.active_market_ids
    assert "m1" in summary.cleared_market_ids

    row = app.sqlite_store.conn.execute("""
        SELECT event_name
        FROM diagnostics_events
        WHERE event_name = 'market_chronic_stale_exclusion_cleared'
        ORDER BY id DESC
        LIMIT 1
        """).fetchone()
    assert row is not None
    assert str(row[0]) == "market_chronic_stale_exclusion_cleared"

    asyncio.run(app.shutdown())


def test_chronic_stale_exclusion_clear_survives_snapshot_before_refresh(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("test_chronic_stale_clear_after_snapshot")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_stale_exclusion_window_minutes": 60,
            "market_stale_exclusion_min_enter_count": 99,
            "market_stale_exclusion_max_single_duration_ms": 999_999,
            "market_stale_exclusion_cooldown_ms": 1_000,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(
        responses=[
            [make_raw_market("m1", "yes1", "no1")],
            [make_raw_market("m2", "yes2", "no2")],
        ]
    )
    asyncio.run(app.load_markets())

    expired_now = datetime.now(tz=UTC)
    app.state.market_chronic_stale_excluded_until["m1"] = expired_now - timedelta(seconds=1)
    app.state.market_chronic_stale_exclusion_started_at["m1"] = expired_now - timedelta(minutes=2)
    app.state.market_chronic_stale_reason_by_market["m1"] = "repeated_stale_enters"
    app.state.market_chronic_stale_details_by_market["m1"] = {
        "market_id": "m1",
        "market_slug": "event-m1",
    }

    previous_asset_ids = set(app.token_to_market_side.keys())
    snapshot = asyncio.run(app._fetch_market_snapshot())
    asyncio.run(
        app._apply_market_snapshot(
            snapshot=snapshot,
            previous_asset_ids=previous_asset_ids,
        )
    )

    assert "m1" in app.state.market_chronic_stale_excluded_until

    summary = app._refresh_chronic_stale_runtime_exclusions(now_utc=expired_now)
    assert "m1" in summary.cleared_market_ids
    assert "m1" not in summary.active_market_ids
    assert app.state.market_chronic_stale_cleared_count_last_cycle == 1
    assert "m1" not in app.state.market_chronic_stale_excluded_until

    row = app.sqlite_store.conn.execute("""
        SELECT COUNT(1)
        FROM diagnostics_events
        WHERE event_name = 'market_chronic_stale_exclusion_cleared'
          AND market_id = 'm1'
        """).fetchone()
    assert row is not None
    assert int(row[0]) == 1

    asyncio.run(app.shutdown())


def test_chronic_stale_excluded_market_not_backfilled_by_default_policy(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("test_chronic_stale_no_backfill_default")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_universe_change_confirmations": 1,
            "min_watched_markets_floor": 2,
            "watched_floor_relax_runtime_exclusion": True,
            "watched_floor_relax_chronic_stale_exclusion": False,
        },
        market_filters={
            "max_markets_to_watch": 2,
            "min_recent_activity": 0.0,
            "min_liquidity_proxy": 0.0,
            "min_volume_24h_proxy": 0.0,
            "min_days_to_expiry": 0.0,
            "max_days_to_expiry": 3650.0,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(
        responses=[
            [make_raw_market("m1", "yes1", "no1")],
            [make_raw_market("m1", "yes1", "no1"), make_raw_market("m2", "yes2", "no2")],
        ]
    )

    asyncio.run(app.load_markets())
    now = datetime.now(tz=UTC)
    app.state.market_chronic_stale_excluded_until["m2"] = now + timedelta(minutes=10)
    app.state.market_chronic_stale_exclusion_started_at["m2"] = now - timedelta(minutes=5)
    app.state.market_chronic_stale_reason_by_market["m2"] = "repeated_stale_enters"
    app.state.market_chronic_stale_details_by_market["m2"] = {
        "market_id": "m2",
        "market_slug": "event-m2",
    }

    asyncio.run(app.refresh_market_universe())

    assert "m1" in app.markets_by_id
    assert "m2" not in app.markets_by_id
    assert app.state.last_refresh_chronic_reintroduced_market_ids == set()
    assert app.state.last_refresh_watched_chronic_stale_market_ids == set()

    asyncio.run(app.shutdown())


def test_chronic_stale_excluded_market_backfilled_when_policy_enabled(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("test_chronic_stale_backfill_enabled")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_universe_change_confirmations": 1,
            "min_watched_markets_floor": 2,
            "watched_floor_relax_runtime_exclusion": True,
            "watched_floor_relax_chronic_stale_exclusion": True,
        },
        market_filters={
            "max_markets_to_watch": 2,
            "min_recent_activity": 0.0,
            "min_liquidity_proxy": 0.0,
            "min_volume_24h_proxy": 0.0,
            "min_days_to_expiry": 0.0,
            "max_days_to_expiry": 3650.0,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(
        responses=[
            [make_raw_market("m1", "yes1", "no1")],
            [make_raw_market("m1", "yes1", "no1"), make_raw_market("m2", "yes2", "no2")],
        ]
    )

    asyncio.run(app.load_markets())
    now = datetime.now(tz=UTC)
    app.state.market_chronic_stale_excluded_until["m2"] = now + timedelta(minutes=10)
    app.state.market_chronic_stale_exclusion_started_at["m2"] = now - timedelta(minutes=5)
    app.state.market_chronic_stale_reason_by_market["m2"] = "repeated_stale_enters"
    app.state.market_chronic_stale_details_by_market["m2"] = {
        "market_id": "m2",
        "market_slug": "event-m2",
    }

    asyncio.run(app.refresh_market_universe())

    assert "m2" in app.markets_by_id
    assert app.state.last_refresh_chronic_reintroduced_market_ids == {"m2"}
    assert app.state.last_refresh_watched_chronic_stale_market_ids == {"m2"}
    assert (
        app.state.last_refresh_chronic_reintroduced_reason_by_market.get("m2")
        == "watched_floor_backfill_chronic_relaxed"
    )
    row = app.sqlite_store.conn.execute("""
        SELECT COUNT(1)
        FROM diagnostics_events
        WHERE event_name = 'market_chronic_stale_reintroduced_for_floor'
          AND market_id = 'm2'
        """).fetchone()
    assert row is not None
    assert int(row[0]) == 1

    asyncio.run(app.shutdown())


def test_chronic_stale_exclusion_extension_is_tracked_separately_from_enter(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("test_chronic_stale_extension_tracking")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_stale_exclusion_window_minutes": 60,
            "market_stale_exclusion_min_enter_count": 1,
            "market_stale_exclusion_max_single_duration_ms": 999_999,
            "market_stale_exclusion_cooldown_ms": 60_000,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])
    asyncio.run(app.load_markets())

    now = datetime.now(tz=UTC)
    app.state.market_chronic_stale_excluded_until["m1"] = now + timedelta(seconds=5)
    app.state.market_chronic_stale_exclusion_started_at["m1"] = now - timedelta(minutes=1)
    app.state.market_chronic_stale_reason_by_market["m1"] = "repeated_stale_enters"
    app.state.market_chronic_stale_details_by_market["m1"] = {
        "market_id": "m1",
        "market_slug": "event-m1",
    }
    with app.sqlite_store.conn:
        app.sqlite_store.conn.execute(
            """
            INSERT INTO diagnostics_events
            (run_id, event_name, asset_id, market_id, reason, latency_ms, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                app.state.run_id,
                "market_stale_entered",
                None,
                "m1",
                "quote_age_exceeded",
                None,
                "",
                now.isoformat(),
            ),
        )

    summary = app._refresh_chronic_stale_runtime_exclusions(now_utc=now + timedelta(milliseconds=1))
    assert "m1" in summary.active_market_ids
    assert "m1" in summary.extended_market_ids
    assert "m1" not in summary.entered_market_ids

    entered_row = app.sqlite_store.conn.execute("""
        SELECT COUNT(1)
        FROM diagnostics_events
        WHERE event_name = 'market_chronic_stale_exclusion_entered'
          AND market_id = 'm1'
        """).fetchone()
    assert entered_row is not None
    assert int(entered_row[0]) == 0

    extended_row = app.sqlite_store.conn.execute("""
        SELECT COUNT(1)
        FROM diagnostics_events
        WHERE event_name = 'market_chronic_stale_exclusion_extended'
          AND market_id = 'm1'
        """).fetchone()
    assert extended_row is not None
    assert int(extended_row[0]) == 1

    asyncio.run(app.shutdown())


def test_watched_market_floor_prevents_extreme_universe_shrink(tmp_path: Path) -> None:
    logger = logging.getLogger("test_watched_floor")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_universe_change_confirmations": 1,
            "low_quality_market_penalty_threshold": 3,
            "low_quality_market_min_observations": 1,
            "low_quality_market_exclusion_consecutive_cycles": 1,
            "min_watched_markets_floor": 2,
            "watched_floor_relax_runtime_exclusion": True,
        },
        market_filters={
            "max_markets_to_watch": 3,
            "min_recent_activity": 0.0,
            "min_liquidity_proxy": 0.0,
            "min_volume_24h_proxy": 0.0,
            "min_days_to_expiry": 0.0,
            "max_days_to_expiry": 3650.0,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(
        responses=[
            [
                make_raw_market("m1", "yes1", "no1"),
                make_raw_market("m2", "yes2", "no2"),
                make_raw_market("m3", "yes3", "no3"),
            ],
            [
                make_raw_market("m1", "yes1", "no1"),
                make_raw_market("m2", "yes2", "no2"),
                make_raw_market("m3", "yes3", "no3"),
                make_raw_market("m4", "yes4", "no4"),
            ],
        ]
    )

    asyncio.run(app.load_markets())
    for market_id in ("m1", "m2", "m3", "m4"):
        app.state.market_quality_penalty_by_market[market_id] = 8
        app.state.market_low_quality_consecutive_cycles[market_id] = 2
        app.state.market_refresh_observed_count[market_id] = 3

    asyncio.run(app.refresh_market_universe())

    assert app.state.watched_markets >= 2
    assert len(app.markets_by_id) >= 2

    asyncio.run(app.shutdown())


def test_market_can_be_watched_while_not_eligible(tmp_path: Path) -> None:
    logger = logging.getLogger("test_watched_vs_eligible")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "initial_market_data_grace_ms": 1,
            "per_asset_book_grace_ms": 1,
            "market_probation_ms": 0,
            "market_eligibility_min_quote_updates_per_asset": 3,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.book_client = FakeBookClientNoData()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    now = datetime.now(tz=UTC)
    app.state.ws_connected_at = now
    app.state.subscription_started_at = now - timedelta(seconds=2)
    app.state.first_quote_received_at = now

    asyncio.run(app.check_data_freshness_and_resync())

    assert app.state.watched_markets == 1
    assert len(app.markets_by_id) == 1
    assert "m1" not in app.state.eligible_markets
    watched_metric = app.sqlite_store.conn.execute("""
        SELECT metric_value
        FROM metrics
        WHERE metric_name = 'market_state_watched_count'
        ORDER BY created_at DESC
        LIMIT 1
        """).fetchone()
    assert watched_metric is not None
    assert int(float(watched_metric[0])) == 1

    asyncio.run(app.shutdown())


def test_market_stale_transition_events_record_reason_side_and_duration(tmp_path: Path) -> None:
    logger = logging.getLogger("test_market_stale_transition_events")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_probation_ms": 0,
            "market_no_signal_reason_cooldown_ms": 0,
        },
        strategy={
            "max_quote_age_ms_for_signal": 100,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    market = app.markets_by_id["m1"]
    base_now = datetime.now(tz=UTC)

    yes_payload = json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": "yes1",
            "ask": "0.47",
            "bid": "0.46",
            "ask_size": "100",
            "bid_size": "100",
        }
    )
    no_payload = json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": "no1",
            "ask": "0.53",
            "bid": "0.52",
            "ask_size": "100",
            "bid_size": "100",
        }
    )
    asyncio.run(app.handle_ws_message(yes_payload))
    asyncio.run(app.handle_ws_message(no_payload))

    ready_ok, _, _ = app._evaluate_market_readiness(market=market, now_utc=base_now)
    assert ready_ok is True

    yes_quote, no_quote = app.quote_manager.get_market_quotes("m1")
    assert yes_quote is not None
    assert no_quote is not None
    yes_quote.timestamp = base_now - timedelta(seconds=10)
    no_quote.timestamp = base_now - timedelta(seconds=2)

    stale_started_at = base_now + timedelta(seconds=1)
    stale_ok, stale_reason, stale_details = app._evaluate_market_readiness(
        market=market,
        now_utc=stale_started_at,
    )
    assert stale_ok is False
    assert stale_reason == "market_quote_stale_quote_age"
    assert stale_details["stale_reason_key"] == "leg_timestamp_mismatch"
    assert stale_details["stale_side"] == "yes"

    stale_recovered_at = stale_started_at + timedelta(seconds=3)
    yes_quote.timestamp = stale_recovered_at
    no_quote.timestamp = stale_recovered_at
    recovered_ok, _, _ = app._evaluate_market_readiness(
        market=market,
        now_utc=stale_recovered_at,
    )
    assert recovered_ok is True

    rows = app.sqlite_store.conn.execute("""
        SELECT event_name, reason, latency_ms, details
        FROM diagnostics_events
        WHERE event_name IN ('market_stale_entered', 'market_stale_recovered')
        ORDER BY id ASC
        """).fetchall()
    assert len(rows) == 2
    assert str(rows[0][0]) == "market_stale_entered"
    assert str(rows[0][1]) == "leg_timestamp_mismatch"
    assert "stale_side=yes" in str(rows[0][3] or "")
    assert str(rows[1][0]) == "market_stale_recovered"
    assert str(rows[1][1]) == "leg_timestamp_mismatch"
    assert float(rows[1][2] or 0.0) == pytest.approx(3000.0, abs=1.0)

    asyncio.run(app.shutdown())


def test_market_stale_reason_change_splits_episode(tmp_path: Path) -> None:
    logger = logging.getLogger("test_market_stale_reason_change_splits_episode")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        runtime={
            "market_refresh_minutes": 1,
            "market_probation_ms": 0,
            "market_no_signal_reason_cooldown_ms": 0,
        },
        strategy={
            "max_quote_age_ms_for_signal": 100,
        },
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    app = PolymarketStructureArbApp(config=config, logger=logger)
    app.tick_size_client = FakeTickSizeClient()
    app.gamma_client = FakeGammaClient(responses=[[make_raw_market("m1", "yes1", "no1")]])

    asyncio.run(app.load_markets())
    market = app.markets_by_id["m1"]
    base_now = datetime.now(tz=UTC)

    yes_payload = json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": "yes1",
            "ask": "0.47",
            "bid": "0.46",
            "ask_size": "100",
            "bid_size": "100",
        }
    )
    no_payload = json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": "no1",
            "ask": "0.53",
            "bid": "0.52",
            "ask_size": "100",
            "bid_size": "100",
        }
    )
    asyncio.run(app.handle_ws_message(yes_payload))
    asyncio.run(app.handle_ws_message(no_payload))

    ready_ok, _, _ = app._evaluate_market_readiness(market=market, now_utc=base_now)
    assert ready_ok is True

    yes_quote, no_quote = app.quote_manager.get_market_quotes("m1")
    assert yes_quote is not None
    assert no_quote is not None
    yes_quote.timestamp = base_now - timedelta(seconds=10)
    no_quote.timestamp = base_now - timedelta(seconds=2)

    stale_started_at = base_now + timedelta(seconds=1)
    stale_ok, stale_reason, stale_details = app._evaluate_market_readiness(
        market=market,
        now_utc=stale_started_at,
    )
    assert stale_ok is False
    assert stale_reason == "market_quote_stale_quote_age"
    assert stale_details["stale_reason_key"] == "leg_timestamp_mismatch"

    stale_reason_switched_at = stale_started_at + timedelta(seconds=2)
    app.state.stale_assets.add("yes1")
    switched_ok, switched_reason, switched_details = app._evaluate_market_readiness(
        market=market,
        now_utc=stale_reason_switched_at,
    )
    assert switched_ok is False
    assert switched_reason == "market_quote_stale_no_recent_quote"
    assert switched_details["stale_reason_key"] == "no_recent_quote"

    stale_recovered_at = stale_reason_switched_at + timedelta(seconds=3)
    app.state.stale_assets.clear()
    yes_quote.timestamp = stale_recovered_at
    no_quote.timestamp = stale_recovered_at
    recovered_ok, _, _ = app._evaluate_market_readiness(
        market=market,
        now_utc=stale_recovered_at,
    )
    assert recovered_ok is True

    rows = app.sqlite_store.conn.execute("""
        SELECT event_name, reason, latency_ms, details
        FROM diagnostics_events
        WHERE event_name IN (
            'market_stale_entered',
            'market_stale_episode_closed',
            'market_stale_recovered'
        )
        ORDER BY id ASC
        """).fetchall()
    assert len(rows) == 4
    assert str(rows[0][0]) == "market_stale_entered"
    assert str(rows[0][1]) == "leg_timestamp_mismatch"
    assert str(rows[1][0]) == "market_stale_episode_closed"
    assert str(rows[1][1]) == "leg_timestamp_mismatch"
    assert float(rows[1][2] or 0.0) == pytest.approx(2000.0, abs=1.0)
    assert "episode_closed_reason=reason_changed" in str(rows[1][3] or "")
    assert "next_state=stale_no_recent_quote" in str(rows[1][3] or "")
    assert str(rows[2][0]) == "market_stale_entered"
    assert str(rows[2][1]) == "no_recent_quote"
    assert str(rows[3][0]) == "market_stale_recovered"
    assert str(rows[3][1]) == "no_recent_quote"
    assert float(rows[3][2] or 0.0) == pytest.approx(3000.0, abs=1.0)

    asyncio.run(app.shutdown())
