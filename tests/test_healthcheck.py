from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.app.bootstrap import PolymarketStructureArbApp
from src.config.loader import AppConfig, MarketsConfig, Settings
from src.domain.market import BinaryMarket
from src.monitoring.healthcheck import HealthCheck


def test_healthcheck_marks_stale_assets() -> None:
    now = datetime(2026, 3, 1, tzinfo=UTC)
    health = HealthCheck(started_at=now - timedelta(minutes=5))
    health.on_asset_quote_update("asset-fresh", now - timedelta(milliseconds=100))
    health.on_asset_quote_update("asset-old", now - timedelta(seconds=5))

    stale = health.stale_assets(
        tracked_assets=["asset-fresh", "asset-old", "asset-missing"],
        now_utc=now,
        max_age_ms=1000,
    )

    assert "asset-old" in stale
    assert "asset-missing" in stale
    assert "asset-fresh" not in stale
    assert health.state.stale_asset_count == 2


def test_stale_asset_set_blocks_signal_target(tmp_path: Path) -> None:
    logger = logging.getLogger("test_healthcheck_app")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    config = AppConfig(
        root_dir=tmp_path,
        settings=Settings(
            storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
        ),
        markets=MarketsConfig(),
    )
    app = PolymarketStructureArbApp(config=config, logger=logger)
    market = BinaryMarket(
        market_id="m1",
        question="Will test happen?",
        slug="test-market",
        category="crypto",
        end_time=datetime(2030, 1, 1, tzinfo=UTC),
        condition_id="c1",
        yes_token_id="yes1",
        no_token_id="no1",
        raw={},
    )
    app.state.stale_assets = {"yes1"}
    assert app._market_has_stale_quotes(market=market, now_utc=datetime(2026, 3, 1, tzinfo=UTC))

    asyncio.run(app.shutdown())
