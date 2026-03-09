from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from src.app.bootstrap import PolymarketStructureArbApp
from src.config.loader import AppConfig, MarketsConfig, Settings


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
