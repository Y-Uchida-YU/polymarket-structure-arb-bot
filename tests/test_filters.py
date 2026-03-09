from __future__ import annotations

from src.config.loader import MarketFilterSettings, MarketsConfig
from src.strategy.filters import extract_binary_markets


def make_raw_market(
    market_id: str,
    outcomes: str = '["Yes", "No"]',
    clob_token_ids: str = '["yes-token", "no-token"]',
    enable_order_book: bool | None = True,
) -> dict[str, object]:
    market: dict[str, object] = {
        "id": market_id,
        "question": "Will sample happen?",
        "slug": f"sample-{market_id}",
        "category": "crypto",
        "active": True,
        "closed": False,
        "archived": False,
        "outcomes": outcomes,
        "clobTokenIds": clob_token_ids,
        "endDate": "2030-01-01T00:00:00Z",
        "conditionId": f"cond-{market_id}",
    }
    if enable_order_book is not None:
        market["enableOrderBook"] = enable_order_book
    return market


def test_extract_binary_markets_excludes_enable_order_book_false() -> None:
    markets = [
        make_raw_market("1", enable_order_book=False),
    ]
    result = extract_binary_markets(
        raw_markets=markets,
        market_filters=MarketFilterSettings(),
        markets_config=MarketsConfig(),
    )
    assert result == []


def test_extract_binary_markets_excludes_when_enable_order_book_missing() -> None:
    markets = [
        make_raw_market("1", enable_order_book=None),
    ]
    result = extract_binary_markets(
        raw_markets=markets,
        market_filters=MarketFilterSettings(),
        markets_config=MarketsConfig(),
    )
    assert result == []


def test_extract_binary_markets_maps_yes_no_with_whitespace_and_case() -> None:
    markets = [
        make_raw_market(
            "1",
            outcomes='[" YES ", " no"]',
            clob_token_ids='["token-yes", "token-no"]',
            enable_order_book=True,
        ),
    ]
    result = extract_binary_markets(
        raw_markets=markets,
        market_filters=MarketFilterSettings(),
        markets_config=MarketsConfig(),
    )
    assert len(result) == 1
    assert result[0].yes_token_id == "token-yes"
    assert result[0].no_token_id == "token-no"


def test_extract_binary_markets_maps_yes_no_when_outcomes_are_reversed() -> None:
    markets = [
        make_raw_market(
            "1",
            outcomes='[" No ", " YES "]',
            clob_token_ids='["token-no", "token-yes"]',
            enable_order_book=True,
        ),
    ]
    result = extract_binary_markets(
        raw_markets=markets,
        market_filters=MarketFilterSettings(),
        markets_config=MarketsConfig(),
    )
    assert len(result) == 1
    assert result[0].yes_token_id == "token-yes"
    assert result[0].no_token_id == "token-no"


def test_extract_binary_markets_excludes_non_yes_no_outcomes() -> None:
    markets = [
        make_raw_market(
            "1",
            outcomes='["Up", "Down"]',
            clob_token_ids='["token-up", "token-down"]',
            enable_order_book=True,
        ),
    ]
    result = extract_binary_markets(
        raw_markets=markets,
        market_filters=MarketFilterSettings(),
        markets_config=MarketsConfig(),
    )
    assert result == []
