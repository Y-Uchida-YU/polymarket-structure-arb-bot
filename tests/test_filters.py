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


def test_extract_binary_markets_respects_max_markets_to_watch() -> None:
    markets = [
        {
            **make_raw_market("1", enable_order_book=True),
            "liquidity": "1000",
            "volume24hr": "10",
        },
        {
            **make_raw_market("2", enable_order_book=True),
            "liquidity": "3000",
            "volume24hr": "10",
        },
        {
            **make_raw_market("3", enable_order_book=True),
            "liquidity": "2000",
            "volume24hr": "10",
        },
    ]
    filters = MarketFilterSettings(
        max_markets_to_watch=2,
        min_recent_activity=0.0,
        min_liquidity_proxy=0.0,
        min_volume_24h_proxy=0.0,
        min_days_to_expiry=0.0,
        max_days_to_expiry=3650.0,
    )
    result = extract_binary_markets(
        raw_markets=markets,
        market_filters=filters,
        markets_config=MarketsConfig(),
    )
    assert len(result) == 2
    assert {market.market_id for market in result} == {"2", "3"}


def test_extract_binary_markets_applies_recent_activity_proxy_filter() -> None:
    markets = [
        {
            **make_raw_market("low", enable_order_book=True),
            "liquidity": "50",
            "volume24hr": "25",
        },
        {
            **make_raw_market("high", enable_order_book=True),
            "liquidity": "5000",
            "volume24hr": "3000",
        },
    ]
    filters = MarketFilterSettings(
        min_recent_activity=1000.0,
        min_liquidity_proxy=0.0,
        min_volume_24h_proxy=0.0,
        min_days_to_expiry=0.0,
        max_days_to_expiry=3650.0,
    )
    result = extract_binary_markets(
        raw_markets=markets,
        market_filters=filters,
        markets_config=MarketsConfig(),
    )
    assert len(result) == 1
    assert result[0].market_id == "high"


def test_extract_binary_markets_prefers_existing_universe_under_hysteresis() -> None:
    markets = [
        {**make_raw_market("1", enable_order_book=True), "liquidity": "100", "volume24hr": "0"},
        {**make_raw_market("2", enable_order_book=True), "liquidity": "99", "volume24hr": "0"},
        {**make_raw_market("3", enable_order_book=True), "liquidity": "110", "volume24hr": "0"},
        {**make_raw_market("4", enable_order_book=True), "liquidity": "109", "volume24hr": "0"},
    ]
    filters = MarketFilterSettings(
        max_markets_to_watch=2,
        min_recent_activity=0.0,
        min_liquidity_proxy=0.0,
        min_volume_24h_proxy=0.0,
        min_days_to_expiry=0.0,
        max_days_to_expiry=3650.0,
        prefer_existing_watched_markets=True,
        existing_market_hysteresis_score_ratio=0.9,
        max_market_replacements_per_refresh=1,
    )
    result = extract_binary_markets(
        raw_markets=markets,
        market_filters=filters,
        markets_config=MarketsConfig(),
        preferred_market_ids={"1", "2"},
    )
    assert len(result) == 2
    assert {market.market_id for market in result} == {"1", "2"}


def test_extract_binary_markets_allows_replacement_when_existing_is_much_weaker() -> None:
    markets = [
        {**make_raw_market("1", enable_order_book=True), "liquidity": "10", "volume24hr": "0"},
        {**make_raw_market("2", enable_order_book=True), "liquidity": "9", "volume24hr": "0"},
        {**make_raw_market("3", enable_order_book=True), "liquidity": "100", "volume24hr": "0"},
        {**make_raw_market("4", enable_order_book=True), "liquidity": "99", "volume24hr": "0"},
    ]
    filters = MarketFilterSettings(
        max_markets_to_watch=2,
        min_recent_activity=0.0,
        min_liquidity_proxy=0.0,
        min_volume_24h_proxy=0.0,
        min_days_to_expiry=0.0,
        max_days_to_expiry=3650.0,
        prefer_existing_watched_markets=True,
        existing_market_hysteresis_score_ratio=0.95,
        max_market_replacements_per_refresh=1,
    )
    result = extract_binary_markets(
        raw_markets=markets,
        market_filters=filters,
        markets_config=MarketsConfig(),
        preferred_market_ids={"1", "2"},
    )
    assert len(result) == 2
    assert {market.market_id for market in result} == {"1", "3"}
