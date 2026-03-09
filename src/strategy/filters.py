from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from src.config.loader import MarketFilterSettings, MarketsConfig
from src.domain.market import BinaryMarket


def parse_bool_field(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n", ""}:
            return False
    return False


def parse_json_array_field(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            decoded = json.loads(stripped)
        except json.JSONDecodeError:
            return []
        if isinstance(decoded, list):
            return [str(item) for item in decoded]
    return []


def normalize_outcome_label(value: str) -> str:
    return value.strip().lower()


def is_order_book_enabled(raw: dict[str, Any]) -> bool:
    """
    Safe-by-default:
    If `enableOrderBook` is missing, treat the market as not eligible.
    """
    if "enableOrderBook" not in raw:
        return False
    return parse_bool_field(raw.get("enableOrderBook"))


def _parse_yes_no_indices(outcomes: list[str]) -> tuple[int, int] | None:
    normalized = [normalize_outcome_label(item) for item in outcomes]
    if len(normalized) != 2:
        return None
    yes_indexes = [index for index, outcome in enumerate(normalized) if outcome == "yes"]
    no_indexes = [index for index, outcome in enumerate(normalized) if outcome == "no"]
    if len(yes_indexes) != 1 or len(no_indexes) != 1:
        return None
    return yes_indexes[0], no_indexes[0]


def parse_end_time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    candidate = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def is_binary_yes_no_market(raw: dict[str, Any]) -> bool:
    outcomes = parse_json_array_field(raw.get("outcomes"))
    token_ids = parse_json_array_field(raw.get("clobTokenIds"))
    return _parse_yes_no_indices(outcomes) is not None and len(token_ids) == 2


def _extract_yes_no_token_ids(raw: dict[str, Any]) -> tuple[str, str] | None:
    outcomes = parse_json_array_field(raw.get("outcomes"))
    token_ids = parse_json_array_field(raw.get("clobTokenIds"))
    yes_no_indices = _parse_yes_no_indices(outcomes)
    if yes_no_indices is None or len(token_ids) != 2:
        return None
    # Assumption from Gamma market data:
    # clobTokenIds index order corresponds to outcomes index order.
    yes_index, no_index = yes_no_indices
    return token_ids[yes_index], token_ids[no_index]


def is_open_market(raw: dict[str, Any]) -> bool:
    active = parse_bool_field(raw.get("active", False))
    closed = parse_bool_field(raw.get("closed", False))
    archived = parse_bool_field(raw.get("archived", False))
    return active and not closed and not archived


def is_excluded_by_filters(
    raw: dict[str, Any],
    market_filters: MarketFilterSettings,
    markets_config: MarketsConfig,
) -> bool:
    slug = str(raw.get("slug", "")).strip().lower()
    category = str(raw.get("category", "")).strip().lower()
    text = " ".join(
        [
            str(raw.get("question", "")),
            str(raw.get("description", "")),
            category,
            slug,
        ]
    ).lower()

    include_slugs = {item.strip().lower() for item in markets_config.include_slugs if item.strip()}
    exclude_slugs = {item.strip().lower() for item in markets_config.exclude_slugs if item.strip()}

    if include_slugs and slug not in include_slugs:
        return True
    if slug and slug in exclude_slugs:
        return True

    excluded_categories = {
        item.strip().lower()
        for item in market_filters.exclude_categories + markets_config.exclude_categories
        if item.strip()
    }
    if category and any(ex_cat in category for ex_cat in excluded_categories):
        return True

    excluded_keywords = {
        item.strip().lower()
        for item in market_filters.exclude_keywords + markets_config.exclude_keywords
        if item.strip()
    }
    return any(keyword in text for keyword in excluded_keywords)


def is_within_expiry_block(
    end_time: datetime | None,
    now_utc: datetime,
    expiry_block_minutes: int,
) -> bool:
    if end_time is None:
        return False
    return end_time - now_utc <= timedelta(minutes=expiry_block_minutes)


def extract_binary_markets(
    raw_markets: list[dict[str, Any]],
    market_filters: MarketFilterSettings,
    markets_config: MarketsConfig,
) -> list[BinaryMarket]:
    binary_markets: list[BinaryMarket] = []

    for raw in raw_markets:
        if not is_open_market(raw):
            continue
        if not is_order_book_enabled(raw):
            continue
        if not is_binary_yes_no_market(raw):
            continue
        if is_excluded_by_filters(
            raw,
            market_filters=market_filters,
            markets_config=markets_config,
        ):
            continue

        token_pair = _extract_yes_no_token_ids(raw)
        if token_pair is None:
            continue
        yes_token_id, no_token_id = token_pair

        binary_markets.append(
            BinaryMarket(
                market_id=str(raw.get("id", "")),
                question=str(raw.get("question", "")),
                slug=str(raw.get("slug", "")),
                category=str(raw.get("category", "")),
                end_time=parse_end_time(raw.get("endDate")),
                condition_id=str(raw.get("conditionId", "")) or None,
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
                raw=raw,
            )
        )
    return binary_markets
