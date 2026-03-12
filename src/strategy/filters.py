from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from src.config.loader import MarketFilterSettings, MarketsConfig
from src.domain.market import BinaryMarket
from src.utils.math_ext import safe_float


@dataclass(slots=True)
class MarketExtractionResult:
    markets: list[BinaryMarket]
    excluded_counts: dict[str, int]
    raw_market_count: int


def parse_float_field(value: object) -> float | None:
    return safe_float(value)


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


def parse_recent_trade_time(raw: dict[str, Any]) -> datetime | None:
    for key in (
        "lastTradeTime",
        "last_trade_time",
        "lastTradeTimestamp",
        "last_trade_timestamp",
        "lastTrade",
    ):
        parsed = parse_end_time(raw.get(key))
        if parsed is not None:
            return parsed
    return None


def parse_volume_proxy(raw: dict[str, Any]) -> float:
    for key in ("volume24hr", "volume24h", "volume", "volumeNum"):
        parsed = parse_float_field(raw.get(key))
        if parsed is not None:
            return max(0.0, parsed)
    return 0.0


def parse_liquidity_proxy(raw: dict[str, Any]) -> float:
    for key in ("liquidity", "liquidityNum"):
        parsed = parse_float_field(raw.get(key))
        if parsed is not None:
            return max(0.0, parsed)
    return 0.0


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


def _exclude_reason(
    *,
    raw: dict[str, Any],
    market_filters: MarketFilterSettings,
    markets_config: MarketsConfig,
    now_utc: datetime,
) -> str | None:
    if not is_open_market(raw):
        return "not_open_market"
    if market_filters.require_orderbook_enabled and not is_order_book_enabled(raw):
        return "orderbook_disabled"
    if not is_binary_yes_no_market(raw):
        return "not_binary_yes_no"
    if is_excluded_by_filters(
        raw,
        market_filters=market_filters,
        markets_config=markets_config,
    ):
        return "market_filtered_out"

    end_time = parse_end_time(raw.get("endDate"))
    if market_filters.min_days_to_expiry is not None:
        if end_time is None:
            return "missing_end_time"
        days_to_expiry = (end_time - now_utc).total_seconds() / 86_400.0
        if days_to_expiry < float(market_filters.min_days_to_expiry):
            return "expiry_too_soon"
    if market_filters.max_days_to_expiry is not None:
        if end_time is None:
            return "missing_end_time"
        days_to_expiry = (end_time - now_utc).total_seconds() / 86_400.0
        if days_to_expiry > float(market_filters.max_days_to_expiry):
            return "expiry_too_far"

    volume_proxy = parse_volume_proxy(raw)
    liquidity_proxy = parse_liquidity_proxy(raw)
    recent_activity = max(volume_proxy, liquidity_proxy)
    if market_filters.min_recent_activity is not None and recent_activity < float(
        market_filters.min_recent_activity
    ):
        return "recent_activity_too_low"
    if market_filters.min_liquidity_proxy is not None and liquidity_proxy < float(
        market_filters.min_liquidity_proxy
    ):
        return "liquidity_too_low"
    if market_filters.min_volume_24h_proxy is not None and volume_proxy < float(
        market_filters.min_volume_24h_proxy
    ):
        return "volume_too_low"

    if market_filters.require_recent_trade_within_minutes is not None:
        last_trade = parse_recent_trade_time(raw)
        if last_trade is None:
            return "missing_recent_trade"
        age_minutes = max(0.0, (now_utc - last_trade).total_seconds() / 60.0)
        if age_minutes > float(market_filters.require_recent_trade_within_minutes):
            return "recent_trade_too_old"

    token_pair = _extract_yes_no_token_ids(raw)
    if token_pair is None:
        return "invalid_yes_no_mapping"
    return None


def _market_score(raw: dict[str, Any]) -> float:
    return parse_liquidity_proxy(raw) + parse_volume_proxy(raw)


def extract_binary_markets_with_stats(
    raw_markets: list[dict[str, Any]],
    market_filters: MarketFilterSettings,
    markets_config: MarketsConfig,
    preferred_market_ids: set[str] | None = None,
    now_utc: datetime | None = None,
) -> MarketExtractionResult:
    reference_now = now_utc or datetime.now(tz=UTC)
    candidates: list[BinaryMarket] = []
    excluded_counts: dict[str, int] = {}

    for raw in raw_markets:
        reason = _exclude_reason(
            raw=raw,
            market_filters=market_filters,
            markets_config=markets_config,
            now_utc=reference_now,
        )
        if reason is not None:
            excluded_counts[reason] = excluded_counts.get(reason, 0) + 1
            continue

        token_pair = _extract_yes_no_token_ids(raw)
        if token_pair is None:
            excluded_counts["invalid_yes_no_mapping"] = (
                excluded_counts.get("invalid_yes_no_mapping", 0) + 1
            )
            continue
        yes_token_id, no_token_id = token_pair

        candidates.append(
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

    max_markets = market_filters.max_markets_to_watch
    if max_markets is not None and max_markets > 0 and len(candidates) > max_markets:
        sorted_candidates = sorted(
            candidates,
            key=lambda market: (_market_score(market.raw), market.slug),
            reverse=True,
        )
        kept = sorted_candidates[:max_markets]
        if market_filters.prefer_existing_watched_markets and preferred_market_ids:
            kept = _stabilize_market_universe_selection(
                sorted_candidates=sorted_candidates,
                max_markets=max_markets,
                preferred_market_ids=preferred_market_ids,
                hysteresis_ratio=market_filters.existing_market_hysteresis_score_ratio,
                max_replacements=market_filters.max_market_replacements_per_refresh,
            )
        excluded_counts["max_markets_to_watch"] = excluded_counts.get("max_markets_to_watch", 0) + (
            len(candidates) - len(kept)
        )
        candidates = kept

    return MarketExtractionResult(
        markets=candidates,
        excluded_counts=excluded_counts,
        raw_market_count=len(raw_markets),
    )


def extract_binary_markets(
    raw_markets: list[dict[str, Any]],
    market_filters: MarketFilterSettings,
    markets_config: MarketsConfig,
    preferred_market_ids: set[str] | None = None,
) -> list[BinaryMarket]:
    return extract_binary_markets_with_stats(
        raw_markets=raw_markets,
        market_filters=market_filters,
        markets_config=markets_config,
        preferred_market_ids=preferred_market_ids,
    ).markets


def _stabilize_market_universe_selection(
    *,
    sorted_candidates: list[BinaryMarket],
    max_markets: int,
    preferred_market_ids: set[str],
    hysteresis_ratio: float,
    max_replacements: int,
) -> list[BinaryMarket]:
    if max_markets <= 0:
        return []
    if not sorted_candidates:
        return []

    clamped_ratio = min(max(hysteresis_ratio, 0.0), 1.0)
    replacement_budget = max(0, min(max_replacements, max_markets))
    min_existing_slots = max(0, max_markets - replacement_budget)
    score_by_market_id = {
        market.market_id: _market_score(market.raw) for market in sorted_candidates
    }

    selected: list[BinaryMarket] = []
    selected_ids: set[str] = set()
    existing_candidates = [
        market for market in sorted_candidates if market.market_id in preferred_market_ids
    ]
    for market in existing_candidates[:min_existing_slots]:
        selected.append(market)
        selected_ids.add(market.market_id)

    for market in sorted_candidates:
        if len(selected) >= max_markets:
            break
        if market.market_id in selected_ids:
            continue
        selected.append(market)
        selected_ids.add(market.market_id)

    if len(selected) < max_markets:
        return selected

    existing_not_selected = [
        market for market in existing_candidates if market.market_id not in selected_ids
    ]
    newcomers = [market for market in selected if market.market_id not in preferred_market_ids]

    while existing_not_selected and newcomers:
        strongest_existing = existing_not_selected[0]
        weakest_newcomer = min(
            newcomers,
            key=lambda market: (score_by_market_id.get(market.market_id, 0.0), market.slug),
        )
        strongest_score = score_by_market_id.get(strongest_existing.market_id, 0.0)
        weakest_new_score = score_by_market_id.get(weakest_newcomer.market_id, 0.0)
        if strongest_score < weakest_new_score * clamped_ratio:
            break
        selected = [
            strongest_existing if item.market_id == weakest_newcomer.market_id else item
            for item in selected
        ]
        selected_ids.add(strongest_existing.market_id)
        selected_ids.discard(weakest_newcomer.market_id)
        existing_not_selected = [
            market
            for market in existing_not_selected
            if market.market_id != strongest_existing.market_id
        ]
        newcomers = [market for market in selected if market.market_id not in preferred_market_ids]

    return sorted(
        selected,
        key=lambda market: (score_by_market_id.get(market.market_id, 0.0), market.slug),
        reverse=True,
    )
