from __future__ import annotations

from collections.abc import Mapping

STALE_REASON_NO_RECENT_QUOTE = "no_recent_quote"
STALE_REASON_QUOTE_AGE_EXCEEDED = "quote_age_exceeded"
STALE_REASON_LEG_TIMESTAMP_MISMATCH = "leg_timestamp_mismatch"
STALE_REASON_MISSING_LEG_QUOTE = "missing_leg_quote"
STALE_REASON_MARKET_RECOVERING_OVERLAP = "market_recovering_overlap"
STALE_REASON_PROBATION_OVERLAP = "probation_overlap"
STALE_REASON_UNKNOWN = "unknown"

STALE_NO_SIGNAL_REASON_NO_RECENT_QUOTE = "market_quote_stale_no_recent_quote"
STALE_NO_SIGNAL_REASON_QUOTE_AGE = "market_quote_stale_quote_age"

STALE_SIDE_YES = "yes"
STALE_SIDE_NO = "no"
STALE_SIDE_BOTH = "both"
STALE_SIDE_UNKNOWN = "unknown"

_STALE_REASON_KEY_SET = {
    STALE_REASON_NO_RECENT_QUOTE,
    STALE_REASON_QUOTE_AGE_EXCEEDED,
    STALE_REASON_LEG_TIMESTAMP_MISMATCH,
    STALE_REASON_MISSING_LEG_QUOTE,
    STALE_REASON_MARKET_RECOVERING_OVERLAP,
    STALE_REASON_PROBATION_OVERLAP,
}

_STALE_REASON_ALIAS_MAP = {
    "quote_age": STALE_REASON_QUOTE_AGE_EXCEEDED,
    "quote_age_exceed": STALE_REASON_QUOTE_AGE_EXCEEDED,
    "stale_quote_age": STALE_REASON_QUOTE_AGE_EXCEEDED,
    "stale_no_recent_quote": STALE_REASON_NO_RECENT_QUOTE,
}

_NO_SIGNAL_REASON_TO_STALE_REASON = {
    STALE_NO_SIGNAL_REASON_NO_RECENT_QUOTE: STALE_REASON_NO_RECENT_QUOTE,
    STALE_NO_SIGNAL_REASON_QUOTE_AGE: STALE_REASON_QUOTE_AGE_EXCEEDED,
}


def normalize_stale_reason_key(reason: str | None) -> str:
    key = (reason or "").strip()
    if not key:
        return STALE_REASON_UNKNOWN
    if key in _STALE_REASON_KEY_SET:
        return key
    alias = _STALE_REASON_ALIAS_MAP.get(key)
    if alias is not None:
        return alias
    mapped = _NO_SIGNAL_REASON_TO_STALE_REASON.get(key)
    if mapped is not None:
        return mapped
    return STALE_REASON_UNKNOWN


def normalize_stale_side(side: str | None) -> str:
    key = (side or "").strip().lower()
    if key in {STALE_SIDE_YES, STALE_SIDE_NO, STALE_SIDE_BOTH}:
        return key
    return STALE_SIDE_UNKNOWN


def stale_reason_key_from_reason_and_details(*, reason: str | None, details: str | None) -> str:
    detail_map = parse_kv_details(details or "")
    if "stale_reason_key" in detail_map:
        return normalize_stale_reason_key(detail_map.get("stale_reason_key"))
    return normalize_stale_reason_key(reason)


def parse_kv_details(details: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for token in details.split(";"):
        candidate = token.strip()
        if not candidate or "=" not in candidate:
            continue
        key, value = candidate.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            continue
        parsed[key] = value
    return parsed


def extract_detail_value(details: str, key: str) -> str | None:
    return parse_kv_details(details).get(key)


def format_kv_details(values: Mapping[str, object | None]) -> str:
    pairs: list[str] = []
    for key, value in values.items():
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        pairs.append(f"{key}={text}")
    return ";".join(pairs)
