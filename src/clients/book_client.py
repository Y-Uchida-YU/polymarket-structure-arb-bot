from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx

from src.domain.book import BookSummary
from src.utils.math_ext import safe_float


class BookClient:
    """
    Thin client for best bid/ask resync.

    Default endpoint uses /book with token-id style query and parses defensively.
    """

    def __init__(
        self,
        base_url: str = "https://clob.polymarket.com",
        timeout_seconds: float = 10.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    async def fetch_book_summary(self, asset_id: str) -> BookSummary | None:
        async with httpx.AsyncClient(
            base_url=self.base_url, timeout=self.timeout_seconds
        ) as client:
            response = await client.get("/book", params={"token_id": asset_id})
            response.raise_for_status()
            payload = response.json()
        return self._parse_book_summary(asset_id=asset_id, payload=payload)

    @staticmethod
    def _parse_book_summary(asset_id: str, payload: Any) -> BookSummary | None:
        if not isinstance(payload, dict):
            return None

        best_ask = BookClient._first_float(
            payload.get("best_ask"),
            payload.get("bestAsk"),
            payload.get("ask"),
            payload.get("a"),
        )
        best_bid = BookClient._first_float(
            payload.get("best_bid"),
            payload.get("bestBid"),
            payload.get("bid"),
            payload.get("b"),
        )
        best_ask_size = BookClient._first_float(
            payload.get("best_ask_size"),
            payload.get("bestAskSize"),
            payload.get("ask_size"),
            payload.get("as"),
        )
        best_bid_size = BookClient._first_float(
            payload.get("best_bid_size"),
            payload.get("bestBidSize"),
            payload.get("bid_size"),
            payload.get("bs"),
        )
        if best_ask is None and best_bid is None:
            return None
        return BookSummary(
            asset_id=asset_id,
            best_bid=best_bid,
            best_ask=best_ask,
            best_bid_size=best_bid_size,
            best_ask_size=best_ask_size,
            timestamp=BookClient._parse_timestamp(payload.get("timestamp")),
            source="book_resync",
        )

    @staticmethod
    def _first_float(*values: object) -> float | None:
        for value in values:
            converted = safe_float(value)
            if converted is not None:
                return converted
        return None

    @staticmethod
    def _parse_timestamp(value: object) -> datetime:
        if isinstance(value, str) and value:
            candidate = value.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(candidate)
            except ValueError:
                return datetime.now(tz=UTC)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        return datetime.now(tz=UTC)
