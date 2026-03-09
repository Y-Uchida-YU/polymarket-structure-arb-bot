from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from src.utils.math_ext import safe_float


@dataclass(slots=True)
class BestBidAskUpdate:
    asset_id: str
    best_bid: float | None
    best_ask: float | None
    best_bid_size: float | None
    best_ask_size: float | None
    event_type: str
    timestamp: datetime


class QuoteManager:
    def __init__(self, token_to_market_side: dict[str, tuple[str, str]]) -> None:
        # token_to_market_side[token_id] = (market_id, "yes"|"no")
        self.token_to_market_side = dict(token_to_market_side)
        self.best_quotes_by_asset: dict[str, BestBidAskUpdate] = {}
        self.market_quotes: dict[str, dict[str, BestBidAskUpdate | None]] = {}

    def update_token_mapping(self, token_to_market_side: dict[str, tuple[str, str]]) -> None:
        self.token_to_market_side = dict(token_to_market_side)
        valid_assets = set(token_to_market_side.keys())
        self.best_quotes_by_asset = {
            asset_id: quote
            for asset_id, quote in self.best_quotes_by_asset.items()
            if asset_id in valid_assets
        }
        valid_markets = {market_id for market_id, _ in token_to_market_side.values()}
        self.market_quotes = {
            market_id: state
            for market_id, state in self.market_quotes.items()
            if market_id in valid_markets
        }

    def ingest_ws_message(self, raw_message: str) -> list[BestBidAskUpdate]:
        payload = self._parse_payload(raw_message)
        if payload is None:
            return []

        events: list[dict[str, Any]]
        if isinstance(payload, list):
            events = [item for item in payload if isinstance(item, dict)]
        elif isinstance(payload, dict):
            events = [payload]
        else:
            return []

        updates: list[BestBidAskUpdate] = []
        for event in events:
            update = self._extract_best_bid_ask(event)
            if update is None:
                continue
            updates.append(update)
            self.best_quotes_by_asset[update.asset_id] = update
            self._update_market_ask(update)

        return updates

    def get_market_asks(self, market_id: str) -> tuple[float | None, float | None]:
        yes_quote, no_quote = self.get_market_quotes(market_id)
        ask_yes = yes_quote.best_ask if yes_quote is not None else None
        ask_no = no_quote.best_ask if no_quote is not None else None
        return ask_yes, ask_no

    def get_market_quotes(
        self,
        market_id: str,
    ) -> tuple[BestBidAskUpdate | None, BestBidAskUpdate | None]:
        state = self.market_quotes.get(market_id, {"yes": None, "no": None})
        return state.get("yes"), state.get("no")

    def _update_market_ask(self, update: BestBidAskUpdate) -> None:
        mapping = self.token_to_market_side.get(update.asset_id)
        if mapping is None:
            return

        market_id, side = mapping
        market_state = self.market_quotes.setdefault(market_id, {"yes": None, "no": None})
        market_state[side] = update

    @staticmethod
    def _parse_payload(raw_message: str) -> Any:
        try:
            return json.loads(raw_message)
        except json.JSONDecodeError:
            return None

    @staticmethod
    def _extract_best_bid_ask(event: dict[str, Any]) -> BestBidAskUpdate | None:
        event_type = str(event.get("event_type") or event.get("eventType") or "").strip().lower()
        if event_type and event_type != "best_bid_ask":
            return None

        asset_id = str(event.get("asset_id") or event.get("assetId") or "").strip()
        if not asset_id:
            return None

        best_ask = QuoteManager._first_float(
            event.get("ask"),
            event.get("best_ask"),
            event.get("bestAsk"),
            event.get("a"),
        )
        best_bid = QuoteManager._first_float(
            event.get("bid"),
            event.get("best_bid"),
            event.get("bestBid"),
            event.get("b"),
        )
        best_ask_size = QuoteManager._first_float(
            event.get("ask_size"),
            event.get("best_ask_size"),
            event.get("bestAskSize"),
            event.get("as"),
        )
        best_bid_size = QuoteManager._first_float(
            event.get("bid_size"),
            event.get("best_bid_size"),
            event.get("bestBidSize"),
            event.get("bs"),
        )

        if best_ask is None and best_bid is None:
            return None

        timestamp = QuoteManager._parse_timestamp(event.get("timestamp"))
        return BestBidAskUpdate(
            asset_id=asset_id,
            best_bid=best_bid,
            best_ask=best_ask,
            best_bid_size=best_bid_size,
            best_ask_size=best_ask_size,
            event_type=event_type or "best_bid_ask",
            timestamp=timestamp,
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
