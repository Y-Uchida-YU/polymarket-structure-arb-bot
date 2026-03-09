from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx

from src.domain.book import TickSizeUpdate
from src.utils.math_ext import safe_float


class TickSizeClient:
    """
    Thin client for reading token tick size.

    Endpoint compatibility:
    - Expected: /tick-size/{token_id}
    - Parsing is defensive to avoid hard-coding one payload shape.
    """

    def __init__(
        self,
        base_url: str = "https://clob.polymarket.com",
        timeout_seconds: float = 10.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    async def fetch_tick_size(self, token_id: str) -> TickSizeUpdate | None:
        endpoint = f"/tick-size/{token_id}"
        async with httpx.AsyncClient(
            base_url=self.base_url, timeout=self.timeout_seconds
        ) as client:
            response = await client.get(endpoint)
            response.raise_for_status()
            payload = response.json()

        tick_size = self._extract_tick_size(payload)
        if tick_size is None or tick_size <= 0:
            return None
        return TickSizeUpdate(
            asset_id=token_id,
            tick_size=tick_size,
            updated_at=datetime.now(tz=UTC),
            source="initial_api",
        )

    @staticmethod
    def _extract_tick_size(payload: Any) -> float | None:
        if isinstance(payload, dict):
            for key in ("minimum_tick_size", "min_tick_size", "tick_size", "tickSize"):
                value = safe_float(payload.get(key))
                if value is not None:
                    return value
        return safe_float(payload)
