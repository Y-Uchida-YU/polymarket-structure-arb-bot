from __future__ import annotations

from typing import Any

import httpx


class GammaClient:
    """Thin wrapper around Polymarket Gamma API."""

    def __init__(
        self,
        base_url: str,
        markets_endpoint: str = "/markets",
        timeout_seconds: float = 20.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.markets_endpoint = markets_endpoint
        self.timeout_seconds = timeout_seconds

    async def fetch_active_markets(
        self,
        page_size: int = 200,
        max_pages: int = 10,
    ) -> list[dict[str, Any]]:
        markets: list[dict[str, Any]] = []
        offset = 0

        async with httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout_seconds) as client:
            for _ in range(max_pages):
                params = {
                    "active": "true",
                    "closed": "false",
                    "order": "id",
                    "ascending": "false",
                    "limit": page_size,
                    "offset": offset,
                }
                response = await client.get(self.markets_endpoint, params=params)
                response.raise_for_status()

                payload = response.json()
                if not isinstance(payload, list):
                    raise ValueError("Gamma /markets response is not a list")

                page = [item for item in payload if isinstance(item, dict)]
                markets.extend(page)

                if len(payload) < page_size:
                    break
                offset += page_size

        return markets
