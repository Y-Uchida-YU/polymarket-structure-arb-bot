from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ClobClient:
    """
    Placeholder for future live trading integration.
    MVP scope uses paper orders only.
    """

    base_url: str = "https://clob.polymarket.com"

    async def healthcheck(self) -> bool:
        return True
