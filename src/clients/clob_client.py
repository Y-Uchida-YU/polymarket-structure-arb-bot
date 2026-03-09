from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ClobClient:
    """
    Paper-trading placeholder for future CLOB integration.

    This class is intentionally non-production:
    - No L1/L2 authentication
    - No POLY_* header handling
    - No real order placement/cancel flow
    """

    base_url: str = "https://clob.polymarket.com"

    async def healthcheck(self) -> bool:
        # Placeholder response only. Does not check authenticated CLOB health.
        return True
