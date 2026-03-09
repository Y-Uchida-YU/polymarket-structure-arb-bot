from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class GeoBlockClient:
    """
    Placeholder for geoblock checks.
    Wire a real provider when needed.
    """

    enabled: bool = False

    async def is_allowed(self) -> bool:
        return True
