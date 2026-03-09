from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class GeoBlockClient:
    """
    Paper-trading placeholder for geoblock checks.

    This class does not call any real geoblocking API.
    Production live trading must replace this with a real compliance check.
    """

    enabled: bool = False

    async def is_allowed(self) -> bool:
        # Placeholder response only.
        return True
