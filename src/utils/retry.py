from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


async def retry_async(
    action: Callable[[], Awaitable[T]],
    retries: int = 3,
    base_delay_seconds: float = 0.5,
) -> T:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            return await action()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == retries - 1:
                break
            await asyncio.sleep(base_delay_seconds * (2**attempt))
    if last_error is None:
        raise RuntimeError("retry_async failed without captured exception")
    raise last_error
