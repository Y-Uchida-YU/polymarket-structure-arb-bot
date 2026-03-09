from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable


async def run_periodic(
    task: Callable[[], Awaitable[None]],
    interval_seconds: float,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        await task()
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue
