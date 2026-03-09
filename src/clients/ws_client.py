from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed


class MarketWebSocketClient:
    def __init__(
        self,
        url: str,
        asset_ids: list[str],
        on_message: Callable[[str], Awaitable[None]],
        logger: logging.Logger,
        reconnect_base_seconds: float = 2.0,
        reconnect_max_seconds: float = 30.0,
        ping_interval_seconds: int = 20,
        ping_timeout_seconds: int = 20,
    ) -> None:
        self.url = url
        self.asset_ids = asset_ids
        self.on_message = on_message
        self.logger = logger
        self.reconnect_base_seconds = reconnect_base_seconds
        self.reconnect_max_seconds = reconnect_max_seconds
        self.ping_interval_seconds = ping_interval_seconds
        self.ping_timeout_seconds = ping_timeout_seconds

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        backoff = self.reconnect_base_seconds
        while not stop_event.is_set():
            try:
                await self._connect_and_stream(stop_event=stop_event)
                backoff = self.reconnect_base_seconds
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.logger.exception("WebSocket loop error: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.reconnect_max_seconds)

    async def _connect_and_stream(self, stop_event: asyncio.Event) -> None:
        if not self.asset_ids:
            self.logger.warning("No asset IDs to subscribe. Retrying.")
            await asyncio.sleep(self.reconnect_base_seconds)
            return

        async with websockets.connect(
            self.url,
            ping_interval=self.ping_interval_seconds,
            ping_timeout=self.ping_timeout_seconds,
            max_queue=4096,
        ) as ws:
            self.logger.info("WebSocket connected: %s", self.url)
            await self._subscribe(ws, self.asset_ids)

            while not stop_event.is_set():
                try:
                    raw_message = await asyncio.wait_for(
                        ws.recv(),
                        timeout=self.ping_interval_seconds + self.ping_timeout_seconds + 10,
                    )
                except TimeoutError:
                    self.logger.warning("WebSocket receive timeout. Reconnecting.")
                    return
                except ConnectionClosed:
                    self.logger.warning("WebSocket closed by remote. Reconnecting.")
                    return

                if isinstance(raw_message, bytes):
                    raw_message = raw_message.decode("utf-8", errors="replace")
                await self.on_message(str(raw_message))

    async def _subscribe(self, ws: Any, asset_ids: list[str]) -> None:
        # As documented in market channel examples, subscription key is `assets_ids`.
        for chunk in self._chunk(asset_ids, size=500):
            payload = {
                "type": "market",
                "assets_ids": chunk,
            }
            await ws.send(json.dumps(payload))
            self.logger.info("Subscribed market channel chunk size=%s", len(chunk))

    @staticmethod
    def _chunk(items: Iterable[str], size: int) -> list[list[str]]:
        chunk: list[str] = []
        result: list[list[str]] = []
        for item in items:
            chunk.append(item)
            if len(chunk) == size:
                result.append(chunk)
                chunk = []
        if chunk:
            result.append(chunk)
        return result
