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
        get_asset_ids: Callable[[], list[str]] | None = None,
        on_connected: Callable[[list[str]], Awaitable[None]] | None = None,
        on_reconnect_required: Callable[[str], None] | None = None,
        on_transport_event: Callable[[str, str, int], None] | None = None,
        reconnect_base_seconds: float = 2.0,
        reconnect_max_seconds: float = 30.0,
        ping_interval_seconds: int = 20,
        ping_timeout_seconds: int = 20,
        receive_timeout_seconds: int = 120,
        receive_timeout_reconnect_count: int = 3,
    ) -> None:
        self.url = url
        self.asset_ids = asset_ids
        self.get_asset_ids = get_asset_ids
        self.on_message = on_message
        self.on_connected = on_connected
        self.on_reconnect_required = on_reconnect_required
        self.on_transport_event = on_transport_event
        self.logger = logger
        self.reconnect_base_seconds = reconnect_base_seconds
        self.reconnect_max_seconds = reconnect_max_seconds
        self.ping_interval_seconds = ping_interval_seconds
        self.ping_timeout_seconds = ping_timeout_seconds
        self.receive_timeout_seconds = max(1, int(receive_timeout_seconds))
        self.receive_timeout_reconnect_count = max(1, int(receive_timeout_reconnect_count))

    async def run_forever(
        self,
        stop_event: asyncio.Event,
        resubscribe_event: asyncio.Event | None = None,
    ) -> None:
        backoff = self.reconnect_base_seconds
        connect_reason = "initial_connect"
        while not stop_event.is_set():
            reconnect_reason: str | None = None
            try:
                reconnect_reason = await self._connect_and_stream(
                    stop_event=stop_event,
                    resubscribe_event=resubscribe_event,
                    connect_reason=connect_reason,
                )
                if reconnect_reason is None:
                    backoff = self.reconnect_base_seconds
                    connect_reason = "normal_loop"
                    continue
                self.logger.warning(
                    "WebSocket reconnect scheduled reason=%s backoff=%.1fs",
                    reconnect_reason,
                    backoff,
                )
                self._emit_transport_event(
                    event_name="reconnect_required",
                    reason=reconnect_reason,
                    asset_count=len(self._current_asset_ids()),
                )
                if self.on_reconnect_required is not None:
                    self.on_reconnect_required(reconnect_reason)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                reconnect_reason = self._map_unexpected_exception(exc)
                self.logger.exception("WebSocket loop error: %s", exc)
                self._emit_transport_event(
                    event_name="reconnect_required",
                    reason=reconnect_reason,
                    asset_count=len(self._current_asset_ids()),
                )
                if self.on_reconnect_required is not None:
                    self.on_reconnect_required(reconnect_reason)

            if stop_event.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self.reconnect_max_seconds)
            connect_reason = (
                f"reconnect_after:{reconnect_reason}"
                if reconnect_reason
                else "reconnect_after:unknown"
            )

    async def _connect_and_stream(
        self,
        stop_event: asyncio.Event,
        resubscribe_event: asyncio.Event | None,
        connect_reason: str,
    ) -> str | None:
        current_asset_ids = self._current_asset_ids()
        if not current_asset_ids:
            self.logger.warning("No asset IDs to subscribe. Retrying.")
            await asyncio.sleep(self.reconnect_base_seconds)
            return "no_assets_to_subscribe"

        try:
            async with websockets.connect(
                self.url,
                ping_interval=self.ping_interval_seconds,
                ping_timeout=self.ping_timeout_seconds,
                max_queue=4096,
            ) as ws:
                self.logger.info("WebSocket connected: %s", self.url)
                self._emit_transport_event(
                    event_name="connected",
                    reason=connect_reason,
                    asset_count=len(current_asset_ids),
                )

                try:
                    await self._subscribe(ws, current_asset_ids)
                except Exception:  # noqa: BLE001
                    self.logger.exception("WebSocket subscribe failed.")
                    return "subscribe_failure"
                if self.on_connected is not None:
                    await self.on_connected(current_asset_ids)

                consecutive_receive_timeouts = 0
                while not stop_event.is_set():
                    if resubscribe_event is not None and resubscribe_event.is_set():
                        resubscribe_event.clear()
                        next_asset_ids = self._current_asset_ids()
                        if self.should_reconnect_for_resubscribe(
                            current_asset_ids=current_asset_ids,
                            next_asset_ids=next_asset_ids,
                        ):
                            self.logger.info("Resubscribe requested, reconnecting websocket.")
                            return "asset_universe_changed"
                        self.logger.info(
                            "Resubscribe requested but asset set unchanged. Skipping reconnect."
                        )
                        continue

                    try:
                        raw_message = await asyncio.wait_for(
                            ws.recv(),
                            timeout=self.receive_timeout_seconds,
                        )
                        consecutive_receive_timeouts = 0
                    except TimeoutError:
                        consecutive_receive_timeouts += 1
                        if consecutive_receive_timeouts < self.receive_timeout_reconnect_count:
                            self.logger.warning(
                                (
                                    "WebSocket receive timeout count=%s/%s. "
                                    "Keeping connection open."
                                ),
                                consecutive_receive_timeouts,
                                self.receive_timeout_reconnect_count,
                            )
                            continue
                        self.logger.warning(
                            "WebSocket receive timeout threshold reached. Reconnecting."
                        )
                        return "idle_watchdog_reconnect"
                    except ConnectionClosed as exc:
                        mapped = self._map_connection_closed_reason(exc)
                        self.logger.warning("WebSocket closed. reason=%s", mapped)
                        return mapped

                    if isinstance(raw_message, bytes):
                        raw_message = raw_message.decode("utf-8", errors="replace")
                    await self.on_message(str(raw_message))
        except ConnectionClosed as exc:
            return self._map_connection_closed_reason(exc)
        except Exception as exc:  # noqa: BLE001
            self.logger.exception("WebSocket transport error before stream loop: %s", exc)
            return self._map_unexpected_exception(exc)
        return None

    async def _subscribe(self, ws: Any, asset_ids: list[str]) -> None:
        # As documented in market channel examples, subscription key is `assets_ids`.
        for chunk in self._chunk(asset_ids, size=500):
            payload = self.build_subscription_payload(chunk)
            await ws.send(json.dumps(payload))
            self.logger.info("Subscribed market channel chunk size=%s", len(chunk))

    @staticmethod
    def build_subscription_payload(asset_ids: list[str]) -> dict[str, object]:
        return {
            "type": "market",
            "assets_ids": asset_ids,
            "custom_feature_enabled": True,
        }

    def _current_asset_ids(self) -> list[str]:
        if self.get_asset_ids is None:
            return list(self.asset_ids)
        return list(self.get_asset_ids())

    @staticmethod
    def should_reconnect_for_resubscribe(
        *,
        current_asset_ids: list[str],
        next_asset_ids: list[str],
    ) -> bool:
        return set(current_asset_ids) != set(next_asset_ids)

    @staticmethod
    def _map_connection_closed_reason(exc: ConnectionClosed) -> str:
        if exc.rcvd is not None and exc.sent is None:
            return "socket_closed_remote"
        if exc.sent is not None and exc.rcvd is None:
            return "socket_closed_local"
        return "socket_closed"

    @staticmethod
    def _map_unexpected_exception(exc: Exception) -> str:
        text = str(exc).lower()
        if "auth" in text or "401" in text or "403" in text:
            return "auth_failure_placeholder"
        return "unknown_transport_error"

    def _emit_transport_event(self, event_name: str, reason: str, asset_count: int) -> None:
        if self.on_transport_event is None:
            return
        self.on_transport_event(event_name, reason, asset_count)

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
