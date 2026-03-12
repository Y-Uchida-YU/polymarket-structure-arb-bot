from __future__ import annotations

from src.clients.ws_client import MarketWebSocketClient


def test_subscription_payload_includes_custom_feature_enabled() -> None:
    payload = MarketWebSocketClient.build_subscription_payload(["asset-1", "asset-2"])
    assert payload["type"] == "market"
    assert payload["assets_ids"] == ["asset-1", "asset-2"]
    assert payload["custom_feature_enabled"] is True


def test_should_reconnect_for_resubscribe_uses_set_comparison() -> None:
    assert (
        MarketWebSocketClient.should_reconnect_for_resubscribe(
            current_asset_ids=["a1", "a2"],
            next_asset_ids=["a2", "a1"],
        )
        is False
    )
    assert (
        MarketWebSocketClient.should_reconnect_for_resubscribe(
            current_asset_ids=["a1", "a2"],
            next_asset_ids=["a1", "a3"],
        )
        is True
    )


def test_map_unexpected_exception_to_auth_placeholder() -> None:
    assert (
        MarketWebSocketClient._map_unexpected_exception(RuntimeError("401 unauthorized"))
        == "auth_failure_placeholder"
    )
    assert (
        MarketWebSocketClient._map_unexpected_exception(RuntimeError("socket boom"))
        == "unknown_transport_error"
    )


def test_map_connection_closed_reason_defaults_to_socket_closed() -> None:
    class _CloseLike:
        def __init__(self, *, rcvd: object, sent: object) -> None:
            self.rcvd = rcvd
            self.sent = sent

    assert (
        MarketWebSocketClient._map_connection_closed_reason(
            _CloseLike(rcvd=None, sent=None)  # type: ignore[arg-type]
        )
        == "socket_closed"
    )
    assert (
        MarketWebSocketClient._map_connection_closed_reason(
            _CloseLike(rcvd=object(), sent=None)  # type: ignore[arg-type]
        )
        == "socket_closed_remote"
    )
    assert (
        MarketWebSocketClient._map_connection_closed_reason(
            _CloseLike(rcvd=None, sent=object())  # type: ignore[arg-type]
        )
        == "socket_closed_local"
    )
