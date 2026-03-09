from __future__ import annotations

from src.clients.ws_client import MarketWebSocketClient


def test_subscription_payload_includes_custom_feature_enabled() -> None:
    payload = MarketWebSocketClient.build_subscription_payload(["asset-1", "asset-2"])
    assert payload["type"] == "market"
    assert payload["assets_ids"] == ["asset-1", "asset-2"]
    assert payload["custom_feature_enabled"] is True
